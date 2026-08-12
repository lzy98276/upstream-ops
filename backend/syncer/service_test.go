package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lzy98276/upstream-ops/backend/connector"
	"github.com/lzy98276/upstream-ops/backend/crypto"
	"github.com/lzy98276/upstream-ops/backend/gateway"
	"github.com/lzy98276/upstream-ops/backend/notify"
	"github.com/lzy98276/upstream-ops/backend/storage"
	"gorm.io/gorm"
)

type fakeChannelService struct {
	keys        []connector.APIKey
	groups      []connector.APIKeyGroup
	searchMiss  bool
	createCount int
	updateCount int
	deleteCount int
	lastCreate  connector.APIKeyCreateRequest
	lastUpdate  connector.APIKeyUpdateRequest
}

func (f *fakeChannelService) RevealAPIKey(ctx context.Context, channelID uint, keyID int64) (string, error) {
	for _, key := range f.keys {
		if key.ID == keyID {
			return key.Key, nil
		}
	}
	return "", fmt.Errorf("key not found: %d", keyID)
}

func (f *fakeChannelService) CreateAPIKey(ctx context.Context, channelID uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	f.createCount++
	f.lastCreate = req
	key := connector.APIKey{ID: int64(len(f.keys) + 1), Name: req.Name, Key: "sk-created", GroupID: req.GroupID, ModelLimits: req.ModelLimits}
	f.keys = append(f.keys, key)
	return &key, nil
}

func (f *fakeChannelService) UpdateAPIKey(ctx context.Context, channelID uint, keyID int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error) {
	f.updateCount++
	f.lastUpdate = req
	for i := range f.keys {
		if f.keys[i].ID == keyID {
			if req.Name != nil {
				f.keys[i].Name = *req.Name
			}
			f.keys[i].GroupID = req.GroupID
			if req.ModelLimits != nil {
				f.keys[i].ModelLimits = *req.ModelLimits
			}
			return &f.keys[i], nil
		}
	}
	return nil, fmt.Errorf("key not found: %d", keyID)
}

func (f *fakeChannelService) DeleteAPIKey(ctx context.Context, channelID uint, keyID int64) error {
	f.deleteCount++
	return nil
}

func (f *fakeChannelService) ListAPIKeys(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	items := make([]connector.APIKey, 0, len(f.keys))
	if query.Search != "" && f.searchMiss {
		return &connector.APIKeyPage{Items: items, Total: 0, Page: 1, PageSize: 100}, nil
	}
	for _, key := range f.keys {
		if query.Search == "" || strings.Contains(key.Name, query.Search) {
			items = append(items, key)
		}
	}
	return &connector.APIKeyPage{Items: items, Total: int64(len(items)), Page: 1, PageSize: 100}, nil
}

func (f *fakeChannelService) ListAPIKeyGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error) {
	return f.groups, nil
}

type adminServerState struct {
	t              *testing.T
	mu             sync.Mutex
	groups         []map[string]any
	accounts       map[int64]map[string]any
	nextAccountID  int64
	createCount    int
	updateCount    int
	deleteAccounts []int64
	deleteGroups   []int64
	updateGroups   []int64
	syncModels     []int64
	failSyncModels map[int64]bool
	testModels     []string
	failTests      map[int64]string
}

func newAdminServer(t *testing.T) (*httptest.Server, *adminServerState) {
	state := &adminServerState{
		t: t,
		groups: []map[string]any{
			{"id": 101, "name": "tg-low", "platform": "openai", "ratio": 0.06, "status": "active", "sort": 1},
			{"id": 102, "name": "tg-high", "platform": "openai", "ratio": 1.1, "status": "active", "sort": 2},
		},
		accounts:      map[int64]map[string]any{},
		nextAccountID: 10,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/admin/groups/all", state.handleGroups)
	mux.HandleFunc("/api/v1/admin/groups/sort-order", state.handleGroupSortOrder)
	mux.HandleFunc("/api/v1/admin/accounts", state.handleAccounts)
	mux.HandleFunc("/api/v1/admin/accounts/", state.handleAccount)
	mux.HandleFunc("/api/v1/admin/groups/", state.handleGroup)
	srv := httptest.NewServer(mux)
	return srv, state
}

func (s *adminServerState) requireAdminKey(r *http.Request) {
	if r.Header.Get("x-api-key") != "admin-key" {
		s.t.Fatalf("x-api-key = %q", r.Header.Get("x-api-key"))
	}
}

func (s *adminServerState) handleGroups(w http.ResponseWriter, r *http.Request) {
	s.requireAdminKey(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	respondJSON(w, map[string]any{"code": 0, "data": s.groups})
}

func (s *adminServerState) handleGroupSortOrder(w http.ResponseWriter, r *http.Request) {
	s.requireAdminKey(r)
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Updates []struct {
			ID        int64 `json:"id"`
			SortOrder int   `json:"sort_order"`
		} `json:"updates"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.t.Fatalf("decode group sort order: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, update := range body.Updates {
		for _, group := range s.groups {
			if groupID, ok := group["id"].(int); ok && int64(groupID) == update.ID {
				group["sort"] = update.SortOrder
				group["sort_order"] = update.SortOrder
			}
		}
	}
	respondJSON(w, map[string]any{"code": 0, "data": map[string]any{}})
}

func (s *adminServerState) handleAccounts(w http.ResponseWriter, r *http.Request) {
	s.requireAdminKey(r)
	switch r.Method {
	case http.MethodGet:
		s.mu.Lock()
		defer s.mu.Unlock()
		items := make([]map[string]any, 0, len(s.accounts))
		for _, account := range s.accounts {
			items = append(items, account)
		}
		respondJSON(w, map[string]any{"code": 0, "data": map[string]any{"items": items}})
	case http.MethodPost:
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.t.Fatalf("decode create body: %v", err)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		body["id"] = s.nextAccountID
		body["status"] = "active"
		body["schedulable"] = true
		s.accounts[s.nextAccountID] = body
		s.nextAccountID++
		s.createCount++
		respondJSON(w, map[string]any{"code": 0, "data": body})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *adminServerState) handleAccount(w http.ResponseWriter, r *http.Request) {
	s.requireAdminKey(r)
	id := int64(10)
	rawID := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/accounts/")
	rawID = strings.TrimSuffix(rawID, "/models/sync-upstream")
	rawID = strings.TrimSuffix(rawID, "/schedulable")
	rawID = strings.TrimSuffix(rawID, "/models")
	rawID = strings.TrimSuffix(rawID, "/test")
	if _, err := fmt.Sscanf(rawID, "%d", &id); err != nil {
		s.t.Fatalf("parse account id: %v", err)
	}
	if strings.HasSuffix(r.URL.Path, "/schedulable") {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Schedulable bool `json:"schedulable"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.t.Fatalf("decode schedulable body: %v", err)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		account, ok := s.accounts[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			respondJSON(w, map[string]any{"code": 1, "message": "account not found"})
			return
		}
		account["schedulable"] = body.Schedulable
		respondJSON(w, map[string]any{"code": 0, "data": account})
		return
	}
	if strings.HasSuffix(r.URL.Path, "/models/sync-upstream") {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.failSyncModels[id] {
			respondJSON(w, map[string]any{"code": 1, "message": "sync upstream models failed"})
			return
		}
		s.syncModels = append(s.syncModels, id)
		respondJSON(w, map[string]any{
			"code": 0,
			"data": map[string]any{"models": []map[string]any{{"id": "gpt-a"}, {"id": "gpt-b"}}},
		})
		return
	}
	if strings.HasSuffix(r.URL.Path, "/models") {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		respondJSON(w, map[string]any{
			"code": 0,
			"data": []map[string]any{{"id": "gpt-a"}, {"id": "gpt-b"}},
		})
		return
	}
	if strings.HasSuffix(r.URL.Path, "/test") {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			ModelID string `json:"model_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.t.Fatalf("decode test body: %v", err)
		}
		s.mu.Lock()
		s.testModels = append(s.testModels, fmt.Sprintf("%d:%s", id, body.ModelID))
		failReason := s.failTests[id]
		s.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: {\"type\":\"test_start\",\"model\":%q}\n\n", body.ModelID)
		if failReason != "" {
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"error\",\"error\":%q}\n\n", failReason)
			return
		}
		_, _ = w.Write([]byte("data: {\"type\":\"content\",\"text\":\"pong\"}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"test_complete\",\"success\":true}\n\n"))
		return
	}
	switch r.Method {
	case http.MethodPut:
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.accounts[id]; !ok {
			w.WriteHeader(http.StatusNotFound)
			respondJSON(w, map[string]any{"code": 1, "message": "account not found"})
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.t.Fatalf("decode update body: %v", err)
		}
		if schedulable, ok := s.accounts[id]["schedulable"]; ok {
			body["schedulable"] = schedulable
		}
		body["id"] = id
		s.accounts[id] = body
		s.updateCount++
		respondJSON(w, map[string]any{"code": 0, "data": body})
	case http.MethodDelete:
		s.mu.Lock()
		defer s.mu.Unlock()
		s.deleteAccounts = append(s.deleteAccounts, id)
		delete(s.accounts, id)
		respondJSON(w, map[string]any{"code": 0, "data": map[string]any{}})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *adminServerState) handleGroup(w http.ResponseWriter, r *http.Request) {
	s.requireAdminKey(r)
	id := int64(0)
	if _, err := fmt.Sscanf(strings.TrimPrefix(r.URL.Path, "/api/v1/admin/groups/"), "%d", &id); err != nil {
		s.t.Fatalf("parse group id: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.Method {
	case http.MethodPut:
		var body struct {
			RateMultiplier float64 `json:"rate_multiplier"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.t.Fatalf("decode group update body: %v", err)
		}
		for _, group := range s.groups {
			if groupID, ok := group["id"].(int); ok && int64(groupID) == id {
				s.updateGroups = append(s.updateGroups, id)
				group["ratio"] = body.RateMultiplier
				group["rate_multiplier"] = body.RateMultiplier
				respondJSON(w, map[string]any{"code": 0, "data": group})
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
		respondJSON(w, map[string]any{"code": 1, "message": "group not found"})
	case http.MethodDelete:
		s.deleteGroups = append(s.deleteGroups, id)
		respondJSON(w, map[string]any{"code": 0, "data": map[string]any{}})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func respondJSON(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}

func openSyncerTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := storage.Open(storage.DBConfig{
		Driver: storage.DBDriverSQLite,
		Path:   filepath.Join(t.TempDir(), "syncer-test.db"),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("migrate db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func newTestService(t *testing.T, db *gorm.DB, fake *fakeChannelService) *Service {
	return newTestServiceWithGateway(t, db, fake, nil, "")
}

func newTestServiceWithGateway(t *testing.T, db *gorm.DB, fake *fakeChannelService, gatewaySvc *gateway.Service, gatewayBaseURL string) *Service {
	t.Helper()
	c, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	return New(
		storage.NewChannels(db),
		storage.NewRates(db),
		c,
		fake,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		storage.NewUpstreamSyncTargets(db),
		storage.NewUpstreamSyncTargetGroups(db),
		storage.NewUpstreamSyncGroups(db),
		storage.NewUpstreamSyncAccounts(db),
		storage.NewUpstreamSyncManagedAccounts(db),
		storage.NewUpstreamSyncLogs(db),
		gatewaySvc,
		gatewayBaseURL,
	)
}

func TestSyncAllOnRateScanSkipsDisabledTarget(t *testing.T) {
	db := openSyncerTestDB(t)
	svc := newTestService(t, db, &fakeChannelService{})
	target, err := svc.CreateTarget(context.Background(), TargetInput{
		Name:        "target",
		BaseURL:     "https://sub2api.example",
		AdminAPIKey: "admin-key",
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	target, err = svc.UpdateTarget(context.Background(), target.ID, TargetInput{
		Name:    target.Name,
		BaseURL: target.BaseURL,
		Enabled: false,
	})
	if err != nil {
		t.Fatalf("disable target: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:    "cron-{同步分组ID}",
		TargetID:        target.ID,
		Platform:        "openai",
		ModelLimitsMode: "all",
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	svc.SyncAllOnRateScan(context.Background())

	logs, total, err := storage.NewUpstreamSyncLogs(db).ListPageBySyncGroupID(rule.ID, 1, 10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if total != 0 || len(logs) != 0 {
		t.Fatalf("logs = total %d %#v", total, logs)
	}
}

func TestSyncAllOnRateScanSkipsDisabledSyncGroupOnly(t *testing.T) {
	db := openSyncerTestDB(t)
	svc := newTestService(t, db, &fakeChannelService{})
	target, err := svc.CreateTarget(context.Background(), TargetInput{
		Name:        "target",
		BaseURL:     "https://sub2api.example",
		AdminAPIKey: "admin-key",
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:    "cron-{同步分组ID}",
		TargetID:        target.ID,
		Platform:        "openai",
		ModelLimitsMode: "all",
		Enabled:         ptrBool(false),
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	svc.SyncAllOnRateScan(context.Background())

	logs, total, err := storage.NewUpstreamSyncLogs(db).ListPageBySyncGroupID(rule.ID, 1, 10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if total != 0 || len(logs) != 0 {
		t.Fatalf("logs = total %d %#v", total, logs)
	}

	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("manual apply: %v", err)
	}
	logs, total, err = storage.NewUpstreamSyncLogs(db).ListPageBySyncGroupID(rule.ID, 1, 10)
	if err != nil {
		t.Fatalf("list logs after manual apply: %v", err)
	}
	if total != 1 || len(logs) != 1 || logs[0].Message != "no sync accounts" {
		t.Fatalf("manual apply logs = total %d %#v", total, logs)
	}
}

func TestSyncAllOnRateScanSkipsUnchangedMultipliers(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source", Ratio: 0.06}}}
	svc := newTestService(t, db, fake)
	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	targetGroups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync target groups: %v", err)
	}
	_, err = svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate: "rate-scan", TargetID: target.ID, TargetGroupIDs: []uint{targetGroups[0].ID}, Platform: "openai",
		Accounts: []SyncAccountDTO{{SourceChannelID: ch.ID, SourceGroupID: &sourceGroupID, RateConvertMode: "raw", RateConvertValue: 1, Enabled: true}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	svc.SyncAllOnRateScan(context.Background())
	if admin.createCount != 1 {
		t.Fatalf("first rate scan create count = %d, want 1", admin.createCount)
	}
	svc.SyncAllOnRateScan(context.Background())
	if admin.createCount != 1 || admin.updateCount != 1 {
		t.Fatalf("unchanged scan account requests = create %d update %d, want 1/1", admin.createCount, admin.updateCount)
	}
	fake.groups[0].Ratio = 0.1
	svc.SyncAllOnRateScan(context.Background())
	if admin.updateCount != 3 {
		t.Fatalf("changed scan account updates = %d, want 3", admin.updateCount)
	}
}

func seedChannel(t *testing.T, db *gorm.DB) *storage.Channel {
	return seedChannelWithType(t, db, storage.ChannelTypeSub2API)
}

func seedChannelWithType(t *testing.T, db *gorm.DB, typ storage.ChannelType) *storage.Channel {
	t.Helper()
	ch := &storage.Channel{
		Name:           "source",
		Type:           typ,
		SiteURL:        "https://source.example",
		PasswordCipher: "cipher",
	}
	if err := storage.NewChannels(db).Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	return ch
}

func TestListSourceModelsUsesSourceChannelKey(t *testing.T) {
	db := openSyncerTestDB(t)
	sourceGroupID := int64(21)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Fatalf("path = %q, want /v1/models", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer sk-source" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		respondJSON(w, map[string]any{"data": []map[string]any{{"id": "gpt-a"}, {"id": "gpt-b"}}})
	}))
	defer gateway.Close()

	ch := &storage.Channel{
		Name:    "source",
		Type:    storage.ChannelTypeSub2API,
		SiteURL: gateway.URL,
	}
	if err := storage.NewChannels(db).Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	fake := &fakeChannelService{
		keys: []connector.APIKey{{
			ID:      7,
			Name:    "source-key",
			Key:     "sk-source",
			Status:  "active",
			GroupID: &sourceGroupID,
		}},
	}
	svc := newTestService(t, db, fake)

	models, err := svc.ListSourceModels(context.Background(), SourceModelsInput{
		ChannelID:     ch.ID,
		SourceGroupID: &sourceGroupID,
		Platform:      "openai",
	})
	if err != nil {
		t.Fatalf("list source models: %v", err)
	}
	if strings.Join(models, ",") != "gpt-a,gpt-b" {
		t.Fatalf("models = %#v", models)
	}
}

func TestListSourceModelsDoesNotFallbackToOtherGroupKey(t *testing.T) {
	db := openSyncerTestDB(t)
	sourceGroupID := int64(8)
	otherGroupID := int64(2)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respondJSON(w, map[string]any{"data": []map[string]any{{"id": "gpt-a"}}})
	}))
	defer gateway.Close()

	ch := &storage.Channel{
		Name:    "source",
		Type:    storage.ChannelTypeSub2API,
		SiteURL: gateway.URL,
	}
	if err := storage.NewChannels(db).Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	fake := &fakeChannelService{
		keys: []connector.APIKey{{
			ID:      7,
			Name:    "other-group-key",
			Key:     "sk-other",
			Status:  "active",
			GroupID: &otherGroupID,
		}},
	}
	svc := newTestService(t, db, fake)

	_, err := svc.ListSourceModels(context.Background(), SourceModelsInput{
		ChannelID:     ch.ID,
		SourceGroupID: &sourceGroupID,
		Platform:      "openai",
	})
	if err == nil || err.Error() != "当前源分组没有可用 API Key，请先创建或应用同步账号" {
		t.Fatalf("error = %v", err)
	}
}

func TestEnsureSourceAPIKeySetsNewAPIDefaults(t *testing.T) {
	db := openSyncerTestDB(t)
	ch := seedChannelWithType(t, db, storage.ChannelTypeNewAPI)
	fake := &fakeChannelService{}
	svc := newTestService(t, db, fake)

	syncGroup := &storage.UpstreamSyncGroup{Name: "sync-1"}
	syncAccount := &storage.UpstreamSyncAccount{
		SourceChannelID: ch.ID,
		SourceGroupName: "Codex Plus",
	}

	if _, _, err := svc.ensureSourceAPIKey(context.Background(), syncGroup, syncAccount, syncGroup.Name); err != nil {
		t.Fatalf("create ensureSourceAPIKey: %v", err)
	}
	if fake.lastCreate.UnlimitedQuota == nil || !*fake.lastCreate.UnlimitedQuota {
		t.Fatalf("create unlimited_quota = %#v", fake.lastCreate.UnlimitedQuota)
	}
	if fake.lastCreate.ExpiredTime == nil || *fake.lastCreate.ExpiredTime != -1 {
		t.Fatalf("create expired_time = %#v", fake.lastCreate.ExpiredTime)
	}

	if _, _, err := svc.ensureSourceAPIKey(context.Background(), syncGroup, syncAccount, syncGroup.Name); err != nil {
		t.Fatalf("update ensureSourceAPIKey: %v", err)
	}
	if fake.lastUpdate.UnlimitedQuota == nil || !*fake.lastUpdate.UnlimitedQuota {
		t.Fatalf("update unlimited_quota = %#v", fake.lastUpdate.UnlimitedQuota)
	}
	if fake.lastUpdate.ExpiredTime == nil || *fake.lastUpdate.ExpiredTime != -1 {
		t.Fatalf("update expired_time = %#v", fake.lastUpdate.ExpiredTime)
	}
}

func TestEnsureSourceAPIKeyFallsBackToFullListWhenSearchMisses(t *testing.T) {
	db := openSyncerTestDB(t)
	ch := seedChannelWithType(t, db, storage.ChannelTypeNewAPI)
	fake := &fakeChannelService{
		searchMiss: true,
		keys: []connector.APIKey{{
			ID:   1864,
			Name: "Plus-4",
			Key:  "sk-existing",
		}},
	}
	svc := newTestService(t, db, fake)

	syncGroup := &storage.UpstreamSyncGroup{Name: "Plus-4"}
	syncAccount := &storage.UpstreamSyncAccount{
		SourceChannelID: ch.ID,
		SourceGroupName: "Codex Plus",
	}

	key, secret, err := svc.ensureSourceAPIKey(context.Background(), syncGroup, syncAccount, syncGroup.Name)
	if err != nil {
		t.Fatalf("ensureSourceAPIKey: %v", err)
	}
	if key.ID != 1864 || secret != "sk-existing" {
		t.Fatalf("reused key = %#v, secret = %q", key, secret)
	}
	if fake.createCount != 0 || fake.updateCount != 1 {
		t.Fatalf("createCount=%d updateCount=%d", fake.createCount, fake.updateCount)
	}
}

func TestApplySyncGroupCreatesThenUpdatesManagedAccount(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	lowerSourceGroupID := int64(2)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{
			{ID: &lowerSourceGroupID, Name: "source-lower", Ratio: 0.01},
			{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06},
		},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:    "sync-{同步分组ID}-{渠道ID}-{源分组ID}",
		TargetID:        target.ID,
		TargetGroupIDs:  []uint{groups[0].ID},
		Platform:        "openai",
		ModelLimitsMode: "custom",
		ModelLimits:     "gpt-4o\nclaude-3",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			Concurrency:      2,
			Weight:           50,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if rule.Name != fmt.Sprintf("sync-%d-%d-%d", rule.ID, ch.ID, sourceGroupID) {
		t.Fatalf("sync group name = %q", rule.Name)
	}
	if len(rule.Accounts) != 1 || rule.Accounts[0].RateConvertMode != "raw" {
		t.Fatalf("accounts = %#v", rule.Accounts)
	}

	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("apply sync group: %v", err)
	}
	if !log.Success {
		t.Fatalf("first apply log = %#v", log)
	}
	log, err = svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("apply sync group again: %v", err)
	}
	if !log.Success {
		t.Fatalf("second apply log = %#v", log)
	}
	for _, want := range []string{"同步分组", "目标上游", "成功账号", "账号1", "倍率 0.06", "权重 50", "并发 2"} {
		if !strings.Contains(log.Message, want) {
			t.Fatalf("log message missing %q: %s", want, log.Message)
		}
	}
	logs, _, err := svc.ListSyncGroupLogs(rule.ID, 1, 10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs len = %d, want 1", len(logs))
	}
	if fake.createCount != 1 || fake.updateCount != 1 {
		t.Fatalf("key create/update = %d/%d, want 1/1", fake.createCount, fake.updateCount)
	}
	if fake.lastCreate.GroupID == nil || *fake.lastCreate.GroupID != sourceGroupID {
		t.Fatalf("create group id = %#v", fake.lastCreate.GroupID)
	}
	if fake.lastCreate.Name != rule.Name {
		t.Fatalf("source key name = %q, want %q", fake.lastCreate.Name, rule.Name)
	}
	if fake.lastCreate.ModelLimitsEnabled != nil || fake.lastCreate.ModelLimits != "" || fake.lastUpdate.ModelLimits != nil {
		t.Fatalf("source key model limits should not be written: create=%#v/%q update=%#v", fake.lastCreate.ModelLimitsEnabled, fake.lastCreate.ModelLimits, fake.lastUpdate.ModelLimits)
	}
	if admin.createCount != 1 || admin.updateCount != 1 {
		t.Fatalf("account create/update = %d/%d, want 1/1", admin.createCount, admin.updateCount)
	}
	account := admin.accounts[10]
	if account["name"] != rule.Name+"-账号1 [source]" {
		t.Fatalf("account name = %#v", account["name"])
	}
	if account["priority"] != float64(1) {
		t.Fatalf("priority = %#v, want 1", account["priority"])
	}
	if account["rate_multiplier"] != float64(0.06) {
		t.Fatalf("rate multiplier = %#v, want 0.06", account["rate_multiplier"])
	}
	if account["load_factor"] != float64(50) {
		t.Fatalf("load factor = %#v, want 50", account["load_factor"])
	}
	credentials := account["credentials"].(map[string]any)
	if credentials["api_key"] != "sk-created" || credentials["base_url"] != ch.SiteURL {
		t.Fatalf("credentials = %#v", credentials)
	}
	modelMapping := credentials["model_mapping"].(map[string]any)
	if modelMapping["gpt-4o"] != "gpt-4o" || modelMapping["claude-3"] != "claude-3" {
		t.Fatalf("model mapping = %#v", modelMapping)
	}
}

func TestApplySyncGroupRestoresSchedulableAfterReEnableAccount(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "reenable-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	rule.Accounts[0].Enabled = false
	rule, err = svc.UpdateSyncGroup(rule.ID, *rule)
	if err != nil {
		t.Fatalf("disable sync account: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("disable apply: %v", err)
	}
	if admin.accounts[10]["status"] != "inactive" || admin.accounts[10]["schedulable"] != false {
		t.Fatalf("disabled remote account = %#v", admin.accounts[10])
	}
	updateCountAfterDisable := admin.updateCount
	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("repeat disabled apply: %v", err)
	}
	if strings.Contains(log.Message, "已自动禁用") || strings.Contains(log.Message, "变动账号") {
		t.Fatalf("repeat disabled apply should not report changes: %q", log.Message)
	}
	if admin.updateCount != updateCountAfterDisable {
		t.Fatalf("repeat disabled apply updated remote account %d times, want %d", admin.updateCount, updateCountAfterDisable)
	}

	rule.Accounts[0].Enabled = true
	rule, err = svc.UpdateSyncGroup(rule.ID, *rule)
	if err != nil {
		t.Fatalf("enable sync account: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("reenable apply: %v", err)
	}
	if admin.accounts[10]["status"] != "active" || admin.accounts[10]["schedulable"] != true {
		t.Fatalf("reenabled remote account = %#v", admin.accounts[10])
	}
}

func TestApplySyncGroupSyncsTargetModelsFromUpstream(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:    "models-{同步分组ID}",
		TargetID:        target.ID,
		TargetGroupIDs:  []uint{groups[0].ID},
		Platform:        "openai",
		ModelLimitsMode: "sync_upstream",
		ModelLimits:     "should-be-cleared",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("apply sync group: %v", err)
	}
	if !log.Success {
		t.Fatalf("apply log = %#v", log)
	}
	if fake.lastCreate.ModelLimitsEnabled != nil || fake.lastCreate.ModelLimits != "" {
		t.Fatalf("source key model limits should not be written: %#v/%q", fake.lastCreate.ModelLimitsEnabled, fake.lastCreate.ModelLimits)
	}
	if len(admin.syncModels) != 1 || admin.syncModels[0] != 10 {
		t.Fatalf("sync models = %#v", admin.syncModels)
	}
	account := admin.accounts[10]
	if account["concurrency"] != float64(10) {
		t.Fatalf("concurrency = %#v, want 10", account["concurrency"])
	}
	if account["load_factor"] != float64(1) {
		t.Fatalf("load factor = %#v, want 1", account["load_factor"])
	}
	credentials := account["credentials"].(map[string]any)
	modelMapping := credentials["model_mapping"].(map[string]any)
	if modelMapping["gpt-a"] != "gpt-a" || modelMapping["gpt-b"] != "gpt-b" {
		t.Fatalf("model mapping = %#v", modelMapping)
	}
	stored, err := storage.NewUpstreamSyncGroups(db).FindByID(rule.ID)
	if err != nil {
		t.Fatalf("find sync group: %v", err)
	}
	if stored.ModelLimitsText != "gpt-a,gpt-b" {
		t.Fatalf("stored model limits = %q, want gpt-a,gpt-b", stored.ModelLimitsText)
	}
}

func TestApplySyncGroupTestsSelectedTargetModel(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "test-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
			TestEnabled:      true,
			TestModel:        "gpt-b",
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("apply sync group: %v", err)
	}
	if !log.Success || !strings.Contains(log.Message, "测试模型 gpt-b 通过，调度已启用") {
		t.Fatalf("apply log = %#v", log)
	}
	if len(admin.testModels) != 1 || admin.testModels[0] != "10:gpt-b" {
		t.Fatalf("test models = %#v", admin.testModels)
	}
	if admin.accounts[10]["schedulable"] != true {
		t.Fatalf("schedulable = %#v, want true", admin.accounts[10]["schedulable"])
	}
	stored, err := storage.NewUpstreamSyncAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list sync accounts: %v", err)
	}
	if len(stored) != 1 || !stored[0].TestEnabled || stored[0].TestModel != "gpt-b" {
		t.Fatalf("stored accounts = %#v", stored)
	}
}

func TestApplySyncGroupDisablesSchedulableWhenTestFails(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	admin.failTests = map[int64]string{10: "upstream unavailable"}
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "test-fail-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
			TestEnabled:      true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("apply sync group: %v", err)
	}
	if !log.Success || !strings.Contains(log.Message, "测试模型 gpt-a 失败，调度已禁用") {
		t.Fatalf("apply log = %#v", log)
	}
	if admin.accounts[10]["status"] != "active" {
		t.Fatalf("status = %#v, want active", admin.accounts[10]["status"])
	}
	if admin.accounts[10]["schedulable"] != false {
		t.Fatalf("schedulable = %#v, want false", admin.accounts[10]["schedulable"])
	}
}

func TestApplySyncGroupNotifiesWhenTestRestoresSchedulable(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	admin.failTests = map[int64]string{10: "upstream unavailable"}
	received := make(chan map[string]any, 2)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode webhook body: %v", err)
		}
		received <- body
		respondJSON(w, map[string]any{"ok": true})
	}))
	defer webhook.Close()

	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	c, err := crypto.NewCipher("notify-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	cfg, err := c.Encrypt(fmt.Sprintf(`{"url":%q}`, webhook.URL))
	if err != nil {
		t.Fatalf("encrypt config: %v", err)
	}
	notifications := storage.NewNotifications(db)
	if err := notifications.CreateChannel(&storage.NotificationChannel{
		Name:          "sync-webhook",
		Type:          storage.NotifyWebhook,
		ConfigCipher:  cfg,
		Subscriptions: `[{"channel_id":999,"mode":"all","events":["upstream_sync_group_changed"]}]`,
		Enabled:       true,
	}); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}
	svc.SetDispatcher(notify.NewDispatcher(
		notifications,
		c,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		notify.Policy{},
	))

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		DisplayName:    "测试恢复通知",
		NameTemplate:   "test-restore-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
			TestEnabled:      true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	var body map[string]any
	select {
	case body = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("create notification not received")
	}
	if !strings.Contains(fmt.Sprint(body["subject"]), "新增") {
		t.Fatalf("create notification body = %#v", body)
	}

	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("first apply sync group: %v", err)
	}
	select {
	case body = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("failure notification not received")
	}
	if !strings.Contains(fmt.Sprint(body["body"]), "测试模型 gpt-a 失败，调度已禁用") {
		t.Fatalf("failure notification body = %#v", body)
	}

	admin.failTests = nil
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("second apply sync group: %v", err)
	}
	select {
	case body = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("restore notification not received")
	}
	if !strings.Contains(fmt.Sprint(body["body"]), "测试模型 gpt-a 通过，调度已启用") {
		t.Fatalf("restore notification body = %#v", body)
	}
}

func TestApplySyncGroupSkipsFailedModelSyncAndDisablesRemoteAccount(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	admin.failSyncModels = map[int64]bool{10: true}
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	otherGroupID := int64(2)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{
			{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06},
			{ID: &otherGroupID, Name: "source-other", Ratio: 0.01},
		},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:    "skip-{同步分组ID}",
		TargetID:        target.ID,
		TargetGroupIDs:  []uint{groups[0].ID},
		Platform:        "openai",
		ModelLimitsMode: "sync_upstream",
		Accounts: []SyncAccountDTO{
			{
				SourceChannelID:  ch.ID,
				SourceGroupID:    &sourceGroupID,
				RateConvertMode:  "raw",
				RateConvertValue: 1,
				Enabled:          true,
			},
			{
				SourceChannelID:  ch.ID,
				SourceGroupID:    &otherGroupID,
				RateConvertMode:  "raw",
				RateConvertValue: 1,
				Enabled:          true,
			},
		},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("apply sync group: %v", err)
	}
	if log.Success || !strings.Contains(log.Message, "applied 1, failed 1") {
		t.Fatalf("apply log = %#v", log)
	}
	if !strings.Contains(log.Message, "已自动禁用") || !strings.Contains(log.Message, "sync upstream models failed") {
		t.Fatalf("apply log missing disable change = %#v", log.Message)
	}
	if len(admin.deleteAccounts) != 0 {
		t.Fatalf("deleted accounts = %#v, want none", admin.deleteAccounts)
	}
	if admin.accounts[10]["status"] != "inactive" {
		t.Fatalf("failed account status = %#v, want inactive", admin.accounts[10]["status"])
	}
	if admin.accounts[10]["schedulable"] != false {
		t.Fatalf("failed account schedulable = %#v, want false", admin.accounts[10]["schedulable"])
	}
	if !strings.Contains(fmt.Sprint(admin.accounts[10]["notes"]), "sync upstream models failed") {
		t.Fatalf("failed account notes = %#v", admin.accounts[10]["notes"])
	}
	if !strings.Contains(fmt.Sprint(admin.accounts[10]["notes"]), "同步时间：") {
		t.Fatalf("failed account notes missing sync time = %#v", admin.accounts[10]["notes"])
	}
	if _, ok := admin.accounts[11]; !ok {
		t.Fatalf("second account was not synced")
	}
	managed, err := storage.NewUpstreamSyncManagedAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list managed accounts: %v", err)
	}
	if len(managed) != 2 || managed[0].TargetAccountID != 10 || managed[1].TargetAccountID != 11 {
		t.Fatalf("managed accounts = %#v", managed)
	}
}

func TestApplySyncGroupCreatesAccountsForMultipleAccounts(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	otherGroupID := int64(2)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{
			{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06},
			{ID: &otherGroupID, Name: "source-other", Ratio: 0.01},
		},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "multi-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{
			{
				SourceChannelID:  ch.ID,
				SourceGroupID:    &sourceGroupID,
				RateConvertMode:  "raw",
				RateConvertValue: 1,
				Concurrency:      1,
				Weight:           10,
				Enabled:          true,
			},
			{
				SourceChannelID:  ch.ID,
				SourceGroupID:    &otherGroupID,
				RateConvertMode:  "multiply_100",
				RateConvertValue: 1,
				Concurrency:      3,
				Weight:           20,
				Enabled:          true,
			},
		},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("apply sync group: %v", err)
	}
	if !log.Success {
		t.Fatalf("apply log = %#v", log)
	}
	if fake.createCount != 1 || fake.updateCount != 1 || admin.createCount != 2 {
		t.Fatalf("key create/update and account create = %d/%d/%d, want 1/1/2", fake.createCount, fake.updateCount, admin.createCount)
	}
	first := admin.accounts[10]
	second := admin.accounts[11]
	if first["name"] != rule.Name+"-账号1 [source]" || second["name"] != rule.Name+"-账号2 [source]" {
		t.Fatalf("account names = %#v / %#v", first["name"], second["name"])
	}
	if first["rate_multiplier"] != float64(0.06) {
		t.Fatalf("first multiplier = %#v, want 0.06", first["rate_multiplier"])
	}
	if second["rate_multiplier"] != float64(1) {
		t.Fatalf("second multiplier = %#v, want 1", second["rate_multiplier"])
	}
	if !strings.Contains(fmt.Sprint(first["notes"]), "同步时间：") || !strings.Contains(fmt.Sprint(second["notes"]), "同步时间：") {
		t.Fatalf("account notes = %#v / %#v", first["notes"], second["notes"])
	}
	if first["priority"] != float64(1) || second["priority"] != float64(2) {
		t.Fatalf("priorities = %#v / %#v, want 1 / 2", first["priority"], second["priority"])
	}
}

func TestApplySyncGroupResortsAccountsByLatestSourceGroupRate(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	firstGroupID := int64(1)
	secondGroupID := int64(2)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{
			{ID: &firstGroupID, Name: "source-1", Ratio: 0.2},
			{ID: &secondGroupID, Name: "source-2", Ratio: 0.3},
		},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:      "resort-{同步分组ID}",
		TargetID:          target.ID,
		TargetGroupIDs:    []uint{groups[0].ID},
		Platform:          "openai",
		RateSortDirection: "asc",
		Accounts: []SyncAccountDTO{
			{
				SourceChannelID:  ch.ID,
				SourceGroupID:    &firstGroupID,
				RateConvertMode:  "raw",
				RateConvertValue: 1,
				Weight:           1,
				Enabled:          true,
			},
			{
				SourceChannelID:  ch.ID,
				SourceGroupID:    &secondGroupID,
				RateConvertMode:  "raw",
				RateConvertValue: 1,
				Weight:           1,
				Enabled:          true,
			},
		},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if admin.accounts[10]["priority"] != float64(1) || admin.accounts[11]["priority"] != float64(2) {
		t.Fatalf("initial priorities = %#v / %#v", admin.accounts[10]["priority"], admin.accounts[11]["priority"])
	}

	fake.groups = []connector.APIKeyGroup{
		{ID: &firstGroupID, Name: "source-1", Ratio: 0.2},
		{ID: &secondGroupID, Name: "source-2", Ratio: 0.1},
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if admin.accounts[11]["name"] != rule.Name+"-账号1 [source]" || admin.accounts[11]["priority"] != float64(1) || admin.accounts[11]["rate_multiplier"] != float64(0.1) {
		t.Fatalf("second account after resort = %#v", admin.accounts[11])
	}
	if admin.accounts[10]["name"] != rule.Name+"-账号2 [source]" || admin.accounts[10]["priority"] != float64(2) || admin.accounts[10]["rate_multiplier"] != float64(0.2) {
		t.Fatalf("first account after resort = %#v", admin.accounts[10])
	}
	stored, err := storage.NewUpstreamSyncAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list sync accounts: %v", err)
	}
	if stored[0].SourceGroupID == nil || *stored[0].SourceGroupID != secondGroupID || stored[1].SourceGroupID == nil || *stored[1].SourceGroupID != firstGroupID {
		t.Fatalf("stored order = %#v", stored)
	}
}

func TestApplySyncGroupKeepsMissingSourceGroupAccountSlot(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	firstGroupID := int64(1)
	secondGroupID := int64(2)
	thirdGroupID := int64(3)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{
			{ID: &firstGroupID, Name: "source-1", Ratio: 0.1},
			{ID: &secondGroupID, Name: "source-2", Ratio: 0.2},
			{ID: &thirdGroupID, Name: "source-3", Ratio: 0.3},
		},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:      "missing-slot-{同步分组ID}",
		TargetID:          target.ID,
		TargetGroupIDs:    []uint{groups[0].ID},
		Platform:          "openai",
		RateSortDirection: "asc",
		Accounts: []SyncAccountDTO{
			{SourceChannelID: ch.ID, SourceGroupID: &firstGroupID, RateConvertMode: "raw", RateConvertValue: 1, Enabled: true},
			{SourceChannelID: ch.ID, SourceGroupID: &secondGroupID, RateConvertMode: "raw", RateConvertValue: 1, Enabled: true},
			{SourceChannelID: ch.ID, SourceGroupID: &thirdGroupID, RateConvertMode: "raw", RateConvertValue: 1, Enabled: true},
		},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	fake.groups = []connector.APIKeyGroup{
		{ID: &secondGroupID, Name: "source-2", Ratio: 0.2},
		{ID: &thirdGroupID, Name: "source-3", Ratio: 0.3},
	}
	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if log.Success || !strings.Contains(log.Message, "source group missing") {
		t.Fatalf("log = %#v", log)
	}
	if len(admin.deleteAccounts) != 0 {
		t.Fatalf("deleted accounts = %#v, want none", admin.deleteAccounts)
	}
	if admin.accounts[10]["name"] != rule.Name+"-账号1 [source]" || admin.accounts[10]["status"] != "inactive" {
		t.Fatalf("first account = %#v", admin.accounts[10])
	}
	if admin.accounts[11]["name"] != rule.Name+"-账号2 [source]" || admin.accounts[11]["priority"] != float64(2) {
		t.Fatalf("second account = %#v", admin.accounts[11])
	}
	if admin.accounts[12]["name"] != rule.Name+"-账号3 [source]" || admin.accounts[12]["priority"] != float64(3) {
		t.Fatalf("third account = %#v", admin.accounts[12])
	}
	stored, err := storage.NewUpstreamSyncAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list sync accounts: %v", err)
	}
	if stored[0].SourceGroupID == nil || *stored[0].SourceGroupID != firstGroupID {
		t.Fatalf("stored order = %#v", stored)
	}
}

func TestApplySyncGroupRecreatesMissingMappedRemoteAccount(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "recreate-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	delete(admin.accounts, 10)
	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if !log.Success {
		t.Fatalf("apply log = %#v", log)
	}
	if admin.createCount != 2 {
		t.Fatalf("create count = %d, want 2", admin.createCount)
	}
	managed, err := storage.NewUpstreamSyncManagedAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list managed accounts: %v", err)
	}
	if len(managed) != 1 || managed[0].TargetAccountID != 11 {
		t.Fatalf("managed accounts = %#v", managed)
	}
}

func TestApplySyncGroupDeletesRemoteAccountWhenLocalAccountRemoved(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	otherGroupID := int64(2)
	thirdGroupID := int64(3)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{
			{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06},
			{ID: &otherGroupID, Name: "source-other", Ratio: 0.01},
			{ID: &thirdGroupID, Name: "source-third", Ratio: 0.07},
		},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "remove-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{
			{
				SourceChannelID:  ch.ID,
				SourceGroupID:    &sourceGroupID,
				RateConvertMode:  "raw",
				RateConvertValue: 1,
				Enabled:          true,
			},
			{
				SourceChannelID:  ch.ID,
				SourceGroupID:    &otherGroupID,
				RateConvertMode:  "raw",
				RateConvertValue: 1,
				Enabled:          true,
			},
			{
				SourceChannelID:  ch.ID,
				SourceGroupID:    &thirdGroupID,
				RateConvertMode:  "raw",
				RateConvertValue: 1,
				Enabled:          true,
			},
		},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if len(admin.accounts) != 3 {
		t.Fatalf("accounts len = %d, want 3", len(admin.accounts))
	}
	rule.Accounts = []SyncAccountDTO{rule.Accounts[0], rule.Accounts[2]}
	if _, err := svc.UpdateSyncGroup(rule.ID, *rule); err != nil {
		t.Fatalf("update sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if len(admin.deleteAccounts) != 1 || admin.deleteAccounts[0] != 10 {
		t.Fatalf("deleted accounts = %#v, want [10]", admin.deleteAccounts)
	}
	if _, ok := admin.accounts[10]; ok {
		t.Fatalf("removed remote account still exists")
	}
	if admin.accounts[11]["name"] != rule.Name+"-账号1 [source]" {
		t.Fatalf("first remaining account name = %#v", admin.accounts[11]["name"])
	}
	if admin.accounts[12]["name"] != rule.Name+"-账号2 [source]" {
		t.Fatalf("shifted account name = %#v", admin.accounts[12]["name"])
	}
	if admin.accounts[12]["rate_multiplier"] != float64(0.07) {
		t.Fatalf("shifted account multiplier = %#v, want 0.07", admin.accounts[12]["rate_multiplier"])
	}
	if admin.accounts[12]["priority"] != float64(2) {
		t.Fatalf("shifted account priority = %#v, want 2", admin.accounts[12]["priority"])
	}
	managed, err := storage.NewUpstreamSyncManagedAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list managed accounts: %v", err)
	}
	if len(managed) != 2 || managed[0].TargetAccountID != 11 || managed[1].TargetAccountID != 12 {
		t.Fatalf("managed accounts = %#v", managed)
	}
}

func TestApplySyncGroupDeletesUnmanagedRemoteDuplicate(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "orphan-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	admin.accounts[99] = map[string]any{
		"id":       int64(99),
		"name":     rule.Name + "-账号1",
		"priority": float64(-1),
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if len(admin.deleteAccounts) != 1 || admin.deleteAccounts[0] != 99 {
		t.Fatalf("deleted accounts = %#v, want [99]", admin.deleteAccounts)
	}
	if _, ok := admin.accounts[99]; ok {
		t.Fatalf("unmanaged duplicate still exists")
	}
}

func TestApplySyncGroupBlocksWhenSourceGroupMissing(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	missingGroupID := int64(999)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{Name: "other", Ratio: 1}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "blocked-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &missingGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Concurrency:      1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("apply sync group: %v", err)
	}
	if log.Success || !strings.Contains(log.Message, "source group missing") {
		t.Fatalf("log = %#v", log)
	}
	stored, err := storage.NewUpstreamSyncGroups(db).FindByID(rule.ID)
	if err != nil {
		t.Fatalf("find sync group: %v", err)
	}
	if stored.ApplyStatus != "blocked_missing_group" {
		t.Fatalf("status = %q", stored.ApplyStatus)
	}
	if admin.createCount != 1 || fake.createCount != 0 {
		t.Fatalf("create counts: account=%d key=%d, want 1/0", admin.createCount, fake.createCount)
	}
	account := admin.accounts[10]
	if account["name"] != rule.Name+"-账号1 [source]" || account["status"] != "inactive" {
		t.Fatalf("placeholder account = %#v", account)
	}
	credentials := account["credentials"].(map[string]any)
	if credentials["api_key"] != "1234" {
		t.Fatalf("placeholder api key = %#v", credentials["api_key"])
	}
	if !strings.Contains(fmt.Sprint(account["notes"]), "source group missing") {
		t.Fatalf("placeholder notes = %#v", account["notes"])
	}
	if !strings.Contains(fmt.Sprint(account["notes"]), "同步时间：") {
		t.Fatalf("placeholder notes missing sync time = %#v", account["notes"])
	}
}

func TestApplySyncGroupCreatesDisabledPlaceholderWhenSourceGroupNotBound(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	fake := &fakeChannelService{}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "unbound-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("apply sync group: %v", err)
	}
	if log.Success || !strings.Contains(log.Message, "source group not bound") {
		t.Fatalf("log = %#v", log)
	}
	if admin.createCount != 1 || fake.createCount != 0 {
		t.Fatalf("create counts: account=%d key=%d, want 1/0", admin.createCount, fake.createCount)
	}
	account := admin.accounts[10]
	if account["name"] != rule.Name+"-账号1 [source]" || account["status"] != "inactive" {
		t.Fatalf("placeholder account = %#v", account)
	}
	credentials := account["credentials"].(map[string]any)
	if credentials["api_key"] != "1234" {
		t.Fatalf("placeholder api key = %#v", credentials["api_key"])
	}
	if !strings.Contains(fmt.Sprint(account["notes"]), "源分组未绑定") {
		t.Fatalf("placeholder notes = %#v", account["notes"])
	}
	if !strings.Contains(fmt.Sprint(account["notes"]), "同步时间：") {
		t.Fatalf("placeholder notes missing sync time = %#v", account["notes"])
	}
	managed, err := storage.NewUpstreamSyncManagedAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list managed accounts: %v", err)
	}
	if len(managed) != 1 || managed[0].TargetAccountID != 10 || managed[0].SourceAPIKeyID != 0 || managed[0].SourceAPIKeyName != "" {
		t.Fatalf("managed accounts = %#v", managed)
	}
}

func TestApplySyncGroupDisablesManagedAccountWhenSourceGroupUnbound(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "unbound-clean-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("first apply: %v", err)
	}

	rule.Accounts[0].SourceGroupID = nil
	if _, err := svc.UpdateSyncGroup(rule.ID, *rule); err != nil {
		t.Fatalf("update sync group: %v", err)
	}
	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if log.Success || !strings.Contains(log.Message, "source group not bound") {
		t.Fatalf("log = %#v", log)
	}
	if !strings.Contains(log.Message, "已自动禁用") || !strings.Contains(log.Message, "源分组未绑定") {
		t.Fatalf("log missing disable change = %#v", log.Message)
	}
	if len(admin.deleteAccounts) != 0 {
		t.Fatalf("deleted accounts = %#v, want none", admin.deleteAccounts)
	}
	if admin.accounts[10]["status"] != "inactive" {
		t.Fatalf("remote account status = %#v, want inactive", admin.accounts[10]["status"])
	}
	if admin.accounts[10]["schedulable"] != false {
		t.Fatalf("remote account schedulable = %#v, want false", admin.accounts[10]["schedulable"])
	}
	if !strings.Contains(fmt.Sprint(admin.accounts[10]["notes"]), "源分组未绑定") {
		t.Fatalf("remote account notes = %#v", admin.accounts[10]["notes"])
	}
	if !strings.Contains(fmt.Sprint(admin.accounts[10]["notes"]), "同步时间：") {
		t.Fatalf("remote account notes missing sync time = %#v", admin.accounts[10]["notes"])
	}
	managed, err := storage.NewUpstreamSyncManagedAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list managed accounts: %v", err)
	}
	if len(managed) != 1 || managed[0].TargetAccountID != 10 {
		t.Fatalf("managed accounts = %#v", managed)
	}
	accounts, err := storage.NewUpstreamSyncAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list sync accounts: %v", err)
	}
	if len(accounts) != 1 || !accounts[0].Enabled {
		t.Fatalf("sync accounts = %#v", accounts)
	}

	rule.Accounts[0].SourceGroupID = &sourceGroupID
	if _, err := svc.UpdateSyncGroup(rule.ID, *rule); err != nil {
		t.Fatalf("restore sync group: %v", err)
	}
	log, err = svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("third apply: %v", err)
	}
	if !log.Success {
		t.Fatalf("restore log = %#v", log)
	}
	if admin.accounts[10]["status"] != "active" {
		t.Fatalf("restored account status = %#v, want active", admin.accounts[10]["status"])
	}
	if admin.accounts[10]["schedulable"] != true {
		t.Fatalf("restored account schedulable = %#v, want true", admin.accounts[10]["schedulable"])
	}
	if !strings.Contains(fmt.Sprint(admin.accounts[10]["notes"]), "网关同步") || !strings.Contains(fmt.Sprint(admin.accounts[10]["notes"]), "同步时间：") {
		t.Fatalf("restored account notes = %#v", admin.accounts[10]["notes"])
	}
}

func TestApplySyncGroupDisablesManagedAccountWhenSourceChannelDeleted(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "deleted-channel-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := storage.NewChannels(db).Delete(ch.ID); err != nil {
		t.Fatalf("delete channel: %v", err)
	}

	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if log.Success || !strings.Contains(log.Message, "source channel missing") {
		t.Fatalf("log = %#v", log)
	}
	if !strings.Contains(log.Message, "已自动禁用") || !strings.Contains(log.Message, "source channel missing") {
		t.Fatalf("log missing disable change = %#v", log.Message)
	}
	if len(admin.deleteAccounts) != 0 {
		t.Fatalf("deleted accounts = %#v, want none", admin.deleteAccounts)
	}
	if admin.accounts[10]["status"] != "inactive" {
		t.Fatalf("remote account status = %#v, want inactive", admin.accounts[10]["status"])
	}
	if admin.accounts[10]["schedulable"] != false {
		t.Fatalf("remote account schedulable = %#v, want false", admin.accounts[10]["schedulable"])
	}
	if !strings.Contains(fmt.Sprint(admin.accounts[10]["notes"]), "source channel missing") {
		t.Fatalf("remote account notes = %#v", admin.accounts[10]["notes"])
	}
	if !strings.Contains(fmt.Sprint(admin.accounts[10]["notes"]), "同步时间：") {
		t.Fatalf("remote account notes missing sync time = %#v", admin.accounts[10]["notes"])
	}
	managed, err := storage.NewUpstreamSyncManagedAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list managed accounts: %v", err)
	}
	if len(managed) != 1 || managed[0].TargetAccountID != 10 {
		t.Fatalf("managed accounts = %#v", managed)
	}
	accounts, err := storage.NewUpstreamSyncAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list sync accounts: %v", err)
	}
	if len(accounts) != 1 || !accounts[0].Enabled {
		t.Fatalf("sync accounts = %#v", accounts)
	}
}

func TestApplySyncGroupDispatchesFailureNotificationWithDisabledPlaceholder(t *testing.T) {
	srv, _ := newAdminServer(t)
	defer srv.Close()
	received := make(chan map[string]any, 1)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode webhook body: %v", err)
		}
		received <- body
		respondJSON(w, map[string]any{"ok": true})
	}))
	defer webhook.Close()

	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	otherGroupID := int64(1)
	missingGroupID := int64(999)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &otherGroupID, Name: "other", Ratio: 1}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		DisplayName:    "失败通知",
		NameTemplate:   "failure-notify-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &missingGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}

	c, err := crypto.NewCipher("notify-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	cfg, err := c.Encrypt(fmt.Sprintf(`{"url":%q}`, webhook.URL))
	if err != nil {
		t.Fatalf("encrypt config: %v", err)
	}
	notifications := storage.NewNotifications(db)
	if err := notifications.CreateChannel(&storage.NotificationChannel{
		Name:          "sync-webhook",
		Type:          storage.NotifyWebhook,
		ConfigCipher:  cfg,
		Subscriptions: `[{"channel_id":999,"mode":"all","events":["upstream_sync_group_changed"]}]`,
		Enabled:       true,
	}); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}
	svc.SetDispatcher(notify.NewDispatcher(
		notifications,
		c,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		notify.Policy{},
	))

	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("apply sync group: %v", err)
	}
	if log.Success || !strings.Contains(log.Message, "source group missing") {
		t.Fatalf("log = %#v", log)
	}

	var body map[string]any
	select {
	case body = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("webhook notification not received")
	}
	if body["event"] != string(storage.EventUpstreamSyncGroupChanged) || !strings.Contains(fmt.Sprint(body["subject"]), "同步账号异常") {
		t.Fatalf("notification body = %#v", body)
	}
	bodyText := fmt.Sprint(body["body"])
	for _, want := range []string{"变动账号：", "失败账号：", "已自动禁用", "source group missing", "变动账号数：1", "失败账号数：1"} {
		if !strings.Contains(bodyText, want) {
			t.Fatalf("notification body missing %q: %s", want, bodyText)
		}
	}
}

func TestDeleteManagedDeletesRemoteAccountAndSourceKeyOnly(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "delete-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Concurrency:      1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("apply sync group: %v", err)
	}

	log, err := svc.DeleteManaged(context.Background(), rule.ID)
	if err != nil {
		t.Fatalf("delete managed: %v", err)
	}
	if !log.Success {
		t.Fatalf("delete log = %#v", log)
	}
	if len(admin.deleteAccounts) != 1 || admin.deleteAccounts[0] != 10 {
		t.Fatalf("deleted accounts = %#v", admin.deleteAccounts)
	}
	if len(admin.deleteGroups) != 0 {
		t.Fatalf("deleted groups = %#v", admin.deleteGroups)
	}
	if fake.deleteCount != 1 {
		t.Fatalf("source key delete count = %d", fake.deleteCount)
	}
	accounts, err := storage.NewUpstreamSyncManagedAccounts(db).ListBySyncGroupID(rule.ID)
	if err != nil {
		t.Fatalf("list sync account mappings: %v", err)
	}
	if len(accounts) != 0 {
		t.Fatalf("sync account mapping still exists")
	}
}

func TestSyncTargetGroupsDeletesLocalGroupsWhenRemoteIsEmpty(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	fake := &fakeChannelService{}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("groups len = %d, want 2", len(groups))
	}

	admin.groups = []map[string]any{}
	groups, err = svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync empty groups: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("returned groups len = %d, want 0", len(groups))
	}
	stored, err := svc.ListTargetGroups(target.ID, true)
	if err != nil {
		t.Fatalf("list local groups: %v", err)
	}
	if len(stored) != 0 {
		t.Fatalf("local groups len = %d, want 0", len(stored))
	}
}

func TestUpdateSyncGroupDoesNotChangePlatform(t *testing.T) {
	srv, _ := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	ch := seedChannel(t, db)
	sourceGroupID := int64(1)
	fake := &fakeChannelService{
		groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source-low", Ratio: 0.06}},
	}
	svc := newTestService(t, db, fake)

	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:   "platform-{同步分组ID}",
		TargetID:       target.ID,
		TargetGroupIDs: []uint{groups[0].ID},
		Platform:       "openai",
		Accounts: []SyncAccountDTO{{
			SourceChannelID:  ch.ID,
			SourceGroupID:    &sourceGroupID,
			RateConvertMode:  "raw",
			RateConvertValue: 1,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	rule.Platform = "gemini"
	updated, err := svc.UpdateSyncGroup(rule.ID, *rule)
	if err != nil {
		t.Fatalf("update sync group: %v", err)
	}
	if updated.Platform != "openai" {
		t.Fatalf("platform = %q, want openai", updated.Platform)
	}
}

func TestSyncGroupChangeDispatchesNotification(t *testing.T) {
	received := make(chan map[string]any, 3)
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode webhook body: %v", err)
		}
		received <- body
		respondJSON(w, map[string]any{"ok": true})
	}))
	defer webhook.Close()

	db := openSyncerTestDB(t)
	fake := &fakeChannelService{}
	svc := newTestService(t, db, fake)
	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: webhook.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}

	c, err := crypto.NewCipher("notify-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	cfg, err := c.Encrypt(fmt.Sprintf(`{"url":%q}`, webhook.URL))
	if err != nil {
		t.Fatalf("encrypt config: %v", err)
	}
	notifications := storage.NewNotifications(db)
	if err := notifications.CreateChannel(&storage.NotificationChannel{
		Name:          "sync-webhook",
		Type:          storage.NotifyWebhook,
		ConfigCipher:  cfg,
		Subscriptions: `[{"channel_id":999,"mode":"all","events":["upstream_sync_group_changed"]}]`,
		Enabled:       true,
	}); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}
	svc.SetDispatcher(notify.NewDispatcher(
		notifications,
		c,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		notify.Policy{},
	))

	readWebhook := func() map[string]any {
		t.Helper()
		select {
		case body := <-received:
			return body
		case <-time.After(2 * time.Second):
			t.Fatal("webhook notification not received")
			return nil
		}
	}

	group, err := svc.CreateSyncGroup(SyncGroupDTO{
		DisplayName:  "通知分组",
		NameTemplate: "notify-{同步分组ID}",
		TargetID:     target.ID,
		Platform:     "openai",
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	body := readWebhook()
	if body["event"] != string(storage.EventUpstreamSyncGroupChanged) || !strings.Contains(fmt.Sprint(body["subject"]), "新增") {
		t.Fatalf("create notification body = %#v", body)
	}

	group.DisplayName = "通知分组更新"
	if _, err := svc.UpdateSyncGroup(group.ID, *group); err != nil {
		t.Fatalf("update sync group: %v", err)
	}
	body = readWebhook()
	if body["event"] != string(storage.EventUpstreamSyncGroupChanged) || !strings.Contains(fmt.Sprint(body["subject"]), "更新") {
		t.Fatalf("update notification body = %#v", body)
	}

	if err := svc.DeleteSyncGroup(group.ID); err != nil {
		t.Fatalf("delete sync group: %v", err)
	}
	body = readWebhook()
	if body["event"] != string(storage.EventUpstreamSyncGroupChanged) || !strings.Contains(fmt.Sprint(body["subject"]), "删除") {
		t.Fatalf("delete notification body = %#v", body)
	}

	logs, err := notifications.ListLogs(10)
	if err != nil {
		t.Fatalf("list notification logs: %v", err)
	}
	if len(logs) != 3 {
		t.Fatalf("notification logs len = %d, want 3", len(logs))
	}
	for _, item := range logs {
		if item.Event != storage.EventUpstreamSyncGroupChanged || !item.Success {
			t.Fatalf("notification log = %#v", item)
		}
	}
}

func TestGatewayRateSyncCreatesKeyAndClampsMultiplier(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	fake := &fakeChannelService{}

	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("new gateway cipher: %v", err)
	}
	gatewaySvc := gateway.NewService(
		storage.NewGatewayGroups(db),
		storage.NewGatewayKeys(db),
		storage.NewGatewayRoutes(db),
		nil,
		nil,
		storage.NewChannels(db),
		fake,
		cipher,
		nil,
	)
	gatewayGroup, err := gatewaySvc.CreateGroup(gateway.CreateGroupInput{
		Name:             "rate-source",
		ModelMappingJSON: `{"gateway-model":"upstream-model","gateway-alt":"upstream-alt"}`,
	})
	if err != nil {
		t.Fatalf("create gateway group: %v", err)
	}
	if err := storage.NewGatewayRoutes(db).SaveForGroup(gatewayGroup.ID, []storage.GatewayRoute{
		{SourceChannelID: 1, SourceGroupName: "low", BillingRateMultiplier: 0.01, Enabled: true},
		{SourceChannelID: 2, SourceGroupName: "high", BillingRateMultiplier: 0.3, Enabled: true},
	}); err != nil {
		t.Fatalf("save gateway routes: %v", err)
	}
	svc := newTestServiceWithGateway(t, db, fake, gatewaySvc, "https://upstream-ops.example")

	target, err := svc.CreateTarget(context.Background(), TargetInput{
		Name:        "target",
		BaseURL:     srv.URL,
		AdminAPIKey: "admin-key",
		Enabled:     true,
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	targetGroups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync target groups: %v", err)
	}

	gatewayGroupID := gatewayGroup.ID
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate:    "gateway-rate",
		TargetID:        target.ID,
		TargetGroupIDs:  []uint{targetGroups[0].ID},
		Platform:        "openai",
		ModelLimitsMode: "all",
		Accounts: []SyncAccountDTO{{
			SourceKind:       "gateway_group",
			GatewayGroupID:   &gatewayGroupID,
			GatewayRateMode:  "min",
			GatewayRateMin:   0.1,
			GatewayRateMax:   0,
			RateConvertMode:  "multiply",
			RateConvertValue: 8,
			Concurrency:      2,
			Weight:           50,
			Enabled:          true,
		}},
	})
	if err != nil {
		t.Fatalf("create gateway rate sync group: %v", err)
	}
	keys, err := gatewaySvc.ListKeysByGroup(gatewayGroup.ID)
	if err != nil {
		t.Fatalf("list gateway keys: %v", err)
	}
	if len(keys) != 1 || keys[0].Name != rule.Name {
		t.Fatalf("gateway keys = %#v, want one named %q", keys, rule.Name)
	}
	secret, err := gatewaySvc.RevealKey(keys[0].ID)
	if err != nil {
		t.Fatalf("reveal gateway key: %v", err)
	}

	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("apply minimum-clamped gateway rate: %v", err)
	}
	account := admin.accounts[10]
	if account["rate_multiplier"] != float64(1) {
		t.Fatalf("gateway account rate multiplier = %#v, want 1", account["rate_multiplier"])
	}
	if got := admin.groups[0]["rate_multiplier"]; got != float64(0.1) {
		t.Fatalf("target group rate multiplier = %#v, want 0.1", got)
	}
	if len(admin.syncModels) != 0 {
		t.Fatalf("gateway rate sync model sync requests = %#v, want none", admin.syncModels)
	}
	credentials := account["credentials"].(map[string]any)
	if credentials["base_url"] != "https://upstream-ops.example" || credentials["api_key"] != secret {
		t.Fatalf("gateway credentials = %#v", credentials)
	}
	modelMapping, ok := credentials["model_mapping"].(map[string]any)
	if !ok || modelMapping["gateway-model"] != "gateway-model" || modelMapping["gateway-alt"] != "gateway-alt" {
		t.Fatalf("gateway model mapping = %#v", credentials["model_mapping"])
	}

	rule.Accounts[0].GatewayRateMode = "max"
	rule.Accounts[0].GatewayRateMin = 0
	rule.Accounts[0].GatewayRateMax = 0.5
	rule.Accounts[0].RateConvertMode = "multiply"
	rule.Accounts[0].RateConvertValue = 10
	rule, err = svc.UpdateSyncGroup(rule.ID, *rule)
	if err != nil {
		t.Fatalf("update gateway rate sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("apply maximum-clamped gateway rate: %v", err)
	}
	if got := admin.accounts[10]["rate_multiplier"]; got != float64(1) {
		t.Fatalf("updated gateway account rate multiplier = %#v, want 1", got)
	}
	if got := admin.groups[0]["rate_multiplier"]; got != float64(0.5) {
		t.Fatalf("updated target group rate multiplier = %#v, want 0.5", got)
	}
}

func TestGatewayRateSyncSkipsUnchangedTargetMultiplier(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	fake := &fakeChannelService{}
	gatewaySvc := gateway.NewService(
		storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db),
		nil, nil, storage.NewChannels(db), fake, cipher, nil,
	)
	group, err := gatewaySvc.CreateGroup(gateway.CreateGroupInput{
		Name: "models", ModelsJSON: `[{"id":"configured-model","source":"custom"}]`, ModelMappingJSON: `{"alias":"upstream"}`,
	})
	if err != nil {
		t.Fatalf("create gateway group: %v", err)
	}
	if err := storage.NewGatewayRoutes(db).SaveForGroup(group.ID, []storage.GatewayRoute{{BillingRateMultiplier: 0.06, Enabled: true}}); err != nil {
		t.Fatalf("save gateway route: %v", err)
	}
	svc := newTestServiceWithGateway(t, db, fake, gatewaySvc, "https://gateway.example")
	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	targetGroups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync target groups: %v", err)
	}
	gatewayGroupID := group.ID
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate: "gateway-unchanged", TargetID: target.ID, TargetGroupIDs: []uint{targetGroups[0].ID},
		Platform: "openai", ModelLimitsMode: "sync_upstream",
		Accounts: []SyncAccountDTO{{SourceKind: "gateway_group", GatewayGroupID: &gatewayGroupID, RateConvertMode: "raw", RateConvertValue: 1, Enabled: true}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	firstUpdates := len(admin.updateGroups)
	if firstUpdates != 0 {
		t.Fatalf("equal target group updates = %d, want 0", firstUpdates)
	}
	if _, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if len(admin.updateGroups) != firstUpdates {
		t.Fatalf("unchanged target group update count = %d, want %d", len(admin.updateGroups), firstUpdates)
	}
	credentials := admin.accounts[10]["credentials"].(map[string]any)
	mapping := credentials["model_mapping"].(map[string]any)
	if mapping["configured-model"] != "configured-model" || mapping["alias"] != "alias" {
		t.Fatalf("gateway model mapping = %#v", mapping)
	}
}

func TestReorderSub2APIGroupsWritesRemoteSortOrder(t *testing.T) {
	srv, admin := newAdminServer(t)
	defer srv.Close()
	db := openSyncerTestDB(t)
	svc := newTestService(t, db, &fakeChannelService{})
	target, err := svc.CreateTarget(context.Background(), TargetInput{
		Name: "target", BaseURL: srv.URL, AdminAPIKey: "admin-key", Enabled: true,
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	if len(groups) != 2 || groups[0].Sort != 1 || groups[1].Sort != 2 {
		t.Fatalf("initial groups = %#v", groups)
	}
	reordered, err := svc.ReorderSub2APIGroups(context.Background(), target.ID, []int64{groups[1].RemoteGroupID, groups[0].RemoteGroupID})
	if err != nil {
		t.Fatalf("reorder groups: %v", err)
	}
	if len(reordered) != 2 || reordered[0].RemoteGroupID != groups[1].RemoteGroupID || reordered[0].Sort != 1 || reordered[1].RemoteGroupID != groups[0].RemoteGroupID || reordered[1].Sort != 2 {
		t.Fatalf("reordered groups = %#v", reordered)
	}
	admin.mu.Lock()
	defer admin.mu.Unlock()
	if admin.groups[0]["sort_order"] != 2 || admin.groups[1]["sort_order"] != 1 {
		t.Fatalf("remote group sort orders = %#v", admin.groups)
	}
}

func TestReorderSyncGroupsPersistsOrderPerTarget(t *testing.T) {
	db := openSyncerTestDB(t)
	svc := newTestService(t, db, &fakeChannelService{})
	firstTarget, err := svc.CreateTarget(context.Background(), TargetInput{
		Name: "first", BaseURL: "https://first.example", AdminAPIKey: "first-key", Enabled: true,
	})
	if err != nil {
		t.Fatalf("create first target: %v", err)
	}
	secondTarget, err := svc.CreateTarget(context.Background(), TargetInput{
		Name: "second", BaseURL: "https://second.example", AdminAPIKey: "second-key", Enabled: true,
	})
	if err != nil {
		t.Fatalf("create second target: %v", err)
	}
	create := func(targetID uint, name string) *SyncGroupDTO {
		item, createErr := svc.CreateSyncGroup(SyncGroupDTO{
			DisplayName: name, NameTemplate: name, TargetID: targetID, Platform: "openai",
		})
		if createErr != nil {
			t.Fatalf("create sync group %s: %v", name, createErr)
		}
		return item
	}
	first := create(firstTarget.ID, "first-one")
	second := create(firstTarget.ID, "first-two")
	other := create(secondTarget.ID, "second-one")
	if first.Sort != 1 || second.Sort != 2 || other.Sort != 1 {
		t.Fatalf("initial sort values = %d, %d, %d", first.Sort, second.Sort, other.Sort)
	}

	if _, err := svc.ReorderSyncGroups(firstTarget.ID, []uint{second.ID, first.ID}); err != nil {
		t.Fatalf("reorder sync groups: %v", err)
	}
	list, err := svc.ListSyncGroups()
	if err != nil {
		t.Fatalf("list sync groups: %v", err)
	}
	var firstTargetGroups []SyncGroupDTO
	var secondTargetGroups []SyncGroupDTO
	for _, item := range list {
		if item.TargetID == firstTarget.ID {
			firstTargetGroups = append(firstTargetGroups, item)
		} else if item.TargetID == secondTarget.ID {
			secondTargetGroups = append(secondTargetGroups, item)
		}
	}
	if len(firstTargetGroups) != 2 || firstTargetGroups[0].ID != second.ID || firstTargetGroups[0].Sort != 1 || firstTargetGroups[1].ID != first.ID || firstTargetGroups[1].Sort != 2 {
		t.Fatalf("first target order = %#v", firstTargetGroups)
	}
	if len(secondTargetGroups) != 1 || secondTargetGroups[0].ID != other.ID || secondTargetGroups[0].Sort != 1 {
		t.Fatalf("second target order = %#v", secondTargetGroups)
	}
	if _, err := svc.ReorderSyncGroups(firstTarget.ID, []uint{first.ID}); err == nil {
		t.Fatal("expected incomplete order to be rejected")
	}
}

func TestAccountItemsSkipsBlankChannelRows(t *testing.T) {
	items := accountItems([]SyncAccountDTO{
		{SourceChannelID: 0},
		{SourceChannelID: 7, RateConvertMode: "raw", Enabled: true},
	})
	if len(items) != 1 {
		t.Fatalf("sync account count = %d, want 1", len(items))
	}
	if items[0].SourceChannelID != 7 || items[0].Position != 0 {
		t.Fatalf("stored account = %#v", items[0])
	}
}

func TestNewAPIAutoGroupsRoundTripPreservesOrder(t *testing.T) {
	var savedValue string
	mux := http.NewServeMux()
	mux.HandleFunc("/api/option/", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer root-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch r.Method {
		case http.MethodGet:
			respondJSON(w, map[string]any{
				"success": true,
				"data": []map[string]string{
					{"key": "AutoGroups", "value": `["vip","default","premium"]`},
					{"key": "GroupRatio", "value": `{"default":1,"vip":2,"shared":0.9,"auto":1}`},
					{"key": "UserUsableGroups", "value": `{"premium":"Premium","limited":"Limited"}`},
					{"key": "TopupGroupRatio", "value": `{"topup-only":1}`},
				},
			})
		case http.MethodPut:
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode update: %v", err)
			}
			if body["key"] != "AutoGroups" {
				t.Fatalf("key = %q", body["key"])
			}
			savedValue = body["value"]
			respondJSON(w, map[string]any{"success": true})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	db := openSyncerTestDB(t)
	svc := newTestService(t, db, &fakeChannelService{})
	target, err := svc.CreateTarget(context.Background(), TargetInput{
		Name: "newapi", TargetType: "newapi", BaseURL: server.URL, AdminAPIKey: "root-token", Enabled: true,
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	got, err := svc.GetNewAPIAutoGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("get auto groups: %v", err)
	}
	if !reflect.DeepEqual(got.Groups, []string{"vip", "default", "premium"}) {
		t.Fatalf("groups = %#v", got.Groups)
	}
	if !reflect.DeepEqual(got.AvailableGroups, []string{"default", "limited", "premium", "shared", "topup-only", "vip"}) {
		t.Fatalf("available groups = %#v", got.AvailableGroups)
	}
	updated, err := svc.UpdateNewAPIAutoGroups(context.Background(), target.ID, []string{"premium", "vip", "premium", ""})
	if err != nil {
		t.Fatalf("update auto groups: %v", err)
	}
	if !reflect.DeepEqual(updated.Groups, []string{"premium", "vip"}) {
		t.Fatalf("updated groups = %#v", updated.Groups)
	}
	if savedValue != `["premium","vip"]` {
		t.Fatalf("saved value = %q", savedValue)
	}
}

func TestNewAPIGroupManagementWritesGroupOptions(t *testing.T) {
	options := map[string]string{
		"AutoGroups":       `["vip","legacy"]`,
		"GroupRatio":       `{"default":1,"vip":2,"legacy":0.5}`,
		"UserUsableGroups": `{"default":"Default","vip":"VIP","legacy":"Legacy"}`,
		"TopupGroupRatio":  `{"vip":1.1,"legacy":0.9}`,
	}
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("/api/option/", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer root-token" {
			t.Fatalf("authorization = %q", got)
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			data := make([]map[string]string, 0, len(options))
			for _, key := range []string{"AutoGroups", "GroupRatio", "UserUsableGroups", "TopupGroupRatio"} {
				data = append(data, map[string]string{"key": key, "value": options[key]})
			}
			respondJSON(w, map[string]any{"success": true, "data": data})
		case http.MethodPut:
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode update: %v", err)
			}
			options[body["key"]] = body["value"]
			respondJSON(w, map[string]any{"success": true})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	db := openSyncerTestDB(t)
	svc := newTestService(t, db, &fakeChannelService{})
	target, err := svc.CreateTarget(context.Background(), TargetInput{
		Name: "newapi", TargetType: "newapi", BaseURL: server.URL, AdminAPIKey: "root-token", Enabled: true,
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	created, err := svc.SaveNewAPIGroup(context.Background(), target.ID, NewAPIGroupInput{
		Name: "premium", Ratio: 1.5, Description: "Premium",
	})
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	if !reflect.DeepEqual(created, &NewAPIGroupDTO{Name: "premium", Ratio: 1.5, Description: "Premium"}) {
		t.Fatalf("created group = %#v", created)
	}

	groups, err := svc.ListNewAPIGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("list groups: %v", err)
	}
	if !reflect.DeepEqual(groups, []NewAPIGroupDTO{
		{Name: "default", Ratio: 1, Description: "Default"},
		{Name: "legacy", Ratio: 0.5, Description: "Legacy"},
		{Name: "premium", Ratio: 1.5, Description: "Premium"},
		{Name: "vip", Ratio: 2, Description: "VIP"},
	}) {
		t.Fatalf("groups = %#v", groups)
	}

	if _, err := svc.SaveNewAPIGroup(context.Background(), target.ID, NewAPIGroupInput{
		Name: "premium", Ratio: 1.25, Description: "Premium Plus",
	}); err != nil {
		t.Fatalf("update group: %v", err)
	}
	if err := svc.DeleteNewAPIGroup(context.Background(), target.ID, "legacy"); err != nil {
		t.Fatalf("delete group: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	var ratios map[string]float64
	if err := json.Unmarshal([]byte(options["GroupRatio"]), &ratios); err != nil {
		t.Fatalf("decode GroupRatio: %v", err)
	}
	if !reflect.DeepEqual(ratios, map[string]float64{"default": 1, "vip": 2, "premium": 1.25}) {
		t.Fatalf("GroupRatio = %#v", ratios)
	}
	var usable map[string]string
	if err := json.Unmarshal([]byte(options["UserUsableGroups"]), &usable); err != nil {
		t.Fatalf("decode UserUsableGroups: %v", err)
	}
	if !reflect.DeepEqual(usable, map[string]string{"default": "Default", "vip": "VIP", "premium": "Premium Plus"}) {
		t.Fatalf("UserUsableGroups = %#v", usable)
	}
	if options["AutoGroups"] != `["vip"]` {
		t.Fatalf("AutoGroups = %q", options["AutoGroups"])
	}
	var topup map[string]float64
	if err := json.Unmarshal([]byte(options["TopupGroupRatio"]), &topup); err != nil {
		t.Fatalf("decode TopupGroupRatio: %v", err)
	}
	if !reflect.DeepEqual(topup, map[string]float64{"vip": 1.1}) {
		t.Fatalf("TopupGroupRatio = %#v", topup)
	}
}

func TestNewAPITargetCanCreateSyncGroup(t *testing.T) {
	db := openSyncerTestDB(t)
	svc := newTestService(t, db, &fakeChannelService{})
	target, err := svc.CreateTarget(context.Background(), TargetInput{
		Name: "newapi", TargetType: "newapi", BaseURL: "https://newapi.example", AdminAPIKey: "root-token", Enabled: true,
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	group, err := svc.CreateSyncGroup(SyncGroupDTO{TargetID: target.ID})
	if err != nil {
		t.Fatalf("create New API sync group: %v", err)
	}
	if group.TargetID != target.ID {
		t.Fatalf("target ID = %d, want %d", group.TargetID, target.ID)
	}
}

func TestReorderNewAPITargetGroupsUsesCachedSnapshotAndKeepsAutoGroups(t *testing.T) {
	for _, name := range []string{"auto", "default", "vip"} {
		id := newAPIGroupRemoteID(name)
		if id <= 0 || id > 9007199254740991 {
			t.Fatalf("remote ID for %q = %d, outside JavaScript safe integer range", name, id)
		}
	}
	options := map[string]string{
		"AutoGroups":       `["vip","default"]`,
		"GroupRatio":       `{"auto":0,"default":1,"vip":2}`,
		"UserUsableGroups": `{"auto":"Automatic","default":"Default","vip":"VIP"}`,
		"TopupGroupRatio":  `{}`,
	}
	var mu sync.Mutex
	getCount := 0
	putKeys := make([]string, 0)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/option/", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer root-token" {
			t.Fatalf("authorization = %q", got)
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			getCount++
			data := make([]map[string]string, 0, len(options))
			for _, key := range []string{"AutoGroups", "GroupRatio", "UserUsableGroups", "TopupGroupRatio"} {
				data = append(data, map[string]string{"key": key, "value": options[key]})
			}
			respondJSON(w, map[string]any{"success": true, "data": data})
		case http.MethodPut:
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode option update: %v", err)
			}
			if body["key"] != "AutoGroups" && body["key"] != "GroupRatio" {
				t.Fatalf("updated option = %q", body["key"])
			}
			putKeys = append(putKeys, body["key"])
			options[body["key"]] = body["value"]
			respondJSON(w, map[string]any{"success": true})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	db := openSyncerTestDB(t)
	svc := newTestService(t, db, &fakeChannelService{})
	target, err := svc.CreateTarget(context.Background(), TargetInput{
		Name: "newapi", TargetType: "newapi", BaseURL: server.URL, AdminAPIKey: "root-token", Enabled: true,
	})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync groups: %v", err)
	}
	if got := []string{groups[0].Name, groups[1].Name, groups[2].Name}; !reflect.DeepEqual(got, []string{"auto", "default", "vip"}) {
		t.Fatalf("initial groups = %#v", got)
	}

	orderedIDs := []int64{
		newAPIGroupRemoteID("vip"),
		newAPIGroupRemoteID("auto"),
		newAPIGroupRemoteID("default"),
	}
	updated, err := svc.ReorderTargetGroups(context.Background(), target.ID, orderedIDs)
	if err != nil {
		t.Fatalf("reorder groups: %v", err)
	}
	mu.Lock()
	saved := options["AutoGroups"]
	savedRatios := options["GroupRatio"]
	gets := getCount
	keys := append([]string(nil), putKeys...)
	mu.Unlock()
	if saved != `["vip","default"]` {
		t.Fatalf("AutoGroups = %q", saved)
	}
	if gets != 2 {
		t.Fatalf("New API option GET count = %d, want initial sync plus one live validation", gets)
	}
	if !reflect.DeepEqual(keys, []string{"GroupRatio"}) {
		t.Fatalf("updated options = %#v", keys)
	}
	if got := []string{updated[0].Name, updated[1].Name, updated[2].Name}; !reflect.DeepEqual(got, []string{"vip", "auto", "default"}) {
		t.Fatalf("updated groups = %#v", got)
	}
	if savedRatios != `{"vip":2,"auto":0,"default":1}` {
		t.Fatalf("GroupRatio = %q", savedRatios)
	}
	if got := options["GroupRatio"]; got != `{"vip":2,"auto":0,"default":1}` {
		t.Fatalf("GroupRatio = %q", got)
	}
}

func TestNewAPIApplySyncGroupCreatesUpdatesAndDeletesChannel(t *testing.T) {
	var mu sync.Mutex
	options := map[string]string{
		"AutoGroups":       `[]`,
		"GroupRatio":       `{"default":1,"premium":2}`,
		"UserUsableGroups": `{"default":"Default","premium":"Premium"}`,
		"TopupGroupRatio":  `{}`,
	}
	channels := map[int64]map[string]any{}
	nextID := int64(10)
	createCount := 0
	updateCount := 0
	deleteIDs := make([]int64, 0)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/option/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer root-token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			data := make([]map[string]string, 0, len(options))
			for _, key := range []string{"AutoGroups", "GroupRatio", "UserUsableGroups", "TopupGroupRatio"} {
				data = append(data, map[string]string{"key": key, "value": options[key]})
			}
			respondJSON(w, map[string]any{"success": true, "data": data})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/channel/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer root-token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			items := make([]map[string]any, 0, len(channels))
			for _, channel := range channels {
				items = append(items, channel)
			}
			respondJSON(w, map[string]any{"success": true, "data": map[string]any{"items": items}})
		case http.MethodPost:
			var body struct {
				Channel map[string]any `json:"channel"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode create channel: %v", err)
			}
			body.Channel["id"] = nextID
			body.Channel["status"] = float64(1)
			channels[nextID] = body.Channel
			nextID++
			createCount++
			respondJSON(w, map[string]any{"success": true, "data": map[string]any{}})
		case http.MethodPut:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode update channel: %v", err)
			}
			id, _ := body["id"].(float64)
			if _, exists := channels[int64(id)]; !exists {
				w.WriteHeader(http.StatusNotFound)
				respondJSON(w, map[string]any{"success": false, "message": "channel not found"})
				return
			}
			body["status"] = channels[int64(id)]["status"]
			channels[int64(id)] = body
			updateCount++
			respondJSON(w, map[string]any{"success": true, "data": map[string]any{}})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/models" {
			if r.Header.Get("Authorization") != "Bearer sk-created" {
				t.Fatalf("source authorization = %q", r.Header.Get("Authorization"))
			}
			respondJSON(w, map[string]any{"data": []map[string]any{{"id": "gpt-a"}, {"id": "gpt-b"}}})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/channel/") && r.URL.Path != "/api/channel/" {
			if r.Header.Get("Authorization") != "Bearer root-token" {
				t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
			}
			var id int64
			if _, err := fmt.Sscanf(strings.TrimPrefix(strings.TrimSuffix(r.URL.Path, "/status"), "/api/channel/"), "%d", &id); err != nil {
				t.Fatalf("parse channel id: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if strings.HasSuffix(r.URL.Path, "/status") {
				var body struct {
					Status int `json:"status"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatalf("decode status: %v", err)
				}
				channels[id]["status"] = float64(body.Status)
				respondJSON(w, map[string]any{"success": true, "data": map[string]any{}})
				return
			}
			if r.Method == http.MethodDelete {
				delete(channels, id)
				deleteIDs = append(deleteIDs, id)
				respondJSON(w, map[string]any{"success": true, "data": map[string]any{}})
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	defer server.Close()

	db := openSyncerTestDB(t)
	ch := &storage.Channel{Name: "source", Type: storage.ChannelTypeSub2API, SiteURL: server.URL}
	if err := storage.NewChannels(db).Create(ch); err != nil {
		t.Fatalf("create source channel: %v", err)
	}
	sourceGroupID := int64(7)
	fake := &fakeChannelService{groups: []connector.APIKeyGroup{{ID: &sourceGroupID, Name: "source", Ratio: 1}}}
	svc := newTestService(t, db, fake)
	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "newapi", TargetType: "newapi", BaseURL: server.URL, AdminAPIKey: "root-token", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	groups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync New API groups: %v", err)
	}
	if len(groups) != 2 || groups[0].RemoteGroupID <= 0 {
		t.Fatalf("cached New API groups = %#v", groups)
	}
	for _, group := range groups {
		if group.Platform != "" {
			t.Fatalf("New API group platform = %q, want empty", group.Platform)
		}
	}
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate: "new-{同步分组ID}", TargetID: target.ID, TargetGroupIDs: []uint{groups[1].ID}, Platform: "openai", ModelLimitsMode: "sync_upstream",
		Accounts: []SyncAccountDTO{{SourceChannelID: ch.ID, SourceGroupID: &sourceGroupID, Weight: 3, Enabled: true}},
	})
	if err != nil {
		t.Fatalf("create sync group: %v", err)
	}
	if log, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil || !log.Success {
		t.Fatalf("first apply = %#v, %v", log, err)
	}
	mu.Lock()
	if createCount != 1 || len(channels) != 1 {
		mu.Unlock()
		t.Fatalf("created channels = %d/%d", createCount, len(channels))
	}
	var remote map[string]any
	for _, channel := range channels {
		remote = channel
	}
	if remote["type"] != float64(60) || remote["group"] != "premium" || remote["models"] != "gpt-a,gpt-b" || remote["key"] != "sk-created" {
		mu.Unlock()
		t.Fatalf("remote channel = %#v", remote)
	}
	if remote["model_mapping"] != "" {
		mu.Unlock()
		t.Fatalf("automatic model mapping = %#v, want empty", remote["model_mapping"])
	}
	if remote["auto_ban"] != float64(0) {
		mu.Unlock()
		t.Fatalf("auto_ban = %#v, want 0", remote["auto_ban"])
	}
	var setting map[string]any
	var settings map[string]any
	_ = json.Unmarshal([]byte(remote["setting"].(string)), &setting)
	_ = json.Unmarshal([]byte(remote["settings"].(string)), &settings)
	for _, key := range []string{"allow_service_tier", "allow_safety_identifier", "upstream_model_update_check_enabled", "upstream_model_update_auto_sync_enabled"} {
		if settings[key] != true {
			mu.Unlock()
			t.Fatalf("settings[%q] = %#v, want true", key, settings[key])
		}
	}
	if setting["pass_through_body_enabled"] != true {
		mu.Unlock()
		t.Fatalf("setting pass-through = %#v, want true", setting["pass_through_body_enabled"])
	}
	remote["setting"] = `{"manual_setting":true}`
	remote["settings"] = `{"manual_setting":true}`
	remote["param_override"] = `{"temperature":0.2}`
	remote["header_override"] = `{"X-Manual":"keep"}`
	mu.Unlock()
	if log, err := svc.ApplySyncGroup(context.Background(), rule.ID); err != nil || !log.Success {
		t.Fatalf("second apply = %#v, %v", log, err)
	}
	mu.Lock()
	if createCount != 1 || updateCount != 1 {
		mu.Unlock()
		t.Fatalf("channel operations create=%d update=%d", createCount, updateCount)
	}
	for _, channel := range channels {
		remote = channel
	}
	if remote["param_override"] != `{"temperature":0.2}` || remote["header_override"] != `{"X-Manual":"keep"}` {
		mu.Unlock()
		t.Fatalf("manual overrides were not preserved: %#v", remote)
	}
	_ = json.Unmarshal([]byte(remote["setting"].(string)), &setting)
	_ = json.Unmarshal([]byte(remote["settings"].(string)), &settings)
	if setting["manual_setting"] != true || settings["manual_setting"] != true {
		mu.Unlock()
		t.Fatalf("manual settings were not preserved: setting=%#v settings=%#v", setting, settings)
	}
	mu.Unlock()
	if _, err := svc.DeleteManaged(context.Background(), rule.ID); err != nil {
		t.Fatalf("delete managed: %v", err)
	}
	mu.Lock()
	deletes := append([]int64(nil), deleteIDs...)
	remaining := len(channels)
	mu.Unlock()
	if !reflect.DeepEqual(deletes, []int64{10}) || remaining != 0 || fake.deleteCount != 1 {
		t.Fatalf("delete IDs=%#v remaining=%d source key deletes=%d", deletes, remaining, fake.deleteCount)
	}
}

func TestNewAPIGatewaySyncUpdatesGroupRatioAndCreatesChannel(t *testing.T) {
	options := map[string]string{
		"AutoGroups":       `[]`,
		"GroupRatio":       `{"default":1}`,
		"UserUsableGroups": `{"default":"Default"}`,
		"TopupGroupRatio":  `{}`,
	}
	var mu sync.Mutex
	updates := make([]string, 0)
	channels := map[int64]map[string]any{}
	nextChannelID := int64(10)
	createCount := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/api/option/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer root-token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			data := make([]map[string]string, 0, len(options))
			for _, key := range []string{"AutoGroups", "GroupRatio", "UserUsableGroups", "TopupGroupRatio"} {
				data = append(data, map[string]string{"key": key, "value": options[key]})
			}
			respondJSON(w, map[string]any{"success": true, "data": data})
		case http.MethodPut:
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode option update: %v", err)
			}
			options[body["key"]] = body["value"]
			updates = append(updates, body["key"])
			respondJSON(w, map[string]any{"success": true, "data": map[string]any{}})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/channel/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer root-token" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			items := make([]map[string]any, 0, len(channels))
			for _, channel := range channels {
				items = append(items, channel)
			}
			respondJSON(w, map[string]any{"success": true, "data": map[string]any{"items": items}})
		case http.MethodPost:
			var body struct {
				Mode    string         `json:"mode"`
				Channel map[string]any `json:"channel"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode channel create: %v", err)
			}
			if body.Mode != "single" {
				t.Fatalf("channel mode = %q, want single", body.Mode)
			}
			body.Channel["id"] = nextChannelID
			body.Channel["status"] = float64(1)
			channels[nextChannelID] = body.Channel
			nextChannelID++
			createCount++
			respondJSON(w, map[string]any{"success": true, "data": map[string]any{}})
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	db := openSyncerTestDB(t)
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("new gateway cipher: %v", err)
	}
	gatewaySvc := gateway.NewService(
		storage.NewGatewayGroups(db),
		storage.NewGatewayKeys(db),
		storage.NewGatewayRoutes(db),
		nil,
		nil,
		storage.NewChannels(db),
		&fakeChannelService{},
		cipher,
		nil,
	)
	group, err := gatewaySvc.CreateGroup(gateway.CreateGroupInput{
		Name: "rate-source", ModelsJSON: `[{"id":"gateway-model","source":"custom"}]`,
		ModelsMode: "manual",
	})
	if err != nil {
		t.Fatalf("create gateway group: %v", err)
	}
	if err := storage.NewGatewayRoutes(db).SaveForGroup(group.ID, []storage.GatewayRoute{{
		SourceChannelID:       1,
		SourceGroupName:       "default",
		Enabled:               true,
		BillingRateMultiplier: 2.5,
	}}); err != nil {
		t.Fatalf("save gateway route: %v", err)
	}
	svc := newTestServiceWithGateway(t, db, &fakeChannelService{}, gatewaySvc, "https://gateway.example")
	target, err := svc.CreateTarget(context.Background(), TargetInput{Name: "newapi", TargetType: "newapi", BaseURL: server.URL, AdminAPIKey: "root-token", Enabled: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	targetGroups, err := svc.SyncTargetGroups(context.Background(), target.ID)
	if err != nil {
		t.Fatalf("sync target groups: %v", err)
	}
	groupID := group.ID
	rule, err := svc.CreateSyncGroup(SyncGroupDTO{
		NameTemplate: "new-rate-{同步分组ID}", TargetID: target.ID, TargetGroupIDs: []uint{targetGroups[0].ID}, Platform: "openai", ModelLimitsMode: "sync_upstream",
		Accounts: []SyncAccountDTO{{SourceKind: "gateway_group", GatewayGroupID: &groupID, GatewayRateMode: "max", RateConvertMode: "raw", RateConvertValue: 1, Enabled: true}},
	})
	if err != nil {
		t.Fatalf("create rate sync group: %v", err)
	}
	log, err := svc.ApplySyncGroup(context.Background(), rule.ID)
	if err != nil || !log.Success {
		t.Fatalf("apply rate sync = %#v, %v", log, err)
	}
	mu.Lock()
	defer mu.Unlock()
	var ratios map[string]float64
	if err := json.Unmarshal([]byte(options["GroupRatio"]), &ratios); err != nil {
		t.Fatalf("decode GroupRatio: %v", err)
	}
	if ratios["default"] != 2.5 {
		t.Fatalf("GroupRatio = %#v", ratios)
	}
	if !reflect.DeepEqual(updates, []string{"GroupRatio", "UserUsableGroups", "TopupGroupRatio", "AutoGroups"}) {
		t.Fatalf("updated options = %#v", updates)
	}
	if createCount != 1 || len(channels) != 1 {
		t.Fatalf("created channels = %d/%d, want 1/1", createCount, len(channels))
	}
	var channel map[string]any
	for _, item := range channels {
		channel = item
	}
	if channel["type"] != float64(60) || channel["key"] == "" || channel["base_url"] != "https://gateway.example" {
		t.Fatalf("created New API channel = %#v", channel)
	}
	if channel["models"] != "gateway-model" || channel["group"] != "default" {
		t.Fatalf("created New API channel models/groups = %#v", channel)
	}
}
