// Package syncer 上游同步服务（公告/余额等相关任务）。
package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lzy98276/upstream-ops/backend/connector"
	"github.com/lzy98276/upstream-ops/backend/connector/newapi"
	"github.com/lzy98276/upstream-ops/backend/connector/sub2api"
	"github.com/lzy98276/upstream-ops/backend/crypto"
	"github.com/lzy98276/upstream-ops/backend/gateway"
	"github.com/lzy98276/upstream-ops/backend/notify"
	"github.com/lzy98276/upstream-ops/backend/pkg/rateconvert"
	"github.com/lzy98276/upstream-ops/backend/storage"
)

type channelSvc interface {
	RevealAPIKey(ctx context.Context, channelID uint, keyID int64) (string, error)
	CreateAPIKey(ctx context.Context, channelID uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error)
	UpdateAPIKey(ctx context.Context, channelID uint, keyID int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error)
	DeleteAPIKey(ctx context.Context, channelID uint, keyID int64) error
	ListAPIKeys(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error)
	ListAPIKeyGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error)
}

// ErrInvalidTargetGroupOrder means the client submitted a stale, incomplete,
// or duplicate remote-group order. Callers can return it as a client error.
var ErrInvalidTargetGroupOrder = errors.New("ordered_ids must include each current remote group exactly once")

type Service struct {
	channels   *storage.Channels
	rates      *storage.Rates
	cipher     *crypto.Cipher
	channelSvc channelSvc
	log        *slog.Logger
	dispatcher *notify.Dispatcher

	targets         *storage.UpstreamSyncTargets
	groups          *storage.UpstreamSyncTargetGroups
	syncGroups      *storage.UpstreamSyncGroups
	syncAccounts    *storage.UpstreamSyncAccounts
	managedAccounts *storage.UpstreamSyncManagedAccounts
	logs            *storage.UpstreamSyncLogs
	gateway         *gateway.Service
	gatewayBaseURL  string
}

func New(
	channels *storage.Channels,
	rates *storage.Rates,
	cipher *crypto.Cipher,
	channelSvc channelSvc,
	log *slog.Logger,
	targets *storage.UpstreamSyncTargets,
	groups *storage.UpstreamSyncTargetGroups,
	syncGroups *storage.UpstreamSyncGroups,
	syncAccounts *storage.UpstreamSyncAccounts,
	managedAccounts *storage.UpstreamSyncManagedAccounts,
	logs *storage.UpstreamSyncLogs,
	gatewaySvc *gateway.Service,
	gatewayBaseURL string,
) *Service {
	return &Service{
		channels:        channels,
		rates:           rates,
		cipher:          cipher,
		channelSvc:      channelSvc,
		log:             log,
		targets:         targets,
		groups:          groups,
		syncGroups:      syncGroups,
		syncAccounts:    syncAccounts,
		managedAccounts: managedAccounts,
		logs:            logs,
		gateway:         gatewaySvc,
		gatewayBaseURL:  strings.TrimRight(strings.TrimSpace(gatewayBaseURL), "/"),
	}
}

func (s *Service) SetDispatcher(dispatcher *notify.Dispatcher) {
	s.dispatcher = dispatcher
}

type TargetDTO struct {
	ID              uint       `json:"id"`
	Name            string     `json:"name"`
	TargetType      string     `json:"target_type"`
	BaseURL         string     `json:"base_url"`
	Enabled         bool       `json:"enabled"`
	LastCheckStatus string     `json:"last_check_status,omitempty"`
	LastCheckAt     *time.Time `json:"last_check_at,omitempty"`
	LastCheckError  string     `json:"last_check_error,omitempty"`
}

type TargetGroupDTO struct {
	ID            uint       `json:"id"`
	TargetID      uint       `json:"target_id"`
	RemoteGroupID int64      `json:"remote_group_id"`
	Name          string     `json:"name"`
	Platform      string     `json:"platform,omitempty"`
	Ratio         float64    `json:"ratio"`
	Status        string     `json:"status"`
	Sort          int        `json:"sort"`
	Description   string     `json:"description,omitempty"`
	LastSyncAt    *time.Time `json:"last_sync_at,omitempty"`
}

type TargetProxyDTO struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Status   string `json:"status"`
}

type SyncGroupDTO struct {
	ID                       uint             `json:"id"`
	Sort                     int              `json:"sort"`
	DisplayName              string           `json:"display_name"`
	NameTemplate             string           `json:"name_template"`
	Name                     string           `json:"name"`
	TargetID                 uint             `json:"target_id"`
	TargetGroupIDs           []uint           `json:"target_group_ids"`
	SyncMode                 string           `json:"sync_mode"`
	Platform                 string           `json:"platform"`
	ModelLimitsMode          string           `json:"model_limits_mode"`
	ModelLimits              string           `json:"model_limits,omitempty"`
	PoolModeEnabled          bool             `json:"pool_mode_enabled"`
	PoolModeRetryCount       int              `json:"pool_mode_retry_count"`
	PoolModeRetryStatusCodes string           `json:"pool_mode_retry_status_codes,omitempty"`
	CustomErrorCodesEnabled  bool             `json:"custom_error_codes_enabled"`
	CustomErrorCodes         string           `json:"custom_error_codes,omitempty"`
	RateSortDirection        string           `json:"rate_sort_direction"`
	Accounts                 []SyncAccountDTO `json:"accounts"`
	Enabled                  *bool            `json:"enabled"`
	ApplyStatus              string           `json:"apply_status,omitempty"`
	ApplyError               string           `json:"apply_error,omitempty"`
	LastAppliedAt            *time.Time       `json:"last_applied_at,omitempty"`
}

type SyncAccountDTO struct {
	ID               uint    `json:"id,omitempty"`
	SourceKind       string  `json:"source_kind,omitempty"`
	SourceChannelID  uint    `json:"source_channel_id"`
	SourceGroupID    *int64  `json:"source_group_id,omitempty"`
	SourceGroupName  string  `json:"source_group_name,omitempty"`
	GatewayGroupID   *uint   `json:"gateway_group_id,omitempty"`
	GatewayRateMode  string  `json:"gateway_rate_mode,omitempty"`
	GatewayRateMin   float64 `json:"gateway_rate_min,omitempty"`
	GatewayRateMax   float64 `json:"gateway_rate_max,omitempty"`
	ProxyID          *int64  `json:"proxy_id,omitempty"`
	Concurrency      int     `json:"concurrency"`
	Weight           int     `json:"weight"`
	Priority         int64   `json:"priority"`
	RateConvertMode  string  `json:"rate_convert_mode"`
	RateConvertValue float64 `json:"rate_convert_value"`
	Enabled          bool    `json:"enabled"`
	TestEnabled      bool    `json:"test_enabled"`
	TestModel        string  `json:"test_model,omitempty"`
}

type SourceModelsInput struct {
	ChannelID       uint
	SyncAccountID   uint
	SourceGroupID   *int64
	SourceGroupName string
	Platform        string
}

type ManagedAccountDTO struct {
	ID                uint       `json:"id"`
	SyncGroupID       uint       `json:"sync_group_id"`
	SyncAccountID     uint       `json:"sync_account_id"`
	SourceAPIKeyID    int64      `json:"source_api_key_id"`
	SourceAPIKeyName  string     `json:"source_api_key_name"`
	TargetAccountID   int64      `json:"target_account_id"`
	TargetAccountName string     `json:"target_account_name"`
	TargetGroupIDs    []uint     `json:"target_group_ids"`
	LastAppliedAt     *time.Time `json:"last_applied_at,omitempty"`
}

type LogDTO struct {
	ID          uint      `json:"id"`
	SyncGroupID uint      `json:"sync_group_id"`
	TargetID    uint      `json:"target_id"`
	Action      string    `json:"action"`
	Success     bool      `json:"success"`
	Message     string    `json:"message,omitempty"`
	Detail      string    `json:"detail,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type accountApplyResult struct {
	SyncedModels []string
	Message      string
	Changes      []string
}

const applyAccountWorkerLimit = 5
const sourceModelsBodyLimit int64 = 8 << 20

type syncAccountApplyOutcome struct {
	Index        int
	Applied      bool
	Failure      string
	Success      string
	Changes      []string
	SyncedModels []string
}

type TargetInput struct {
	Name        string `json:"name"`
	TargetType  string `json:"target_type"`
	BaseURL     string `json:"base_url"`
	AdminAPIKey string `json:"admin_api_key"`
	Enabled     bool   `json:"enabled"`
}

type NewAPIAutoGroupsDTO struct {
	Groups          []string `json:"groups"`
	AvailableGroups []string `json:"available_groups"`
}

type NewAPIGroupDTO struct {
	Name        string  `json:"name"`
	Ratio       float64 `json:"ratio"`
	Description string  `json:"description,omitempty"`
}

type NewAPIGroupInput struct {
	Name        string  `json:"name"`
	Ratio       float64 `json:"ratio"`
	Description string  `json:"description"`
}

func (s *Service) ListTargets() ([]TargetDTO, error) {
	list, err := s.targets.List()
	if err != nil {
		return nil, err
	}
	out := make([]TargetDTO, 0, len(list))
	for _, item := range list {
		out = append(out, TargetDTO{
			ID:              item.ID,
			Name:            item.Name,
			TargetType:      normalizeTargetType(item.TargetType),
			BaseURL:         item.BaseURL,
			Enabled:         item.Enabled,
			LastCheckStatus: item.LastCheckStatus,
			LastCheckAt:     item.LastCheckAt,
			LastCheckError:  item.LastCheckError,
		})
	}
	return out, nil
}

func (s *Service) CreateTarget(ctx context.Context, in TargetInput) (*TargetDTO, error) {
	if err := validateTargetInput(in, true); err != nil {
		return nil, err
	}
	enc, err := s.cipher.Encrypt(strings.TrimSpace(in.AdminAPIKey))
	if err != nil {
		return nil, err
	}
	item := &storage.UpstreamSyncTarget{
		Name:              strings.TrimSpace(in.Name),
		TargetType:        normalizeTargetType(in.TargetType),
		BaseURL:           strings.TrimSpace(in.BaseURL),
		AdminAPIKeyCipher: enc,
		Enabled:           in.Enabled,
	}
	if err := s.targets.Create(item); err != nil {
		return nil, err
	}
	return s.toTargetDTO(item), nil
}

func (s *Service) UpdateTarget(ctx context.Context, id uint, in TargetInput) (*TargetDTO, error) {
	item, err := s.targets.FindByID(id)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.TargetType) == "" {
		in.TargetType = item.TargetType
	}
	if err := validateTargetInput(in, false); err != nil {
		return nil, err
	}
	item.Name = strings.TrimSpace(in.Name)
	item.TargetType = normalizeTargetType(in.TargetType)
	item.BaseURL = strings.TrimSpace(in.BaseURL)
	if strings.TrimSpace(in.AdminAPIKey) != "" {
		enc, err := s.cipher.Encrypt(strings.TrimSpace(in.AdminAPIKey))
		if err != nil {
			return nil, err
		}
		item.AdminAPIKeyCipher = enc
	}
	item.Enabled = in.Enabled
	if err := s.targets.Update(item); err != nil {
		return nil, err
	}
	return s.toTargetDTO(item), nil
}

func (s *Service) DeleteTarget(id uint) error {
	return s.targets.Delete(id)
}

func (s *Service) CheckTarget(ctx context.Context, id uint) error {
	item, err := s.targets.FindByID(id)
	if err != nil {
		return err
	}
	plain, err := s.cipher.Decrypt(item.AdminAPIKeyCipher)
	if err != nil {
		_ = s.targets.UpdateCheck(id, "failed", ptrTime(time.Now()), err.Error())
		return err
	}
	if normalizeTargetType(item.TargetType) == "newapi" {
		_, err = s.getNewAPIAutoGroups(ctx, item.BaseURL, plain)
	} else {
		client := sub2api.NewAdminClient()
		err = client.Ping(ctx, sub2api.AdminTarget{BaseURL: item.BaseURL, APIKey: plain})
	}
	status := "ok"
	errText := ""
	if err != nil {
		status = "failed"
		errText = err.Error()
	}
	now := time.Now()
	_ = s.targets.UpdateCheck(id, status, &now, errText)
	return err
}

func (s *Service) SyncTargetGroups(ctx context.Context, targetID uint) ([]TargetGroupDTO, error) {
	target, err := s.targets.FindByID(targetID)
	if err != nil {
		return nil, err
	}
	plain, err := s.cipher.Decrypt(target.AdminAPIKeyCipher)
	if err != nil {
		return nil, err
	}
	if normalizeTargetType(target.TargetType) == "newapi" {
		return s.syncNewAPITargetGroups(ctx, target, plain)
	}
	client := sub2api.NewAdminClient()
	groups, err := client.ListGroups(ctx, sub2api.AdminTarget{BaseURL: target.BaseURL, APIKey: plain}, true)
	if err != nil {
		_ = s.targets.UpdateCheck(targetID, "failed", ptrTime(time.Now()), err.Error())
		return nil, err
	}
	seen := make([]int64, 0, len(groups))
	now := time.Now()
	out := make([]TargetGroupDTO, 0, len(groups))
	for _, g := range groups {
		seen = append(seen, g.ID)
		item, err := s.groups.FindByTargetAndRemote(targetID, g.ID)
		if err != nil {
			item = &storage.UpstreamSyncTargetGroup{TargetID: targetID, RemoteGroupID: g.ID}
		}
		item.TargetID = targetID
		item.RemoteGroupID = g.ID
		item.Name = g.Name
		item.Platform = g.Platform
		item.Ratio = g.Ratio
		item.Sort = g.Sort
		item.Description = g.Description
		item.Status = strings.TrimSpace(g.Status)
		if item.Status == "" {
			item.Status = "active"
		}
		item.LastSyncAt = &now
		if err := s.groups.Upsert(item); err != nil {
			return nil, err
		}
		out = append(out, s.toGroupDTO(item))
	}
	_ = s.groups.DeleteMissing(targetID, seen)
	return out, nil
}

// syncNewAPITargetGroups turns New API's administrator option maps into the
// same local group cache used by the common synchronization-group editor.
// New API groups do not have server-issued numeric IDs, so a stable FNV hash
// of their name is used only as the cache's remote-group identifier.
func (s *Service) syncNewAPITargetGroups(ctx context.Context, target *storage.UpstreamSyncTarget, apiKey string) ([]TargetGroupDTO, error) {
	settings, err := s.getNewAPIGroupSettings(ctx, target.BaseURL, apiKey)
	if err != nil {
		s.recordTargetCheck(target.ID, err)
		return nil, err
	}
	return s.cacheNewAPITargetGroups(target, settings)
}

// cacheNewAPITargetGroups stores a New API option snapshot without issuing a
// second remote request. This lets sort operations validate against the live
// snapshot once, then return an updated local list immediately after writing.
func (s *Service) cacheNewAPITargetGroups(target *storage.UpstreamSyncTarget, settings *newAPIGroupSettings) ([]TargetGroupDTO, error) {
	groups := newAPIRemoteGroups(settings)
	existingGroups, err := s.groups.ListByTarget(target.ID, true)
	if err != nil {
		return nil, err
	}
	existingByName := make(map[string]*storage.UpstreamSyncTargetGroup, len(existingGroups))
	for index := range existingGroups {
		existingByName[existingGroups[index].Name] = &existingGroups[index]
	}

	seen := make([]int64, 0, len(groups))
	out := make([]TargetGroupDTO, 0, len(groups))
	now := time.Now()
	for index, group := range groups {
		remoteID := newAPIGroupRemoteID(group.Name)
		seen = append(seen, remoteID)
		item, findErr := s.groups.FindByTargetAndRemote(target.ID, remoteID)
		if findErr != nil {
			// Versions before the JavaScript-safe ID fix stored full 63-bit
			// hashes. Reuse the local row by name so existing sync-group links
			// keep working while its external ID is migrated.
			item = existingByName[group.Name]
			if item == nil {
				item = &storage.UpstreamSyncTargetGroup{TargetID: target.ID, RemoteGroupID: remoteID}
			} else if item.RemoteGroupID != remoteID {
				if err := s.groups.UpdateRemoteGroupID(item.ID, target.ID, remoteID); err != nil {
					return nil, err
				}
				item.RemoteGroupID = remoteID
			}
		}
		item.TargetID = target.ID
		item.RemoteGroupID = remoteID
		item.Name = group.Name
		// New API groups are shared by every protocol. Platform remains a
		// synchronization-channel setting and must not constrain group selection.
		item.Platform = ""
		item.Ratio = group.Ratio
		item.Sort = index + 1
		item.Description = group.Description
		item.Status = "active"
		item.LastSyncAt = &now
		if err := s.groups.Upsert(item); err != nil {
			return nil, err
		}
		out = append(out, s.toGroupDTO(item))
	}
	if err := s.groups.DeleteMissing(target.ID, seen); err != nil {
		return nil, err
	}
	s.recordTargetCheck(target.ID, nil)
	return out, nil
}

func newAPIGroupRemoteID(name string) int64 {
	// TargetGroupDTO is consumed by the browser as a JSON number. Keep the
	// synthetic ID within Number.MAX_SAFE_INTEGER so drag-sort round-trips it
	// without precision loss in JavaScript.
	const maxSafeInteger uint64 = 9007199254740991
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.ToLower(strings.TrimSpace(name))))
	id := h.Sum64() % maxSafeInteger
	if id == 0 {
		id = 1
	}
	return int64(id)
}

// ReorderTargetGroups persists remote group order for either supported target.
// Sub2API has a dedicated sort-order endpoint. New API's remote order is the
// member order of its GroupRatio option.
func (s *Service) ReorderTargetGroups(ctx context.Context, targetID uint, orderedIDs []int64) ([]TargetGroupDTO, error) {
	target, err := s.targets.FindByID(targetID)
	if err != nil {
		return nil, err
	}
	plain, err := s.cipher.Decrypt(target.AdminAPIKeyCipher)
	if err != nil {
		return nil, err
	}
	orderedIDs = compactPositiveIDs(orderedIDs)
	if normalizeTargetType(target.TargetType) == "newapi" {
		return s.reorderNewAPITargetGroups(ctx, target, plain, orderedIDs)
	}
	client := sub2api.NewAdminClient()
	remoteGroups, err := client.ListGroups(ctx, sub2api.AdminTarget{BaseURL: target.BaseURL, APIKey: plain}, true)
	if err != nil {
		s.recordTargetCheck(targetID, err)
		return nil, err
	}
	remoteIDs := make([]int64, 0, len(remoteGroups))
	for _, group := range remoteGroups {
		remoteIDs = append(remoteIDs, group.ID)
	}
	if !samePositiveIDSet(orderedIDs, remoteIDs) {
		return nil, ErrInvalidTargetGroupOrder
	}
	if err := client.UpdateGroupSortOrders(ctx, sub2api.AdminTarget{BaseURL: target.BaseURL, APIKey: plain}, orderedIDs); err != nil {
		s.recordTargetCheck(targetID, err)
		return nil, err
	}
	if _, err := s.SyncTargetGroups(ctx, targetID); err != nil {
		return nil, err
	}
	return s.ListTargetGroups(targetID, true)
}

func (s *Service) reorderNewAPITargetGroups(ctx context.Context, target *storage.UpstreamSyncTarget, apiKey string, orderedIDs []int64) ([]TargetGroupDTO, error) {
	// Validate the browser's list against the live GroupRatio snapshot. A stale
	// browser must not overwrite a group created remotely after the dialog was
	// opened. The snapshot is then reused to update the local cache so sorting
	// does not issue the old, redundant post-write GET request.
	settings, err := s.getNewAPIGroupSettings(ctx, target.BaseURL, apiKey)
	if err != nil {
		s.recordTargetCheck(target.ID, err)
		return nil, err
	}
	remoteGroups := newAPIRemoteGroups(settings)
	remoteIDs := make([]int64, 0, len(remoteGroups))
	nameByID := make(map[int64]string, len(remoteGroups))
	for _, group := range remoteGroups {
		id := newAPIGroupRemoteID(group.Name)
		remoteIDs = append(remoteIDs, id)
		nameByID[id] = group.Name
	}
	if !samePositiveIDSet(orderedIDs, remoteIDs) {
		return nil, ErrInvalidTargetGroupOrder
	}

	// `auto` is a real visible remote group. It participates in GroupRatio's
	// order, but AutoGroups is a separate routing configuration and must not be
	// changed merely because the displayed group list was reordered.
	groupOrder := make([]string, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		groupOrder = append(groupOrder, nameByID[id])
	}
	ratioValue, err := marshalNewAPIGroupRatios(settings.GroupRatios, groupOrder)
	if err != nil {
		return nil, err
	}
	if err := s.putNewAPIOption(ctx, target.BaseURL, apiKey, "GroupRatio", string(ratioValue)); err != nil {
		s.recordTargetCheck(target.ID, err)
		return nil, err
	}

	if _, err := s.cacheNewAPITargetGroups(target, settings); err != nil {
		return nil, err
	}
	cachedGroups, err := s.groups.ListByTarget(target.ID, false)
	if err != nil {
		return nil, err
	}
	groupByID := make(map[int64]*storage.UpstreamSyncTargetGroup, len(cachedGroups))
	for index := range cachedGroups {
		groupByID[cachedGroups[index].RemoteGroupID] = &cachedGroups[index]
	}
	now := time.Now()
	for index, id := range orderedIDs {
		group := groupByID[id]
		group.Sort = index + 1
		group.LastSyncAt = &now
		if err := s.groups.Upsert(group); err != nil {
			return nil, err
		}
	}
	s.recordTargetCheck(target.ID, nil)
	return s.ListTargetGroups(target.ID, true)
}

func marshalNewAPIGroupRatios(ratios map[string]float64, order []string) ([]byte, error) {
	keys := make([]string, 0, len(ratios))
	seen := make(map[string]struct{}, len(ratios))
	for _, name := range order {
		if _, ok := ratios[name]; ok {
			keys = append(keys, name)
			seen[name] = struct{}{}
		}
	}
	missing := make([]string, 0)
	for name := range ratios {
		if _, ok := seen[name]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return strings.ToLower(missing[i]) < strings.ToLower(missing[j]) })
	keys = append(keys, missing...)
	var out strings.Builder
	out.WriteByte('{')
	for i, name := range keys {
		if i > 0 {
			out.WriteByte(',')
		}
		encodedName, err := json.Marshal(name)
		if err != nil {
			return nil, err
		}
		encodedRatio, err := json.Marshal(ratios[name])
		if err != nil {
			return nil, err
		}
		out.Write(encodedName)
		out.WriteByte(':')
		out.Write(encodedRatio)
	}
	out.WriteByte('}')
	return []byte(out.String()), nil
}

// ReorderSub2APIGroups remains as a compatibility alias for callers compiled
// against the previous service method.
func (s *Service) ReorderSub2APIGroups(ctx context.Context, targetID uint, orderedIDs []int64) ([]TargetGroupDTO, error) {
	return s.ReorderTargetGroups(ctx, targetID, orderedIDs)
}

func compactPositiveIDs(ids []int64) []int64 {
	result := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func samePositiveIDSet(left, right []int64) bool {
	left = compactPositiveIDs(left)
	right = compactPositiveIDs(right)
	if len(left) != len(right) {
		return false
	}
	seen := make(map[int64]struct{}, len(left))
	for _, id := range left {
		seen[id] = struct{}{}
	}
	for _, id := range right {
		if _, ok := seen[id]; !ok {
			return false
		}
	}
	return true
}

// GetNewAPIAutoGroups reads the administrator setting that controls New API's
// automatic-group priority. The remote root token remains server-side.
func (s *Service) GetNewAPIAutoGroups(ctx context.Context, targetID uint) (*NewAPIAutoGroupsDTO, error) {
	target, apiKey, err := s.newAPITargetCredential(targetID)
	if err != nil {
		return nil, err
	}
	settings, err := s.getNewAPIGroupSettings(ctx, target.BaseURL, apiKey)
	s.recordTargetCheck(targetID, err)
	if err != nil {
		return nil, err
	}
	return &NewAPIAutoGroupsDTO{
		Groups:          settings.AutoGroups,
		AvailableGroups: settings.AvailableGroups,
	}, nil
}

// UpdateNewAPIAutoGroups replaces AutoGroups while preserving its ordered
// semantics. Empty values and duplicates are discarded before writing.
func (s *Service) UpdateNewAPIAutoGroups(ctx context.Context, targetID uint, groups []string) (*NewAPIAutoGroupsDTO, error) {
	target, apiKey, err := s.newAPITargetCredential(targetID)
	if err != nil {
		return nil, err
	}
	normalized := normalizeAutoGroups(groups)
	if err := s.putNewAPIAutoGroups(ctx, target.BaseURL, apiKey, normalized); err != nil {
		s.recordTargetCheck(targetID, err)
		return nil, err
	}
	s.recordTargetCheck(targetID, nil)
	return &NewAPIAutoGroupsDTO{Groups: normalized}, nil
}

// ListNewAPIGroups returns the groups configured by the New API administrator.
// New API represents them through its root-only option values rather than a
// standalone group resource.
func (s *Service) ListNewAPIGroups(ctx context.Context, targetID uint) ([]NewAPIGroupDTO, error) {
	target, apiKey, err := s.newAPITargetCredential(targetID)
	if err != nil {
		return nil, err
	}
	settings, err := s.getNewAPIGroupSettings(ctx, target.BaseURL, apiKey)
	s.recordTargetCheck(targetID, err)
	if err != nil {
		return nil, err
	}
	return settings.Groups, nil
}

// SaveNewAPIGroup creates or updates one New API group. The group is made
// usable by keeping GroupRatio and UserUsableGroups in sync.
func (s *Service) SaveNewAPIGroup(ctx context.Context, targetID uint, in NewAPIGroupInput) (*NewAPIGroupDTO, error) {
	target, apiKey, err := s.newAPITargetCredential(targetID)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return nil, errors.New("New API group name is required")
	}
	if strings.EqualFold(name, "auto") {
		return nil, errors.New("New API group name cannot be auto")
	}
	if in.Ratio < 0 {
		return nil, errors.New("New API group ratio must not be negative")
	}
	settings, err := s.getNewAPIGroupSettings(ctx, target.BaseURL, apiKey)
	if err != nil {
		s.recordTargetCheck(targetID, err)
		return nil, err
	}
	settings.GroupRatios[name] = in.Ratio
	description := strings.TrimSpace(in.Description)
	if description == "" {
		description = name
	}
	settings.UserUsableGroups[name] = description
	if err := s.putNewAPIGroupSettings(ctx, target.BaseURL, apiKey, settings); err != nil {
		s.recordTargetCheck(targetID, err)
		return nil, err
	}
	s.recordTargetCheck(targetID, nil)
	return &NewAPIGroupDTO{Name: name, Ratio: in.Ratio, Description: description}, nil
}

func (s *Service) DeleteNewAPIGroup(ctx context.Context, targetID uint, name string) error {
	target, apiKey, err := s.newAPITargetCredential(targetID)
	if err != nil {
		return err
	}
	name = strings.TrimSpace(name)
	if name == "" || strings.EqualFold(name, "auto") {
		return errors.New("New API group name is invalid")
	}
	settings, err := s.getNewAPIGroupSettings(ctx, target.BaseURL, apiKey)
	if err != nil {
		s.recordTargetCheck(targetID, err)
		return err
	}
	if _, exists := settings.GroupRatios[name]; !exists {
		return errors.New("New API group not found")
	}
	delete(settings.GroupRatios, name)
	delete(settings.UserUsableGroups, name)
	delete(settings.TopupGroupRatios, name)
	settings.AutoGroups = removeAutoGroup(settings.AutoGroups, name)
	if err := s.putNewAPIGroupSettings(ctx, target.BaseURL, apiKey, settings); err != nil {
		s.recordTargetCheck(targetID, err)
		return err
	}
	s.recordTargetCheck(targetID, nil)
	return nil
}

func normalizeTargetType(targetType string) string {
	if strings.EqualFold(strings.TrimSpace(targetType), "newapi") {
		return "newapi"
	}
	return "sub2api"
}

func validateTargetInput(in TargetInput, creating bool) error {
	if strings.TrimSpace(in.Name) == "" {
		return errors.New("target name is required")
	}
	if strings.TrimSpace(in.BaseURL) == "" {
		return errors.New("target base_url is required")
	}
	parsed, err := url.ParseRequestURI(strings.TrimSpace(in.BaseURL))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("target base_url must be an absolute HTTP URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("target base_url must use HTTP or HTTPS")
	}
	if creating && strings.TrimSpace(in.AdminAPIKey) == "" {
		return errors.New("target admin_api_key is required")
	}
	return nil
}

func (s *Service) requireSyncTarget(targetID uint) error {
	if targetID == 0 {
		return errors.New("target_id is required")
	}
	_, err := s.targets.FindByID(targetID)
	return err
}

func (s *Service) newAPITargetCredential(targetID uint) (*storage.UpstreamSyncTarget, string, error) {
	target, err := s.targets.FindByID(targetID)
	if err != nil {
		return nil, "", err
	}
	if normalizeTargetType(target.TargetType) != "newapi" {
		return nil, "", errors.New("target is not a New API target")
	}
	apiKey, err := s.cipher.Decrypt(target.AdminAPIKeyCipher)
	if err != nil {
		return nil, "", err
	}
	if strings.TrimSpace(apiKey) == "" {
		return nil, "", errors.New("New API root token is empty")
	}
	return target, apiKey, nil
}

func (s *Service) getNewAPIAutoGroups(ctx context.Context, baseURL, apiKey string) ([]string, error) {
	settings, err := s.getNewAPIGroupSettings(ctx, baseURL, apiKey)
	if err != nil {
		return nil, err
	}
	return settings.AutoGroups, nil
}

type newAPIGroupSettings struct {
	AutoGroups       []string
	AvailableGroups  []string
	Groups           []NewAPIGroupDTO
	GroupOrder       []string
	GroupRatios      map[string]float64
	UserUsableGroups map[string]string
	TopupGroupRatios map[string]json.RawMessage
}

const newAPIOptionTimeout = 30 * time.Second

// getNewAPIGroupSettings reads AutoGroups along with the groups already
// configured in New API's administrator settings. GroupRatio is the source of
// routable groups; the other maps retain groups that operators have configured
// elsewhere in the same settings page.
func (s *Service) getNewAPIGroupSettings(ctx context.Context, baseURL, apiKey string) (*newAPIGroupSettings, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/api/option/", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := (&http.Client{Timeout: newAPIOptionTimeout}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("New API option request failed: %s", resp.Status)
	}
	var body struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
		Data    []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&body); err != nil {
		return nil, err
	}
	if !body.Success {
		if body.Message == "" {
			body.Message = "New API rejected option request"
		}
		return nil, errors.New(body.Message)
	}
	settings := &newAPIGroupSettings{
		GroupRatios:      make(map[string]float64),
		UserUsableGroups: make(map[string]string),
		TopupGroupRatios: make(map[string]json.RawMessage),
	}
	availableSet := make(map[string]struct{})
	for _, option := range body.Data {
		switch option.Key {
		case "AutoGroups":
			var groups []string
			if err := json.Unmarshal([]byte(option.Value), &groups); err != nil {
				return nil, fmt.Errorf("decode New API AutoGroups: %w", err)
			}
			settings.AutoGroups = normalizeAutoGroups(groups)
		case "GroupRatio":
			ratios, order, err := decodeNewAPIGroupRatios(option.Value)
			if err != nil {
				return nil, fmt.Errorf("decode New API GroupRatio: %w", err)
			}
			settings.GroupRatios = ratios
			settings.GroupOrder = order
			for group := range settings.GroupRatios {
				availableSet[group] = struct{}{}
			}
		case "UserUsableGroups":
			if err := json.Unmarshal([]byte(option.Value), &settings.UserUsableGroups); err != nil {
				return nil, fmt.Errorf("decode New API UserUsableGroups: %w", err)
			}
			for group := range settings.UserUsableGroups {
				availableSet[group] = struct{}{}
			}
		case "TopupGroupRatio":
			if err := json.Unmarshal([]byte(option.Value), &settings.TopupGroupRatios); err != nil {
				return nil, fmt.Errorf("decode New API TopupGroupRatio: %w", err)
			}
			for _, group := range newAPIGroupNames(option.Value) {
				availableSet[group] = struct{}{}
			}
		}
	}
	for _, group := range settings.AutoGroups {
		availableSet[group] = struct{}{}
	}
	settings.AvailableGroups = sortedGroupNames(availableSet)
	settings.Groups = make([]NewAPIGroupDTO, 0, len(settings.GroupRatios))
	for _, group := range newAPIRemoteGroups(settings) {
		if !strings.EqualFold(group.Name, "auto") {
			settings.Groups = append(settings.Groups, group)
		}
	}
	sort.SliceStable(settings.Groups, func(i, j int) bool {
		return strings.ToLower(settings.Groups[i].Name) < strings.ToLower(settings.Groups[j].Name)
	})
	return settings, nil
}

func newAPIGroupNames(raw string) []string {
	var values map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil
	}
	groups := make([]string, 0, len(values))
	for group := range values {
		group = strings.TrimSpace(group)
		if group != "" && !strings.EqualFold(group, "auto") {
			groups = append(groups, group)
		}
	}
	return groups
}

func sortedGroupNames(groups map[string]struct{}) []string {
	result := make([]string, 0, len(groups))
	for group := range groups {
		if strings.EqualFold(group, "auto") {
			continue
		}
		result = append(result, group)
	}
	sort.Slice(result, func(i, j int) bool {
		return strings.ToLower(result[i]) < strings.ToLower(result[j])
	})
	return result
}

func decodeNewAPIGroupRatios(raw string) (map[string]float64, []string, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return nil, nil, err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return nil, nil, errors.New("expected group ratio object")
	}
	ratios := make(map[string]float64)
	order := make([]string, 0)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, nil, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, nil, errors.New("expected group name")
		}
		var ratio float64
		if err := decoder.Decode(&ratio); err != nil {
			return nil, nil, err
		}
		ratios[name] = ratio
		order = append(order, name)
	}
	if _, err := decoder.Token(); err != nil {
		return nil, nil, err
	}
	return ratios, order, nil
}

func newAPIRemoteGroups(settings *newAPIGroupSettings) []NewAPIGroupDTO {
	if settings == nil {
		return nil
	}
	groups := make([]NewAPIGroupDTO, 0, len(settings.GroupRatios))
	seen := make(map[string]struct{}, len(settings.GroupRatios))
	for _, name := range settings.GroupOrder {
		ratio, ok := settings.GroupRatios[name]
		if !ok || strings.TrimSpace(name) == "" {
			continue
		}
		groups = append(groups, NewAPIGroupDTO{Name: name, Ratio: ratio, Description: settings.UserUsableGroups[name]})
		seen[name] = struct{}{}
	}
	missing := make([]string, 0)
	for name := range settings.GroupRatios {
		if _, ok := seen[name]; !ok {
			missing = append(missing, name)
		}
	}
	sort.Slice(missing, func(i, j int) bool { return strings.ToLower(missing[i]) < strings.ToLower(missing[j]) })
	for _, name := range missing {
		groups = append(groups, NewAPIGroupDTO{Name: name, Ratio: settings.GroupRatios[name], Description: settings.UserUsableGroups[name]})
	}
	return groups
}

func (s *Service) putNewAPIAutoGroups(ctx context.Context, baseURL, apiKey string, groups []string) error {
	value, err := json.Marshal(groups)
	if err != nil {
		return err
	}
	return s.putNewAPIOption(ctx, baseURL, apiKey, "AutoGroups", string(value))
}

func (s *Service) putNewAPIGroupSettings(ctx context.Context, baseURL, apiKey string, settings *newAPIGroupSettings) error {
	if settings == nil {
		return errors.New("New API group settings are required")
	}
	groupRatios, err := marshalNewAPIGroupRatios(settings.GroupRatios, settings.GroupOrder)
	if err != nil {
		return err
	}
	usableGroups, err := json.Marshal(settings.UserUsableGroups)
	if err != nil {
		return err
	}
	topupRatios, err := json.Marshal(settings.TopupGroupRatios)
	if err != nil {
		return err
	}
	autoGroups, err := json.Marshal(normalizeAutoGroups(settings.AutoGroups))
	if err != nil {
		return err
	}
	for _, option := range []struct {
		key   string
		value string
	}{
		{"GroupRatio", string(groupRatios)},
		{"UserUsableGroups", string(usableGroups)},
		{"TopupGroupRatio", string(topupRatios)},
		{"AutoGroups", string(autoGroups)},
	} {
		if err := s.putNewAPIOption(ctx, baseURL, apiKey, option.key, option.value); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) putNewAPIOption(ctx context.Context, baseURL, apiKey, key, value string) error {
	body, err := json.Marshal(map[string]string{"key": key, "value": value})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, strings.TrimRight(baseURL, "/")+"/api/option/", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := (&http.Client{Timeout: newAPIOptionTimeout}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("New API option %s update failed: %s", key, resp.Status)
	}
	var result struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&result); err != nil {
		return err
	}
	if !result.Success {
		if result.Message == "" {
			result.Message = "New API rejected option update"
		}
		return errors.New(result.Message)
	}
	return nil
}

func removeAutoGroup(groups []string, name string) []string {
	result := make([]string, 0, len(groups))
	for _, group := range groups {
		if group != name {
			result = append(result, group)
		}
	}
	return result
}

func normalizeAutoGroups(groups []string) []string {
	result := make([]string, 0, len(groups))
	seen := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		group = strings.TrimSpace(group)
		if group == "" {
			continue
		}
		if _, exists := seen[group]; exists {
			continue
		}
		seen[group] = struct{}{}
		result = append(result, group)
	}
	return result
}

func (s *Service) recordTargetCheck(targetID uint, checkErr error) {
	status, errText := "ok", ""
	if checkErr != nil {
		status, errText = "failed", checkErr.Error()
	}
	now := time.Now()
	_ = s.targets.UpdateCheck(targetID, status, &now, errText)
}

func (s *Service) ListTargetGroups(targetID uint, includeMissing bool) ([]TargetGroupDTO, error) {
	list, err := s.groups.ListByTarget(targetID, includeMissing)
	if err != nil {
		return nil, err
	}
	out := make([]TargetGroupDTO, 0, len(list))
	for _, item := range list {
		out = append(out, s.toGroupDTO(&item))
	}
	return out, nil
}

func (s *Service) ListTargetProxies(ctx context.Context, targetID uint) ([]TargetProxyDTO, error) {
	target, err := s.targets.FindByID(targetID)
	if err != nil {
		return nil, err
	}
	if normalizeTargetType(target.TargetType) != "sub2api" {
		return []TargetProxyDTO{}, nil
	}
	plain, err := s.cipher.Decrypt(target.AdminAPIKeyCipher)
	if err != nil {
		return nil, err
	}
	client := sub2api.NewAdminClient()
	proxies, err := client.ListProxies(ctx, sub2api.AdminTarget{BaseURL: target.BaseURL, APIKey: plain})
	if err != nil {
		_ = s.targets.UpdateCheck(targetID, "failed", ptrTime(time.Now()), err.Error())
		return nil, err
	}
	out := make([]TargetProxyDTO, 0, len(proxies))
	for _, proxy := range proxies {
		out = append(out, TargetProxyDTO{
			ID:       proxy.ID,
			Name:     proxy.Name,
			Protocol: proxy.Protocol,
			Host:     proxy.Host,
			Port:     proxy.Port,
			Status:   strings.TrimSpace(proxy.Status),
		})
	}
	return out, nil
}

func (s *Service) ListSourceModels(ctx context.Context, in SourceModelsInput) ([]string, error) {
	if in.ChannelID == 0 {
		return nil, errors.New("source channel is required")
	}
	ch, err := s.channels.FindByID(in.ChannelID)
	if err != nil {
		return nil, err
	}
	page, err := s.channelSvc.ListAPIKeys(ctx, in.ChannelID, connector.APIKeyQuery{Page: 1, PageSize: 100})
	if err != nil {
		return nil, err
	}
	var managedKeyID int64
	if in.SyncAccountID > 0 {
		if managed, findErr := s.managedAccounts.FindByAccountID(in.SyncAccountID); findErr == nil && managed != nil {
			managedKeyID = managed.SourceAPIKeyID
		}
	}
	key := selectSourceModelKey(page.Items, managedKeyID, in.SourceGroupID, in.SourceGroupName)
	if key == nil {
		if sourceModelGroupSpecified(in.SourceGroupID, in.SourceGroupName) {
			return nil, errors.New("当前源分组没有可用 API Key，请先创建或应用同步账号")
		}
		return nil, errors.New("该渠道没有可用于获取模型的 API Key")
	}
	secret, err := s.channelSvc.RevealAPIKey(ctx, in.ChannelID, key.ID)
	if err != nil {
		return nil, err
	}
	return fetchGatewayModels(ctx, ch.SiteURL, in.Platform, secret)
}

func (s *Service) ListSyncGroups() ([]SyncGroupDTO, error) {
	list, err := s.syncGroups.List()
	if err != nil {
		return nil, err
	}
	out := make([]SyncGroupDTO, 0, len(list))
	for _, item := range list {
		ids, _ := s.syncGroups.ParseTargetGroupIDs(&item)
		accounts, err := s.syncAccounts.ListBySyncGroupID(item.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, s.toSyncGroupDTO(&item, ids, accounts))
	}
	return out, nil
}

// ReorderSyncGroups persists the display order for one upstream target. This
// applies equally to Sub2API and New API targets because the ordering belongs
// to the local synchronization-group configuration.
func (s *Service) ReorderSyncGroups(targetID uint, ids []uint) ([]SyncGroupDTO, error) {
	if err := s.requireSyncTarget(targetID); err != nil {
		return nil, err
	}
	if err := s.syncGroups.Reorder(targetID, ids); err != nil {
		return nil, err
	}
	return s.ListSyncGroups()
}

func (s *Service) CreateSyncGroup(in SyncGroupDTO) (*SyncGroupDTO, error) {
	if err := s.requireSyncTarget(in.TargetID); err != nil {
		return nil, err
	}
	accounts := accountItems(in.Accounts)
	syncMode := syncModeFromInput(in.SyncMode, accounts)
	if err := s.validateGatewaySyncAccounts(context.Background(), syncMode, accounts); err != nil {
		return nil, err
	}
	sourceGroupID := int64(0)
	sourceChannelID := uint(0)
	if len(accounts) > 0 {
		sourceChannelID = accounts[0].SourceChannelID
		if accounts[0].SourceGroupID != nil {
			sourceGroupID = *accounts[0].SourceGroupID
		}
	}
	name := renderSyncGroupName(in.NameTemplate, 0, sourceChannelID, sourceGroupID)
	displayName := strings.TrimSpace(in.DisplayName)
	if displayName == "" {
		displayName = name
	}
	sort, err := s.syncGroups.NextSort(in.TargetID)
	if err != nil {
		return nil, err
	}
	item := &storage.UpstreamSyncGroup{
		DisplayName:              displayName,
		NameTemplate:             strings.TrimSpace(in.NameTemplate),
		Name:                     name,
		TargetID:                 in.TargetID,
		TargetGroupIDsJSON:       marshalUintArray(in.TargetGroupIDs),
		SyncMode:                 syncMode,
		Platform:                 strings.TrimSpace(in.Platform),
		ModelLimitsMode:          normalizeModelLimitsMode(in.ModelLimitsMode),
		ModelLimitsText:          strings.TrimSpace(in.ModelLimits),
		PoolModeEnabled:          in.PoolModeEnabled,
		PoolModeRetryCount:       in.PoolModeRetryCount,
		PoolModeRetryStatusCodes: strings.TrimSpace(in.PoolModeRetryStatusCodes),
		CustomErrorCodesEnabled:  in.CustomErrorCodesEnabled,
		CustomErrorCodes:         strings.TrimSpace(in.CustomErrorCodes),
		RateSortDirection:        strings.TrimSpace(in.RateSortDirection),
		Sort:                     sort,
		Enabled:                  boolValue(in.Enabled, true),
	}
	if item.RateSortDirection == "" {
		item.RateSortDirection = "asc"
	}
	if item.PoolModeRetryCount == 0 {
		item.PoolModeRetryCount = 10
	}
	if err := s.syncGroups.Create(item); err != nil {
		return nil, err
	}
	// 同步分组 ID 只有入库后才确定；这里立刻回写最终名称，保证后续远端对象命名稳定。
	item.Name = renderSyncGroupName(item.NameTemplate, item.ID, sourceChannelID, sourceGroupID)
	if strings.TrimSpace(in.DisplayName) == "" {
		item.DisplayName = item.Name
	}
	item.Enabled = boolValue(in.Enabled, true)
	if err := s.syncGroups.Update(item); err != nil {
		return nil, err
	}
	if err := s.syncAccounts.SaveForGroup(item.ID, accounts); err != nil {
		return nil, err
	}
	s.clearRateScanFingerprint(item.ID)
	if err := s.ensureGatewayKeysForGroup(item, accounts); err != nil {
		return nil, err
	}
	s.notifySyncGroupChanged("新增", item, accounts)
	return s.syncGroupDTOByItem(item), nil
}

func (s *Service) UpdateSyncGroup(id uint, in SyncGroupDTO) (*SyncGroupDTO, error) {
	item, err := s.syncGroups.FindByID(id)
	if err != nil {
		return nil, err
	}
	if err := s.requireSyncTarget(in.TargetID); err != nil {
		return nil, err
	}
	existingAccounts, err := s.syncAccounts.ListBySyncGroupID(item.ID)
	if err != nil {
		return nil, err
	}
	accounts := accountItems(in.Accounts)
	syncMode := strings.TrimSpace(in.SyncMode)
	if syncMode == "" {
		syncMode = syncModeForGroup(item, existingAccounts)
	} else {
		syncMode = normalizeSyncMode(syncMode)
	}
	if err := s.validateGatewaySyncAccounts(context.Background(), syncMode, accounts); err != nil {
		return nil, err
	}
	item.TargetID = in.TargetID
	item.DisplayName = strings.TrimSpace(in.DisplayName)
	item.TargetGroupIDsJSON = marshalUintArray(in.TargetGroupIDs)
	item.SyncMode = syncMode
	item.ModelLimitsMode = normalizeModelLimitsMode(in.ModelLimitsMode)
	item.ModelLimitsText = strings.TrimSpace(in.ModelLimits)
	item.PoolModeEnabled = in.PoolModeEnabled
	item.PoolModeRetryCount = in.PoolModeRetryCount
	item.PoolModeRetryStatusCodes = strings.TrimSpace(in.PoolModeRetryStatusCodes)
	item.CustomErrorCodesEnabled = in.CustomErrorCodesEnabled
	item.CustomErrorCodes = strings.TrimSpace(in.CustomErrorCodes)
	item.RateSortDirection = strings.TrimSpace(in.RateSortDirection)
	item.Enabled = boolValue(in.Enabled, item.Enabled)
	if item.DisplayName == "" {
		item.DisplayName = item.Name
	}
	if item.RateSortDirection == "" {
		item.RateSortDirection = "asc"
	}
	if err := s.syncGroups.Update(item); err != nil {
		return nil, err
	}
	if err := s.syncAccounts.SaveForGroup(item.ID, accounts); err != nil {
		return nil, err
	}
	s.clearRateScanFingerprint(item.ID)
	if err := s.ensureGatewayKeysForGroup(item, accounts); err != nil {
		return nil, err
	}
	ids, _ := s.syncGroups.ParseTargetGroupIDs(item)
	accounts, err = s.syncAccounts.ListBySyncGroupID(item.ID)
	if err != nil {
		return nil, err
	}
	s.notifySyncGroupChanged("更新", item, accounts)
	dto := s.toSyncGroupDTO(item, ids, accounts)
	return &dto, nil
}

func (s *Service) DeleteSyncGroup(id uint) error {
	item, err := s.syncGroups.FindByID(id)
	if err != nil {
		return err
	}
	accounts, _ := s.syncAccounts.ListBySyncGroupID(id)
	if err := s.syncGroups.Delete(id); err != nil {
		return err
	}
	s.notifySyncGroupChanged("删除", item, accounts)
	return nil
}

func (s *Service) notifySyncGroupChanged(action string, item *storage.UpstreamSyncGroup, accounts []storage.UpstreamSyncAccount) {
	if s.dispatcher == nil || item == nil {
		return
	}
	targetName := fmt.Sprintf("目标 ID %d", item.TargetID)
	if target, err := s.targets.FindByID(item.TargetID); err == nil {
		targetName = target.Name
	}
	targetGroupIDs, _ := s.syncGroups.ParseTargetGroupIDs(item)
	displayName := strings.TrimSpace(item.DisplayName)
	if displayName == "" {
		displayName = item.Name
	}
	body := fmt.Sprintf(
		"动作：%s\n同步分组：%s\n同步名称：%s\n目标上游：%s\n平台：%s\n目标分组数：%d\n同步账号数：%d\n时间：%s",
		action,
		displayName,
		item.Name,
		targetName,
		item.Platform,
		len(targetGroupIDs),
		len(accounts),
		time.Now().Format("2006-01-02 15:04:05"),
	)
	if err := s.dispatcher.Dispatch(context.Background(), notify.Message{
		Event:   storage.EventUpstreamSyncGroupChanged,
		Subject: fmt.Sprintf("[同步分组变动] %s · %s", action, displayName),
		Body:    body,
		Extra: map[string]any{
			"sync_group_id": item.ID,
			"action":        action,
		},
	}); err != nil && s.log != nil {
		s.log.Warn("dispatch sync group change notification", "err", err)
	}
}

func (s *Service) notifySyncGroupApplyChanged(ctx context.Context, item *storage.UpstreamSyncGroup, target *storage.UpstreamSyncTarget, applied int, failures []string, changes []string) {
	if s.dispatcher == nil || item == nil || target == nil || (len(changes) == 0 && len(failures) == 0) {
		return
	}
	displayName := strings.TrimSpace(item.DisplayName)
	if displayName == "" {
		displayName = item.Name
	}
	details := make([]string, 0, 2)
	if len(changes) > 0 {
		details = append(details, "变动账号：\n"+strings.Join(prefixLines(changes, "- "), "\n"))
	}
	if len(failures) > 0 {
		details = append(details, "失败账号：\n"+strings.Join(prefixLines(failures, "- "), "\n"))
	}
	body := fmt.Sprintf(
		"动作：应用同步\n同步分组：%s\n同步名称：%s\n目标上游：%s\n应用账号数：%d\n变动账号数：%d\n失败账号数：%d\n时间：%s\n\n%s",
		displayName,
		item.Name,
		target.Name,
		applied,
		len(changes),
		len(failures),
		time.Now().Format("2006-01-02 15:04:05"),
		strings.Join(details, "\n\n"),
	)
	subject := fmt.Sprintf("[同步账号变动] %s · %d 项", displayName, len(changes))
	if len(failures) > 0 {
		subject = fmt.Sprintf("[同步账号异常] %s · 失败 %d / 变动 %d", displayName, len(failures), len(changes))
	}
	if err := s.dispatcher.Dispatch(ctx, notify.Message{
		Event:   storage.EventUpstreamSyncGroupChanged,
		Subject: subject,
		Body:    body,
		Extra: map[string]any{
			"sync_group_id": item.ID,
			"action":        "apply",
		},
	}); err != nil && s.log != nil {
		s.log.Warn("dispatch sync apply change notification", "err", err)
	}
}

func (s *Service) ApplySyncGroup(ctx context.Context, syncGroupID uint) (*LogDTO, error) {
	syncGroup, err := s.syncGroups.FindByID(syncGroupID)
	if err != nil {
		return nil, err
	}
	if normalized := normalizeModelLimitsMode(syncGroup.ModelLimitsMode); syncGroup.ModelLimitsMode != normalized {
		syncGroup.ModelLimitsMode = normalized
		if err := s.syncGroups.Update(syncGroup); err != nil {
			return nil, err
		}
	}
	target, err := s.targets.FindByID(syncGroup.TargetID)
	if err != nil {
		return nil, err
	}
	if normalizeTargetType(target.TargetType) == "newapi" {
		return s.applyNewAPISyncGroup(ctx, syncGroup, target)
	}
	if !target.Enabled {
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, "target disabled")
	}
	accounts, err := s.syncAccounts.ListBySyncGroupID(syncGroup.ID)
	if err != nil {
		return nil, err
	}
	if len(accounts) == 0 {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", "no sync accounts", nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, "no sync accounts")
	}
	accounts = s.sortAccountsForApply(ctx, syncGroup, accounts)
	if err := s.syncAccounts.SaveForGroup(syncGroup.ID, accounts); err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	if hasGatewaySyncAccount(accounts) {
		if _, err := s.SyncTargetGroups(ctx, target.ID); err != nil {
			_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
			return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
		}
	}
	targetGroups, selectedGroups, remoteGroupIDs, err := s.selectedTargetGroups(syncGroup)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "blocked_missing_group", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	plain, err := s.cipher.Decrypt(target.AdminAPIKeyCipher)
	if err != nil {
		return nil, err
	}
	client := sub2api.NewAdminClient()
	adminTarget := sub2api.AdminTarget{BaseURL: target.BaseURL, APIKey: plain}
	if err := client.Ping(ctx, adminTarget); err != nil {
		_ = s.targets.UpdateCheck(target.ID, "failed", ptrTime(time.Now()), err.Error())
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	gatewayRateChanges, err := s.syncGatewayTargetGroupRates(
		ctx,
		accounts,
		adminTarget,
		client,
		selectedGroups,
	)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	deletedManaged, err := s.cleanupDeletedManagedAccounts(ctx, syncGroup, accounts, adminTarget, client)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	deletedUnmanaged, err := s.cleanupUnmanagedRemoteAccounts(ctx, syncGroup, adminTarget, client)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	remoteAccounts, err := client.ListAccounts(ctx, adminTarget, 1, 1000)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	remoteBeforeByID := make(map[int64]sub2api.AdminAccount, len(remoteAccounts))
	for _, account := range remoteAccounts {
		remoteBeforeByID[account.ID] = account
	}
	applied := 0
	failures := make([]string, 0)
	successes := make([]string, 0)
	changes := append([]string(nil), gatewayRateChanges...)
	if deletedManaged+deletedUnmanaged > 0 {
		changes = append(changes, fmt.Sprintf("清理：删除失效托管账号 %d 个，重复远端账号 %d 个", deletedManaged, deletedUnmanaged))
	}
	syncedModels := make([]string, 0)
	now := time.Now()
	outcomes := s.applyAccountsConcurrently(ctx, syncGroup, accounts, adminTarget, client, targetGroups, selectedGroups, remoteGroupIDs, remoteBeforeByID, now)
	for _, outcome := range outcomes {
		if outcome.Applied {
			applied++
		}
		if outcome.Failure != "" {
			failures = append(failures, outcome.Failure)
		}
		if outcome.Success != "" {
			successes = append(successes, outcome.Success)
		}
		syncedModels = append(syncedModels, outcome.SyncedModels...)
		changes = append(changes, outcome.Changes...)
	}
	if applied == 0 {
		msg := buildApplyLogMessage(syncGroup, target, selectedGroups, applied, failures, successes, changes, deletedManaged, deletedUnmanaged)
		status := "failed"
		if strings.Contains(msg, "missing") {
			status = "blocked_missing_group"
		}
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, status, msg, nil)
		s.notifySyncGroupApplyChanged(ctx, syncGroup, target, applied, failures, changes)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, msg)
	}
	if len(failures) > 0 {
		if err := s.cacheSyncedModels(syncGroup, syncedModels); err != nil {
			failures = append(failures, err.Error())
		}
		msg := buildApplyLogMessage(syncGroup, target, selectedGroups, applied, failures, successes, changes, deletedManaged, deletedUnmanaged)
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", msg, &now)
		s.notifySyncGroupApplyChanged(ctx, syncGroup, target, applied, failures, changes)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, msg)
	}
	if err := s.cacheSyncedModels(syncGroup, syncedModels); err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), &now)
		s.notifySyncGroupApplyChanged(ctx, syncGroup, target, applied, []string{err.Error()}, changes)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	_ = s.syncGroups.UpdateStatus(syncGroup.ID, "applied", "", &now)
	msg := buildApplyLogMessage(syncGroup, target, selectedGroups, applied, failures, successes, changes, deletedManaged, deletedUnmanaged)
	if len(changes) == 0 {
		return &LogDTO{
			SyncGroupID: syncGroup.ID,
			TargetID:    target.ID,
			Action:      "apply",
			Success:     true,
			Message:     msg,
			CreatedAt:   now,
		}, nil
	}
	s.notifySyncGroupApplyChanged(ctx, syncGroup, target, applied, failures, changes)
	return s.appendLog(
		syncGroup.ID,
		target.ID,
		"apply",
		true,
		msg,
	)
}

// applyNewAPISyncGroup materializes sync accounts as New API-compatible
// channels (type 60). The local managed-account mapping stores the New API
// channel ID, allowing subsequent applies and cleanup to stay idempotent.
func (s *Service) applyNewAPISyncGroup(ctx context.Context, syncGroup *storage.UpstreamSyncGroup, target *storage.UpstreamSyncTarget) (*LogDTO, error) {
	accounts, err := s.syncAccounts.ListBySyncGroupID(syncGroup.ID)
	if err != nil {
		return nil, err
	}
	if syncModeForGroup(syncGroup, accounts) == "gateway_rate" {
		return s.applyNewAPIRateSyncGroup(ctx, syncGroup, target, accounts)
	}
	if !target.Enabled {
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, "target disabled")
	}
	if len(accounts) == 0 {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", "no sync accounts", nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, "no sync accounts")
	}
	accounts = s.sortAccountsForApply(ctx, syncGroup, accounts)
	if err := s.syncAccounts.SaveForGroup(syncGroup.ID, accounts); err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	if _, err := s.SyncTargetGroups(ctx, target.ID); err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	_, selectedGroups, _, err := s.selectedTargetGroups(syncGroup)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "blocked_missing_group", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	plain, err := s.cipher.Decrypt(target.AdminAPIKeyCipher)
	if err != nil {
		return nil, err
	}
	rateChanges := make([]string, 0)
	rateMessages := make([]string, 0)
	if hasGatewaySyncAccount(accounts) {
		messages, changes, syncErr := s.syncNewAPIGroupRates(ctx, syncGroup, target, accounts, selectedGroups, plain)
		if syncErr != nil {
			_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", syncErr.Error(), nil)
			return s.appendLog(syncGroup.ID, target.ID, "apply", false, syncErr.Error())
		}
		rateChanges = changes
		rateMessages = messages
	}
	client := newapi.NewAdminClient()
	adminTarget := newapi.AdminTarget{BaseURL: target.BaseURL, APIKey: plain}
	if err := client.Ping(ctx, adminTarget); err != nil {
		s.recordTargetCheck(target.ID, err)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	if deleted, err := s.cleanupDeletedNewAPIChannels(ctx, syncGroup, accounts, adminTarget, client); err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	} else if deleted > 0 && s.log != nil {
		s.log.Info("removed stale managed New API channels", "syncGroupID", syncGroup.ID, "count", deleted)
	}

	channels, err := client.ListAllChannels(ctx, adminTarget)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	beforeByID := make(map[int64]newapi.AdminChannel, len(channels))
	for _, channel := range channels {
		beforeByID[channel.ID] = channel
	}
	now := time.Now()
	applied := 0
	failures := make([]string, 0)
	successes := make([]string, 0, len(accounts))
	changes := append([]string(nil), rateChanges...)
	syncedModels := make([]string, 0)
	for i := range accounts {
		result, applyErr := s.applyNewAPIChannel(ctx, syncGroup, &accounts[i], selectedGroups, len(accounts), adminTarget, client, beforeByID, now)
		if applyErr != nil {
			failures = append(failures, fmt.Sprintf("同步账号%d: %s", accounts[i].Position+1, applyErr.Error()))
			continue
		}
		applied++
		successes = append(successes, result.Message)
		changes = append(changes, result.Changes...)
		syncedModels = append(syncedModels, result.SyncedModels...)
	}
	successes = append(successes, rateMessages...)
	msg := buildApplyLogMessage(syncGroup, target, selectedGroups, applied, failures, successes, changes, 0, 0)
	if applied == 0 || len(failures) > 0 {
		status := "failed"
		if strings.Contains(msg, "missing") {
			status = "blocked_missing_group"
		}
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, status, msg, &now)
		s.notifySyncGroupApplyChanged(ctx, syncGroup, target, applied, failures, changes)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, msg)
	}
	if err := s.cacheSyncedModels(syncGroup, syncedModels); err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), &now)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	_ = s.syncGroups.UpdateStatus(syncGroup.ID, "applied", "", &now)
	if len(changes) == 0 {
		return &LogDTO{SyncGroupID: syncGroup.ID, TargetID: target.ID, Action: "apply", Success: true, Message: msg, CreatedAt: now}, nil
	}
	s.notifySyncGroupApplyChanged(ctx, syncGroup, target, applied, failures, changes)
	return s.appendLog(syncGroup.ID, target.ID, "apply", true, msg)
}

// applyNewAPIRateSyncGroup updates only the selected New API groups' ratios.
// Multiplier synchronization deliberately does not create a type-60 channel.
func (s *Service) applyNewAPIRateSyncGroup(ctx context.Context, syncGroup *storage.UpstreamSyncGroup, target *storage.UpstreamSyncTarget, accounts []storage.UpstreamSyncAccount) (*LogDTO, error) {
	if !target.Enabled {
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, "target disabled")
	}
	if len(accounts) != 1 || !isGatewaySyncAccount(&accounts[0]) {
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, "New API multiplier sync requires exactly one gateway group")
	}
	if _, err := s.SyncTargetGroups(ctx, target.ID); err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	_, selectedGroups, _, err := s.selectedTargetGroups(syncGroup)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "blocked_missing_group", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	apiKey, err := s.cipher.Decrypt(target.AdminAPIKeyCipher)
	if err != nil {
		return nil, err
	}
	messages, changes, err := s.syncNewAPIGroupRates(ctx, syncGroup, target, accounts, selectedGroups, apiKey)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	now := time.Now()
	_ = s.syncGroups.UpdateStatus(syncGroup.ID, "applied", "", &now)
	msg := buildApplyLogMessage(syncGroup, target, selectedGroups, 1, nil, messages, changes, 0, 0)
	if len(changes) == 0 {
		return &LogDTO{SyncGroupID: syncGroup.ID, TargetID: target.ID, Action: "apply", Success: true, Message: msg, CreatedAt: now}, nil
	}
	s.notifySyncGroupApplyChanged(ctx, syncGroup, target, 1, nil, changes)
	return s.appendLog(syncGroup.ID, target.ID, "apply", true, msg)
}

// syncNewAPIGroupRates mirrors Sub2API's gateway-rate phase: every gateway
// source applies its calculated rate to the selected target groups before all
// sync accounts are materialized as remote channels.
func (s *Service) syncNewAPIGroupRates(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	target *storage.UpstreamSyncTarget,
	accounts []storage.UpstreamSyncAccount,
	selected []storage.UpstreamSyncTargetGroup,
	apiKey string,
) ([]string, []string, error) {
	settings, err := s.getNewAPIGroupSettings(ctx, target.BaseURL, apiKey)
	if err != nil {
		return nil, nil, err
	}
	changes := make([]string, 0)
	messages := make([]string, 0)
	for i := range accounts {
		account := &accounts[i]
		if !isGatewaySyncAccount(account) {
			continue
		}
		rate, rateErr := s.gatewayRateMultiplierForAccount(ctx, account)
		if rateErr != nil {
			return nil, nil, rateErr
		}
		messages = append(messages, fmt.Sprintf("账号%d 倍率同步 %s", account.Position+1, formatNumber(rate)))
		for _, group := range selected {
			old, ok := settings.GroupRatios[group.Name]
			if !ok {
				return nil, nil, fmt.Errorf("New API 分组缺失: %s", group.Name)
			}
			if nearlyEqualRate(old, rate) {
				continue
			}
			settings.GroupRatios[group.Name] = rate
			changes = append(changes, fmt.Sprintf("目标分组 %s 倍率 %s -> %s", group.Name, formatNumber(old), formatNumber(rate)))
		}
	}
	if len(changes) > 0 {
		groupRatios, err := marshalNewAPIGroupRatios(settings.GroupRatios, settings.GroupOrder)
		if err != nil {
			return nil, nil, err
		}
		// Multiplier synchronization owns only GroupRatio. Keep the other
		// administrator options untouched, including AutoGroups and labels.
		if err := s.putNewAPIOption(ctx, target.BaseURL, apiKey, "GroupRatio", string(groupRatios)); err != nil {
			return nil, nil, err
		}
		if _, err := s.SyncTargetGroups(ctx, target.ID); err != nil {
			return nil, nil, errors.New("New API multiplier was updated remotely, but local group cache refresh failed: " + err.Error())
		}
	}
	return messages, changes, nil
}

func (s *Service) applyNewAPIChannel(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	totalAccounts int,
	adminTarget newapi.AdminTarget,
	client *newapi.AdminClient,
	beforeByID map[int64]newapi.AdminChannel,
	now time.Time,
) (*accountApplyResult, error) {
	if syncAccount == nil {
		return nil, errors.New("sync account is required")
	}
	if !isGatewaySyncAccount(syncAccount) && syncAccount.SourceGroupID == nil && strings.TrimSpace(syncAccount.SourceGroupName) == "" {
		return nil, errors.New("source group not bound")
	}
	keyName := sourceAPIKeyName(syncGroup)
	var (
		key           *connector.APIKey
		secret        string
		sourceName    string
		sourceID      uint
		sourceBaseURL string
		models        []string
		err           error
	)
	if isGatewaySyncAccount(syncAccount) {
		if syncAccount.GatewayGroupID == nil || *syncAccount.GatewayGroupID == 0 {
			return nil, errors.New("gateway group is required")
		}
		gatewayGroup, groupErr := s.gateway.GetGroup(*syncAccount.GatewayGroupID)
		if groupErr != nil {
			return nil, fmt.Errorf("gateway group missing: %d", *syncAccount.GatewayGroupID)
		}
		key, secret, err = s.ensureGatewayKey(syncGroup, syncAccount, keyName)
		if err != nil {
			return nil, err
		}
		sourceName = "网关 " + gatewayGroup.Name
		sourceID = gatewayGroup.ID
		sourceBaseURL = s.gatewayBaseURL
		if syncGroup.ModelLimitsMode == "sync_upstream" {
			models = gatewaySyncModels(s.gateway, gatewayGroup)
		}
	} else {
		ch, channelErr := s.channels.FindByID(syncAccount.SourceChannelID)
		if channelErr != nil {
			return nil, fmt.Errorf("source channel missing: %d", syncAccount.SourceChannelID)
		}
		if _, err := s.checkSourceGroup(ctx, syncAccount); err != nil {
			return nil, err
		}
		key, secret, err = s.ensureSourceAPIKey(ctx, syncGroup, syncAccount, keyName)
		if err != nil {
			return nil, err
		}
		sourceName = ch.Name
		sourceID = ch.ID
		sourceBaseURL = ch.SiteURL
	}
	if syncGroup.ModelLimitsMode != "sync_upstream" {
		models = splitList(syncGroup.ModelLimitsText)
	}
	if syncGroup.ModelLimitsMode == "sync_upstream" && !isGatewaySyncAccount(syncAccount) {
		models, err = fetchGatewayModels(ctx, sourceBaseURL, syncGroup.Platform, secret)
		if err != nil {
			return nil, err
		}
		if len(models) == 0 {
			return nil, errors.New("synced upstream models is empty")
		}
	}
	models = uniqueStrings(models)
	if len(models) == 0 {
		return nil, errors.New("channel models are empty")
	}
	channelName := newAPIChannelName(syncGroup, syncAccount, totalAccounts)
	channelReq := newapi.AdminChannel{
		Type:     newapi.ChannelTypeNewAPI,
		Key:      secret,
		Status:   newapi.ChannelStatusOn,
		Name:     channelName,
		Weight:   uint(positiveOrDefault(syncAccount.Weight, 1)),
		BaseURL:  sourceBaseURL,
		Models:   strings.Join(models, ","),
		Group:    strings.Join(newAPITargetGroupNames(selectedGroups), ","),
		Priority: newAPIChannelPriority(syncAccount, totalAccounts),
		Remark:   syncedAccountNotes(now),
	}
	channelReq = newAPIChannelDefaults(channelReq, nil)
	if !syncAccount.Enabled {
		channelReq.Status = newapi.ChannelStatusOff
		channelReq.Remark = disabledManagedAccountDescription("同步账号已禁用", now)
	}
	var previous *newapi.AdminChannel
	var managed *storage.UpstreamSyncManagedAccount
	action := "创建"
	if found, findErr := s.managedAccounts.FindByAccountID(syncAccount.ID); findErr == nil && found != nil {
		managed = found
		action = "更新"
		if before, ok := beforeByID[managed.TargetAccountID]; ok {
			previous = &before
			channelReq = newAPIChannelDefaults(channelReq, previous)
			channelReq.ID = before.ID
			if err := client.UpdateChannel(ctx, adminTarget, channelReq); err == nil {
				if before.Status != channelReq.Status {
					if err := client.SetChannelStatus(ctx, adminTarget, before.ID, channelReq.Status); err != nil {
						return nil, err
					}
				}
			} else if !isHTTPNotFound(err) {
				return nil, err
			} else {
				managed = nil
				action = "重建"
			}
		} else {
			managed = nil
			action = "重建"
		}
	}
	if managed == nil {
		existing, err := client.FindChannelByName(ctx, adminTarget, channelName)
		if err != nil {
			return nil, err
		}
		if existing == nil {
			baseName := managedObjectBaseName(syncGroup, syncAccount)
			if channelName != baseName {
				existing, err = client.FindChannelByName(ctx, adminTarget, baseName)
				if err != nil {
					return nil, err
				}
			}
		}
		if existing != nil {
			previous = existing
			channelReq = newAPIChannelDefaults(channelReq, previous)
			channelReq.ID = existing.ID
			action = "复用更新"
			if err := client.UpdateChannel(ctx, adminTarget, channelReq); err != nil {
				return nil, err
			}
			if existing.Status != channelReq.Status {
				if err := client.SetChannelStatus(ctx, adminTarget, existing.ID, channelReq.Status); err != nil {
					return nil, err
				}
			}
			managed = &storage.UpstreamSyncManagedAccount{TargetAccountID: existing.ID}
		} else {
			if err := client.CreateChannel(ctx, adminTarget, channelReq); err != nil {
				return nil, err
			}
			created, err := client.FindChannelByName(ctx, adminTarget, channelName)
			if err != nil {
				return nil, err
			}
			if created == nil {
				return nil, errors.New("New API created channel was not found")
			}
			if channelReq.Status != newapi.ChannelStatusOn {
				if err := client.SetChannelStatus(ctx, adminTarget, created.ID, channelReq.Status); err != nil {
					return nil, err
				}
			}
			managed = &storage.UpstreamSyncManagedAccount{TargetAccountID: created.ID}
		}
	}
	if err := s.managedAccounts.Upsert(&storage.UpstreamSyncManagedAccount{
		SyncGroupID:        syncGroup.ID,
		SyncAccountID:      syncAccount.ID,
		SourceAPIKeyID:     key.ID,
		SourceAPIKeyName:   keyName,
		TargetAccountID:    managed.TargetAccountID,
		TargetAccountName:  channelName,
		TargetGroupIDsJSON: marshalUintArray(groupIDs(selectedGroups)),
		LastAppliedAt:      &now,
	}); err != nil {
		return nil, err
	}
	message := fmt.Sprintf("账号%d：%s New API 渠道 %s(ID %d)，源渠道 %s(ID %d)，模型 %d 个", syncAccount.Position+1, action, channelName, managed.TargetAccountID, sourceName, sourceID, len(models))
	return &accountApplyResult{
		SyncedModels: models,
		Message:      message,
		Changes:      newAPIChannelChangeDetails(syncAccount, previous, channelReq, managed.TargetAccountID),
	}, nil
}

func targetGroupNameList(groups []storage.UpstreamSyncTargetGroup) []string {
	names := make([]string, 0, len(groups))
	for _, group := range groups {
		if name := strings.TrimSpace(group.Name); name != "" {
			names = append(names, name)
		}
	}
	return names
}

func newAPITargetGroupNames(groups []storage.UpstreamSyncTargetGroup) []string {
	names := uniqueStrings(targetGroupNameList(groups))
	if len(names) == 0 {
		return []string{"default"}
	}
	return names
}

func newAPIChannelPriority(account *storage.UpstreamSyncAccount, total int) int64 {
	if account != nil && account.Priority > 0 {
		return account.Priority
	}
	if total <= 0 {
		total = 1
	}
	position := 0
	if account != nil && account.Position >= 0 {
		position = account.Position
	}
	priority := total - position
	if priority < 1 {
		priority = 1
	}
	return int64(priority)
}

func newAPIChannelName(syncGroup *storage.UpstreamSyncGroup, account *storage.UpstreamSyncAccount, total int) string {
	if syncGroup == nil {
		return ""
	}
	base := strings.TrimSpace(syncGroup.Name)
	if base == "" {
		base = strings.TrimSpace(syncGroup.NameTemplate)
	}
	if total > 1 && account != nil && account.Position > 0 {
		return fmt.Sprintf("%s-%d", base, account.Position+1)
	}
	return base
}

func newAPIChannelDefaults(next newapi.AdminChannel, previous *newapi.AdminChannel) newapi.AdminChannel {
	if previous != nil {
		if next.OpenAIOrganization == "" {
			next.OpenAIOrganization = previous.OpenAIOrganization
		}
		if next.TestModel == "" {
			next.TestModel = previous.TestModel
		}
		if next.Other == "" {
			next.Other = previous.Other
		}
		if next.OtherInfo == "" {
			next.OtherInfo = previous.OtherInfo
		}
		if next.StatusCodeMapping == "" {
			next.StatusCodeMapping = previous.StatusCodeMapping
		}
		if next.Tag == "" {
			next.Tag = previous.Tag
		}
		if next.ParamOverride == "" {
			next.ParamOverride = previous.ParamOverride
		}
		if next.HeaderOverride == "" {
			next.HeaderOverride = previous.HeaderOverride
		}
		if next.ChannelInfo == nil {
			next.ChannelInfo = previous.ChannelInfo
		}
		if next.ModelMapping == "" {
			next.ModelMapping = preserveNewAPIModelMapping(previous.ModelMapping)
		}
	}
	setting := map[string]any{}
	if previous != nil && strings.TrimSpace(previous.Setting) != "" {
		var inherited map[string]any
		if json.Unmarshal([]byte(previous.Setting), &inherited) == nil {
			for key, value := range inherited {
				setting[key] = value
			}
		}
	}
	if strings.TrimSpace(next.Setting) != "" {
		var requested map[string]any
		if json.Unmarshal([]byte(next.Setting), &requested) == nil {
			for key, value := range requested {
				setting[key] = value
			}
		}
	}
	setting["pass_through_body_enabled"] = true
	if raw, err := json.Marshal(setting); err == nil {
		next.Setting = string(raw)
	}
	other := map[string]any{}
	if previous != nil && strings.TrimSpace(previous.OtherSettings) != "" {
		var inherited map[string]any
		if json.Unmarshal([]byte(previous.OtherSettings), &inherited) == nil {
			for key, value := range inherited {
				other[key] = value
			}
		}
	}
	if strings.TrimSpace(next.OtherSettings) != "" {
		var requested map[string]any
		if json.Unmarshal([]byte(next.OtherSettings), &requested) == nil {
			for key, value := range requested {
				other[key] = value
			}
		}
	}
	other["allow_service_tier"] = true
	other["allow_safety_identifier"] = true
	other["upstream_model_update_check_enabled"] = true
	other["upstream_model_update_auto_sync_enabled"] = true
	if raw, err := json.Marshal(other); err == nil {
		next.OtherSettings = string(raw)
	}
	next.AutoBan = 0
	return next
}

// preserveNewAPIModelMapping keeps only meaningful remote model aliases. The
// synchronizer owns the channel model list, but must not erase an operator's
// real alias mapping. Identity entries generated by older versions are no-op
// noise and are intentionally dropped.
func preserveNewAPIModelMapping(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		return ""
	}
	var mapping map[string]string
	if err := json.Unmarshal([]byte(raw), &mapping); err != nil {
		return raw
	}
	meaningful := make(map[string]string, len(mapping))
	for source, target := range mapping {
		source = strings.TrimSpace(source)
		target = strings.TrimSpace(target)
		if source == "" || target == "" || source == target {
			continue
		}
		meaningful[source] = target
	}
	if len(meaningful) == 0 {
		return ""
	}
	encoded, err := json.Marshal(meaningful)
	if err != nil {
		return raw
	}
	return string(encoded)
}

func newAPIChannelChangeDetails(syncAccount *storage.UpstreamSyncAccount, previous *newapi.AdminChannel, next newapi.AdminChannel, id int64) []string {
	prefix := fmt.Sprintf("账号%d：%s(ID %d)", syncAccount.Position+1, next.Name, id)
	if previous == nil {
		return []string{prefix + " 新增"}
	}
	parts := make([]string, 0)
	if previous.Group != next.Group {
		parts = append(parts, fmt.Sprintf("目标分组 %s -> %s", previous.Group, next.Group))
	}
	if previous.Models != next.Models {
		parts = append(parts, "模型已更新")
	}
	if previous.Priority != next.Priority {
		parts = append(parts, fmt.Sprintf("优先级 %d -> %d", previous.Priority, next.Priority))
	}
	if previous.Weight != next.Weight {
		parts = append(parts, fmt.Sprintf("权重 %d -> %d", previous.Weight, next.Weight))
	}
	if previous.Status != next.Status {
		parts = append(parts, fmt.Sprintf("状态 %d -> %d", previous.Status, next.Status))
	}
	if len(parts) == 0 {
		return nil
	}
	return []string{prefix + " " + strings.Join(parts, "，")}
}

func (s *Service) sortAccountsForApply(ctx context.Context, syncGroup *storage.UpstreamSyncGroup, accounts []storage.UpstreamSyncAccount) []storage.UpstreamSyncAccount {
	groupsByChannel := make(map[uint][]connector.APIKeyGroup)
	for _, account := range accounts {
		if account.SourceChannelID == 0 {
			continue
		}
		if _, ok := groupsByChannel[account.SourceChannelID]; ok {
			continue
		}
		groups, err := s.channelSvc.ListAPIKeyGroups(ctx, account.SourceChannelID)
		if err != nil {
			groups = nil
		}
		groupsByChannel[account.SourceChannelID] = groups
	}
	fixed := make(map[int]storage.UpstreamSyncAccount)
	sortable := make([]storage.UpstreamSyncAccount, 0, len(accounts))
	for _, account := range accounts {
		if !account.Enabled || sourceGroupMissingForSort(&account, groupsByChannel[account.SourceChannelID]) {
			pos := account.Position
			if mapped, err := s.managedAccounts.FindByAccountID(account.ID); err == nil && mapped != nil {
				if mappedPos, ok := managedAccountPosition(syncGroup, mapped.TargetAccountName); ok && mappedPos >= 0 && mappedPos < len(accounts) {
					pos = mappedPos
				}
			}
			for {
				if _, exists := fixed[pos]; !exists {
					break
				}
				pos++
			}
			fixed[pos] = account
			continue
		}
		sortable = append(sortable, account)
	}
	direction := 1.0
	if strings.EqualFold(syncGroup.RateSortDirection, "desc") {
		direction = -1
	}
	sort.SliceStable(sortable, func(i, j int) bool {
		leftRate := rateMultiplierForAccount(&sortable[i], groupsByChannel[sortable[i].SourceChannelID])
		rightRate := rateMultiplierForAccount(&sortable[j], groupsByChannel[sortable[j].SourceChannelID])
		if leftRate != rightRate {
			return (leftRate-rightRate)*direction < 0
		}
		if sortable[i].Weight != sortable[j].Weight {
			return sortable[i].Weight > sortable[j].Weight
		}
		return sortable[i].Position < sortable[j].Position
	})
	out := make([]storage.UpstreamSyncAccount, 0, len(accounts))
	sortableIndex := 0
	for pos := 0; len(out) < len(accounts); pos++ {
		if account, ok := fixed[pos]; ok {
			account.Position = len(out)
			out = append(out, account)
			continue
		}
		if sortableIndex >= len(sortable) {
			break
		}
		account := sortable[sortableIndex]
		sortableIndex++
		account.Position = len(out)
		out = append(out, account)
	}
	for i := range out {
		out[i].Position = i
	}
	return out
}

func sourceGroupMissingForSort(account *storage.UpstreamSyncAccount, groups []connector.APIKeyGroup) bool {
	sourceGroupName := strings.TrimSpace(account.SourceGroupName)
	if account.SourceGroupID == nil && sourceGroupName == "" {
		return true
	}
	for _, group := range groups {
		if account.SourceGroupID != nil && group.ID != nil && *group.ID == *account.SourceGroupID {
			return false
		}
		if sourceGroupName != "" && strings.EqualFold(group.Name, sourceGroupName) {
			return false
		}
	}
	return true
}

func (s *Service) applyAccountsConcurrently(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	accounts []storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	targetGroups []storage.UpstreamSyncTargetGroup,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	remoteGroupIDs []int64,
	remoteBeforeByID map[int64]sub2api.AdminAccount,
	now time.Time,
) []syncAccountApplyOutcome {
	// 同一源渠道会复用同一个源 Key，必须按账号顺序处理；不同源渠道才并发。
	indexesBySourceChannel := make(map[uint][]int)
	sourceChannelIDs := make([]uint, 0)
	for i, account := range accounts {
		if _, ok := indexesBySourceChannel[account.SourceChannelID]; !ok {
			sourceChannelIDs = append(sourceChannelIDs, account.SourceChannelID)
		}
		indexesBySourceChannel[account.SourceChannelID] = append(indexesBySourceChannel[account.SourceChannelID], i)
	}
	workerCount := len(sourceChannelIDs)
	if workerCount > applyAccountWorkerLimit {
		workerCount = applyAccountWorkerLimit
	}
	if workerCount <= 0 {
		return nil
	}
	jobs := make(chan uint)
	results := make(chan syncAccountApplyOutcome, len(accounts))
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sourceChannelID := range jobs {
				for _, index := range indexesBySourceChannel[sourceChannelID] {
					account := accounts[index]
					outcome := s.applyAccountWithCleanup(ctx, syncGroup, &account, adminTarget, client, targetGroups, selectedGroups, remoteGroupIDs, remoteBeforeByID, now)
					outcome.Index = index
					results <- outcome
				}
			}
		}()
	}
	for _, sourceChannelID := range sourceChannelIDs {
		jobs <- sourceChannelID
	}
	close(jobs)
	wg.Wait()
	close(results)

	ordered := make([]syncAccountApplyOutcome, len(accounts))
	for outcome := range results {
		ordered[outcome.Index] = outcome
	}
	return ordered
}

func (s *Service) applyAccountWithCleanup(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	account *storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	targetGroups []storage.UpstreamSyncTargetGroup,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	remoteGroupIDs []int64,
	remoteBeforeByID map[int64]sub2api.AdminAccount,
	now time.Time,
) syncAccountApplyOutcome {
	if !account.Enabled {
		change, err := s.disableManagedTargetForSkippedAccount(ctx, syncGroup, account, adminTarget, client, remoteBeforeByID, now, "同步账号已禁用")
		if err != nil {
			msg := fmt.Sprintf("同步账号%d: disable managed target: %s", account.Position+1, err.Error())
			return syncAccountApplyOutcome{Failure: msg}
		}
		return syncAccountApplyOutcome{Changes: singleChange(change)}
	}
	if !isGatewaySyncAccount(account) && account.SourceGroupID == nil && strings.TrimSpace(account.SourceGroupName) == "" {
		msg := fmt.Sprintf("同步账号%d: source group not bound", account.Position+1)
		change, err := s.ensureDisabledPlaceholderTargetForAccount(ctx, syncGroup, account, adminTarget, client, selectedGroups, remoteGroupIDs, remoteBeforeByID, now, "源分组未绑定")
		if err != nil {
			msg = msg + "; create disabled placeholder: " + err.Error()
		}
		return syncAccountApplyOutcome{Failure: msg, Changes: singleChange(change)}
	}
	result, err := s.applyAccount(ctx, syncGroup, account, adminTarget, client, targetGroups, selectedGroups, remoteGroupIDs, remoteBeforeByID, now)
	if err != nil {
		msg := fmt.Sprintf("同步账号%d: %s", account.Position+1, err.Error())
		changes := changesFromApplyError(err)
		if shouldCreateDisabledPlaceholderOnApplyError(err) {
			change, placeholderErr := s.ensureDisabledPlaceholderTargetForAccount(ctx, syncGroup, account, adminTarget, client, selectedGroups, remoteGroupIDs, remoteBeforeByID, now, err.Error())
			if placeholderErr != nil {
				msg = msg + "; create disabled placeholder: " + placeholderErr.Error()
			}
			changes = append(changes, singleChange(change)...)
		} else if shouldDisableManagedTargetOnApplyError(err) {
			change, disableErr := s.disableManagedTargetForSkippedAccount(ctx, syncGroup, account, adminTarget, client, remoteBeforeByID, now, err.Error())
			if disableErr != nil {
				msg = msg + "; disable managed target: " + disableErr.Error()
			}
			changes = append(changes, singleChange(change)...)
		}
		return syncAccountApplyOutcome{Failure: msg, Changes: changes}
	}
	return syncAccountApplyOutcome{
		Applied:      true,
		Success:      result.Message,
		Changes:      result.Changes,
		SyncedModels: result.SyncedModels,
	}
}

func (s *Service) applyAccount(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	targetGroups []storage.UpstreamSyncTargetGroup,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	remoteGroupIDs []int64,
	remoteBeforeByID map[int64]sub2api.AdminAccount,
	now time.Time,
) (*accountApplyResult, error) {
	var (
		sourceGroups   []connector.APIKeyGroup
		key            *connector.APIKey
		secret         string
		sourceName     string
		sourceID       uint
		sourceBaseURL  string
		gatewayModels  []string
		rateMultiplier float64
		err            error
	)
	keyName := sourceAPIKeyName(syncGroup)
	if isGatewaySyncAccount(syncAccount) {
		if syncAccount.GatewayGroupID == nil || *syncAccount.GatewayGroupID == 0 {
			return nil, errors.New("gateway group is required")
		}
		gatewayGroup, groupErr := s.gateway.GetGroup(*syncAccount.GatewayGroupID)
		if groupErr != nil {
			return nil, fmt.Errorf("gateway group missing: %d", *syncAccount.GatewayGroupID)
		}
		gatewayModels = gatewaySyncModels(s.gateway, gatewayGroup)
		key, secret, err = s.ensureGatewayKey(syncGroup, syncAccount, keyName)
		if err != nil {
			return nil, err
		}
		rateMultiplier = 1
		sourceName = "网关 " + gatewayGroup.Name
		sourceID = gatewayGroup.ID
		sourceBaseURL = s.gatewayBaseURL
	} else {
		ch, channelErr := s.channels.FindByID(syncAccount.SourceChannelID)
		if channelErr != nil {
			return nil, fmt.Errorf("source channel missing: %d", syncAccount.SourceChannelID)
		}
		sourceGroups, err = s.checkSourceGroup(ctx, syncAccount)
		if err != nil {
			return nil, err
		}
		key, secret, err = s.ensureSourceAPIKey(ctx, syncGroup, syncAccount, keyName)
		if err != nil {
			return nil, err
		}
		rateMultiplier = rateMultiplierForAccount(syncAccount, sourceGroups)
		sourceName = ch.Name
		sourceID = ch.ID
		sourceBaseURL = ch.SiteURL
	}
	accountBaseName := managedObjectBaseName(syncGroup, syncAccount)
	accountName := managedObjectNameForSource(syncGroup, syncAccount, sourceName)
	accountReq := s.buildAdminAccountWithBaseURL(
		syncGroup,
		syncAccount,
		sourceBaseURL,
		secret,
		remoteGroupIDs,
		syncAccount.Position+1,
		rateMultiplier,
	)
	if isGatewaySyncAccount(syncAccount) && syncGroup.ModelLimitsMode == "sync_upstream" && len(gatewayModels) > 0 {
		accountReq.Credentials["model_mapping"] = modelMappingFromModels(gatewayModels)
	}
	accountReq.Name = accountName
	accountReq.Notes = syncedAccountNotes(now)
	var account *sub2api.AdminAccount
	var mapped *storage.UpstreamSyncManagedAccount
	var previous *sub2api.AdminAccount
	action := "创建"
	if found, err := s.managedAccounts.FindByAccountID(syncAccount.ID); err == nil && found != nil {
		mapped = found
		action = "更新"
		if before, ok := remoteBeforeByID[mapped.TargetAccountID]; ok {
			previous = &before
		}
		account, err = client.UpdateAccount(ctx, adminTarget, mapped.TargetAccountID, accountReq)
		if err != nil && !isHTTPNotFound(err) {
			return nil, err
		}
		if err != nil && isHTTPNotFound(err) {
			action = "重建"
		}
	}
	if account == nil {
		existing, err := client.FindAccountByName(ctx, adminTarget, accountName)
		if err != nil {
			return nil, err
		}
		if existing == nil && accountName != accountBaseName {
			existing, err = client.FindAccountByName(ctx, adminTarget, accountBaseName)
			if err != nil {
				return nil, err
			}
		}
		if existing != nil {
			before := *existing
			previous = &before
			if action == "创建" {
				action = "复用更新"
			} else {
				action = "重建更新"
			}
			account, err = client.UpdateAccount(ctx, adminTarget, existing.ID, accountReq)
		} else {
			account, err = client.CreateAccount(ctx, adminTarget, accountReq)
		}
		if err != nil {
			return nil, err
		}
	}
	syncedModels := []string(nil)
	if syncGroup.ModelLimitsMode == "sync_upstream" && isGatewaySyncAccount(syncAccount) {
		syncedModels = gatewayModels
	} else if syncGroup.ModelLimitsMode == "sync_upstream" {
		models, err := client.SyncAccountModelsFromUpstream(ctx, adminTarget, account.ID)
		if err != nil {
			change, _ := s.disableManagedTargetAfterApplyFailure(ctx, syncGroup, syncAccount, adminTarget, client, account, accountName, selectedGroups, key, keyName, now, err.Error())
			return nil, errorWithChanges(err, change)
		}
		if len(models) == 0 {
			err := errors.New("synced upstream models is empty")
			change, _ := s.disableManagedTargetAfterApplyFailure(ctx, syncGroup, syncAccount, adminTarget, client, account, accountName, selectedGroups, key, keyName, now, err.Error())
			return nil, errorWithChanges(err, change)
		}
		syncedModels = models
		accountReq.Credentials["model_mapping"] = modelMappingFromModels(models)
		account, err = client.UpdateAccount(ctx, adminTarget, account.ID, accountReq)
		if err != nil {
			change, _ := s.disableManagedTargetAfterApplyFailure(ctx, syncGroup, syncAccount, adminTarget, client, account, accountName, selectedGroups, key, keyName, now, err.Error())
			return nil, errorWithChanges(err, change)
		}
	}
	if err := s.syncRemoteAccountSchedulable(ctx, adminTarget, client, account); err != nil {
		return nil, err
	}
	if err := s.managedAccounts.Upsert(&storage.UpstreamSyncManagedAccount{
		SyncGroupID:        syncGroup.ID,
		SyncAccountID:      syncAccount.ID,
		SourceAPIKeyID:     key.ID,
		SourceAPIKeyName:   keyName,
		TargetAccountID:    account.ID,
		TargetAccountName:  accountName,
		TargetGroupIDsJSON: marshalUintArray(groupIDs(selectedGroups)),
		LastAppliedAt:      &now,
	}); err != nil {
		return nil, err
	}
	testMessage, testChange, err := s.testManagedTargetAccount(ctx, adminTarget, client, syncAccount, account)
	if err != nil {
		return nil, err
	}
	sourceGroupLabelText := sourceGroupLabel(syncAccount.SourceGroupID, syncAccount.SourceGroupName, sourceGroups)
	if isGatewaySyncAccount(syncAccount) && syncAccount.GatewayGroupID != nil {
		sourceGroupLabelText = fmt.Sprintf("网关组 ID %d", *syncAccount.GatewayGroupID)
	}
	msg := fmt.Sprintf(
		"账号%d：%s远端账号 %s(ID %d)，源渠道 %s(ID %d)，源分组 %s，倍率 %s，权重 %d，并发 %d",
		syncAccount.Position+1,
		action,
		accountName,
		account.ID,
		sourceName,
		sourceID,
		sourceGroupLabelText,
		formatNumber(accountReq.RateMultiplier),
		syncAccount.Weight,
		positiveOrDefault(syncAccount.Concurrency, 10),
	)
	if syncAccount.ProxyID != nil {
		msg += fmt.Sprintf("，代理 ID %d", *syncAccount.ProxyID)
	}
	if syncGroup.ModelLimitsMode == "sync_upstream" && len(syncedModels) > 0 {
		msg += fmt.Sprintf("，同步模型 %d 个", len(syncedModels))
	}
	if testMessage != "" {
		msg += "，" + testMessage
	}
	return &accountApplyResult{
		SyncedModels: syncedModels,
		Message:      msg,
		Changes:      append(accountChangeDetails(syncAccount, previous, accountReq, account.ID), singleChange(testChange)...),
	}, nil
}

func (s *Service) testManagedTargetAccount(
	ctx context.Context,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	syncAccount *storage.UpstreamSyncAccount,
	account *sub2api.AdminAccount,
) (string, string, error) {
	if syncAccount == nil || account == nil || !syncAccount.TestEnabled {
		return "", "", nil
	}
	model := strings.TrimSpace(syncAccount.TestModel)
	if model == "" {
		models, err := client.ListAccountModels(ctx, adminTarget, account.ID)
		if err != nil {
			change := testRemoteAccountChange(syncAccount, account.Name, account.ID, "", false, err.Error())
			if _, setErr := client.SetAccountSchedulable(ctx, adminTarget, account.ID, false); setErr != nil {
				return "", "", setErr
			}
			return fmt.Sprintf("测试失败，调度已禁用：%s", err.Error()), change, nil
		}
		if len(models) == 0 {
			err := errors.New("account models is empty")
			change := testRemoteAccountChange(syncAccount, account.Name, account.ID, "", false, err.Error())
			if _, setErr := client.SetAccountSchedulable(ctx, adminTarget, account.ID, false); setErr != nil {
				return "", "", setErr
			}
			return fmt.Sprintf("测试失败，调度已禁用：%s", err.Error()), change, nil
		}
		model = models[0]
	}
	if _, err := client.TestAccount(ctx, adminTarget, account.ID, model); err != nil {
		change := testRemoteAccountChange(syncAccount, account.Name, account.ID, model, false, err.Error())
		if _, setErr := client.SetAccountSchedulable(ctx, adminTarget, account.ID, false); setErr != nil {
			return "", "", setErr
		}
		return fmt.Sprintf("测试模型 %s 失败，调度已禁用：%s", model, err.Error()), change, nil
	}
	if _, err := client.SetAccountSchedulable(ctx, adminTarget, account.ID, true); err != nil {
		return "", "", err
	}
	change := ""
	if !account.Schedulable {
		change = testRemoteAccountChange(syncAccount, account.Name, account.ID, model, true, "")
	}
	return fmt.Sprintf("测试模型 %s 通过，调度已启用", model), change, nil
}

func (s *Service) ensureDisabledPlaceholderTargetForAccount(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	remoteGroupIDs []int64,
	remoteBeforeByID map[int64]sub2api.AdminAccount,
	now time.Time,
	reason string,
) (string, error) {
	ch, err := s.channels.FindByID(syncAccount.SourceChannelID)
	if err != nil {
		return "", err
	}
	accountName := managedObjectName(syncGroup, syncAccount, ch)
	accountBaseName := managedObjectBaseName(syncGroup, syncAccount)
	accountReq := s.buildAdminAccount(
		syncGroup,
		syncAccount,
		ch,
		"1234",
		remoteGroupIDs,
		syncAccount.Position+1,
		rateMultiplierForAccount(syncAccount, nil),
	)
	accountReq.Name = accountName
	accountReq.Status = "inactive"
	disabledDescription := disabledManagedAccountDescription(reason, now)
	accountReq.Notes = disabledDescription

	var account *sub2api.AdminAccount
	if mapped, err := s.managedAccounts.FindByAccountID(syncAccount.ID); err == nil && mapped != nil {
		if before, ok := remoteBeforeByID[mapped.TargetAccountID]; ok {
			account, err = client.UpdateAccount(ctx, adminTarget, before.ID, accountReq)
			if err != nil && !isHTTPNotFound(err) {
				return "", err
			}
			if err == nil {
				if err := s.syncRemoteAccountSchedulable(ctx, adminTarget, client, account); err != nil {
					return "", err
				}
				return s.upsertDisabledPlaceholderMapping(syncGroup, syncAccount, selectedGroups, account, accountName, now, reason)
			}
		}
	}
	existing, err := client.FindAccountByName(ctx, adminTarget, accountName)
	if err != nil {
		return "", err
	}
	if existing == nil && accountName != accountBaseName {
		existing, err = client.FindAccountByName(ctx, adminTarget, accountBaseName)
		if err != nil {
			return "", err
		}
	}
	if existing != nil {
		account, err = client.UpdateAccount(ctx, adminTarget, existing.ID, accountReq)
	} else {
		account, err = client.CreateAccount(ctx, adminTarget, accountReq)
	}
	if err != nil {
		return "", err
	}
	if strings.EqualFold(strings.TrimSpace(account.Status), "active") {
		if err := s.disableRemoteAccount(ctx, adminTarget, client, *account, now, reason); err != nil {
			return "", err
		}
		account.Status = "inactive"
	} else if err := s.syncRemoteAccountSchedulable(ctx, adminTarget, client, account); err != nil {
		return "", err
	}
	return s.upsertDisabledPlaceholderMapping(syncGroup, syncAccount, selectedGroups, account, accountName, now, reason)
}

func (s *Service) upsertDisabledPlaceholderMapping(
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	account *sub2api.AdminAccount,
	accountName string,
	now time.Time,
	reason string,
) (string, error) {
	if account == nil {
		return "", nil
	}
	if err := s.managedAccounts.Upsert(&storage.UpstreamSyncManagedAccount{
		SyncGroupID:        syncGroup.ID,
		SyncAccountID:      syncAccount.ID,
		SourceAPIKeyID:     0,
		SourceAPIKeyName:   "",
		TargetAccountID:    account.ID,
		TargetAccountName:  accountName,
		TargetGroupIDsJSON: marshalUintArray(groupIDs(selectedGroups)),
		LastAppliedAt:      &now,
	}); err != nil {
		return "", err
	}
	return disabledRemoteAccountChange(syncAccount, accountName, account.ID, reason), nil
}

func (s *Service) disableManagedTargetForSkippedAccount(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	remoteBeforeByID map[int64]sub2api.AdminAccount,
	now time.Time,
	reason string,
) (string, error) {
	mapped, err := s.managedAccounts.FindByAccountID(syncAccount.ID)
	if err != nil {
		return "", nil
	}
	if !isManagedAccountName(syncGroup, mapped.TargetAccountName) {
		return "", nil
	}
	account, ok := remoteBeforeByID[mapped.TargetAccountID]
	if !ok {
		return "", nil
	}
	if isRemoteAccountDisabledForReason(account, reason) {
		return "", nil
	}
	if err := s.disableRemoteAccount(ctx, adminTarget, client, account, now, reason); err != nil {
		return "", err
	}
	return disabledRemoteAccountChange(syncAccount, account.Name, account.ID, reason), nil
}

func (s *Service) disableManagedTargetAfterApplyFailure(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	account *sub2api.AdminAccount,
	accountName string,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	key *connector.APIKey,
	keyName string,
	now time.Time,
	reason string,
) (string, error) {
	if account == nil {
		return "", nil
	}
	if err := s.disableRemoteAccount(ctx, adminTarget, client, *account, now, reason); err != nil {
		return "", err
	}
	if err := s.managedAccounts.Upsert(&storage.UpstreamSyncManagedAccount{
		SyncGroupID:        syncGroup.ID,
		SyncAccountID:      syncAccount.ID,
		SourceAPIKeyID:     key.ID,
		SourceAPIKeyName:   keyName,
		TargetAccountID:    account.ID,
		TargetAccountName:  accountName,
		TargetGroupIDsJSON: marshalUintArray(groupIDs(selectedGroups)),
		LastAppliedAt:      &now,
	}); err != nil {
		return "", err
	}
	return disabledRemoteAccountChange(syncAccount, accountName, account.ID, reason), nil
}

func (s *Service) disableRemoteAccount(
	ctx context.Context,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	account sub2api.AdminAccount,
	now time.Time,
	reason string,
) error {
	account.Status = "inactive"
	disabledDescription := disabledManagedAccountDescription(reason, now)
	account.Notes = disabledDescription
	updated, err := client.UpdateAccount(ctx, adminTarget, account.ID, account)
	if err != nil && isHTTPNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if updated == nil {
		updated = &account
	}
	return s.syncRemoteAccountSchedulable(ctx, adminTarget, client, updated)
}

func (s *Service) syncRemoteAccountSchedulable(
	ctx context.Context,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	account *sub2api.AdminAccount,
) error {
	if account == nil {
		return nil
	}
	schedulable := strings.EqualFold(strings.TrimSpace(account.Status), "active")
	_, err := client.SetAccountSchedulable(ctx, adminTarget, account.ID, schedulable)
	return err
}

func disabledManagedAccountDescription(reason string, at time.Time) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "同步失败"
	}
	return "Upstream Ops 自动禁用：" + reason + "\n同步时间：" + formatSyncNoteTime(at)
}

func isRemoteAccountDisabledForReason(account sub2api.AdminAccount, reason string) bool {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "同步失败"
	}
	if !strings.EqualFold(strings.TrimSpace(account.Status), "inactive") || account.Schedulable {
		return false
	}
	return strings.Contains(account.Notes, "Upstream Ops 自动禁用："+reason) ||
		strings.Contains(account.Notes, "Upstream Hub 自动禁用："+reason)
}

func disabledRemoteAccountChange(syncAccount *storage.UpstreamSyncAccount, accountName string, accountID int64, reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "同步失败"
	}
	return fmt.Sprintf("账号%d：%s(ID %d) 已自动禁用，原因：%s", syncAccount.Position+1, accountName, accountID, reason)
}

func testRemoteAccountChange(syncAccount *storage.UpstreamSyncAccount, accountName string, accountID int64, model string, success bool, reason string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		model = "未获取"
	}
	if success {
		return fmt.Sprintf("账号%d：%s(ID %d) 测试模型 %s 通过，调度已启用", syncAccount.Position+1, accountName, accountID, model)
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "测试失败"
	}
	return fmt.Sprintf("账号%d：%s(ID %d) 测试模型 %s 失败，调度已禁用，原因：%s", syncAccount.Position+1, accountName, accountID, model, reason)
}

func syncedAccountNotes(at time.Time) string {
	return "网关同步\n同步时间：" + formatSyncNoteTime(at)
}

func formatSyncNoteTime(at time.Time) string {
	if at.IsZero() {
		at = time.Now()
	}
	return at.Format("2006-01-02 15:04:05")
}

func singleChange(change string) []string {
	if strings.TrimSpace(change) == "" {
		return nil
	}
	return []string{change}
}

type applyErrorWithChanges struct {
	err     error
	changes []string
}

func (e applyErrorWithChanges) Error() string { return e.err.Error() }
func (e applyErrorWithChanges) Unwrap() error { return e.err }

func errorWithChanges(err error, change string) error {
	changes := singleChange(change)
	if len(changes) == 0 {
		return err
	}
	return applyErrorWithChanges{err: err, changes: changes}
}

func changesFromApplyError(err error) []string {
	var wrapped applyErrorWithChanges
	if errors.As(err, &wrapped) {
		return wrapped.changes
	}
	return nil
}

func shouldDisableManagedTargetOnApplyError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "source group missing") || strings.Contains(msg, "source channel missing")
}

func shouldCreateDisabledPlaceholderOnApplyError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "source group missing")
}

func buildApplyLogMessage(
	syncGroup *storage.UpstreamSyncGroup,
	target *storage.UpstreamSyncTarget,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	applied int,
	failures []string,
	successes []string,
	changes []string,
	deletedManaged int,
	deletedUnmanaged int,
) string {
	displayName := strings.TrimSpace(syncGroup.DisplayName)
	if displayName == "" {
		displayName = syncGroup.Name
	}
	var b strings.Builder
	if len(failures) == 0 {
		fmt.Fprintf(&b, "applied %d accounts", applied)
	} else {
		fmt.Fprintf(&b, "applied %d, failed %d", applied, len(failures))
	}
	fmt.Fprintf(&b, "\n同步分组：%s (%s)", displayName, syncGroup.Name)
	fmt.Fprintf(&b, "\n目标上游：%s", target.Name)
	fmt.Fprintf(&b, "\n目标分组：%s", targetGroupNames(selectedGroups))
	fmt.Fprintf(&b, "\n排序方向：%s", rateSortDirectionLabel(syncGroup.RateSortDirection))
	if deletedManaged+deletedUnmanaged > 0 {
		fmt.Fprintf(&b, "\n清理：已删除失效托管账号 %d 个，重复远端账号 %d 个", deletedManaged, deletedUnmanaged)
	}
	if len(changes) > 0 {
		b.WriteString("\n\n变动账号：")
		for _, item := range changes {
			b.WriteString("\n- ")
			b.WriteString(item)
		}
	}
	if len(successes) > 0 {
		b.WriteString("\n\n成功账号：")
		for _, item := range successes {
			b.WriteString("\n- ")
			b.WriteString(item)
		}
	}
	if len(failures) > 0 {
		b.WriteString("\n\n失败账号：")
		for _, item := range failures {
			b.WriteString("\n- ")
			b.WriteString(item)
		}
	}
	return b.String()
}

func targetGroupNames(groups []storage.UpstreamSyncTargetGroup) string {
	if len(groups) == 0 {
		return "未选择"
	}
	out := make([]string, 0, len(groups))
	for _, group := range groups {
		out = append(out, fmt.Sprintf("%s(ID %d，倍率 %s)", group.Name, group.RemoteGroupID, formatNumber(group.Ratio)))
	}
	return strings.Join(out, "、")
}

func sourceGroupLabel(sourceGroupID *int64, sourceGroupName string, groups []connector.APIKeyGroup) string {
	sourceGroupName = strings.TrimSpace(sourceGroupName)
	if sourceGroupID == nil && sourceGroupName == "" {
		return "未绑定"
	}
	for _, group := range groups {
		if sourceGroupID != nil && group.ID != nil && *group.ID == *sourceGroupID {
			return fmt.Sprintf("%s(ID %d，倍率 %s)", group.Name, *group.ID, formatNumber(group.Ratio))
		}
		if sourceGroupName != "" && strings.EqualFold(group.Name, sourceGroupName) {
			return fmt.Sprintf("%s，倍率 %s", group.Name, formatNumber(group.Ratio))
		}
	}
	if sourceGroupName != "" {
		return sourceGroupName
	}
	return fmt.Sprintf("ID %d", *sourceGroupID)
}

func accountChangeDetails(syncAccount *storage.UpstreamSyncAccount, previous *sub2api.AdminAccount, next sub2api.AdminAccount, nextID int64) []string {
	prefix := fmt.Sprintf("账号%d：%s(ID %d)", syncAccount.Position+1, next.Name, nextID)
	if previous == nil {
		return []string{prefix + " 新增"}
	}
	parts := make([]string, 0)
	if previous.Name != next.Name {
		parts = append(parts, fmt.Sprintf("名称 %s -> %s", previous.Name, next.Name))
	}
	if previous.Priority != next.Priority {
		parts = append(parts, fmt.Sprintf("优先级 %d -> %d", previous.Priority, next.Priority))
	}
	if previous.RateMultiplier != next.RateMultiplier {
		parts = append(parts, fmt.Sprintf("倍率 %s -> %s", formatNumber(previous.RateMultiplier), formatNumber(next.RateMultiplier)))
	}
	if previous.LoadFactor != next.LoadFactor {
		parts = append(parts, fmt.Sprintf("权重 %s -> %s", formatNumber(previous.LoadFactor), formatNumber(next.LoadFactor)))
	}
	if previous.Concurrency != next.Concurrency {
		parts = append(parts, fmt.Sprintf("并发 %d -> %d", previous.Concurrency, next.Concurrency))
	}
	if !sameInt64Slice(previous.GroupIDs, next.GroupIDs) {
		parts = append(parts, fmt.Sprintf("目标分组 %s -> %s", int64SliceLabel(previous.GroupIDs), int64SliceLabel(next.GroupIDs)))
	}
	if !sameInt64Ptr(previous.ProxyID, next.ProxyID) {
		parts = append(parts, fmt.Sprintf("代理 %s -> %s", int64PtrLabel(previous.ProxyID), int64PtrLabel(next.ProxyID)))
	}
	if len(parts) == 0 {
		return nil
	}
	if len(parts) > 0 {
		return []string{prefix + " " + strings.Join(parts, "，")}
	}
	return nil
}

func sameInt64Slice(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameInt64Ptr(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func int64SliceLabel(list []int64) string {
	if len(list) == 0 {
		return "空"
	}
	parts := make([]string, 0, len(list))
	for _, v := range list {
		parts = append(parts, strconv.FormatInt(v, 10))
	}
	return strings.Join(parts, ",")
}

func int64PtrLabel(v *int64) string {
	if v == nil {
		return "不使用"
	}
	return strconv.FormatInt(*v, 10)
}

func prefixLines(list []string, prefix string) []string {
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, prefix+item)
	}
	return out
}

func rateSortDirectionLabel(direction string) string {
	if strings.EqualFold(direction, "desc") {
		return "倍率降序，倍率相同按权重从大到小"
	}
	return "倍率升序，倍率相同按权重从大到小"
}

func formatNumber(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func (s *Service) cacheSyncedModels(syncGroup *storage.UpstreamSyncGroup, models []string) error {
	if syncGroup.ModelLimitsMode != "sync_upstream" || len(models) == 0 {
		return nil
	}
	syncGroup.ModelLimitsText = strings.Join(uniqueStrings(models), ",")
	return s.syncGroups.Update(syncGroup)
}

func (s *Service) cleanupDeletedManagedAccounts(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	accounts []storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
) (int, error) {
	current := make(map[uint]struct{}, len(accounts))
	for _, account := range accounts {
		if account.ID != 0 {
			current[account.ID] = struct{}{}
		}
	}
	managedAccounts, err := s.managedAccounts.ListBySyncGroupID(syncGroup.ID)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, account := range managedAccounts {
		if _, ok := current[account.SyncAccountID]; ok {
			continue
		}
		if isManagedAccountName(syncGroup, account.TargetAccountName) {
			if err := client.DeleteAccount(ctx, adminTarget, account.TargetAccountID); err != nil {
				return deleted, err
			}
		}
		if err := s.managedAccounts.Delete(account.ID); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (s *Service) cleanupUnmanagedRemoteAccounts(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
) (int, error) {
	managedAccounts, err := s.managedAccounts.ListBySyncGroupID(syncGroup.ID)
	if err != nil {
		return 0, err
	}
	managedTargetIDs := make(map[int64]struct{}, len(managedAccounts))
	for _, account := range managedAccounts {
		managedTargetIDs[account.TargetAccountID] = struct{}{}
	}
	remoteAccounts, err := client.ListAccounts(ctx, adminTarget, 1, 1000)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, account := range remoteAccounts {
		if _, ok := managedTargetIDs[account.ID]; ok {
			continue
		}
		if !isManagedAccountName(syncGroup, account.Name) {
			continue
		}
		if err := client.DeleteAccount(ctx, adminTarget, account.ID); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (s *Service) cleanupDeletedNewAPIChannels(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	accounts []storage.UpstreamSyncAccount,
	target newapi.AdminTarget,
	client *newapi.AdminClient,
) (int, error) {
	current := make(map[uint]struct{}, len(accounts))
	for _, account := range accounts {
		if account.ID != 0 {
			current[account.ID] = struct{}{}
		}
	}
	managed, err := s.managedAccounts.ListBySyncGroupID(syncGroup.ID)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, item := range managed {
		if _, exists := current[item.SyncAccountID]; exists {
			continue
		}
		// The managed mapping is authoritative for New API channels. Names can
		// legitimately differ after a remote rename, so do not gate cleanup on
		// the generated-name heuristic used for unmanaged-object discovery.
		if err := client.DeleteChannel(ctx, target, item.TargetAccountID); err != nil && !isHTTPNotFound(err) {
			return deleted, err
		}
		if err := s.managedAccounts.Delete(item.ID); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (s *Service) DeleteManaged(ctx context.Context, syncGroupID uint) (*LogDTO, error) {
	syncGroup, err := s.syncGroups.FindByID(syncGroupID)
	if err != nil {
		return nil, err
	}
	managedAccounts, err := s.managedAccounts.ListBySyncGroupID(syncGroup.ID)
	if err != nil {
		return nil, err
	}
	if len(managedAccounts) > 0 {
		target, err := s.targets.FindByID(syncGroup.TargetID)
		if err == nil {
			if plain, decErr := s.cipher.Decrypt(target.AdminAPIKeyCipher); decErr == nil {
				if normalizeTargetType(target.TargetType) == "newapi" {
					client := newapi.NewAdminClient()
					adminTarget := newapi.AdminTarget{BaseURL: target.BaseURL, APIKey: plain}
					for _, account := range managedAccounts {
						_ = client.DeleteChannel(ctx, adminTarget, account.TargetAccountID)
					}
				} else {
					client := sub2api.NewAdminClient()
					adminTarget := sub2api.AdminTarget{BaseURL: target.BaseURL, APIKey: plain}
					for _, account := range managedAccounts {
						if isManagedAccountName(syncGroup, account.TargetAccountName) {
							_ = client.DeleteAccount(ctx, adminTarget, account.TargetAccountID)
						}
					}
				}
			}
		}
		syncAccounts, _ := s.syncAccounts.ListBySyncGroupID(syncGroup.ID)
		accountByID := make(map[uint]storage.UpstreamSyncAccount, len(syncAccounts))
		for _, account := range syncAccounts {
			accountByID[account.ID] = account
		}
		for _, account := range managedAccounts {
			if account.SourceAPIKeyName == sourceAPIKeyName(syncGroup) || strings.HasPrefix(account.SourceAPIKeyName, syncGroup.Name+"-账号") {
				syncAccount, ok := accountByID[account.SyncAccountID]
				if !ok {
					continue
				}
				if isGatewaySyncAccount(&syncAccount) {
					if s.gateway != nil && account.SourceAPIKeyID > 0 {
						_ = s.gateway.DeleteKey(uint(account.SourceAPIKeyID))
					}
				} else if syncAccount.SourceChannelID != 0 {
					_ = s.channelSvc.DeleteAPIKey(ctx, syncAccount.SourceChannelID, account.SourceAPIKeyID)
				}
			}
		}
		_ = s.managedAccounts.DeleteBySyncGroupID(syncGroup.ID)
	}
	return s.appendLog(syncGroup.ID, syncGroup.TargetID, "delete_managed", true, "deleted")
}

func (s *Service) ListSyncGroupLogs(syncGroupID uint, page, pageSize int) ([]LogDTO, int64, error) {
	list, total, err := s.logs.ListPageBySyncGroupID(syncGroupID, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	out := make([]LogDTO, 0, len(list))
	for _, item := range list {
		out = append(out, LogDTO{
			ID:          item.ID,
			SyncGroupID: item.SyncGroupID,
			TargetID:    item.TargetID,
			Action:      item.Action,
			Success:     item.Success,
			Message:     item.Message,
			CreatedAt:   item.CreatedAt,
		})
	}
	return out, total, nil
}

func (s *Service) SyncAllOnRateScan(ctx context.Context) {
	// The channel rate scan has just refreshed upstream ratios. Gateway rate
	// sources derive their effective rate from the same data, so discard its
	// short-lived cache before building synchronization fingerprints.
	if s.gateway != nil {
		s.gateway.InvalidateChannelGroupsCache()
	}
	syncGroups, err := s.syncGroups.List()
	if err != nil {
		if s.log != nil {
			s.log.Warn("list sync groups for rate scan", "err", err)
		}
		return
	}
	refreshedTargets := make(map[uint]bool)
	for _, syncGroup := range syncGroups {
		if !syncGroup.Enabled {
			continue
		}
		target, err := s.targets.FindByID(syncGroup.TargetID)
		if err != nil {
			if s.log != nil {
				s.log.Warn("find sync target for rate scan", "syncGroupID", syncGroup.ID, "targetID", syncGroup.TargetID, "err", err)
			}
			continue
		}
		if !target.Enabled {
			continue
		}
		if !refreshedTargets[target.ID] {
			if _, err := s.SyncTargetGroups(ctx, target.ID); err != nil {
				if s.log != nil {
					s.log.Warn("refresh target groups for rate scan", "syncGroupID", syncGroup.ID, "targetID", target.ID, "err", err)
				}
				continue
			}
			refreshedTargets[target.ID] = true
		}
		accounts, err := s.syncAccounts.ListBySyncGroupID(syncGroup.ID)
		if err != nil {
			if s.log != nil {
				s.log.Warn("list sync accounts for rate scan", "syncGroupID", syncGroup.ID, "err", err)
			}
			continue
		}
		fingerprint, err := s.rateScanFingerprint(ctx, &syncGroup, accounts)
		if err != nil {
			if s.log != nil {
				s.log.Warn("build sync rate fingerprint", "syncGroupID", syncGroup.ID, "err", err)
			}
			continue
		}
		unchanged := syncGroup.RateScanFingerprint != "" && syncGroup.RateScanFingerprint == fingerprint
		if unchanged {
			continue
		}
		log, err := s.ApplySyncGroup(ctx, syncGroup.ID)
		if err != nil && s.log != nil {
			s.log.Warn("apply sync group after rate scan", "syncGroupID", syncGroup.ID, "err", err)
			continue
		}
		if log != nil && log.Success {
			storedFingerprint := fingerprint
			if refreshed, refreshErr := s.rateScanFingerprint(ctx, &syncGroup, accounts); refreshErr == nil {
				storedFingerprint = refreshed
			}
			if err := s.syncGroups.UpdateRateScanFingerprint(syncGroup.ID, storedFingerprint); err != nil {
				if s.log != nil {
					s.log.Warn("save sync rate fingerprint", "syncGroupID", syncGroup.ID, "err", err)
				}
				continue
			}
		}
	}
}

// rateScanFingerprint captures only the source values that affect a dynamic
// sync group's generated account/group multipliers. It intentionally excludes
// model settings: those are changed by explicit sync-group application.
func (s *Service) rateScanFingerprint(ctx context.Context, syncGroup *storage.UpstreamSyncGroup, accounts []storage.UpstreamSyncAccount) (string, error) {
	if syncGroup == nil {
		return "", errors.New("sync group is required")
	}
	groupsByChannel := make(map[uint][]connector.APIKeyGroup)
	for _, account := range accounts {
		if isGatewaySyncAccount(&account) || account.SourceChannelID == 0 {
			continue
		}
		if _, ok := groupsByChannel[account.SourceChannelID]; ok {
			continue
		}
		if s.channelSvc != nil {
			groups, err := s.channelSvc.ListAPIKeyGroups(ctx, account.SourceChannelID)
			if err != nil {
				return "", err
			}
			groupsByChannel[account.SourceChannelID] = groups
		}
	}
	parts := make([]string, 0, len(accounts))
	for _, account := range accounts {
		sourceGroupID := int64(0)
		if account.SourceGroupID != nil {
			sourceGroupID = *account.SourceGroupID
		}
		gatewayGroupID := uint(0)
		if account.GatewayGroupID != nil {
			gatewayGroupID = *account.GatewayGroupID
		}
		part := fmt.Sprintf("%d:%t:%s:%d:%d:%s:%d:%s:%s", account.ID, account.Enabled, normalizeSyncAccountSourceKind(account.SourceKind), account.SourceChannelID, sourceGroupID, strings.TrimSpace(account.SourceGroupName), gatewayGroupID, strings.TrimSpace(account.RateConvertMode), formatNumber(account.RateConvertValue))
		if isGatewaySyncAccount(&account) {
			rate, err := s.gatewayRateMultiplierForAccount(ctx, &account)
			if err != nil {
				return "", err
			}
			part += ":gateway:" + formatNumber(rate)
		} else {
			part += ":source:" + formatNumber(rateMultiplierForAccount(&account, groupsByChannel[account.SourceChannelID]))
		}
		parts = append(parts, part)
	}
	groupIDs, err := s.syncGroups.ParseTargetGroupIDs(syncGroup)
	if err != nil {
		return "", err
	}
	for _, groupID := range groupIDs {
		group, err := s.groups.FindByID(groupID)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("target:%d:%d:%s", group.RemoteGroupID, group.ID, formatNumber(group.Ratio)))
	}
	if len(parts) == 0 {
		return "<empty>", nil
	}
	return strings.Join(parts, "|"), nil
}

func (s *Service) clearRateScanFingerprint(syncGroupID uint) {
	_ = s.syncGroups.UpdateRateScanFingerprint(syncGroupID, "")
}

func (s *Service) checkSourceGroup(ctx context.Context, syncAccount *storage.UpstreamSyncAccount) ([]connector.APIKeyGroup, error) {
	sourceGroupName := strings.TrimSpace(syncAccount.SourceGroupName)
	if syncAccount.SourceGroupID == nil && sourceGroupName == "" {
		return nil, nil
	}
	groups, err := s.channelSvc.ListAPIKeyGroups(ctx, syncAccount.SourceChannelID)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		if syncAccount.SourceGroupID != nil && g.ID != nil && *g.ID == *syncAccount.SourceGroupID {
			return groups, nil
		}
		if sourceGroupName != "" && strings.EqualFold(g.Name, sourceGroupName) {
			return groups, nil
		}
	}
	if sourceGroupName != "" {
		return groups, fmt.Errorf("source group missing: %s", sourceGroupName)
	}
	return groups, fmt.Errorf("source group missing: %d", *syncAccount.SourceGroupID)
}

func (s *Service) selectedTargetGroups(syncGroup *storage.UpstreamSyncGroup) ([]storage.UpstreamSyncTargetGroup, []storage.UpstreamSyncTargetGroup, []int64, error) {
	all, err := s.groups.ListByTarget(syncGroup.TargetID, true)
	if err != nil {
		return nil, nil, nil, err
	}
	ids, err := s.syncGroups.ParseTargetGroupIDs(syncGroup)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(ids) == 0 {
		if target, targetErr := s.targets.FindByID(syncGroup.TargetID); targetErr == nil && normalizeTargetType(target.TargetType) == "newapi" {
			for _, group := range all {
				if strings.EqualFold(strings.TrimSpace(group.Name), "default") && group.Status != "missing" {
					ids = []uint{group.ID}
					break
				}
			}
		}
		if len(ids) == 0 {
			return nil, nil, nil, errors.New("target group missing")
		}
	}
	byID := make(map[uint]storage.UpstreamSyncTargetGroup, len(all))
	for _, g := range all {
		byID[g.ID] = g
	}
	targetType := ""
	if target, targetErr := s.targets.FindByID(syncGroup.TargetID); targetErr == nil {
		targetType = normalizeTargetType(target.TargetType)
	}
	selected := make([]storage.UpstreamSyncTargetGroup, 0, len(ids))
	remoteIDs := make([]int64, 0, len(ids))
	validIDs := make([]uint, 0, len(ids))
	staleIDs := make([]uint, 0)
	for _, id := range ids {
		g, ok := byID[id]
		if !ok || g.Status == "missing" {
			if targetType == "newapi" {
				staleIDs = append(staleIDs, id)
				continue
			}
			return all, selected, remoteIDs, fmt.Errorf("target group missing: %d", id)
		}
		selected = append(selected, g)
		remoteIDs = append(remoteIDs, g.RemoteGroupID)
		validIDs = append(validIDs, g.ID)
	}
	if len(staleIDs) > 0 && targetType == "newapi" {
		// New API group IDs are local cache IDs and can change when a remote
		// group is removed and recreated. Keep valid selections usable and
		// persist the cleaned selection so every subsequent apply is stable.
		if len(selected) == 0 {
			for _, g := range all {
				if strings.EqualFold(strings.TrimSpace(g.Name), "default") && g.Status != "missing" {
					selected = append(selected, g)
					remoteIDs = append(remoteIDs, g.RemoteGroupID)
					validIDs = append(validIDs, g.ID)
					break
				}
			}
		}
		if len(selected) == 0 {
			return all, selected, remoteIDs, errors.New("target group missing")
		}
		syncGroup.TargetGroupIDsJSON = marshalUintArray(validIDs)
		if err := s.syncGroups.Update(syncGroup); err != nil {
			return all, selected, remoteIDs, err
		}
	}
	return all, selected, remoteIDs, nil
}

func (s *Service) ensureSourceAPIKey(ctx context.Context, syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, keyName string) (*connector.APIKey, string, error) {
	sourceChannel, err := s.channels.FindByID(syncAccount.SourceChannelID)
	if err != nil {
		return nil, "", err
	}
	unlimitedQuota := boolPtrIf(sourceChannel.Type == storage.ChannelTypeNewAPI)
	neverExpire := int64PtrIf(sourceChannel.Type == storage.ChannelTypeNewAPI, -1)
	var managedKeyID int64
	if mapped, err := s.managedAccounts.FindByAccountID(syncAccount.ID); err == nil && mapped != nil && mapped.SourceAPIKeyID > 0 {
		managedKeyID = mapped.SourceAPIKeyID
	}
	page, err := s.channelSvc.ListAPIKeys(ctx, syncAccount.SourceChannelID, connector.APIKeyQuery{
		Page:     1,
		PageSize: 100,
		Search:   keyName,
	})
	if err != nil {
		return nil, "", err
	}
	var key *connector.APIKey
	if managedKeyID > 0 {
		key = findAPIKeyByID(page.Items, managedKeyID)
	}
	if key == nil {
		key = findAPIKeyByName(page.Items, keyName)
	}
	if key == nil {
		page, err = s.channelSvc.ListAPIKeys(ctx, syncAccount.SourceChannelID, connector.APIKeyQuery{
			Page:     1,
			PageSize: 100,
		})
		if err != nil {
			return nil, "", err
		}
		if managedKeyID > 0 {
			key = findAPIKeyByID(page.Items, managedKeyID)
		}
		if key == nil {
			key = findAPIKeyByName(page.Items, keyName)
		}
	}
	if key != nil {
		name := keyName
		groupName := strings.TrimSpace(syncAccount.SourceGroupName)
		updated, err := s.channelSvc.UpdateAPIKey(ctx, syncAccount.SourceChannelID, key.ID, connector.APIKeyUpdateRequest{
			Name:           &name,
			Group:          stringPtrOrNil(groupName),
			GroupID:        syncAccount.SourceGroupID,
			UnlimitedQuota: unlimitedQuota,
			ExpiredTime:    neverExpire,
		})
		if err != nil {
			return nil, "", err
		}
		key = updated
	} else {
		groupName := strings.TrimSpace(syncAccount.SourceGroupName)
		key, err = s.channelSvc.CreateAPIKey(ctx, syncAccount.SourceChannelID, connector.APIKeyCreateRequest{
			Name:           keyName,
			Group:          groupName,
			GroupID:        syncAccount.SourceGroupID,
			UnlimitedQuota: unlimitedQuota,
			ExpiredTime:    neverExpire,
		})
		if err != nil {
			return nil, "", err
		}
	}
	secret, err := s.channelSvc.RevealAPIKey(ctx, syncAccount.SourceChannelID, key.ID)
	if err != nil {
		return nil, "", err
	}
	return key, secret, nil
}

func findAPIKeyByName(items []connector.APIKey, name string) *connector.APIKey {
	for i := range items {
		if strings.TrimSpace(items[i].Name) == name {
			return &items[i]
		}
	}
	return nil
}

func findAPIKeyByID(items []connector.APIKey, id int64) *connector.APIKey {
	for i := range items {
		if items[i].ID == id {
			return &items[i]
		}
	}
	return nil
}

func (s *Service) buildAdminAccount(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, ch *storage.Channel, apiKey string, remoteGroupIDs []int64, priority int, rateMultiplier float64) sub2api.AdminAccount {
	return s.buildAdminAccountWithBaseURL(syncGroup, syncAccount, ch.SiteURL, apiKey, remoteGroupIDs, priority, rateMultiplier)
}

func (s *Service) buildAdminAccountWithBaseURL(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, baseURL, apiKey string, remoteGroupIDs []int64, priority int, rateMultiplier float64) sub2api.AdminAccount {
	credentials := map[string]any{
		"api_key":  apiKey,
		"base_url": baseURL,
	}
	if syncGroup.PoolModeEnabled {
		credentials["pool_mode"] = true
		credentials["pool_mode_retry_count"] = syncGroup.PoolModeRetryCount
		credentials["pool_mode_retry_status_codes"] = parseIntList(syncGroup.PoolModeRetryStatusCodes)
	}
	if syncGroup.CustomErrorCodesEnabled {
		credentials["custom_error_codes_enabled"] = true
		credentials["custom_error_codes"] = parseIntList(syncGroup.CustomErrorCodes)
	}
	if syncGroup.ModelLimitsMode == "custom" {
		if mapping := modelMappingFromModels(splitList(syncGroup.ModelLimitsText)); len(mapping) > 0 {
			credentials["model_mapping"] = mapping
		}
	}
	return sub2api.AdminAccount{
		Platform:       syncGroup.Platform,
		Type:           "apikey",
		Status:         "active",
		Notes:          "",
		Credentials:    credentials,
		ProxyID:        syncAccount.ProxyID,
		Concurrency:    positiveOrDefault(syncAccount.Concurrency, 10),
		Priority:       priority,
		RateMultiplier: rateMultiplier,
		LoadFactor:     float64(syncAccount.Weight),
		GroupIDs:       remoteGroupIDs,
	}
}

func priorityForSourceGroup(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, groups []connector.APIKeyGroup) int {
	ratios := make([]float64, 0, len(groups))
	seen := map[string]struct{}{}
	selectedRatio := 0.0
	sourceGroupName := strings.TrimSpace(syncAccount.SourceGroupName)
	for _, g := range groups {
		ratio := convertRate(g.Ratio, syncAccount.RateConvertMode, syncAccount.RateConvertValue)
		if (syncAccount.SourceGroupID != nil && g.ID != nil && *g.ID == *syncAccount.SourceGroupID) ||
			(sourceGroupName != "" && strings.EqualFold(g.Name, sourceGroupName)) {
			selectedRatio = ratio
		}
		key := strconv.FormatFloat(ratio, 'f', 8, 64)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ratios = append(ratios, ratio)
	}
	sort.Float64s(ratios)
	if strings.EqualFold(syncGroup.RateSortDirection, "desc") {
		sort.Sort(sort.Reverse(sort.Float64Slice(ratios)))
	}
	rank := 0
	for i, ratio := range ratios {
		if ratio == selectedRatio {
			rank = i
			break
		}
	}
	return rank*1000 - syncAccount.Weight
}

func convertRate(v float64, mode string, customValue float64) float64 {
	return rateconvert.Convert(v, mode, customValue)
}

func rateMultiplierForAccount(syncAccount *storage.UpstreamSyncAccount, groups []connector.APIKeyGroup) float64 {
	if isGatewaySyncAccount(syncAccount) {
		return 1
	}
	if strings.TrimSpace(syncAccount.RateConvertMode) == "custom" {
		return syncAccount.RateConvertValue
	}
	sourceGroupName := strings.TrimSpace(syncAccount.SourceGroupName)
	if syncAccount.SourceGroupID == nil && sourceGroupName == "" {
		return convertRate(1, syncAccount.RateConvertMode, syncAccount.RateConvertValue)
	}
	for _, group := range groups {
		if (syncAccount.SourceGroupID != nil && group.ID != nil && *group.ID == *syncAccount.SourceGroupID) ||
			(sourceGroupName != "" && strings.EqualFold(group.Name, sourceGroupName)) {
			return convertRate(group.Ratio, syncAccount.RateConvertMode, syncAccount.RateConvertValue)
		}
	}
	return convertRate(1, syncAccount.RateConvertMode, syncAccount.RateConvertValue)
}

func normalizeSyncAccountSourceKind(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "gateway_group") {
		return "gateway_group"
	}
	return "channel"
}

func normalizeSyncMode(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "gateway_rate") {
		return "gateway_rate"
	}
	return "account"
}

// syncModeForGroup defaults records without an explicit mode to account sync.
// A gateway group is a valid upstream source for a New API channel; it must not
// be silently reinterpreted as a multiplier-only operation.
func syncModeForGroup(group *storage.UpstreamSyncGroup, accounts []storage.UpstreamSyncAccount) string {
	if group != nil && strings.TrimSpace(group.SyncMode) != "" {
		return normalizeSyncMode(group.SyncMode)
	}
	return "account"
}

func syncModeFromInput(value string, accounts []storage.UpstreamSyncAccount) string {
	if strings.TrimSpace(value) == "" {
		return syncModeForGroup(nil, accounts)
	}
	return normalizeSyncMode(value)
}

func normalizeGatewayRateMode(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "min") {
		return "min"
	}
	return "max"
}

func isGatewaySyncAccount(account *storage.UpstreamSyncAccount) bool {
	return account != nil && normalizeSyncAccountSourceKind(account.SourceKind) == "gateway_group"
}

func hasGatewaySyncAccount(accounts []storage.UpstreamSyncAccount) bool {
	for i := range accounts {
		if isGatewaySyncAccount(&accounts[i]) {
			return true
		}
	}
	return false
}

func (s *Service) validateGatewaySyncAccounts(ctx context.Context, syncMode string, accounts []storage.UpstreamSyncAccount) error {
	count := 0
	for i := range accounts {
		if !isGatewaySyncAccount(&accounts[i]) {
			continue
		}
		count++
		if accounts[i].GatewayGroupID == nil || *accounts[i].GatewayGroupID == 0 {
			return fmt.Errorf("gateway multiplier sync account %d: gateway group is required", i+1)
		}
		if accounts[i].GatewayRateMin > 0 && accounts[i].GatewayRateMax > 0 && accounts[i].GatewayRateMin > accounts[i].GatewayRateMax {
			return fmt.Errorf("gateway multiplier sync account %d: minimum rate cannot exceed maximum rate", i+1)
		}
	}
	if count == 0 {
		return nil
	}
	if syncMode == "gateway_rate" && (len(accounts) != 1 || count != 1) {
		return errors.New("gateway multiplier sync requires exactly one gateway group")
	}
	if s.gateway == nil {
		return errors.New("gateway service is unavailable")
	}
	if syncMode == "gateway_rate" {
		status, err := s.gateway.GetRateSyncStatus(ctx, *accounts[0].GatewayGroupID)
		if err != nil {
			return err
		}
		if !status.Ready {
			return errors.New(status.Error())
		}
		return nil
	}
	if strings.TrimSpace(s.gatewayBaseURL) == "" {
		return errors.New("server.baseURL is required for gateway multiplier sync")
	}
	return nil
}

func (s *Service) ensureGatewayKeysForGroup(syncGroup *storage.UpstreamSyncGroup, accounts []storage.UpstreamSyncAccount) error {
	for i := range accounts {
		if !isGatewaySyncAccount(&accounts[i]) {
			continue
		}
		_, _, err := s.ensureGatewayKey(syncGroup, &accounts[i], sourceAPIKeyName(syncGroup))
		return err
	}
	return nil
}

func (s *Service) ensureGatewayKey(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, keyName string) (*connector.APIKey, string, error) {
	if s.gateway == nil {
		return nil, "", errors.New("gateway service is unavailable")
	}
	if syncAccount == nil || syncAccount.GatewayGroupID == nil || *syncAccount.GatewayGroupID == 0 {
		return nil, "", errors.New("gateway group is required")
	}
	groupID := *syncAccount.GatewayGroupID
	if _, err := s.gateway.GetGroup(groupID); err != nil {
		return nil, "", fmt.Errorf("gateway group missing: %d", groupID)
	}
	keys, err := s.gateway.ListKeysByGroup(groupID)
	if err != nil {
		return nil, "", err
	}
	if mapped, err := s.managedAccounts.FindByAccountID(syncAccount.ID); err == nil && mapped != nil && mapped.SourceAPIKeyID > 0 {
		for _, item := range keys {
			if int64(item.ID) != mapped.SourceAPIKeyID {
				continue
			}
			secret, revealErr := s.gateway.RevealKey(item.ID)
			if revealErr != nil {
				return nil, "", revealErr
			}
			return &connector.APIKey{ID: int64(item.ID), Name: item.Name}, secret, nil
		}
	}
	for _, item := range keys {
		if strings.TrimSpace(item.Name) != strings.TrimSpace(keyName) {
			continue
		}
		secret, revealErr := s.gateway.RevealKey(item.ID)
		if revealErr != nil {
			return nil, "", revealErr
		}
		return &connector.APIKey{ID: int64(item.ID), Name: item.Name}, secret, nil
	}
	created, err := s.gateway.CreateKey(gateway.CreateKeyInput{GroupID: groupID, Name: strings.TrimSpace(keyName)})
	if err != nil {
		return nil, "", err
	}
	return &connector.APIKey{ID: int64(created.Key.ID), Name: created.Key.Name}, created.Secret, nil
}

func (s *Service) gatewayRateMultiplierForAccount(ctx context.Context, syncAccount *storage.UpstreamSyncAccount) (float64, error) {
	if s.gateway == nil || syncAccount == nil || syncAccount.GatewayGroupID == nil || *syncAccount.GatewayGroupID == 0 {
		return 0, errors.New("gateway group is required")
	}
	status, err := s.gateway.GetRateSyncStatus(ctx, *syncAccount.GatewayGroupID)
	if err != nil {
		return 0, err
	}
	if !status.Ready {
		return 0, errors.New(status.Error())
	}
	base := status.MaxRate
	if normalizeGatewayRateMode(syncAccount.GatewayRateMode) == "min" {
		base = status.MinRate
	}
	computed := applyGatewayRateOperation(base, syncAccount.RateConvertMode, syncAccount.RateConvertValue)
	return clampGatewayRate(computed, syncAccount.GatewayRateMin, syncAccount.GatewayRateMax), nil
}

func (s *Service) syncGatewayTargetGroupRates(
	ctx context.Context,
	accounts []storage.UpstreamSyncAccount,
	target sub2api.AdminTarget,
	client *sub2api.AdminClient,
	selectedGroups []storage.UpstreamSyncTargetGroup,
) ([]string, error) {
	for i := range accounts {
		account := &accounts[i]
		if !isGatewaySyncAccount(account) {
			continue
		}
		rate, err := s.gatewayRateMultiplierForAccount(ctx, account)
		if err != nil {
			return nil, err
		}
		changes := make([]string, 0, len(selectedGroups))
		for _, group := range selectedGroups {
			if group.RemoteGroupID <= 0 {
				return nil, fmt.Errorf("target group %q has no remote ID", group.Name)
			}
			if nearlyEqualRate(group.Ratio, rate) {
				continue
			}
			if err := client.UpdateGroupRateMultiplier(ctx, target, group.RemoteGroupID, rate); err != nil {
				return nil, fmt.Errorf("update target group %q rate multiplier: %w", group.Name, err)
			}
			if err := s.groups.UpdateRatio(group.ID, rate); err != nil {
				return nil, fmt.Errorf("save target group %q rate multiplier: %w", group.Name, err)
			}
			changes = append(changes, fmt.Sprintf(
				"目标分组 %s 倍率 %s -> %s",
				group.Name,
				formatNumber(group.Ratio),
				formatNumber(rate),
			))
		}
		return changes, nil
	}
	return nil, nil
}

func applyGatewayRateOperation(base float64, operation string, value float64) float64 {
	switch strings.TrimSpace(operation) {
	case "add":
		return base + value
	case "subtract":
		return base - value
	case "multiply":
		return base * value
	case "divide":
		if value != 0 {
			return base / value
		}
	}
	return base
}

func clampGatewayRate(value, minValue, maxValue float64) float64 {
	minValue = nonNegativeFloat(minValue)
	maxValue = nonNegativeFloat(maxValue)
	if minValue > 0 && value < minValue {
		value = minValue
	}
	if maxValue > 0 && value > maxValue {
		value = maxValue
	}
	return roundGatewayRate(value)
}

const gatewayRateDecimalPlaces = 8

// roundGatewayRate removes binary floating-point tails before a calculated
// multiplier is compared, logged, cached, or sent to an upstream target.
func roundGatewayRate(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return value
	}
	scale := math.Pow10(gatewayRateDecimalPlaces)
	return math.Round(value*scale) / scale
}

func nonNegativeFloat(value float64) float64 {
	if value < 0 {
		return 0
	}
	return value
}

func managedObjectBaseName(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount) string {
	return fmt.Sprintf("%s-账号%d", syncGroup.Name, syncAccount.Position+1)
}

func managedObjectName(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, ch *storage.Channel) string {
	return managedObjectNameForSource(syncGroup, syncAccount, ch.Name)
}

func managedObjectNameForSource(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, sourceName string) string {
	base := managedObjectBaseName(syncGroup, syncAccount)
	sourceName = strings.TrimSpace(sourceName)
	if sourceName == "" {
		return base
	}
	return fmt.Sprintf("%s [%s]", base, sourceName)
}

func managedObjectMatchName(name string) string {
	if idx := strings.Index(name, " ["); idx >= 0 {
		return name[:idx]
	}
	return name
}

func isManagedAccountName(syncGroup *storage.UpstreamSyncGroup, name string) bool {
	base := managedObjectMatchName(name)
	if strings.HasPrefix(base, syncGroup.Name+"-账号") {
		return true
	}
	if strings.HasPrefix(base, syncGroup.Name+"-") {
		rest := strings.TrimPrefix(base, syncGroup.Name+"-")
		if rest != "" {
			for _, r := range rest {
				if r < '0' || r > '9' {
					return false
				}
			}
			return true
		}
	}
	return strings.EqualFold(strings.TrimSpace(base), strings.TrimSpace(syncGroup.Name))
}

func managedAccountPosition(syncGroup *storage.UpstreamSyncGroup, name string) (int, bool) {
	base := managedObjectMatchName(name)
	prefix := syncGroup.Name + "-账号"
	if !strings.HasPrefix(base, prefix) {
		prefix = syncGroup.Name + "-"
	}
	if !strings.HasPrefix(base, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(base, prefix)
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}
	n, err := strconv.Atoi(rest[:end])
	if err != nil || n <= 0 {
		return 0, false
	}
	return n - 1, true
}

func sourceAPIKeyName(syncGroup *storage.UpstreamSyncGroup) string {
	return syncGroup.Name
}

func normalizeModelLimits(raw string) string {
	parts := splitList(raw)
	return strings.Join(parts, ",")
}

func normalizeModelLimitsMode(raw string) string {
	if strings.EqualFold(strings.TrimSpace(raw), "custom") {
		return "custom"
	}
	return "sync_upstream"
}

func selectSourceModelKey(items []connector.APIKey, managedKeyID int64, groupID *int64, groupName string) *connector.APIKey {
	if managedKeyID > 0 {
		for i := range items {
			if items[i].ID == managedKeyID && apiKeyUsableForModels(items[i]) && sourceAPIKeyMatchesGroup(items[i], groupID, groupName) {
				return &items[i]
			}
		}
	}
	for i := range items {
		if !apiKeyUsableForModels(items[i]) {
			continue
		}
		if sourceAPIKeyMatchesGroup(items[i], groupID, groupName) {
			return &items[i]
		}
	}
	if sourceModelGroupSpecified(groupID, groupName) {
		return nil
	}
	for i := range items {
		if apiKeyUsableForModels(items[i]) {
			return &items[i]
		}
	}
	return nil
}

func sourceModelGroupSpecified(groupID *int64, groupName string) bool {
	return groupID != nil || strings.TrimSpace(groupName) != ""
}

func sourceAPIKeyMatchesGroup(key connector.APIKey, groupID *int64, groupName string) bool {
	if !sourceModelGroupSpecified(groupID, groupName) {
		return true
	}
	if groupID != nil && key.GroupID != nil && *key.GroupID == *groupID {
		return true
	}
	name := strings.TrimSpace(groupName)
	return name != "" && (strings.EqualFold(strings.TrimSpace(key.GroupName), name) || strings.EqualFold(strings.TrimSpace(key.Group), name))
}

func apiKeyUsableForModels(key connector.APIKey) bool {
	switch strings.ToLower(strings.TrimSpace(key.Status)) {
	case "disabled", "inactive", "expired", "quota_exhausted":
		return false
	default:
		return true
	}
}

func fetchGatewayModels(ctx context.Context, baseURL, platform, apiKey string) ([]string, error) {
	endpoint := buildGatewayModelsURL(baseURL, platform)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	apiKey = strings.TrimSpace(apiKey)
	req.Header.Set("Accept", "application/json")
	setGatewayModelAuthHeaders(req, platform, apiKey)
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, sourceModelsBodyLimit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > sourceModelsBodyLimit {
		return nil, errors.New("model list response is too large")
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("model list request failed with HTTP %d", resp.StatusCode)
	}
	models, err := decodeGatewayModels(body)
	if err != nil {
		return nil, err
	}
	if len(models) == 0 {
		return nil, errors.New("model list is empty")
	}
	return models, nil
}

func buildGatewayModelsURL(base, platform string) string {
	normalized := strings.TrimRight(strings.TrimSpace(base), "/")
	if strings.EqualFold(strings.TrimSpace(platform), "gemini") {
		if strings.HasSuffix(normalized, "/v1beta/models") {
			return normalized
		}
		if strings.HasSuffix(normalized, "/v1beta") {
			return normalized + "/models"
		}
		return normalized + "/v1beta/models"
	}
	if strings.HasSuffix(normalized, "/v1/models") {
		return normalized
	}
	if strings.HasSuffix(normalized, "/v1") {
		return normalized + "/models"
	}
	return normalized + "/v1/models"
}

func setGatewayModelAuthHeaders(req *http.Request, platform, apiKey string) {
	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "gemini":
		req.Header.Set("x-goog-api-key", apiKey)
	case "anthropic", "antigravity":
		req.Header.Set("x-api-key", apiKey)
		req.Header.Set("anthropic-version", "2023-06-01")
		req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	default:
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
}

func decodeGatewayModels(body []byte) ([]string, error) {
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	return uniqueStrings(collectGatewayModelIDs(raw)), nil
}

func collectGatewayModelIDs(raw any) []string {
	switch value := raw.(type) {
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			out = append(out, collectGatewayModelIDs(item)...)
		}
		return out
	case map[string]any:
		out := []string(nil)
		if models, ok := value["models"]; ok {
			if modelMap, ok := models.(map[string]any); ok {
				for id := range modelMap {
					out = append(out, id)
				}
			} else {
				out = append(out, collectGatewayModelIDs(models)...)
			}
		}
		if data, ok := value["data"]; ok {
			out = append(out, collectGatewayModelIDs(data)...)
		}
		for _, key := range []string{"id", "name", "model"} {
			if text, ok := value[key].(string); ok {
				out = append(out, text)
				break
			}
		}
		return out
	case string:
		return []string{value}
	default:
		return nil
	}
}

func parseIntList(raw string) []int {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return []int{}
	}
	var list []int
	if err := json.Unmarshal([]byte(trimmed), &list); err == nil {
		return list
	}
	parts := splitList(raw)
	list = make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err == nil {
			list = append(list, n)
		}
	}
	return list
}

func modelMappingFromModels(models []string) map[string]string {
	out := make(map[string]string, len(models))
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		out[model] = model
	}
	return out
}

func gatewaySyncModels(gatewaySvc *gateway.Service, group *storage.GatewayGroup) []string {
	if group == nil {
		return nil
	}
	models := make([]string, 0)
	if gatewaySvc != nil {
		for _, item := range gatewaySvc.ParseModelsJSON(group.ModelsJSON) {
			models = append(models, item.ID)
		}
	}
	mapping := gateway.ParseModelMapping(group.ModelMappingJSON)
	for model := range mapping {
		model = strings.TrimSpace(model)
		if model == "" || model == "*" {
			continue
		}
		models = append(models, model)
	}
	models = uniqueStrings(models)
	sort.Strings(models)
	return models
}

func nearlyEqualRate(left, right float64) bool {
	const epsilon = 1e-9
	return left-right < epsilon && right-left < epsilon
}

func uniqueStrings(list []string) []string {
	out := make([]string, 0, len(list))
	seen := map[string]struct{}{}
	for _, item := range list {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func isHTTPNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "status 404")
}

func splitList(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t'
	})
	out := make([]string, 0, len(fields))
	seen := map[string]struct{}{}
	for _, field := range fields {
		item := strings.TrimSpace(field)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func accountItems(list []SyncAccountDTO) []storage.UpstreamSyncAccount {
	out := make([]storage.UpstreamSyncAccount, 0, len(list))
	for _, item := range list {
		sourceKind := normalizeSyncAccountSourceKind(item.SourceKind)
		if sourceKind == "channel" && item.SourceChannelID == 0 {
			continue
		}
		mode := strings.TrimSpace(item.RateConvertMode)
		if mode == "" {
			mode = "raw"
		}
		value := item.RateConvertValue
		if mode != "custom" && value == 0 {
			value = 1
		}
		concurrency := item.Concurrency
		if concurrency <= 0 {
			concurrency = 10
		}
		weight := item.Weight
		if weight <= 0 {
			weight = 1
		}
		out = append(out, storage.UpstreamSyncAccount{
			ID:               item.ID,
			Position:         len(out),
			SourceKind:       sourceKind,
			SourceChannelID:  item.SourceChannelID,
			SourceGroupID:    item.SourceGroupID,
			SourceGroupName:  strings.TrimSpace(item.SourceGroupName),
			GatewayGroupID:   item.GatewayGroupID,
			GatewayRateMode:  normalizeGatewayRateMode(item.GatewayRateMode),
			GatewayRateMin:   nonNegativeFloat(item.GatewayRateMin),
			GatewayRateMax:   nonNegativeFloat(item.GatewayRateMax),
			ProxyID:          item.ProxyID,
			Concurrency:      concurrency,
			Weight:           weight,
			Priority:         item.Priority,
			RateConvertMode:  mode,
			RateConvertValue: value,
			Enabled:          item.Enabled,
			TestEnabled:      item.TestEnabled,
			TestModel:        strings.TrimSpace(item.TestModel),
		})
	}
	return out
}

func marshalUintArray(list []uint) string {
	if len(list) == 0 {
		return "[]"
	}
	body, _ := json.Marshal(list)
	return string(body)
}

func parseJSONUintArray(raw string) []uint {
	var list []uint
	_ = json.Unmarshal([]byte(raw), &list)
	return list
}

func groupIDs(groups []storage.UpstreamSyncTargetGroup) []uint {
	out := make([]uint, 0, len(groups))
	for _, g := range groups {
		out = append(out, g.ID)
	}
	return out
}

func boolValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func ptrBool(value bool) *bool {
	return &value
}

func positiveOrDefault(v, fallback int) int {
	if v <= 0 {
		return fallback
	}
	return v
}

func positiveFloatOrDefault(v, fallback float64) float64 {
	if v <= 0 {
		return fallback
	}
	return v
}

func (s *Service) appendLog(syncGroupID, targetID uint, action string, success bool, msg string) (*LogDTO, error) {
	item := &storage.UpstreamSyncLog{
		SyncGroupID: syncGroupID,
		TargetID:    targetID,
		Action:      action,
		Success:     success,
		Message:     msg,
	}
	if err := s.logs.Append(item); err != nil {
		return nil, err
	}
	return &LogDTO{
		ID:          item.ID,
		SyncGroupID: item.SyncGroupID,
		TargetID:    item.TargetID,
		Action:      item.Action,
		Success:     item.Success,
		Message:     item.Message,
		CreatedAt:   item.CreatedAt,
	}, nil
}

func (s *Service) toTargetDTO(item *storage.UpstreamSyncTarget) *TargetDTO {
	return &TargetDTO{
		ID:              item.ID,
		Name:            item.Name,
		TargetType:      normalizeTargetType(item.TargetType),
		BaseURL:         item.BaseURL,
		Enabled:         item.Enabled,
		LastCheckStatus: item.LastCheckStatus,
		LastCheckAt:     item.LastCheckAt,
		LastCheckError:  item.LastCheckError,
	}
}

func (s *Service) toGroupDTO(item *storage.UpstreamSyncTargetGroup) TargetGroupDTO {
	return TargetGroupDTO{
		ID:            item.ID,
		TargetID:      item.TargetID,
		RemoteGroupID: item.RemoteGroupID,
		Name:          item.Name,
		Platform:      item.Platform,
		Ratio:         item.Ratio,
		Status:        item.Status,
		Sort:          item.Sort,
		Description:   item.Description,
		LastSyncAt:    item.LastSyncAt,
	}
}

func (s *Service) toSyncGroupDTO(item *storage.UpstreamSyncGroup, ids []uint, accounts []storage.UpstreamSyncAccount) SyncGroupDTO {
	return SyncGroupDTO{
		ID:                       item.ID,
		Sort:                     item.Sort,
		DisplayName:              item.DisplayName,
		NameTemplate:             item.NameTemplate,
		Name:                     item.Name,
		TargetID:                 item.TargetID,
		TargetGroupIDs:           ids,
		SyncMode:                 syncModeForGroup(item, accounts),
		Platform:                 item.Platform,
		ModelLimitsMode:          normalizeModelLimitsMode(item.ModelLimitsMode),
		ModelLimits:              item.ModelLimitsText,
		PoolModeEnabled:          item.PoolModeEnabled,
		PoolModeRetryCount:       item.PoolModeRetryCount,
		PoolModeRetryStatusCodes: item.PoolModeRetryStatusCodes,
		CustomErrorCodesEnabled:  item.CustomErrorCodesEnabled,
		CustomErrorCodes:         item.CustomErrorCodes,
		RateSortDirection:        item.RateSortDirection,
		Accounts:                 accountDTOs(accounts),
		Enabled:                  ptrBool(item.Enabled),
		ApplyStatus:              item.ApplyStatus,
		ApplyError:               item.ApplyError,
		LastAppliedAt:            item.LastAppliedAt,
	}
}

func (s *Service) syncGroupDTOByItem(item *storage.UpstreamSyncGroup) *SyncGroupDTO {
	ids, _ := s.syncGroups.ParseTargetGroupIDs(item)
	accounts, _ := s.syncAccounts.ListBySyncGroupID(item.ID)
	dto := s.toSyncGroupDTO(item, ids, accounts)
	return &dto
}

func accountDTOs(list []storage.UpstreamSyncAccount) []SyncAccountDTO {
	out := make([]SyncAccountDTO, 0, len(list))
	for _, item := range list {
		out = append(out, SyncAccountDTO{
			ID:               item.ID,
			SourceKind:       normalizeSyncAccountSourceKind(item.SourceKind),
			SourceChannelID:  item.SourceChannelID,
			SourceGroupID:    item.SourceGroupID,
			SourceGroupName:  item.SourceGroupName,
			GatewayGroupID:   item.GatewayGroupID,
			GatewayRateMode:  normalizeGatewayRateMode(item.GatewayRateMode),
			GatewayRateMin:   item.GatewayRateMin,
			GatewayRateMax:   item.GatewayRateMax,
			ProxyID:          item.ProxyID,
			Concurrency:      item.Concurrency,
			Weight:           item.Weight,
			Priority:         item.Priority,
			RateConvertMode:  item.RateConvertMode,
			RateConvertValue: item.RateConvertValue,
			Enabled:          item.Enabled,
			TestEnabled:      item.TestEnabled,
			TestModel:        item.TestModel,
		})
	}
	return out
}

func renderSyncGroupName(tpl string, syncGroupID uint, channelID uint, sourceGroupID int64) string {
	out := strings.ReplaceAll(tpl, "{同步分组ID}", strconv.FormatUint(uint64(syncGroupID), 10))
	out = strings.ReplaceAll(out, "{渠道ID}", strconv.FormatUint(uint64(channelID), 10))
	out = strings.ReplaceAll(out, "{源分组ID}", strconv.FormatInt(sourceGroupID, 10))
	return strings.TrimSpace(out)
}

func stringPtrOrNil(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func boolPtrIf(value bool) *bool {
	if !value {
		return nil
	}
	return &value
}

func int64PtrIf(ok bool, value int64) *int64 {
	if !ok {
		return nil
	}
	return &value
}

func ptrTime(t time.Time) *time.Time { return &t }
