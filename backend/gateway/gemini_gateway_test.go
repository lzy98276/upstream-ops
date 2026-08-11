package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/lzy98276/upstream-ops/backend/crypto"
	"github.com/lzy98276/upstream-ops/backend/storage"
)

func TestHandleGeminiGenerateForwardsNativeGeminiRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1beta/models/gemini-2.5-pro:generateContent" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("x-goog-api-key"); got != "google-key" {
			http.Error(w, "missing google api key", http.StatusUnauthorized)
			return
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if _, ok := body["contents"]; !ok {
			http.Error(w, "Gemini contents were not preserved", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"candidates": []any{map[string]any{
				"content":      map[string]any{"role": "model", "parts": []any{map[string]any{"text": "pong"}}},
				"finishReason": "STOP",
			}},
		})
	}))
	t.Cleanup(upstream.Close)

	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	keys := storage.NewGatewayKeys(db)
	routes := storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	usage := storage.NewGatewayUsageLogs(db)
	cipher, err := crypto.NewCipher("gemini-gateway-test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	group := &storage.GatewayGroup{
		Name:                "gemini",
		Status:              storage.GatewayGroupStatusActive,
		RateSortDirection:   "asc",
		ModelRoutingEnabled: true,
	}
	if err := groups.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	providerKey, _ := cipher.Encrypt("google-key")
	provider := &storage.GatewayProvider{
		Name:             "Google Gemini",
		BaseURL:          upstream.URL,
		APIKeyCipher:     providerKey,
		Enabled:          true,
		UpstreamProtocol: storage.GatewayUpstreamProtocolGemini,
		AuthStyle:        storage.GatewayProviderAuthGoogle,
	}
	if err := providers.Create(provider); err != nil {
		t.Fatalf("create provider: %v", err)
	}
	if err := routes.SaveForGroup(group.ID, []storage.GatewayRoute{{
		GatewayGroupID:      group.ID,
		SourceKind:          storage.GatewayRouteSourceProvider,
		GatewayProviderID:   provider.ID,
		Weight:              1,
		Enabled:             true,
		RateConvertMode:     "custom",
		RateConvertValue:    1,
		SupportedModelsJSON: `["gemini-2.5-pro"]`,
		UpstreamProtocol:    storage.GatewayUpstreamProtocolGemini,
	}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	savedRoutes, err := routes.ListByGroupID(group.ID)
	if err != nil || len(savedRoutes) != 1 {
		t.Fatalf("load route: %v routes=%d", err, len(savedRoutes))
	}
	if err := routes.UpdateModelCapabilities(savedRoutes[0].ID, `["gemini-2.5-pro"]`, nil, ""); err != nil {
		t.Fatalf("set route capabilities: %v", err)
	}
	clientKey := "sk-gemini-client"
	clientCipher, _ := cipher.Encrypt(clientKey)
	if err := keys.Create(&storage.GatewayKey{
		GroupID: group.ID, Name: "client", KeyHash: HashAPIKey(clientKey), KeyPrefix: KeyPrefix(clientKey),
		KeyCipher: clientCipher, Status: storage.GatewayKeyStatusActive,
	}); err != nil {
		t.Fatalf("create client key: %v", err)
	}

	svc := NewService(groups, keys, routes, usage, nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-2.5-pro:generateContent", http.NoBody)
	c.Request.Header.Set("Authorization", "Bearer "+clientKey)
	c.Request.Body = http.NoBody
	// Gin's route wildcard normally includes the leading slash.
	c.Params = gin.Params{{Key: "modelAction", Value: "/gemini-2.5-pro:generateContent"}}
	c.Request.Body = ioNopCloser(`{"contents":[{"role":"user","parts":[{"text":"ping"}]}]}`)
	c.Request.ContentLength = -1
	svc.HandleGeminiGenerate(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("response is not Gemini JSON: %v", err)
	}
	if _, ok := response["candidates"]; !ok {
		t.Fatalf("Gemini response not preserved: %s", w.Body.String())
	}
}

func ioNopCloser(body string) io.ReadCloser { return io.NopCloser(strings.NewReader(body)) }
