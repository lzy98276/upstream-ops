package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/lzy98276/upstream-ops/backend/gateway"
	"github.com/lzy98276/upstream-ops/backend/storage"
)

func TestGatewayRateSyncStatusEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	groups := storage.NewGatewayGroups(db)
	routes := storage.NewGatewayRoutes(db)
	svc := gateway.NewService(
		groups,
		storage.NewGatewayKeys(db),
		routes,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	group := &storage.GatewayGroup{Name: "empty-rate-source", Status: storage.GatewayGroupStatusActive}
	if err := groups.Create(group); err != nil {
		t.Fatalf("create gateway group: %v", err)
	}

	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{Gateway: svc})

	request := httptest.NewRequest(http.MethodGet, "/api/gateway/groups/"+strconv.FormatUint(uint64(group.ID), 10)+"/rate-sync-status", nil)
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var status gateway.RateSyncStatus
	if err := json.Unmarshal(recorder.Body.Bytes(), &status); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if status.GroupID != group.ID || status.Ready || status.ReasonCode != gateway.GatewayRateSyncReasonNoRoutes {
		t.Fatalf("response = %#v", status)
	}

	badRequest := httptest.NewRequest(http.MethodGet, "/api/gateway/groups/not-a-number/rate-sync-status", nil)
	badRecorder := httptest.NewRecorder()
	r.ServeHTTP(badRecorder, badRequest)
	if badRecorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid id status = %d, body = %s", badRecorder.Code, badRecorder.Body.String())
	}
}
