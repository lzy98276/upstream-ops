package gateway

import (
	"context"
	"testing"

	"github.com/lzy98276/upstream-ops/backend/connector"
	"github.com/lzy98276/upstream-ops/backend/storage"
)

func TestGetRateSyncStatusUsesEffectiveEnabledRouteRates(t *testing.T) {
	db := openGatewayTestDB(t)
	groupsRepo := storage.NewGatewayGroups(db)
	routesRepo := storage.NewGatewayRoutes(db)
	group := &storage.GatewayGroup{Name: "rate-source", Status: storage.GatewayGroupStatusActive}
	if err := groupsRepo.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := routesRepo.SaveForGroup(group.ID, []storage.GatewayRoute{
		{SourceChannelID: 1, SourceGroupName: "standard", Enabled: true, BillingRateMultiplier: 4},
		{SourceChannelID: 2, SourceGroupName: "disabled", Enabled: false, BillingRateMultiplier: 9},
		{SourceChannelID: 3, SourceGroupName: "zero", Enabled: true, RateConvertMode: "raw", RateConvertValue: 1},
	}); err != nil {
		t.Fatalf("save routes: %v", err)
	}
	routes, err := routesRepo.ListByGroupID(group.ID)
	if err != nil {
		t.Fatalf("list routes: %v", err)
	}
	if err := db.Model(&storage.GatewayRoute{}).Where("id = ?", routes[1].ID).Update("enabled", false).Error; err != nil {
		t.Fatalf("disable route: %v", err)
	}
	channelAPI := &fakeChannelAPIForResort{groups: map[uint][]connector.APIKeyGroup{
		1: {{Name: "standard", Ratio: 0.25}},
		2: {{Name: "disabled", Ratio: 9}},
		3: {{Name: "zero", Ratio: 0}},
	}}
	svc := NewService(groupsRepo, storage.NewGatewayKeys(db), routesRepo, nil, nil, nil, channelAPI, nil, nil)

	status, err := svc.GetRateSyncStatus(context.Background(), group.ID)
	if err != nil {
		t.Fatalf("get rate sync status: %v", err)
	}
	if !status.Ready || status.RouteCount != 3 || status.EnabledRouteCount != 2 || status.PositiveRateRouteCount != 1 {
		t.Fatalf("status = %#v", status)
	}
	if status.MinRate != 0.25 || status.MaxRate != 0.25 {
		t.Fatalf("rates = %v/%v, want 0.25/0.25", status.MinRate, status.MaxRate)
	}
}

func TestGetRateSyncStatusExplainsUnavailableGroup(t *testing.T) {
	db := openGatewayTestDB(t)
	groupsRepo := storage.NewGatewayGroups(db)
	routesRepo := storage.NewGatewayRoutes(db)
	group := &storage.GatewayGroup{Name: "empty", Status: storage.GatewayGroupStatusActive}
	if err := groupsRepo.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	svc := NewService(groupsRepo, storage.NewGatewayKeys(db), routesRepo, nil, nil, nil, nil, nil, nil)

	status, err := svc.GetRateSyncStatus(context.Background(), group.ID)
	if err != nil {
		t.Fatalf("get rate sync status: %v", err)
	}
	if status.Ready || status.ReasonCode != GatewayRateSyncReasonNoRoutes {
		t.Fatalf("status = %#v", status)
	}
}
