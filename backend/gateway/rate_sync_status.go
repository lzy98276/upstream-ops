package gateway

import (
	"context"
	"math"
	"strings"

	"github.com/lzy98276/upstream-ops/backend/storage"
)

const (
	GatewayRateSyncReasonGroupDisabled        = "group_disabled"
	GatewayRateSyncReasonNoRoutes             = "no_routes"
	GatewayRateSyncReasonNoEnabledRoutes      = "no_enabled_routes"
	GatewayRateSyncReasonNoPositiveRateRoutes = "no_positive_rate_routes"
)

// RateSyncStatus describes whether a gateway group can provide a billing
// multiplier for downstream group-rate synchronization. Its rates use the
// same live source-group resolution as gateway scheduling.
type RateSyncStatus struct {
	GroupID                uint    `json:"group_id"`
	GroupName              string  `json:"group_name"`
	GroupStatus            string  `json:"group_status"`
	RouteCount             int     `json:"route_count"`
	EnabledRouteCount      int     `json:"enabled_route_count"`
	PositiveRateRouteCount int     `json:"positive_rate_route_count"`
	MinRate                float64 `json:"min_rate"`
	MaxRate                float64 `json:"max_rate"`
	Ready                  bool    `json:"ready"`
	ReasonCode             string  `json:"reason_code,omitempty"`
	Reason                 string  `json:"reason,omitempty"`
}

// GetRateSyncStatus returns a single source of truth for gateway-rate sync
// validation, UI preview, and the multiplier selected during application.
func (a *AdminService) GetRateSyncStatus(ctx context.Context, groupID uint) (*RateSyncStatus, error) {
	group, err := a.Groups.FindByID(groupID)
	if err != nil {
		return nil, err
	}
	status := &RateSyncStatus{
		GroupID:     group.ID,
		GroupName:   group.Name,
		GroupStatus: group.Status,
	}
	if group.Status != storage.GatewayGroupStatusActive {
		status.ReasonCode = GatewayRateSyncReasonGroupDisabled
		status.Reason = "gateway group is disabled"
		return status, nil
	}

	routes, err := a.Routes.ListByGroupID(groupID)
	if err != nil {
		return nil, err
	}
	status.RouteCount = len(routes)
	if len(routes) == 0 {
		status.ReasonCode = GatewayRateSyncReasonNoRoutes
		status.Reason = "gateway group has no routes"
		return status, nil
	}

	groupsByChannel := a.loadGroupsByChannel(ctx, routes)
	for i := range routes {
		route := &routes[i]
		if !route.Enabled {
			continue
		}
		status.EnabledRouteCount++
		rate := RateForRoute(route, groupsByChannel[route.SourceChannelID])
		if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
			continue
		}
		if status.PositiveRateRouteCount == 0 || rate < status.MinRate {
			status.MinRate = rate
		}
		if rate > status.MaxRate {
			status.MaxRate = rate
		}
		status.PositiveRateRouteCount++
	}
	if status.EnabledRouteCount == 0 {
		status.ReasonCode = GatewayRateSyncReasonNoEnabledRoutes
		status.Reason = "gateway group has no enabled routes"
		return status, nil
	}
	if status.PositiveRateRouteCount == 0 {
		status.ReasonCode = GatewayRateSyncReasonNoPositiveRateRoutes
		status.Reason = "gateway group has no enabled routes with a positive effective billing multiplier"
		return status, nil
	}
	status.Ready = true
	return status, nil
}

func (s *Service) GetRateSyncStatus(ctx context.Context, groupID uint) (*RateSyncStatus, error) {
	return s.admin().GetRateSyncStatus(ctx, groupID)
}

func (s *RateSyncStatus) Error() string {
	if s == nil {
		return "gateway rate sync status is unavailable"
	}
	name := strings.TrimSpace(s.GroupName)
	if name == "" {
		name = "selected gateway group"
	}
	if s.Reason != "" {
		return name + ": " + s.Reason
	}
	return name + ": gateway rate synchronization is unavailable"
}
