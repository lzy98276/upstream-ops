// 数据面：公开 HTTP 端点（models / count_tokens / gemini 等）。
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lzy98276/upstream-ops/backend/connector"
	"github.com/lzy98276/upstream-ops/backend/gateway/protocol"
	"github.com/lzy98276/upstream-ops/backend/storage"
)

// HandleModels 返回 OpenAI 风格模型列表。
func (rt *Runtime) HandleModels(c *gin.Context) {
	_ = rt.ensureGatewayRequestID(c)
	auth, err := rt.Authenticate(c)
	if err != nil {
		rt.writeAuthError(c, protocolOpenAI, err.Error())
		return
	}
	key, group := auth.Key, auth.Group
	_ = rt.Keys.TouchLastUsed(key.ID, time.Now())

	rt.modelsCacheMu.Lock()
	if ent, ok := rt.modelsCache[group.ID]; ok && time.Since(ent.at) < rt.gatewayRuntime().ModelsCacheTTL() {
		body := ent.body
		rt.modelsCacheMu.Unlock()
		c.Data(http.StatusOK, "application/json", body)
		return
	}
	rt.modelsCacheMu.Unlock()

	type modelObj struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	seen := map[string]struct{}{}
	data := make([]modelObj, 0)
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		data = append(data, modelObj{
			ID:      id,
			Object:  "model",
			Created: time.Now().Unix(),
			OwnedBy: "upstream-ops",
		})
	}

	mode := group.ModelsMode
	if mode == "" {
		mode = storage.GatewayModelsModeAuto
	}
	stored := rt.ParseModelsJSON(group.ModelsJSON)

	if mode == storage.GatewayModelsModeManual {
		for _, it := range stored {
			add(it.ID)
		}
	} else {
		// auto / hybrid: live aggregate
		routes, err := rt.Routes.ListByGroupID(group.ID)
		if err != nil {
			rt.writeGatewayError(c, protocolOpenAI, http.StatusInternalServerError, "api_error", err.Error())
			return
		}
		groupsByChannel := rt.loadGroupsByChannel(c.Request.Context(), routes)
		candidates := SortRoutes(routes, groupsByChannel, group.RateSortDirection, time.Now(), nil)
		groupMapping := ParseModelMapping(group.ModelMappingJSON)

		for _, cand := range candidates {
			route := cand.Route
			// 与 pullRouteModels / 转发一致：监控渠道与直连 provider 都走 resolveUpstreamTarget。
			// 旧逻辑只 FindByID(SourceChannelID)，直连路由 channel_id=0 会被整段跳过 → /v1/models 空列表。
			target, err := rt.resolveUpstreamTarget(&route)
			if err != nil {
				continue
			}
			// 拉模型：组+路由 UA，空则默认 UA（与模型测试一致；转发仍透传客户端）
			rt.applyRouteUserAgentForAdmin(target, group, &route)
			models, err := rt.fetchRouteModels(c.Request.Context(), target, route.UpstreamProtocol)
			if err != nil {
				continue
			}
			for _, m := range models {
				for _, id := range mappedClientModelIDs(m, ParseModelMapping(route.ModelMappingJSON), groupMapping) {
					add(id)
				}
			}
		}
		if mode == storage.GatewayModelsModeHybrid {
			for _, it := range stored {
				if it.Source == "custom" {
					add(it.ID)
				}
			}
		}
	}

	payload, _ := json.Marshal(gin.H{"object": "list", "data": data})
	rt.modelsCacheMu.Lock()
	rt.modelsCache[group.ID] = modelsCacheEntry{at: time.Now(), body: payload}
	rt.modelsCacheMu.Unlock()
	c.Data(http.StatusOK, "application/json", payload)
}

// fetchUpstreamModels 拉取上游 /v1/models。
// userAgent 为组+路由解析结果；空则回落默认 UA（无客户端可透传）。

func (rt *Runtime) fetchUpstreamModels(ctx context.Context, ch *storage.Channel, apiKey, userAgent string) ([]string, error) {
	client := rt.httpClientForChannel(ch)
	client.Timeout = 30 * time.Second
	url := joinUpstreamURL(ch.SiteURL, "/v1/models")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET %s: %w", url, err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", withDefaultUserAgent(userAgent, rt.defaultUpstreamUserAgent()))
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("GET %s: read body: %w", url, err)
	}
	if resp.StatusCode >= 400 {
		// 保留 URL + 状态码 + 上游 body 摘要，便于模型同步结果页排查（如 base_url 多写/少写 /v1）
		return nil, fmt.Errorf("GET %s: %w", url, connector.HTTPStatusError(resp.StatusCode, body))
	}
	var parsed struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		snippet := rt.extractUpstreamErrorSnippet(body)
		if snippet == "" {
			snippet = rt.truncateRunes(string(body), 200)
		}
		if snippet == "" {
			return nil, fmt.Errorf("GET %s: HTTP %d invalid models JSON: %w", url, resp.StatusCode, err)
		}
		return nil, fmt.Errorf("GET %s: HTTP %d invalid models JSON (%v): %s", url, resp.StatusCode, err, snippet)
	}
	out := make([]string, 0, len(parsed.Data))
	for _, d := range parsed.Data {
		if strings.TrimSpace(d.ID) != "" {
			out = append(out, d.ID)
		}
	}
	return out, nil
}

// HandleCountTokens POST /v1/messages/count_tokens
// 对齐 sub2api：优先透传上游；失败则本地粗估。

func (rt *Runtime) HandleCountTokens(c *gin.Context) {
	_ = rt.ensureGatewayRequestID(c)
	auth, err := rt.Authenticate(c)
	if err != nil {
		rt.writeAuthError(c, protocolAnthropic, err.Error())
		return
	}
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		rt.writeGatewayError(c, protocolAnthropic, http.StatusBadRequest, "invalid_request_error", "failed to read body")
		return
	}
	body, err = rt.applySystemPrompt(auth.Group, auth.Key, "/v1/messages/count_tokens", protocol.KindAnthropic, body)
	if err != nil {
		rt.writeGatewayError(c, protocolAnthropic, http.StatusBadRequest, "invalid_request_error", "system prompt injection failed: "+err.Error())
		return
	}
	// 尝试转发到支持该模型的第一条可用路由。count_tokens 也是一次
	// 模型请求，不能绕过严格模型路由后打到不具备该模型的上游。
	routes, _ := rt.Routes.ListByGroupID(auth.Group.ID)
	groupsByChannel := rt.loadGroupsByChannel(c.Request.Context(), routes)
	requestedModel := ExtractModelFromBody(body)
	cands := ResolveRoutesForModel(
		routes, groupsByChannel, auth.Group.RateSortDirection, time.Now(), nil,
		requestedModel, ParseModelMapping(auth.Group.ModelMappingJSON), auth.Group.ModelRoutingEnabled,
	)
	for _, cand := range cands {
		route := cand.Route
		target, rerr := rt.resolveUpstreamTarget(&route)
		if rerr != nil {
			continue
		}
		rt.applyRouteUserAgent(target, auth.Group, &route)
		upstreamModel := cand.UpstreamModel
		forwardBody := body
		if upstreamModel != "" && upstreamModel != requestedModel {
			forwardBody = RewriteModelInBody(body, upstreamModel)
		}
		status, _, respBody, _, ferr := rt.forwardOnce(
			c.Request.Context(), c, target, "/v1/messages/count_tokens",
			http.MethodPost, c.Request.Header, forwardBody, false, protocol.KindAnthropic, 0,
		)
		if ferr == nil && status >= 200 && status < 300 && len(respBody) > 0 {
			c.Data(status, "application/json", respBody)
			return
		}
	}
	// 本地粗估（字符/4）
	inputTokens := rt.estimateTokenCount(body)
	c.JSON(http.StatusOK, gin.H{
		"input_tokens": inputTokens,
		"type":         "message_count_tokens_result",
	})
}

// HandleUsage 用量相关端点。
func (rt *Runtime) HandleUsage(c *gin.Context) {
	_ = rt.ensureGatewayRequestID(c)
	auth, err := rt.Authenticate(c)
	if err != nil {
		rt.writeAuthError(c, protocolOpenAI, err.Error())
		return
	}
	if rt.Usage == nil {
		c.JSON(http.StatusOK, gin.H{"object": "list", "data": []any{}})
		return
	}
	page, err := rt.Usage.List(storage.GatewayUsageQuery{
		GatewayKeyID: auth.Key.ID,
		Page:         1,
		PageSize:     50,
	})
	if err != nil {
		rt.writeGatewayError(c, protocolOpenAI, http.StatusInternalServerError, "api_error", err.Error())
		return
	}
	stats, _ := rt.Usage.Stats(storage.GatewayUsageQuery{GatewayKeyID: auth.Key.ID})
	c.JSON(http.StatusOK, gin.H{
		"object": "usage",
		"key_id": auth.Key.ID,
		"stats":  stats,
		"recent": page.Items,
	})
}

// HandleGeminiModels GET /v1beta/models

func (rt *Runtime) HandleGeminiModels(c *gin.Context) {
	// 复用 OpenAI models 列表，包装为 Gemini 风格（简化兼容）
	reqID := rt.ensureGatewayRequestID(c)
	auth, err := rt.Authenticate(c)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{
			"error":                     gin.H{"message": err.Error(), "code": 401},
			jsonKeyUpstreamOpsRequestID: reqID,
		})
		return
	}
	// 调内部 HandleModels 逻辑太重，直接读组 models
	group := auth.Group
	list := rt.ParseModelsJSON(group.ModelsJSON)
	if len(list) == 0 && rt.Routes != nil {
		// Before the first explicit sync, still expose a native Gemini list by
		// querying the configured routes. This path is display-only; request
		// dispatch remains strict once a group has been synced.
		routes, _ := rt.Routes.ListByGroupID(group.ID)
		groupMapping := ParseModelMapping(group.ModelMappingJSON)
		seen := map[string]struct{}{}
		for _, route := range routes {
			target, targetErr := rt.resolveUpstreamTarget(&route)
			if targetErr != nil || !route.Enabled {
				continue
			}
			rt.applyRouteUserAgentForAdmin(target, group, &route)
			models, modelsErr := rt.fetchRouteModels(c.Request.Context(), target, route.UpstreamProtocol)
			if modelsErr != nil {
				continue
			}
			for _, upstream := range models {
				for _, publicID := range mappedClientModelIDs(upstream, ParseModelMapping(route.ModelMappingJSON), groupMapping) {
					if _, ok := seen[publicID]; ok {
						continue
					}
					seen[publicID] = struct{}{}
					list = append(list, ModelListItem{ID: publicID, Source: "sync"})
				}
			}
		}
	}
	models := make([]gin.H, 0, len(list))
	for _, m := range list {
		name := "models/" + m.ID
		models = append(models, gin.H{
			"name":                       name,
			"displayName":                m.ID,
			"supportedGenerationMethods": []string{"generateContent", "countTokens"},
		})
	}
	c.JSON(http.StatusOK, gin.H{"models": models})
}

// HandleGeminiGenerate keeps the native Gemini request shape all the way to
// routing. It can therefore reach a native Gemini upstream directly or be
// converted only when the selected route uses a different protocol.
func (rt *Runtime) HandleGeminiGenerate(c *gin.Context) {
	action := strings.TrimPrefix(c.Param("modelAction"), "/")
	action = strings.TrimPrefix(action, "models/")
	if protocol.GeminiModelFromPath("/models/"+action) == "" || protocol.GeminiActionFromPath(action) == "" {
		rt.writeGatewayError(c, protocol.KindGemini, http.StatusBadRequest, "invalid_request_error", "invalid Gemini model action")
		return
	}
	rt.HandleForward(c, "/v1beta/models/"+action, protocol.KindGemini)
}

// fetchRouteModels uses the route's actual protocol and authentication style.
// Native Gemini returns {models:[{name:"models/..."}]}; other routes retain
// the OpenAI-compatible /v1/models shape.
func (rt *Runtime) fetchRouteModels(ctx context.Context, target *upstreamTarget, routeProtocol string) ([]string, error) {
	if target == nil {
		return nil, fmt.Errorf("upstream target is nil")
	}
	if strings.EqualFold(strings.TrimSpace(routeProtocol), storage.GatewayUpstreamProtocolAuto) && target.Provider != nil {
		routeProtocol = target.Provider.UpstreamProtocol
	}
	upstreamKind := protocol.ResolveUpstream(routeProtocol, protocol.KindOpenAIChat, "")
	path := "/v1/models"
	if upstreamKind == protocol.KindGemini {
		path = "/v1beta/models"
	}
	req, err := rt.buildUpstreamHTTPRequest(ctx, target, path, http.MethodGet, http.Header{}, nil, upstreamKind, false)
	if err != nil {
		return nil, fmt.Errorf("build GET %s: %w", path, err)
	}
	req.Header.Set("Accept", "application/json")
	client := rt.httpClientForTarget(target.Channel, target.Provider)
	client.Timeout = 30 * time.Second
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", req.URL.String(), err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("GET %s: read body: %w", req.URL.String(), err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("GET %s: %w", req.URL.String(), connector.HTTPStatusError(resp.StatusCode, body))
	}
	var models []string
	if upstreamKind == protocol.KindGemini {
		var parsed struct {
			Models []struct {
				Name string `json:"name"`
			} `json:"models"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("GET %s: invalid Gemini models JSON: %w", req.URL.String(), err)
		}
		for _, item := range parsed.Models {
			name := strings.TrimPrefix(strings.TrimSpace(item.Name), "models/")
			if name != "" {
				models = append(models, name)
			}
		}
	} else {
		var parsed struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("GET %s: invalid models JSON: %w", req.URL.String(), err)
		}
		for _, item := range parsed.Data {
			if id := strings.TrimSpace(item.ID); id != "" {
				models = append(models, id)
			}
		}
	}
	return models, nil
}

// JSON / Header 中的网关请求 ID 字段（与使用记录 request_id 一致，便于排查）
const (
	ctxKeyUpstreamOpsRequestID  = "upstream_ops_request_id"
	headerUpstreamOpsRequestID  = "X-Upstream-Ops-Request-Id"
	jsonKeyUpstreamOpsRequestID = "upstream_ops_request_id"
)

// clientRequestIDHeaders 客户端请求相关 ID 头：原样透传到上游，网关不改写、不采纳为 usage.request_id。
var clientRequestIDHeaders = []string{
	"X-Request-Id",
	"X-Client-Request-Id",
	"X-Openai-Request-Id",
	"X-Correlation-Id",
	"Request-Id",
}

// copyClientRequestIDHeaders 把入站请求相关 ID 原样写到上游请求；不碰 X-Upstream-Ops-Request-Id。
