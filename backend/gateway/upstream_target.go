// 上游转发目标（baseURL / key / channel / provider）。
package gateway

import (
	"strings"

	"github.com/lzy98276/upstream-ops/backend/storage"
)

type upstreamTarget struct {
	BaseURL  string
	APIKey   string
	Channel  *storage.Channel
	Provider *storage.GatewayProvider
	// UserAgentOverride 非空时覆盖发往上游的 User-Agent（组+路由策略解析结果）。
	UserAgentOverride string
}

// joinUpstreamURL accepts provider base URLs both with and without their API
// version suffix. This matters for xAI's documented https://api.x.ai/v1 as
// well as Gemini's unversioned generativelanguage.googleapis.com base.
func joinUpstreamURL(baseURL, path string) string {
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	for _, version := range []string{"/v1beta", "/v1"} {
		if strings.HasSuffix(base, version) && (path == version || strings.HasPrefix(path, version+"/")) {
			return base + strings.TrimPrefix(path, version)
		}
	}
	return base + path
}
