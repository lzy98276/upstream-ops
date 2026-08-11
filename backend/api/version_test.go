package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/lzy98276/upstream-ops/backend/global"
)

func TestIsVersionNewer(t *testing.T) {
	tests := []struct {
		latest  string
		current string
		want    bool
	}{
		{latest: "0.2.1", current: "0.2.0", want: true},
		{latest: "v0.2.1", current: "0.2.0", want: true},
		{latest: "0.2.0", current: "v0.2.0", want: false},
		{latest: "0.1.9", current: "0.2.0", want: false},
	}
	for _, tt := range tests {
		if got := isVersionNewer(tt.latest, tt.current); got != tt.want {
			t.Fatalf("isVersionNewer(%q, %q) = %v, want %v", tt.latest, tt.current, got, tt.want)
		}
	}
}

func TestVersionEndpointReportsUpdate(t *testing.T) {
	gin.SetMode(gin.TestMode)

	withGitHubReleaseServer(t, http.StatusOK, `{"tag_name":"v999.0.0","name":"Version 999","body":"- Important fix","published_at":"2026-08-11T00:00:00Z","html_url":"https://github.com/lzy98276/upstream-ops/releases/tag/v999.0.0"}`)
	resp := requestVersion(t)

	if !resp.UpdateAvailable {
		t.Fatalf("update_available = false, want true")
	}
	if resp.LatestVersion != "v999.0.0" {
		t.Fatalf("latest_version = %q, want v999.0.0", resp.LatestVersion)
	}
	if resp.ReleaseURL == "" {
		t.Fatalf("release_url is empty")
	}
	if resp.ReleaseName != "Version 999" || resp.ReleaseNotes != "- Important fix" || resp.PublishedAt != "2026-08-11T00:00:00Z" {
		t.Fatalf("release metadata = %#v, want latest release metadata", resp)
	}
}

func TestVersionEndpointReportsNoUpdate(t *testing.T) {
	gin.SetMode(gin.TestMode)

	withGitHubReleaseServer(t, http.StatusOK, `{"tag_name":"`+global.VERSION+`","html_url":"https://github.com/lzy98276/upstream-ops/releases/tag/v`+global.VERSION+`"}`)
	resp := requestVersion(t)

	if resp.UpdateAvailable {
		t.Fatalf("update_available = true, want false")
	}
	if resp.LatestVersion != global.VERSION {
		t.Fatalf("latest_version = %q, want %s", resp.LatestVersion, global.VERSION)
	}
}

func TestVersionEndpointKeepsResponseOnGitHubError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	withGitHubReleaseServer(t, http.StatusInternalServerError, `{"message":"error"}`)
	resp := requestVersion(t)

	if resp.UpdateAvailable {
		t.Fatalf("update_available = true, want false")
	}
	if strings.TrimSpace(resp.UpdateError) == "" {
		t.Fatalf("update_error is empty")
	}
	if resp.Version != global.VERSION {
		t.Fatalf("version = %q, want %s", resp.Version, global.VERSION)
	}
}

func TestVersionEndpointFallsBackToLatestTagWhenReleaseIsMissing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "tags") {
			_, _ = w.Write([]byte(`[{"name":"v999.0.0"},{"name":"v0.0.13-lzy"},{"name":"not-a-version"}]`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"Not Found"}`))
	}))
	t.Cleanup(srv.Close)

	oldReleaseURL := githubLatestReleaseURL
	oldTagsURL := githubTagsURL
	oldClient := githubReleaseClient
	githubLatestReleaseURL = srv.URL + "/releases/latest"
	githubTagsURL = srv.URL + "/tags"
	githubReleaseClient = srv.Client()
	t.Cleanup(func() {
		githubLatestReleaseURL = oldReleaseURL
		githubTagsURL = oldTagsURL
		githubReleaseClient = oldClient
	})

	resp := requestVersion(t)
	if !resp.UpdateAvailable || resp.LatestVersion != "v999.0.0" {
		t.Fatalf("tag fallback response = %#v, want v999.0.0 update", resp)
	}
	if resp.ReleaseNotes != "" {
		t.Fatalf("tag fallback release notes = %q, want empty", resp.ReleaseNotes)
	}
}

func withGitHubReleaseServer(t *testing.T, status int, body string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	oldURL := githubLatestReleaseURL
	oldClient := githubReleaseClient
	githubLatestReleaseURL = srv.URL
	githubReleaseClient = srv.Client()
	t.Cleanup(func() {
		githubLatestReleaseURL = oldURL
		githubReleaseClient = oldClient
	})
}

func requestVersion(t *testing.T) versionResponse {
	t.Helper()
	r := gin.New()
	registerVersion(r.Group("/api"), &Deps{})

	req := httptest.NewRequest(http.MethodGet, "/api/version", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp versionResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return resp
}
