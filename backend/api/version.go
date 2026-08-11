package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lzy98276/upstream-ops/backend/config"
	"github.com/lzy98276/upstream-ops/backend/global"
)

const (
	githubRepoURL              = "https://github.com/lzy98276/upstream-ops"
	defaultGitHubLatestRelease = "https://api.github.com/repos/lzy98276/upstream-ops/releases/latest"
	defaultGitHubTagsURL       = "https://api.github.com/repos/lzy98276/upstream-ops/tags?per_page=100"
)

var (
	githubLatestReleaseURL = defaultGitHubLatestRelease
	githubTagsURL          = defaultGitHubTagsURL
	githubReleaseClient    = &http.Client{Timeout: 2 * time.Second}
)

type versionResponse struct {
	Name            string `json:"name"`
	Title           string `json:"title"`
	Version         string `json:"version"`
	LatestVersion   string `json:"latest_version"`
	UpdateAvailable bool   `json:"update_available"`
	RepoURL         string `json:"repo_url"`
	ReleaseURL      string `json:"release_url"`
	ReleaseName     string `json:"release_name"`
	ReleaseNotes    string `json:"release_notes"`
	PublishedAt     string `json:"published_at"`
	UpdateError     string `json:"update_error"`
}

type githubReleaseResponse struct {
	TagName     string `json:"tag_name"`
	Name        string `json:"name"`
	Body        string `json:"body"`
	PublishedAt string `json:"published_at"`
	HTMLURL     string `json:"html_url"`
}

type githubTagResponse struct {
	Name string `json:"name"`
}

type latestGitHubRelease struct {
	Version     string
	URL         string
	Name        string
	Notes       string
	PublishedAt string
}

func registerVersion(api *gin.RouterGroup, d *Deps) {
	api.GET("/version", func(c *gin.Context) {
		force := c.Query("force") == "1" || strings.EqualFold(c.Query("force"), "true")
		c.JSON(http.StatusOK, buildVersionResponse(c.Request.Context(), d, force))
	})
}

func buildVersionResponse(ctx context.Context, d *Deps, force bool) versionResponse {
	app := config.AppConfig{Title: "UpstreamOps"}
	proxyCfg := config.ProxyConfig{}
	if d != nil && d.Runtime != nil {
		if cfg, err := config.LoadFile(d.Runtime.ConfigPath()); err == nil {
			app = cfg.App
		}
		proxyCfg = d.Runtime.CurrentProxy()
	}

	resp := versionResponse{
		Name:    "upstream-ops",
		Title:   app.Title,
		Version: global.VERSION,
		RepoURL: githubRepoURL,
	}

	release, err := fetchLatestGitHubRelease(ctx, versionCheckClient(proxyCfg, force))
	if err != nil {
		resp.UpdateError = err.Error()
		return resp
	}
	resp.LatestVersion = release.Version
	resp.ReleaseURL = release.URL
	resp.ReleaseName = release.Name
	resp.ReleaseNotes = release.Notes
	resp.PublishedAt = release.PublishedAt
	resp.UpdateAvailable = isVersionNewer(release.Version, global.VERSION)
	return resp
}

func versionCheckClient(proxyCfg config.ProxyConfig, force bool) *http.Client {
	if !proxyCfg.VersionCheckEnabled && !force {
		return githubReleaseClient
	}
	proxyURL, err := proxyCfg.ActiveURL()
	if err != nil || proxyURL == "" {
		return githubReleaseClient
	}
	u, err := url.Parse(proxyURL)
	if err != nil {
		return githubReleaseClient
	}
	return &http.Client{
		Timeout: githubReleaseClient.Timeout,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(u),
		},
	}
}

func fetchLatestGitHubRelease(ctx context.Context, client *http.Client) (*latestGitHubRelease, error) {
	if client == nil {
		client = githubReleaseClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubLatestReleaseURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "upstream-ops")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fetchLatestGitHubTag(ctx, client)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("github latest release status %d", resp.StatusCode)
	}

	var release githubReleaseResponse
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, err
	}
	if strings.TrimSpace(release.TagName) == "" {
		return nil, errors.New("github latest release missing tag_name")
	}
	if strings.TrimSpace(release.HTMLURL) == "" {
		release.HTMLURL = githubRepoURL
	}
	return &latestGitHubRelease{
		Version:     release.TagName,
		URL:         release.HTMLURL,
		Name:        release.Name,
		Notes:       release.Body,
		PublishedAt: release.PublishedAt,
	}, nil
}

// fetchLatestGitHubTag is used for repositories that publish container images
// from Git tags but have not created GitHub Release objects yet.
func fetchLatestGitHubTag(ctx context.Context, client *http.Client) (*latestGitHubRelease, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, githubTagsURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "upstream-ops")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("github tags status %d", resp.StatusCode)
	}

	var tags []githubTagResponse
	if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
		return nil, err
	}

	latest := ""
	for _, tag := range tags {
		if _, ok := parseVersion(tag.Name); !ok {
			continue
		}
		if latest == "" || compareVersions(tag.Name, latest) > 0 {
			latest = tag.Name
		}
	}
	if latest == "" {
		return nil, errors.New("github tags contain no semantic version")
	}

	return &latestGitHubRelease{
		Version: latest,
		URL:     githubRepoURL + "/tree/" + url.PathEscape(strings.TrimSpace(latest)),
		Name:    latest,
	}, nil
}

func isVersionNewer(latest, current string) bool {
	return compareVersions(latest, current) > 0
}

func compareVersions(left, right string) int {
	lv, lok := parseVersion(left)
	rv, rok := parseVersion(right)
	if !lok || !rok {
		return 0
	}
	for i := range lv {
		if lv[i] > rv[i] {
			return 1
		}
		if lv[i] < rv[i] {
			return -1
		}
	}
	return 0
}

func parseVersion(v string) ([3]int, bool) {
	var out [3]int
	v = strings.TrimSpace(strings.TrimPrefix(v, "v"))
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return out, false
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return out, false
		}
		out[i] = n
	}
	return out, true
}
