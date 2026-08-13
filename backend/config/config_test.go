package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesUpstreamDefaults(t *testing.T) {
	cfg, err := LoadFile(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Upstream.TimeoutSeconds != DefaultUpstreamTimeoutSeconds {
		t.Fatalf("timeout seconds = %d", cfg.Upstream.TimeoutSeconds)
	}
	if cfg.Upstream.UserAgent != DefaultUpstreamUserAgent {
		t.Fatalf("user agent = %q", cfg.Upstream.UserAgent)
	}
}

func TestUpstreamConfigWithDefaultsKeepsCustomUserAgent(t *testing.T) {
	cfg := UpstreamConfig{
		TimeoutSeconds: 0,
		UserAgent:      "custom-agent",
	}.WithDefaults()
	if cfg.TimeoutSeconds != DefaultUpstreamTimeoutSeconds {
		t.Fatalf("timeout seconds = %d", cfg.TimeoutSeconds)
	}
	if cfg.UserAgent != "custom-agent" {
		t.Fatalf("user agent = %q", cfg.UserAgent)
	}
}

func TestGatewayConfigWithDefaults(t *testing.T) {
	cfg := GatewayConfig{}.WithDefaults()
	if cfg.TempPauseSeconds != DefaultGatewayTempPauseSeconds {
		t.Fatalf("temp pause = %d", cfg.TempPauseSeconds)
	}
	if cfg.ForwardTimeoutSeconds != DefaultGatewayForwardTimeoutSeconds {
		t.Fatalf("forward timeout = %d", cfg.ForwardTimeoutSeconds)
	}
	if cfg.RouteBatchConcurrency != DefaultGatewayRouteBatchConcurrency {
		t.Fatalf("batch concurrency = %d", cfg.RouteBatchConcurrency)
	}
	custom := GatewayConfig{RouteBatchConcurrency: 16, ForwardTimeoutSeconds: 120}.WithDefaults()
	if custom.RouteBatchConcurrency != 16 || custom.ForwardTimeoutSeconds != 120 {
		t.Fatalf("custom = %#v", custom)
	}
	if custom.ModelsCacheTTLSeconds != DefaultGatewayModelsCacheTTLSeconds {
		t.Fatalf("models cache ttl = %d", custom.ModelsCacheTTLSeconds)
	}
}

func TestLoadKeepsPersistedAuthConfigWhenAuthEnvironmentIsSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	persisted := &Config{
		Auth: AuthConfig{
			Enabled:         true,
			Username:        "saved-admin",
			Password:        "saved-password",
			TokenSecret:     "saved-token-secret",
			SessionTTLHours: 48,
		},
	}
	if err := Save(path, persisted); err != nil {
		t.Fatalf("Save: %v", err)
	}

	for key, value := range map[string]string{
		"AUTH_ENABLED":      "false",
		"ADMIN_USERNAME":    "admin",
		"ADMIN_PASSWORD":    "",
		"AUTH_TOKEN_SECRET": "",
	} {
		oldValue, wasSet := os.LookupEnv(key)
		if err := os.Setenv(key, value); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
		t.Cleanup(func() {
			if wasSet {
				_ = os.Setenv(key, oldValue)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth != persisted.Auth {
		t.Fatalf("auth = %#v, want %#v", cfg.Auth, persisted.Auth)
	}
}
