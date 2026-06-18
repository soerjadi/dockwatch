// Package config loads dockwatch configuration from environment variables.
// There is no config file — everything is env-driven for container-native use.
package config

import (
	"os"
	"time"
)

// Config holds all runtime configuration for dockwatch.
type Config struct {
	// Addr is the address the HTTP/SSE server listens on.
	// Default: ":3010"
	Addr string

	// DockerHost overrides the Docker daemon endpoint (e.g.
	// "unix:///var/run/docker.sock" or "tcp://1.2.3.4:2376"). Empty means use
	// the standard DOCKER_HOST / default-socket resolution.
	DockerHost string

	// LogLevel controls log verbosity: debug, info, warn, error.
	// Default: "info"
	LogLevel string

	// RegistryCron is the fallback cron schedule for registry polling
	// (used when Docker Hub push events are not available).
	// Default: "0 0 4 * * *" (daily at 4 AM)
	RegistryCron string

	// HealthGrace is how long to observe a container post-update before
	// declaring it stable and clearing the rollback window.
	// Default: 30s
	HealthGrace time.Duration

	// DryRun disables all destructive operations (pull, restart, rollback).
	// Events are still published and logged.
	// Default: false
	DryRun bool

	// WebhookSecret is the HMAC-SHA256 shared secret used to validate
	// inbound webhook requests on POST /webhook/push.
	// Set to a long random string (e.g. openssl rand -hex 32).
	// If empty, signature validation is disabled — unsafe for production.
	WebhookSecret string

	// GitHubToken is an optional GitHub personal access token used when fetching
	// release notes for breaking-change enrichment. Without a token, the GitHub
	// API allows 60 unauthenticated requests per hour; with one, 5000 req/hr.
	GitHubToken string

	// HistoryDBPath is the path to the SQLite database used to persist update
	// history. The directory is created on startup if it doesn't exist.
	// Default: "/data/dockwatch.db"
	HistoryDBPath string

	// HistoryDir is the directory where compose file backups are stored before
	// each update so that rollbacks can restore the exact prior state.
	// Default: "/data/history"
	HistoryDir string

	// ZeroDTTimeout is how long to wait for a new container to become healthy
	// before aborting a zero-downtime update and rolling back.
	// Default: 60s
	ZeroDTTimeout time.Duration

	// AgentToken is the shared pre-authentication token used to authenticate
	// remote agents connecting to the controller's WebSocket endpoint.
	// If empty, any agent may connect (dev only — set in production).
	AgentToken string
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	c := &Config{
		Addr:          getEnv("DOCKWATCH_ADDR", ":3010"),
		DockerHost:    getEnv("DOCKWATCH_DOCKER_HOST", ""),
		LogLevel:      getEnv("DOCKWATCH_LOG_LEVEL", "info"),
		RegistryCron:  getEnv("DOCKWATCH_REGISTRY_CRON", "0 0 4 * * *"),
		HealthGrace:   getDuration("DOCKWATCH_HEALTH_GRACE", 30*time.Second),
		DryRun:        getBool("DOCKWATCH_DRY_RUN", false),
		WebhookSecret: getEnv("DOCKWATCH_WEBHOOK_SECRET", ""),
		GitHubToken:   getEnv("GITHUB_TOKEN", ""),
		HistoryDBPath: getEnv("DOCKWATCH_HISTORY_DB", "/data/dockwatch.db"),
		HistoryDir:    getEnv("DOCKWATCH_HISTORY_DIR", "/data/history"),
		ZeroDTTimeout: getDuration("DOCKWATCH_ZERO_DT_TIMEOUT", 60*time.Second),
		AgentToken:    getEnv("DOCKWATCH_AGENT_TOKEN", ""),
	}
	return c
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func getBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v == "true" || v == "1" || v == "yes"
}
