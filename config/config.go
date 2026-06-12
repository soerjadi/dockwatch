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
	// Default: ":3000"
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
}

// Load reads configuration from environment variables with sensible defaults.
func Load() *Config {
	c := &Config{
		Addr:          getEnv("DOCKWATCH_ADDR", ":3000"),
		DockerHost:    getEnv("DOCKWATCH_DOCKER_HOST", ""),
		LogLevel:      getEnv("DOCKWATCH_LOG_LEVEL", "info"),
		RegistryCron:  getEnv("DOCKWATCH_REGISTRY_CRON", "0 0 4 * * *"),
		HealthGrace:   getDuration("DOCKWATCH_HEALTH_GRACE", 30*time.Second),
		DryRun:        getBool("DOCKWATCH_DRY_RUN", false),
		WebhookSecret: getEnv("DOCKWATCH_WEBHOOK_SECRET", ""),
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
