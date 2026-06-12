// dockwatch — event-driven, in-memory Docker container update manager.
//
// Architecture summary:
//
//	Docker daemon ──event stream──► Watcher
//	Registry      ──HEAD manifest──► Registry Poller (fallback cron)
//	                                      │
//	                                      ▼
//	                            In-Memory Event Bus
//	                         (Go channels, no broker)
//	                     ┌─────────┬──────────┬─────────┐
//	                     ▼         ▼          ▼         ▼
//	                 Executor  HealthMon  Rollback  Notifier
//	                     │         │          │         │
//	                     └─────────┴──────────┴─────────►  API / SSE / Logs
//
// Single binary. No Kafka. No MQTT. No Redis. No polling (except registry fallback).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/soerjadi/dockwatch/config"
	"github.com/soerjadi/dockwatch/internal/api"
	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/dockerclient"
	"github.com/soerjadi/dockwatch/internal/executor"
	"github.com/soerjadi/dockwatch/internal/healthmon"
	"github.com/soerjadi/dockwatch/internal/notifier"
	"github.com/soerjadi/dockwatch/internal/rollback"
	"github.com/soerjadi/dockwatch/internal/store"
	"github.com/soerjadi/dockwatch/internal/watcher"
)

func main() {
	cfg := config.Load()

	// ── Logger ────────────────────────────────────────────────────────────────
	logLevel := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(log)
	log.Info("dockwatch starting",
		"addr", cfg.Addr,
		"log_level", cfg.LogLevel,
		"dry_run", cfg.DryRun,
	)

	// ── Context — shut down cleanly on SIGINT / SIGTERM ───────────────────────
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// ── Core wiring ───────────────────────────────────────────────────────────
	b := bus.New()
	st := store.New()

	// Scoped Docker client — the ONLY surface that touches the daemon. Every
	// component below receives this same client; none can call anything the
	// dockerclient.Scoped interface doesn't expose.
	dock, err := dockerclient.New(cfg.DockerHost, cfg.DryRun)
	if err != nil {
		log.Error("failed to init docker client", "err", err)
		os.Exit(1)
	}
	defer dock.Close()

	w := watcher.New(dock, b, st, log)

	exec := executor.New(dock, b, st, log)
	hmon := healthmon.New(dock, b, st, log)
	rb := rollback.New(dock, b, st, log)
	ntfy := notifier.New(b, log)
	srv := api.New(cfg.Addr, b, st, ntfy, cfg.WebhookSecret, log)

	// ── Run all components concurrently ───────────────────────────────────────
	var wg sync.WaitGroup

	run := func(name string, fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("component started", "name", name)
			fn()
			log.Info("component stopped", "name", name)
		}()
	}

	run("watcher", func() {
		if err := w.Run(ctx); err != nil && err != context.Canceled {
			log.Error("watcher error", "err", err)
			cancel()
		}
	})
	run("executor", func() { exec.Run(ctx) })
	run("healthmon", func() { hmon.Run(ctx) })
	run("rollback", func() { rb.Run(ctx) })
	run("notifier", func() { ntfy.Run(ctx) })
	run("api", func() {
		if err := srv.Run(ctx); err != nil {
			log.Error("api server error", "err", err)
			cancel()
		}
	})

	log.Info("all components running — send SIGINT or SIGTERM to stop")
	wg.Wait()
	log.Info("dockwatch stopped cleanly")
}
