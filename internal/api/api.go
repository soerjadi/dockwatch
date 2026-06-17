// Package api serves the REST API, SSE endpoint, and inbound webhook routes.
//
// Endpoints:
//
//	GET  /healthz                — liveness probe
//	GET  /api/containers         — list all tracked containers + current state
//	POST /api/update/:id         — manually trigger an update check
//	POST /api/rollback/:id       — manually trigger a rollback
//	GET  /api/events             — SSE stream of all bus events (real-time UI)
//
// Webhook endpoints (inbound push from CI/CD — no polling needed):
//
//	POST /webhook/push           — generic push notification (custom payload)
//	POST /webhook/dockerhub      — native Docker Hub webhook format
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/notifier"
	"github.com/soerjadi/dockwatch/internal/registry"
	"github.com/soerjadi/dockwatch/internal/store"
	"github.com/soerjadi/dockwatch/internal/webhook"
)

// Server is the HTTP API server.
type Server struct {
	bus      *bus.Bus
	store    *store.Store
	notifier *notifier.Notifier
	registry *registry.Client
	log      *slog.Logger
	server   *http.Server
}

// New creates an API Server bound to addr (e.g. ":3010").
// webhookSecret is the HMAC-SHA256 shared secret for /webhook/push;
// pass "" to disable signature validation (dev only).
func New(addr string, b *bus.Bus, st *store.Store, n *notifier.Notifier, reg *registry.Client, webhookSecret string, log *slog.Logger) *Server {
	s := &Server{bus: b, store: st, notifier: n, registry: reg, log: log}

	mux := http.NewServeMux()

	// Core API
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/containers", s.handleContainers)
	mux.HandleFunc("/api/update/", s.handleUpdate)
	mux.HandleFunc("/api/rollback/", s.handleRollback)
	mux.HandleFunc("/api/events", s.handleSSE)

	// Inbound webhooks — CI/CD pushes here instead of dockwatch polling
	wh := webhook.New(b, st, webhookSecret, log)
	wh.RegisterRoutes(mux)

	s.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // SSE needs unlimited write timeout
	}
	return s
}

// Run starts the HTTP server. Blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.server.Shutdown(shutCtx)
	}()

	s.log.Info("api server listening", "addr", s.server.Addr)
	if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// handleHealthz is a simple liveness probe.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleContainers returns all tracked containers and their current state.
func (s *Server) handleContainers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	containers := s.store.All()
	type row struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Image  string `json:"image"`
		Digest string `json:"digest"`
	}

	out := make([]row, 0, len(containers))
	for _, c := range containers {
		out = append(out, row{
			ID:     c.ID,
			Name:   c.Name,
			Image:  c.Image,
			Digest: c.CurrentDigest(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// handleUpdate manually triggers an update check for a specific container.
func (s *Server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/update/")
	cs := s.store.Get(id)
	if cs == nil {
		http.Error(w, "container not found", http.StatusNotFound)
		return
	}

	current := cs.CurrentDigest()
	latest, err := s.registry.HeadDigest(r.Context(), cs.Image)
	if err != nil {
		s.log.Warn("handleUpdate: registry check failed", "container", cs.Name, "err", err)
		http.Error(w, "registry check failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	if latest == current {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "up to date", "container": cs.Name})
		return
	}
	s.bus.Publish(bus.TopicImageUpdated, bus.ImageUpdatedPayload{
		ContainerID:   cs.ID,
		ContainerName: cs.Name,
		Image:         cs.Image,
		OldDigest:     current,
		NewDigest:     latest,
		DetectedAt:    time.Now(),
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "update triggered", "container": cs.Name})
}

// handleRollback manually triggers a rollback for a specific container.
func (s *Server) handleRollback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/rollback/")
	cs := s.store.Get(id)
	if cs == nil {
		http.Error(w, "container not found", http.StatusNotFound)
		return
	}

	prev := cs.PreviousDigest()
	if prev == "" {
		http.Error(w, "no previous digest available for rollback", http.StatusConflict)
		return
	}

	s.bus.Publish(bus.TopicContainerUnhealthy, bus.ContainerUnhealthyPayload{
		ContainerID:   cs.ID,
		ContainerName: cs.Name,
		Image:         cs.Image,
		AppliedDigest: cs.CurrentDigest(),
		PrevDigest:    prev,
		FailedAt:      time.Now(),
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "rollback triggered", "container": cs.Name})
}

// handleSSE streams all bus events to the browser as Server-Sent Events.
// The browser connects once and receives real-time pushes — no polling needed.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch, remove := s.notifier.AddSSEClient()
	defer remove()

	s.log.Info("sse client connected", "remote", r.RemoteAddr)

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		}
	}
}
