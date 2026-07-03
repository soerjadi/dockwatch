// Package api serves the REST API, SSE endpoint, and inbound webhook routes.
//
// Endpoints:
//
//	GET  /healthz                        — liveness probe
//	GET  /api/containers                 — list all tracked containers + current state
//	POST /api/update/:id                 — manually trigger an update check
//	POST /api/rollback/:id               — manually trigger a rollback
//	GET  /api/events                     — SSE stream of all bus events (real-time UI)
//	GET  /api/history                    — list update history (optional ?service=<name>)
//	GET  /api/agents                     — list connected remote agents
//	POST /api/agents/:hostname/update    — dispatch update command to a remote agent
//	GET  /agent/connect                  — WebSocket upgrade endpoint for agents
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
	"strconv"
	"strings"
	"time"

	"github.com/soerjadi/dockwatch/internal/agentproto"
	"github.com/soerjadi/dockwatch/internal/agentserver"
	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/deploy"
	"github.com/soerjadi/dockwatch/internal/history"
	"github.com/soerjadi/dockwatch/internal/notifier"
	"github.com/soerjadi/dockwatch/internal/registry"
	"github.com/soerjadi/dockwatch/internal/serviceconfig"
	"github.com/soerjadi/dockwatch/internal/store"
	"github.com/soerjadi/dockwatch/internal/webhook"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

// Server is the HTTP API server.
type Server struct {
	bus         *bus.Bus
	store       *store.Store
	notifier    *notifier.Notifier
	registry    *registry.Client
	history     *history.Store
	config      *serviceconfig.Manager
	agentHub    *agentserver.Server
	jobRegistry *deploy.JobRegistry
	log         *slog.Logger
	server      *http.Server
	startedAt   time.Time
}

// New creates an API Server bound to addr (e.g. ":3010").
// webhookSecret is the HMAC-SHA256 shared secret for /webhook/push;
// pass "" to disable signature validation (dev only).
// jobRegistry tracks in-flight deploys and is exposed via GET /api/deploys/:id/status.
func New(addr string, b *bus.Bus, st *store.Store, n *notifier.Notifier, reg *registry.Client, hist *history.Store, config *serviceconfig.Manager, agentHub *agentserver.Server, webhookSecret string, log *slog.Logger, jobRegistry *deploy.JobRegistry) *Server {
	s := &Server{bus: b, store: st, notifier: n, registry: reg, history: hist, config: config, agentHub: agentHub, jobRegistry: jobRegistry, log: log, startedAt: time.Now()}

	mux := http.NewServeMux()

	// Core API
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/api/containers", s.handleContainers)
	mux.HandleFunc("/api/update/", s.handleUpdate)
	mux.HandleFunc("/api/rollback/", s.handleRollback)
	mux.HandleFunc("/api/history", s.handleHistory)
	mux.HandleFunc("/api/agents", s.handleAgents)
	mux.HandleFunc("/api/agents/", s.handleAgentDispatch)
	mux.Handle("/agent/connect", s.agentHub)
	mux.HandleFunc("/api/events", s.handleSSE)
	mux.HandleFunc("/api/deploys/", s.handleDeployStatus)
	mux.HandleFunc("GET /api/deploys/{deploy_id}/logs", s.handleDeployLogs)
	
	// Services Config
	mux.HandleFunc("GET /api/services", s.handleGetServices)
	mux.HandleFunc("POST /api/services", s.handleCreateService)
	mux.HandleFunc("GET /api/services/{name}", s.handleGetService)
	mux.HandleFunc("PATCH /api/services/{name}/config", s.handlePatchServiceConfig)

	// Inbound webhooks — CI/CD pushes here instead of dockwatch polling
	wh := webhook.New(b, st, webhookSecret, log, jobRegistry, config, hist)
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
	
	activeDeploys := 0
	if s.jobRegistry != nil {
		activeDeploys = s.jobRegistry.ActiveCount()
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":         "ok",
		"active_deploys": activeDeploys,
		"uptime_seconds": int64(time.Since(s.startedAt).Seconds()),
	})
}

// handleDeployStatus handles GET /api/deploys/:id/status.
// Returns the current status snapshot for a deploy job (AC#2: ≤50 ms).
//
//	GET /api/deploys/{deploy_id}/status
//
// Response:
//
//	{"id":"...","status":"running","started_at":"...","log_count":42}
func (s *Server) handleDeployStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.jobRegistry == nil {
		http.Error(w, "deploy tracking not available", http.StatusServiceUnavailable)
		return
	}

	// Path: /api/deploys/{id}/status — extract the UUID segment.
	path := strings.TrimPrefix(r.URL.Path, "/api/deploys/")
	parts := strings.SplitN(path, "/", 2)
	id := parts[0]
	if id == "" {
		http.Error(w, "deploy id required", http.StatusBadRequest)
		return
	}

	job, ok := s.jobRegistry.Get(id)
	if !ok {
		http.Error(w, "deploy not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(job.Snap())
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

// handleHistory returns paginated update history, optionally filtered by service.
//
//	GET /api/history?service=myapp&limit=20&offset=0
//
// Response: {"total":N,"limit":20,"offset":0,"items":[...]}
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.history == nil {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"total": 0, "limit": 50, "offset": 0, "items": []struct{}{}})
		return
	}

	q := r.URL.Query()
	service := q.Get("service")
	limit := parseIntParam(q.Get("limit"), 50)
	offset := parseIntParam(q.Get("offset"), 0)

	total, err := s.history.Count(service)
	if err != nil {
		s.log.Error("handleHistory: count failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	entries, err := s.history.List(service, limit, offset)
	if err != nil {
		s.log.Error("handleHistory: db query failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if entries == nil {
		entries = []history.Entry{}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"total":  total,
		"limit":  limit,
		"offset": offset,
		"items":  entries,
	})
}

func parseIntParam(s string, defaultVal int) int {
	if s == "" {
		return defaultVal
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return defaultVal
	}
	return v
}

// handleAgents lists all currently connected remote agents.
func (s *Server) handleAgents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	agents := s.agentHub.ListAgents()
	if agents == nil {
		agents = []agentserver.AgentInfo{}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(agents)
}

// handleAgentDispatch dispatches an update command to a named agent.
// POST /api/agents/{hostname}/update
func (s *Server) handleAgentDispatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Extract hostname from path: /api/agents/{hostname}/update
	path := strings.TrimPrefix(r.URL.Path, "/api/agents/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) < 2 || parts[1] != "update" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	hostname := parts[0]

	var req struct {
		Service string `json:"service"`
		Image   string `json:"image"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Service == "" {
		http.Error(w, "service is required", http.StatusBadRequest)
		return
	}

	cmd := agentproto.CommandMsg{
		Type:    agentproto.TypeCommand,
		ID:      fmt.Sprintf("cmd-%d", time.Now().UnixNano()),
		Action:  "update",
		Service: req.Service,
		Image:   req.Image,
	}
	if err := s.agentHub.Dispatch(hostname, cmd); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "dispatched", "id": cmd.ID})
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

// WSMessage Type
type WSMessage struct {
	Type   string          `json:"type"`             // "log" | "done"
	Line   *deploy.LogLine `json:"line,omitempty"`
	Status string          `json:"status,omitempty"` // present on "done"
}

// handleDeployLogs streams deploy logs over WebSocket.
func (s *Server) handleDeployLogs(w http.ResponseWriter, r *http.Request) {
	deployID := r.PathValue("deploy_id")
	if deployID == "" {
		http.Error(w, "deploy id required", http.StatusBadRequest)
		return
	}

	if s.jobRegistry == nil {
		http.Error(w, "deploy tracking not available", http.StatusServiceUnavailable)
		return
	}

	job, ok := s.jobRegistry.Get(deployID)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()

	// Replay
	for _, line := range job.Logs() {
		msg := WSMessage{Type: "log", Line: &line}
		if err := wsjson.Write(ctx, conn, msg); err != nil {
			return
		}
	}

	// Live stream
	for line := range job.LogCh {
		msg := WSMessage{Type: "log", Line: &line}
		if err := wsjson.Write(ctx, conn, msg); err != nil {
			return
		}
	}

	// After LogCh closes
	doneMsg := WSMessage{Type: "done", Status: string(job.Snap().Status)}
	_ = wsjson.Write(ctx, conn, doneMsg)
}

// handleGetServices lists all registered services with config.
func (s *Server) handleGetServices(w http.ResponseWriter, r *http.Request) {
	services := s.config.All()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(services)
}

// handleCreateService registers a new service.
func (s *Server) handleCreateService(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name           string `json:"name"`
		Image          string `json:"image"`
		ComposeFile    string `json:"compose_file"`
		TriggerMode    string `json:"trigger_mode"`
		PollerInterval int    `json:"poller_interval"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Image == "" {
		http.Error(w, "name and image are required", http.StatusBadRequest)
		return
	}
	if req.TriggerMode == "" {
		req.TriggerMode = "poll"
	}
	if req.PollerInterval == 0 {
		req.PollerInterval = 300
	}
	if req.PollerInterval < 30 {
		http.Error(w, "poller_interval must be >= 30", http.StatusBadRequest)
		return
	}
	if req.TriggerMode != "poll" && req.TriggerMode != "webhook" {
		http.Error(w, "trigger_mode must be poll or webhook", http.StatusBadRequest)
		return
	}

	if err := s.config.CreateConfig(req.Name, req.Image, req.ComposeFile, req.TriggerMode, req.PollerInterval); err != nil {
		s.log.Error("failed to create service config", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

// handleGetService gets config for one service.
func (s *Server) handleGetService(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}

	svc, ok := s.config.Get(name)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(svc)
}

// handlePatchServiceConfig updates trigger_mode / poller_interval.
func (s *Server) handlePatchServiceConfig(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}

	svc, ok := s.config.Get(name)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	var req struct {
		TriggerMode    string `json:"trigger_mode"`
		PollerInterval int    `json:"poller_interval"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if req.TriggerMode == "" {
		req.TriggerMode = svc.TriggerMode
	}
	if req.PollerInterval == 0 {
		req.PollerInterval = int(svc.PollerInterval.Seconds())
	}

	if req.PollerInterval < 30 {
		http.Error(w, "poller_interval must be >= 30", http.StatusBadRequest)
		return
	}
	if req.TriggerMode != "poll" && req.TriggerMode != "webhook" {
		http.Error(w, "trigger_mode must be poll or webhook", http.StatusBadRequest)
		return
	}

	if err := s.config.UpdateConfig(name, req.TriggerMode, req.PollerInterval); err != nil {
		s.log.Error("failed to patch service config", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}


