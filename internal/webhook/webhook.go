// Package webhook provides an inbound HTTP endpoint that CI/CD pipelines
// call after pushing a new image. This replaces the registry polling fallback
// entirely — instead of dockwatch asking "is there anything new?", the pipeline
// tells dockwatch "I just pushed image X with digest Y".
//
// Endpoint: POST /webhook/push
//
// Payload (JSON):
//
//	{
//	  "image":  "yourrepo/myapp",      // required — image name without tag
//	  "tag":    "1.2.3",               // required — new tag
//	  "digest": "sha256:abc...",       // optional but recommended
//	  "source": "github-actions"       // optional — for logging/tracing
//	}
//
// Security: requests are validated with an HMAC-SHA256 signature in the
// X-Dockwatch-Signature header, computed over the raw request body using
// the shared secret set in DOCKWATCH_WEBHOOK_SECRET.
// If the secret is empty, signature validation is skipped (dev only).
//
// Compatible with Docker Hub webhooks (use /webhook/dockerhub),
// GitHub Actions (use /webhook/push with custom payload),
// and any CI system that can POST JSON.
package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/deploy"
	"github.com/soerjadi/dockwatch/internal/history"
	"github.com/soerjadi/dockwatch/internal/serviceconfig"
	"github.com/soerjadi/dockwatch/internal/store"
)

const (
	signatureHeader = "X-Dockwatch-Signature"
	maxBodyBytes    = 1 << 16 // 64 KB — more than enough for any webhook payload
)

// PushPayload is the JSON body sent by CI/CD to /webhook/push.
type PushPayload struct {
	Image  string `json:"image"`  // e.g. "yourrepo/myapp"
	Tag    string `json:"tag"`    // e.g. "1.2.3" or "latest"
	Digest string `json:"digest"` // e.g. "sha256:abc..." (optional)
	Source string `json:"source"` // e.g. "github-actions" (optional, for logs)
}

// DockerHubPayload is the shape of a Docker Hub webhook POST.
// See: https://docs.docker.com/docker-hub/webhooks/
type DockerHubPayload struct {
	Repository struct {
		RepoName string `json:"repo_name"` // e.g. "library/nginx"
	} `json:"repository"`
	PushData struct {
		Tag    string `json:"tag"`
		Pusher string `json:"pusher"`
	} `json:"push_data"`
}

// Handler handles inbound webhook requests.
type Handler struct {
	bus         *bus.Bus
	store       *store.Store
	secret      string
	log         *slog.Logger
	jobRegistry *deploy.JobRegistry
	config      *serviceconfig.Manager
	history     *history.Store
}

func New(b *bus.Bus, st *store.Store, secret string, log *slog.Logger, jobRegistry *deploy.JobRegistry, config *serviceconfig.Manager, hist *history.Store) *Handler {
	if secret == "" {
		log.Warn("webhook secret is empty — signature validation disabled (not safe for production)")
	}
	return &Handler{bus: b, store: st, secret: secret, log: log, jobRegistry: jobRegistry, config: config, history: hist}
}

// RegisterRoutes mounts the webhook endpoints onto the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/webhook/push", h.handlePush)
	mux.HandleFunc("/webhook/dockerhub", h.handleDockerHub)
}

// handlePush handles a generic CI/CD push notification.
//
// Example curl:
//
//	curl -X POST http://dockwatch:3010/webhook/push \
//	  -H "Content-Type: application/json" \
//	  -H "X-Dockwatch-Signature: sha256=<hmac>" \
//	  -d '{"image":"yourrepo/myapp","tag":"1.2.3","digest":"sha256:abc..."}'
func (h *Handler) handlePush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, ok := h.readAndVerify(w, r)
	if !ok {
		return
	}

	var p PushPayload
	if err := json.Unmarshal(body, &p); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if p.Image == "" || p.Tag == "" {
		http.Error(w, "image and tag are required", http.StatusBadRequest)
		return
	}

	// We also need to fail the API request if ANY container matches and is in poll mode.
	// But it's easier to check upfront if any container is in poll mode.
	affected := h.containersForImage(p.Image, p.Tag)
	hasPollMode := false
	for _, cs := range affected {
		if svc, ok := h.config.Get(cs.Name); ok && svc.TriggerMode == "poll" {
			hasPollMode = true
			if h.history != nil {
				if err := h.history.RecordSkipped(cs.Name, p.Image+":"+p.Tag,
					"webhook rejected: service trigger_mode is poll"); err != nil {
					h.log.Warn("history: failed to record skipped deploy", "container", cs.Name, "err", err)
				}
			}
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error":        "deploy rejected: service is in poll mode",
				"service":      cs.Name,
				"trigger_mode": "poll",
				"hint":         "::notice::dockwatch: " + cs.Name + " uses poll mode — webhook ignored",
			})
			return
		}
	}

	h.dispatch(p.Image, p.Tag, p.Digest, p.Source)

	// Create a queued DeployJob so the caller gets a deploy_id to track progress
	// (AC#1). The executor will transition it to running/success/failed when it
	// picks up the TopicImageUpdated event from the bus.
	deployID := ""
	if h.jobRegistry != nil && !hasPollMode {
		job := deploy.NewJob(p.Image+":"+p.Tag, p.Image+":"+p.Tag, "webhook")
		_ = h.jobRegistry.Register(job)
		deployID = job.ID
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"deploy_id": deployID,
		"status":    "queued",
		"image":     p.Image + ":" + p.Tag,
	})
}

// handleDockerHub handles the native Docker Hub webhook format.
//
// Configure in Docker Hub: Repository → Webhooks → Add webhook URL:
//
//	http://your-dockwatch-host:3010/webhook/dockerhub
//
// Docker Hub does not send an HMAC signature, so this endpoint skips
// signature validation and instead relies on network-level access control.
func (h *Handler) handleDockerHub(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	var p DockerHubPayload
	if err := json.Unmarshal(body, &p); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	image := p.Repository.RepoName
	tag := p.PushData.Tag
	if image == "" || tag == "" {
		http.Error(w, "missing repository.repo_name or push_data.tag", http.StatusBadRequest)
		return
	}

	h.dispatch(image, tag, "", "docker-hub")

	// Docker Hub expects a 200 — otherwise it retries.
	w.WriteHeader(http.StatusOK)
}

// dispatch resolves which tracked containers use this image+tag, then
// publishes TopicImageUpdated for each one. The executor will apply
// the semver strategy and decide whether to actually update.
func (h *Handler) dispatch(image, tag, digest, source string) {
	fullImage := image + ":" + tag

	h.log.Info("webhook received",
		"image", fullImage,
		"digest", digest,
		"source", source,
	)

	// Find all containers that are running this image.
	affected := h.containersForImage(image, tag)

	if len(affected) == 0 {
		h.log.Info("webhook: no tracked containers match image", "image", fullImage)
		return
	}

	for _, cs := range affected {
		// Check trigger mode
		if svc, ok := h.config.Get(cs.Name); ok && svc.TriggerMode == "poll" {
			h.log.Info("webhook: ignored because service is in poll mode", "container", cs.Name)
			continue
		}

		h.log.Info("webhook: publishing image.updated",
			"container", cs.Name,
			"image", fullImage,
		)
		h.bus.Publish(bus.TopicImageUpdated, bus.ImageUpdatedPayload{
			ContainerID:   cs.ID,
			ContainerName: cs.Name,
			Image:         fullImage,
			OldDigest:     cs.CurrentDigest(),
			NewDigest:     digest, // may be "" if caller didn't include it
			DetectedAt:    time.Now(),
		})
	}
}

// containersForImage returns all tracked containers whose image matches.
// Matching is done on image name (without tag) so a push of "myapp:1.2.3"
// matches containers running "myapp:1.2.2".
func (h *Handler) containersForImage(image, tag string) []*store.ContainerState {
	all := h.store.All()
	var matched []*store.ContainerState
	for _, cs := range all {
		// Normalise: strip tag from stored image name for comparison.
		storedImage := cs.Image
		if idx := strings.LastIndex(storedImage, ":"); idx != -1 {
			storedImage = storedImage[:idx]
		}
		if storedImage == image {
			matched = append(matched, cs)
		}
	}
	return matched
}

// readAndVerify reads the request body and validates the HMAC signature.
// Returns the body bytes on success; writes an HTTP error and returns false on failure.
func (h *Handler) readAndVerify(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusInternalServerError)
		return nil, false
	}
	defer r.Body.Close()

	// Skip validation if no secret configured (dev mode).
	if h.secret == "" {
		return body, true
	}

	sig := r.Header.Get(signatureHeader)
	if sig == "" {
		h.log.Warn("webhook: missing signature header", "remote", r.RemoteAddr)
		http.Error(w, "missing "+signatureHeader, http.StatusUnauthorized)
		return nil, false
	}

	if !h.validSignature(body, sig) {
		h.log.Warn("webhook: invalid signature", "remote", r.RemoteAddr)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return nil, false
	}

	return body, true
}

// validSignature checks that sig == "sha256=<hmac-sha256(secret, body)>".
// Uses hmac.Equal to prevent timing attacks.
func (h *Handler) validSignature(body []byte, sig string) bool {
	sig = strings.TrimPrefix(sig, "sha256=")
	mac := hmac.New(sha256.New, []byte(h.secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(sig), []byte(expected))
}
