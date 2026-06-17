// Package executor consumes TopicImageUpdated events from the bus and decides
// whether to apply the update based on a per-container semver strategy.
//
// Strategy rules (configurable via container labels):
//
//	dockwatch.update=auto      → always update (default)
//	dockwatch.update=patch     → auto on patch, notify-only on minor/major
//	dockwatch.update=minor     → auto on minor+patch, notify-only on major
//	dockwatch.update=notify    → never auto-update, always notify
package executor

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/compose"
	"github.com/soerjadi/dockwatch/internal/dockerclient"
	"github.com/soerjadi/dockwatch/internal/github"
	"github.com/soerjadi/dockwatch/internal/history"
	"github.com/soerjadi/dockwatch/internal/store"
)

// Strategy defines how aggressive automatic updates are for a container.
type Strategy string

const (
	StrategyAuto   Strategy = "auto"   // always update
	StrategyMinor  Strategy = "minor"  // update on minor + patch
	StrategyPatch  Strategy = "patch"  // update on patch only
	StrategyNotify Strategy = "notify" // never auto-update
)

const (
	labelKey           = "dockwatch.update"         // update strategy
	labelWatch         = "dockwatch.watch"           // opt-out: set "false" to exclude container
	labelComposeUpdate = "dockwatch.compose.update"  // "auto" enables compose-first path
)

// Executor subscribes to image.updated events and applies updates.
type Executor struct {
	bus     *bus.Bus
	store   *store.Store
	docker  dockerclient.Scoped
	gh      *github.Client
	history *history.Store
	log     *slog.Logger
}

// New creates an Executor.
func New(docker dockerclient.Scoped, b *bus.Bus, st *store.Store, gh *github.Client, hist *history.Store, log *slog.Logger) *Executor {
	return &Executor{bus: b, store: st, docker: docker, gh: gh, history: hist, log: log}
}

// Run starts the executor loop. Blocks until ctx is cancelled.
func (e *Executor) Run(ctx context.Context) {
	ch, unsub := e.bus.Subscribe(bus.TopicImageUpdated)
	defer unsub()

	e.log.Info("executor ready", "topic", bus.TopicImageUpdated)

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			payload, ok := ev.Payload.(bus.ImageUpdatedPayload)
			if !ok {
				continue
			}
			e.handle(ctx, payload)
		}
	}
}

// handle processes a single image.updated event.
func (e *Executor) handle(ctx context.Context, p bus.ImageUpdatedPayload) {
	cs := e.store.Get(p.ContainerID)
	if cs == nil {
		e.log.Warn("executor: unknown container", "id", p.ContainerID)
		return
	}

	if !watchEnabled(cs.Labels) {
		e.log.Debug("update skipped: container excluded via dockwatch.watch=false",
			"container", p.ContainerName,
		)
		return
	}

	strategy := strategyFromLabels(cs.Labels)

	// Compose-managed containers: default to notify-only unless the user has
	// explicitly opted in to the compose-first auto-update path via label.
	if info := compose.FromLabels(cs.Labels); info != nil {
		if cs.Labels[labelComposeUpdate] != "auto" {
			strategy = StrategyNotify
		}
	}

	e.log.Info("update candidate",
		"container", p.ContainerName,
		"image", p.Image,
		"old_digest", shortDigest(p.OldDigest),
		"new_digest", shortDigest(p.NewDigest),
		"strategy", strategy,
	)

	// Determine whether to auto-apply or skip.
	shouldApply := e.evaluate(strategy, p, cs.Image)

	// Detect breaking change (major semver bump) independently of strategy.
	breakingChange := isMajorBump(cs.Image, p.Image)
	if breakingChange && shouldApply {
		e.log.Warn("applying update with major version bump — potential breaking change",
			"container", p.ContainerName, "image", p.Image)
	}

	if !shouldApply {
		skip := bus.UpdateSkippedPayload{
			ContainerID:    p.ContainerID,
			ContainerName:  p.ContainerName,
			Image:          p.Image,
			NewDigest:      p.NewDigest,
			Reason:         "semver strategy: " + string(strategy),
			BreakingChange: breakingChange,
			SkippedAt:      time.Now(),
		}
		if breakingChange {
			skip.ReleaseNotes = e.fetchReleaseNotes(ctx, cs.Labels, cs.Image, p.Image)
		}
		e.bus.Publish(bus.TopicUpdateSkipped, skip)
		e.log.Info("update skipped", "container", p.ContainerName, "strategy", strategy,
			"breaking_change", breakingChange)
		return
	}

	newID, backupPath, err := e.apply(ctx, p, cs)
	if err != nil {
		e.log.Error("update failed", "container", p.ContainerName, "err", err)
		return
	}

	// For the compose path newID is "" — watcher rebuilds state on container:start.
	if newID != "" {
		moved := e.store.Rekey(p.ContainerID, newID)
		if moved == nil {
			moved = cs
		}
		moved.PushDigest(p.Image, p.NewDigest)
	}

	if e.history != nil {
		if _, err := e.history.Record(history.Entry{
			AppName:    p.ContainerName,
			Service:    p.ContainerName,
			OldImage:   cs.Image,
			NewImage:   p.Image,
			BackupPath: backupPath,
			CreatedAt:  time.Now(),
		}); err != nil {
			e.log.Warn("history: failed to record update", "container", p.ContainerName, "err", err)
		}
	}

	e.bus.Publish(bus.TopicUpdateApplied, bus.UpdateAppliedPayload{
		ContainerID:   newID,
		ContainerName: p.ContainerName,
		Image:         p.Image,
		OldDigest:     p.OldDigest,
		NewDigest:     p.NewDigest,
		AppliedAt:     time.Now(),
	})
}

// evaluate returns true if the update should be automatically applied.
// currentImage is the image reference the container is currently running.
func (e *Executor) evaluate(strategy Strategy, p bus.ImageUpdatedPayload, currentImage string) bool {
	switch strategy {
	case StrategyNotify:
		return false
	case StrategyAuto:
		return true
	case StrategyPatch:
		return semverAllow(currentImage, p.Image, false)
	case StrategyMinor:
		return semverAllow(currentImage, p.Image, true)
	default:
		return true
	}
}

// semverAllow returns true if the version bump from oldImage to newImage is
// within the allowed scope. allowMinor permits minor+patch bumps; when false
// only patch bumps are allowed. Major bumps always return false.
// Falls back to true when either tag isn't parseable semver (e.g. "latest").
func semverAllow(oldImage, newImage string, allowMinor bool) bool {
	oldMaj, oldMin, _, oldOk := parseSemver(imageTag(oldImage))
	newMaj, newMin, _, newOk := parseSemver(imageTag(newImage))
	if !oldOk || !newOk {
		return true
	}
	if newMaj != oldMaj {
		return false
	}
	if newMin != oldMin {
		return allowMinor
	}
	return true
}

// imageTag extracts the version tag from an image reference like
// "nginx:1.2.3" → "1.2.3". Strips any digest suffix first.
func imageTag(image string) string {
	if idx := strings.Index(image, "@"); idx != -1 {
		image = image[:idx]
	}
	if idx := strings.LastIndex(image, ":"); idx != -1 {
		return image[idx+1:]
	}
	return ""
}

// parseSemver parses tags like "1.2.3", "v1.2", "1" into numeric components.
// Returns ok=false when the tag isn't a numeric version string.
func parseSemver(tag string) (major, minor, patch int, ok bool) {
	tag = strings.TrimPrefix(tag, "v")
	parts := strings.SplitN(tag, ".", 3)
	var err error
	if major, err = strconv.Atoi(parts[0]); err != nil {
		return
	}
	if len(parts) >= 2 {
		if minor, err = strconv.Atoi(parts[1]); err != nil {
			return
		}
	}
	if len(parts) >= 3 {
		patchStr, _, _ := strings.Cut(parts[2], "-") // strip pre-release suffix
		if patch, err = strconv.Atoi(patchStr); err != nil {
			return
		}
	}
	ok = true
	return
}

// shortDigest safely returns the first 16 chars of a digest for logging.
func shortDigest(d string) string {
	if len(d) > 16 {
		return d[:16]
	}
	return d
}

// apply updates the container to the new image. For compose-auto containers it
// patches the compose file and delegates to `docker compose up`; for all others
// it uses the direct Docker API path (Recreate). Returns the new container ID
// for the direct path, or "" for the compose path (the watcher rebuilds state
// from the container:start event that compose triggers).
func (e *Executor) apply(ctx context.Context, p bus.ImageUpdatedPayload, cs *store.ContainerState) (string, string, error) {
	// Compose-first path: edit compose file, run docker compose up -d
	if info := compose.FromLabels(cs.Labels); info != nil && cs.Labels[labelComposeUpdate] == "auto" {
		newTag := imageTag(p.Image)
		histDir := ""
		if e.history != nil {
			histDir = e.history.HistDir()
		}
		e.log.Info("applying compose update",
			"service", info.Service,
			"image", p.Image,
			"tag", newTag,
		)
		backupPath, err := compose.ApplyUpdate(ctx, info, newTag, histDir, e.log)
		return "", backupPath, err
	}

	// Direct Docker API path
	ref := imageRef(p.Image, p.NewDigest)
	e.log.Info("applying update",
		"container", p.ContainerName,
		"image", p.Image,
		"digest", shortDigest(p.NewDigest),
		"ref", ref,
	)
	newID, err := dockerclient.Recreate(ctx, e.docker, p.ContainerID, ref)
	return newID, "", err
}

// imageRef pins an image to a specific digest: "repo:tag@sha256:...". The
// digest is authoritative — the daemon resolves to exactly that content.
func imageRef(image, digest string) string {
	if digest == "" {
		return image
	}
	return image + "@" + digest
}

func strategyFromLabels(labels map[string]string) Strategy {
	if v, ok := labels[labelKey]; ok {
		return Strategy(v)
	}
	return StrategyAuto
}

// watchEnabled returns false only when the container explicitly sets
// dockwatch.watch=false. All other values (including absent) return true.
func watchEnabled(labels map[string]string) bool {
	return labels[labelWatch] != "false"
}

// isMajorBump returns true when the image tag represents a major version
// increase (e.g. 3.1.0 → 4.0.0). Returns false when either tag is not
// parseable semver — in that case we cannot make the call.
func isMajorBump(oldImage, newImage string) bool {
	oldMaj, _, _, oldOk := parseSemver(imageTag(oldImage))
	newMaj, _, _, newOk := parseSemver(imageTag(newImage))
	if !oldOk || !newOk {
		return false
	}
	return newMaj > oldMaj
}

// fetchReleaseNotes fetches GitHub release notes as optional enrichment when
// the container label "org.opencontainers.image.source" points to a GitHub
// repo. Returns nil when the label is absent or the lookup fails.
func (e *Executor) fetchReleaseNotes(ctx context.Context, labels map[string]string, oldImage, newImage string) []string {
	source := labels["org.opencontainers.image.source"]
	if source == "" || e.gh == nil {
		return nil
	}
	oldTag := imageTag(oldImage)
	newTag := imageTag(newImage)
	notes := e.gh.FetchReleaseNotes(ctx, source, oldTag, newTag)
	if len(notes) > 0 {
		e.log.Info("breaking change: release notes fetched", "count", len(notes))
	}
	return notes
}
