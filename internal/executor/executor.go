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
	"github.com/soerjadi/dockwatch/internal/deploy"
	"github.com/soerjadi/dockwatch/internal/dockerclient"
	"github.com/soerjadi/dockwatch/internal/github"
	"github.com/soerjadi/dockwatch/internal/history"
	"github.com/soerjadi/dockwatch/internal/store"
)

// zeroDTDefaultTimeout is the default maximum wait time for zero-downtime updates.
const zeroDTDefaultTimeout = 60 * time.Second

// Strategy defines how aggressive automatic updates are for a container.
type Strategy string

const (
	StrategyAuto   Strategy = "auto"   // always update
	StrategyMinor  Strategy = "minor"  // update on minor + patch
	StrategyPatch  Strategy = "patch"  // update on patch only
	StrategyNotify Strategy = "notify" // never auto-update
)

const (
	labelKey           = "dockwatch.update"          // update strategy
	labelWatch         = "dockwatch.watch"            // opt-out: set "false" to exclude container
	labelComposeUpdate = "dockwatch.compose.update"   // "auto" enables compose-first path
	labelZeroDT        = "dockwatch.zero-downtime"    // "true" enables zero-downtime mode (compose only)
)

// Executor subscribes to image.updated events and applies updates.
type Executor struct {
	bus           *bus.Bus
	store         *store.Store
	docker        dockerclient.Scoped
	gh            *github.Client
	history       *history.Store
	zeroDTTimeout time.Duration
	log           *slog.Logger
	jobRegistry   *deploy.JobRegistry
}

// New creates an Executor. jobRegistry is injected so the executor can create
// and track a DeployJob for every apply it performs.
func New(docker dockerclient.Scoped, b *bus.Bus, st *store.Store, gh *github.Client, hist *history.Store, zeroDTTimeout time.Duration, log *slog.Logger, jobRegistry *deploy.JobRegistry) *Executor {
	if zeroDTTimeout == 0 {
		zeroDTTimeout = zeroDTDefaultTimeout
	}
	return &Executor{bus: b, store: st, docker: docker, gh: gh, history: hist, zeroDTTimeout: zeroDTTimeout, log: log, jobRegistry: jobRegistry}
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

	// Create and register a DeployJob before applying. The trigger source for
	// bus-driven updates is always "poll" (webhook handler registers its own job).
	var job *deploy.DeployJob
	if e.jobRegistry != nil {
		job = deploy.NewJob(p.ContainerName, p.Image, "poll")
		_ = e.jobRegistry.Register(job)
		defer e.jobRegistry.Finish(job.ID)
		job.Transition(deploy.StatusRunning)
	}

	// Record the deploy as "running" before the apply so every deploy has a row
	// regardless of outcome. deploy_id links this row to the job for FinishDeploy.
	if e.history != nil && job != nil {
		snap := job.Snap()
		if _, err := e.history.RecordDeploy(history.Entry{
			AppName:       p.ContainerName,
			Service:       p.ContainerName,
			OldImage:      cs.Image,
			NewImage:      p.Image,
			ImageTag:      imageTag(p.Image),
			DeployID:      job.ID,
			TriggerSource: job.TriggerSource,
			Status:        "running",
			StartedAt:     snap.StartedAt,
		}); err != nil {
			e.log.Warn("history: failed to record deploy start", "container", p.ContainerName, "err", err)
		}
	}

	newID, _, err := e.apply(ctx, p, cs, job)
	if err != nil {
		if job != nil {
			job.Transition(deploy.StatusFailed)
		}
		e.log.Error("update failed", "container", p.ContainerName, "err", err)
		if e.history != nil && job != nil {
			if ferr := e.history.FinishDeploy(job.ID, "failed", time.Now()); ferr != nil {
				e.log.Warn("history: failed to finish deploy", "container", p.ContainerName, "err", ferr)
			}
		}
		return
	}
	if job != nil {
		job.Transition(deploy.StatusSuccess)
	}
	if e.history != nil && job != nil {
		endedAt := time.Now()
		if snap := job.Snap(); snap.EndedAt != nil {
			endedAt = *snap.EndedAt
		}
		if ferr := e.history.FinishDeploy(job.ID, "success", endedAt); ferr != nil {
			e.log.Warn("history: failed to finish deploy", "container", p.ContainerName, "err", ferr)
		}
	}

	// For the compose path newID is "" — watcher rebuilds state on container:start.
	if newID != "" {
		moved := e.store.Rekey(p.ContainerID, newID)
		if moved == nil {
			moved = cs
		}
		moved.PushDigest(p.Image, p.NewDigest)
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
// job is passed through for log streaming and may be nil (direct/docker path).
func (e *Executor) apply(ctx context.Context, p bus.ImageUpdatedPayload, cs *store.ContainerState, job *deploy.DeployJob) (string, string, error) {
	// Compose paths: edit compose file, run docker compose up
	if info := compose.FromLabels(cs.Labels); info != nil && cs.Labels[labelComposeUpdate] == "auto" {
		newTag := imageTag(p.Image)
		histDir := ""
		if e.history != nil {
			histDir = e.history.HistDir()
		}

		// Zero-downtime path: scale-up → health-check → scale-down
		if cs.Labels[labelZeroDT] == "true" {
			e.log.Info("applying zero-downtime compose update",
				"service", info.Service, "tag", newTag)
			newID, backupPath, err := compose.ZeroDowntimeUpdate(ctx, info, newTag, histDir,
				compose.ZeroDTConfig{Timeout: e.zeroDTTimeout}, e.docker, e.log)
			return newID, backupPath, err
		}

		// Standard compose path: patch file + docker compose up -d
		e.log.Info("applying compose update", "service", info.Service, "tag", newTag)
		backupPath, err := compose.ApplyUpdate(ctx, info, newTag, histDir, e.log, job)
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
