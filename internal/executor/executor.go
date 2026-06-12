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
	"time"

	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/dockerclient"
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

// labelKey is the Docker label that controls update strategy per container.
const labelKey = "dockwatch.update"

// Executor subscribes to image.updated events and applies updates.
type Executor struct {
	bus    *bus.Bus
	store  *store.Store
	docker dockerclient.Scoped
	log    *slog.Logger
}

// New creates an Executor.
func New(docker dockerclient.Scoped, b *bus.Bus, st *store.Store, log *slog.Logger) *Executor {
	return &Executor{bus: b, store: st, docker: docker, log: log}
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

	strategy := strategyFromLabels(cs.Labels)

	e.log.Info("update candidate",
		"container", p.ContainerName,
		"image", p.Image,
		"old_digest", p.OldDigest[:16],
		"new_digest", p.NewDigest[:16],
		"strategy", strategy,
	)

	// Determine whether to auto-apply or skip.
	shouldApply := e.evaluate(strategy, p)

	if !shouldApply {
		e.bus.Publish(bus.TopicUpdateSkipped, bus.UpdateSkippedPayload{
			ContainerID:   p.ContainerID,
			ContainerName: p.ContainerName,
			Image:         p.Image,
			NewDigest:     p.NewDigest,
			Reason:        "semver strategy: " + string(strategy),
		})
		e.log.Info("update skipped", "container", p.ContainerName, "strategy", strategy)
		return
	}

	newID, err := e.apply(ctx, p)
	if err != nil {
		e.log.Error("update failed", "container", p.ContainerName, "err", err)
		return
	}

	// Recreating the container produced a new ID; move the state (and its
	// digest ring buffer) so rollback can still find the previous digest, then
	// record the newly-applied digest.
	moved := e.store.Rekey(p.ContainerID, newID)
	if moved == nil {
		moved = cs
	}
	moved.PushDigest(p.Image, p.NewDigest)

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
func (e *Executor) evaluate(strategy Strategy, p bus.ImageUpdatedPayload) bool {
	switch strategy {
	case StrategyNotify:
		return false
	case StrategyAuto:
		return true
	case StrategyPatch:
		// TODO: parse semver from image tag and check if only patch bumped
		return true
	case StrategyMinor:
		// TODO: parse semver and check major hasn't changed
		return true
	default:
		return true
	}
}

// apply pulls the new image and recreates the container against it, returning
// the new container ID. The pull/stop/remove/create/start sequence is composed
// from the scoped client's primitives by dockerclient.Recreate.
func (e *Executor) apply(ctx context.Context, p bus.ImageUpdatedPayload) (string, error) {
	ref := imageRef(p.Image, p.NewDigest)
	e.log.Info("applying update",
		"container", p.ContainerName,
		"image", p.Image,
		"digest", p.NewDigest[:16],
		"ref", ref,
	)
	return dockerclient.Recreate(ctx, e.docker, p.ContainerID, ref)
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
