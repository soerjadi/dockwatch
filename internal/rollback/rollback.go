// Package rollback consumes TopicContainerUnhealthy events and restores the
// previous image digest from the in-memory store's ring buffer.
//
// This is the key feature missing from both Watchtower and WUD.
// No external state is needed — the previous digest is kept in the store.
package rollback

import (
	"context"
	"log/slog"
	"time"

	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/dockerclient"
	"github.com/soerjadi/dockwatch/internal/store"
)

// Engine subscribes to container.unhealthy and performs rollbacks.
type Engine struct {
	bus    *bus.Bus
	store  *store.Store
	docker dockerclient.Scoped
	log    *slog.Logger
}

// New creates a rollback Engine.
func New(docker dockerclient.Scoped, b *bus.Bus, st *store.Store, log *slog.Logger) *Engine {
	return &Engine{bus: b, store: st, docker: docker, log: log}
}

// Run starts the rollback loop. Blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	ch, unsub := e.bus.Subscribe(bus.TopicContainerUnhealthy)
	defer unsub()

	e.log.Info("rollback engine ready", "topic", bus.TopicContainerUnhealthy)

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			payload, ok := ev.Payload.(bus.ContainerUnhealthyPayload)
			if !ok {
				continue
			}
			e.handle(ctx, payload)
		}
	}
}

// handle performs a rollback for a single unhealthy container.
func (e *Engine) handle(ctx context.Context, p bus.ContainerUnhealthyPayload) {
	e.log.Warn("rollback triggered",
		"container", p.ContainerName,
		"applied_digest", p.AppliedDigest[:16],
		"restoring", func() string {
			if len(p.PrevDigest) >= 16 {
				return p.PrevDigest[:16]
			}
			return "(none)"
		}(),
	)

	if p.PrevDigest == "" {
		e.log.Error("rollback aborted: no previous digest in store",
			"container", p.ContainerName,
		)
		return
	}

	newID, err := e.restore(ctx, p)
	if err != nil {
		e.log.Error("rollback failed", "container", p.ContainerName, "err", err)
		return
	}

	// Recreate produced a new container ID; carry the state over and record the
	// restored (previous) digest as current.
	cs := e.store.Rekey(p.ContainerID, newID)
	if cs == nil {
		cs = e.store.Get(p.ContainerID)
	}
	if cs != nil {
		cs.PushDigest(p.Image, p.PrevDigest)
	}

	e.bus.Publish(bus.TopicRollbackDone, bus.RollbackDonePayload{
		ContainerID:    newID,
		ContainerName:  p.ContainerName,
		Image:          p.Image,
		RestoredDigest: p.PrevDigest,
		RolledBackAt:   time.Now(),
	})

	e.log.Info("rollback complete", "container", p.ContainerName)
}

// restore pulls the previous image by digest and recreates the container
// against it, returning the new container ID. Same flow as executor.apply but
// targeting the previous digest from the store's ring buffer.
func (e *Engine) restore(ctx context.Context, p bus.ContainerUnhealthyPayload) (string, error) {
	ref := p.Image + "@" + p.PrevDigest
	e.log.Info("restoring container", "container", p.ContainerName, "ref", ref)
	return dockerclient.Recreate(ctx, e.docker, p.ContainerID, ref)
}
