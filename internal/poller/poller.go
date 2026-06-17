// Package poller provides a fallback registry poller for containers not reached
// by webhooks. It runs on the DOCKWATCH_REGISTRY_CRON schedule and publishes
// TopicImageUpdated when a new digest is detected.
package poller

import (
	"context"
	"log/slog"

	"github.com/robfig/cron/v3"
	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/registry"
	"github.com/soerjadi/dockwatch/internal/store"
)

// Poller polls the registry on a cron schedule and fires image.updated events.
type Poller struct {
	bus      *bus.Bus
	store    *store.Store
	registry *registry.Client
	schedule string
	log      *slog.Logger
}

// New creates a Poller.
func New(b *bus.Bus, st *store.Store, reg *registry.Client, schedule string, log *slog.Logger) *Poller {
	return &Poller{bus: b, store: st, registry: reg, schedule: schedule, log: log}
}

// Run starts the cron scheduler. Blocks until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	c := cron.New(cron.WithSeconds())

	_, err := c.AddFunc(p.schedule, func() {
		p.poll(ctx)
	})
	if err != nil {
		p.log.Error("poller: invalid cron schedule", "schedule", p.schedule, "err", err)
		return
	}

	c.Start()
	p.log.Info("poller ready", "schedule", p.schedule)
	<-ctx.Done()
	c.Stop()
}

func (p *Poller) poll(ctx context.Context) {
	p.log.Info("poller: starting registry sweep")
	containers := p.store.All()
	for _, cs := range containers {
		digest, err := p.registry.HeadDigest(ctx, cs.Image)
		if err != nil {
			p.log.Warn("poller: registry check failed", "container", cs.Name, "image", cs.Image, "err", err)
			continue
		}
		current := cs.CurrentDigest()
		if digest == current {
			p.log.Debug("poller: no update", "container", cs.Name)
			continue
		}
		p.log.Info("poller: new digest detected", "container", cs.Name, "image", cs.Image,
			"old", current[:min(16, len(current))], "new", digest[:min(16, len(digest))],
		)
		p.bus.Publish(bus.TopicImageUpdated, bus.ImageUpdatedPayload{
			ContainerID:   cs.ID,
			ContainerName: cs.Name,
			Image:         cs.Image,
			OldDigest:     current,
			NewDigest:     digest,
		})
	}
}
