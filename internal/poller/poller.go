// Package poller provides a fallback registry poller for containers not reached
// by webhooks. It runs on the DOCKWATCH_REGISTRY_CRON schedule and publishes
// TopicImageUpdated when a new digest is detected.
package poller

import (
	"context"
	"log/slog"

	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/registry"
	"github.com/soerjadi/dockwatch/internal/serviceconfig"
	"github.com/soerjadi/dockwatch/internal/store"
)

type Poller struct {
	bus       *bus.Bus
	store     *store.Store
	registry  *registry.Client
	config    *serviceconfig.Manager
	schedule  string
	lastCheck map[string]time.Time
	mu        sync.Mutex
	log       *slog.Logger
}

// New creates a Poller.
func New(b *bus.Bus, st *store.Store, reg *registry.Client, cfg *serviceconfig.Manager, schedule string, log *slog.Logger) *Poller {
	return &Poller{
		bus:       b,
		store:     st,
		registry:  reg,
		config:    cfg,
		schedule:  schedule,
		lastCheck: make(map[string]time.Time),
		log:       log,
	}
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

	// Fast ticker for per-service polling
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.Stop()
			return
		case <-ticker.C:
			p.dispatch(ctx)
		}
	}
}

func (p *Poller) poll(ctx context.Context) {
	p.log.Info("poller: starting registry sweep")
	containers := p.store.All()
	for _, cs := range containers {
		// Skip if container is managed via services table
		if _, ok := p.config.Get(cs.Name); ok {
			continue
		}

		digest, err := p.registry.HeadDigest(ctx, cs.Image)
		if err != nil {
			p.log.Warn("poller: registry check failed", "container", cs.Name, "image", cs.Image, "err", err)
			continue
		}
		p.compareAndPublish(cs, nil, digest)
	}
}

func (p *Poller) dispatch(ctx context.Context) {
	services := p.config.All()
	for _, svc := range services {
		if svc.TriggerMode != "poll" {
			continue
		}
		p.mu.Lock()
		last := p.lastCheck[svc.Name]
		p.mu.Unlock()
		if time.Since(last) < svc.PollerInterval {
			continue
		}

		p.mu.Lock()
		p.lastCheck[svc.Name] = time.Now()
		p.mu.Unlock()

		go p.checkService(ctx, svc)
	}
}

func (p *Poller) checkService(ctx context.Context, svc *serviceconfig.ServiceConfig) {
	digest, err := p.registry.HeadDigest(ctx, svc.Image)
	if err != nil {
		p.log.Warn("poller: service check failed", "service", svc.Name, "image", svc.Image, "err", err)
		return
	}
	
	// Ensure we update DB with the last checked digest
	_ = p.config.UpdateDigest(svc.Name, digest)

	cs := p.store.GetByName(svc.Name)
	if cs == nil {
		// Service not running yet, nothing to update
		return
	}

	p.compareAndPublish(cs, svc, digest)
}

func (p *Poller) compareAndPublish(cs *store.ContainerState, svc *serviceconfig.ServiceConfig, digest string) {
	current := cs.CurrentDigest()
	if current == "" && svc != nil && svc.LastDigest != "" {
		// Fallback to the last known digest from the service DB.
		// This prevents dockwatch from triggering a redundant update on restart
		// when the in-memory digest history is empty.
		current = svc.LastDigest
		
		// Seed the in-memory store so it's not empty for subsequent checks
		cs.PushDigest(cs.Image, current)
	}

	if digest == current {
		p.log.Debug("poller: no update", "container", cs.Name)
		return
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

