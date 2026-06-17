// Package healthmon watches containers after an update is applied.
// If the container becomes unhealthy within the grace window, it publishes
// TopicContainerUnhealthy so the rollback engine can restore the previous image.
//
// This solves the core gap in both Watchtower and WUD — neither has any
// awareness of whether a newly started container is actually healthy.
package healthmon

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/dockerclient"
	"github.com/soerjadi/dockwatch/internal/store"
)

// Inspector is the single Docker operation healthmon needs: read a container's
// current state. Satisfied by *dockerclient.Client.
type Inspector interface {
	InspectContainer(ctx context.Context, id string) (dockerclient.InspectResult, error)
}

// defaultGrace is how long to watch a container after update before declaring
// it stable. Configurable via dockwatch.health.grace label.
const defaultGrace = 30 * time.Second

// Monitor watches containers post-update and detects health failures.
type Monitor struct {
	bus    *bus.Bus
	store  *store.Store
	docker Inspector
	log    *slog.Logger

	mu      sync.Mutex
	watched map[string]context.CancelFunc // containerID → cancel watching
}

// New creates a Monitor.
func New(docker Inspector, b *bus.Bus, st *store.Store, log *slog.Logger) *Monitor {
	return &Monitor{
		bus:     b,
		store:   st,
		docker:  docker,
		log:     log,
		watched: make(map[string]context.CancelFunc),
	}
}

// Run subscribes to update.applied events and starts a health watch window
// for each newly updated container. Blocks until ctx is cancelled.
func (m *Monitor) Run(ctx context.Context) {
	ch, unsub := m.bus.Subscribe(bus.TopicUpdateApplied)
	defer unsub()

	m.log.Info("health monitor ready", "topic", bus.TopicUpdateApplied)

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			payload, ok := ev.Payload.(bus.UpdateAppliedPayload)
			if !ok {
				continue
			}
			m.startWatch(ctx, payload)
		}
	}
}

// startWatch begins monitoring a container for the grace window.
func (m *Monitor) startWatch(parent context.Context, p bus.UpdateAppliedPayload) {
	grace := m.graceFor(p.ContainerID)

	m.mu.Lock()
	// Cancel any existing watch for this container (e.g. rapid successive updates).
	if cancel, ok := m.watched[p.ContainerID]; ok {
		cancel()
	}
	ctx, cancel := context.WithCancel(parent)
	m.watched[p.ContainerID] = cancel
	m.mu.Unlock()

	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.watched, p.ContainerID)
			m.mu.Unlock()
		}()

		m.log.Info("health watch started",
			"container", p.ContainerName,
			"grace", grace,
		)

		// Poll the container's health status during the grace window.
		// TODO: replace ticker with Docker event stream health_status events
		//       (already wired in watcher.go) for zero-latency detection.
		ticker := time.NewTicker(3 * time.Second)
		deadline := time.NewTimer(grace)
		defer ticker.Stop()
		defer deadline.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-deadline.C:
				m.log.Info("container stable after update", "container", p.ContainerName)
				return

			case <-ticker.C:
				healthy, err := m.checkHealth(ctx, p.ContainerID)
				if err != nil {
					m.log.Warn("health check error", "container", p.ContainerName, "err", err)
					continue
				}
				if !healthy {
					m.log.Warn("container unhealthy — triggering rollback",
						"container", p.ContainerName,
					)
					cs := m.store.Get(p.ContainerID)
					prev := ""
					if cs != nil {
						prev = cs.PreviousDigest()
					}
					m.bus.Publish(bus.TopicContainerUnhealthy, bus.ContainerUnhealthyPayload{
						ContainerID:   p.ContainerID,
						ContainerName: p.ContainerName,
						Image:         p.Image,
						AppliedDigest: p.NewDigest,
						PrevDigest:    prev,
						FailedAt:      time.Now(),
					})
					return
				}
			}
		}
	}()
}

// graceFor returns the health grace window for a container, reading
// dockwatch.health.grace label if present, falling back to defaultGrace.
func (m *Monitor) graceFor(containerID string) time.Duration {
	cs := m.store.Get(containerID)
	if cs != nil {
		if v := cs.Labels["dockwatch.health.grace"]; v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				return d
			}
		}
	}
	return defaultGrace
}

// checkHealth inspects the container's health state via the scoped client.
// Returns true if the container is healthy, still in its "starting" probe
// window, or has no health check configured at all (nothing to fail on).
// Only an explicit "unhealthy" status returns false.
func (m *Monitor) checkHealth(ctx context.Context, containerID string) (bool, error) {
	info, err := m.docker.InspectContainer(ctx, containerID)
	if err != nil {
		return false, err
	}
	// No State or no Health block => the image defines no HEALTHCHECK. We can't
	// judge it, so treat it as healthy and let the grace window expire.
	if info.State == nil || info.State.Health == nil {
		return true, nil
	}
	// "starting" means probes haven't concluded yet; don't roll back prematurely.
	return info.State.Health.Status != "unhealthy", nil
}
