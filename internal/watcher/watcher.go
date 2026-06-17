// Package watcher subscribes to the Docker daemon's real-time event stream
// and translates raw Docker events into bus publications.
//
// This replaces the poll loop used by Watchtower and WUD. Instead of waking
// on a timer, we receive a push notification from the daemon the moment
// something changes (image pull, container die, health_status change, etc.).
//
// The watcher talks to the daemon only through dockerclient.Scoped — it can
// read the event stream and inspect containers, and nothing else.
package watcher

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/soerjadi/dockwatch/internal/bus"
	"github.com/soerjadi/dockwatch/internal/dockerclient"
	"github.com/soerjadi/dockwatch/internal/store"
)

// DockerClient is the narrow slice of dockerclient.Scoped the watcher needs:
// the event stream plus the ability to inspect a container when it starts.
// Both *dockerclient.Client and test fakes satisfy it.
type DockerClient interface {
	StreamEvents(ctx context.Context) (<-chan dockerclient.Event, <-chan error)
	InspectContainer(ctx context.Context, id string) (inspect, error)
	Close() error
}

// inspect is an alias for the SDK inspect result, re-exported through
// dockerclient so the watcher's interface stays in this package's vocabulary.
// (Declared as a type alias to types.ContainerJSON via dockerclient.)
type inspect = dockerclient.InspectResult

// Watcher listens to the Docker event stream and publishes to the bus.
type Watcher struct {
	docker DockerClient
	bus    *bus.Bus
	store  *store.Store
	log    *slog.Logger
}

// New creates a Watcher backed by a scoped Docker client.
func New(docker DockerClient, b *bus.Bus, st *store.Store, log *slog.Logger) *Watcher {
	return &Watcher{docker: docker, bus: b, store: st, log: log}
}

// Run starts the event stream loop. Blocks until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context) error {
	eventCh, errCh := w.docker.StreamEvents(ctx)
	w.log.Info("docker event stream subscribed")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			if err != nil {
				return err
			}
		case ev := <-eventCh:
			w.handleEvent(ctx, ev)
		}
	}
}

func (w *Watcher) handleEvent(ctx context.Context, ev dockerclient.Event) {
	switch ev.Type {
	case "image":
		w.handleImageEvent(ev)
	case "container":
		w.handleContainerEvent(ctx, ev)
	}
}

// handleImageEvent fires when a new image is pulled on the host.
func (w *Watcher) handleImageEvent(ev dockerclient.Event) {
	if ev.Action != "pull" {
		return
	}
	// ev.Name = "image:tag" (Actor.Attributes["name"]), ev.ID = image content digest.
	imageName := ev.Name
	if imageName == "" {
		return
	}
	newDigest := ev.ID

	for _, cs := range w.store.All() {
		// Strip any digest suffix from the stored image for comparison — this
		// handles containers created via "image@sha256:..." references.
		storedImage := cs.Image
		if idx := strings.Index(storedImage, "@"); idx != -1 {
			storedImage = storedImage[:idx]
		}
		if storedImage != imageName {
			continue
		}
		if newDigest != "" && cs.CurrentDigest() == newDigest {
			continue // already at this digest
		}
		w.log.Info("image pull detected for watched container",
			"container", cs.Name,
			"image", imageName,
			"digest", newDigest,
		)
		w.bus.Publish(bus.TopicImageUpdated, bus.ImageUpdatedPayload{
			ContainerID:   cs.ID,
			ContainerName: cs.Name,
			Image:         imageName,
			OldDigest:     cs.CurrentDigest(),
			NewDigest:     newDigest,
			DetectedAt:    time.Now(),
		})
	}
}

// handleContainerEvent handles container lifecycle events.
func (w *Watcher) handleContainerEvent(ctx context.Context, ev dockerclient.Event) {
	switch ev.Action {
	case "start":
		w.log.Info("container started", "name", ev.Name)
		info, err := w.docker.InspectContainer(ctx, ev.ID)
		if err != nil {
			w.log.Warn("inspect on start failed", "name", ev.Name, "err", err)
			return
		}
		name := strings.TrimPrefix(info.Name, "/")
		var labels map[string]string
		image := ev.Image
		if info.Config != nil {
			labels = info.Config.Labels
			if image == "" {
				image = info.Config.Image
			}
		}
		w.store.Upsert(ev.ID, name, image, labels)

	case "destroy":
		w.store.Remove(ev.ID)
		w.log.Info("container removed from store", "name", ev.Name)

	case "health_status: unhealthy":
		w.log.Warn("container unhealthy", "name", ev.Name)
		cs := w.store.Get(ev.ID)
		if cs == nil {
			return
		}
		w.bus.Publish(bus.TopicContainerUnhealthy, bus.ContainerUnhealthyPayload{
			ContainerID:   cs.ID,
			ContainerName: cs.Name,
			Image:         cs.Image,
			AppliedDigest: cs.CurrentDigest(),
			PrevDigest:    cs.PreviousDigest(),
			FailedAt:      time.Now(),
		})
	}
}

// Close releases the Docker connection.
func (w *Watcher) Close() error { return w.docker.Close() }
