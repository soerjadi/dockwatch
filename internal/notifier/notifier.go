// Package notifier subscribes to every bus topic and dispatches notifications.
//
// Unlike Watchtower (which sends a single session-end summary) and WUD (which
// requires an external broker for real-time events), this notifier fires
// per-event via Server-Sent Events (SSE) to the web UI and structured logs.
// Optional outbound webhook POST can be added without changing the bus.
package notifier

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/soerjadi/dockwatch/internal/bus"
)

// SSEClient is a connected browser waiting for Server-Sent Events.
type SSEClient struct {
	ch chan string
}

// Notifier fans out bus events to SSE clients and structured logs.
type Notifier struct {
	bus *bus.Bus
	log *slog.Logger

	mu      sync.RWMutex
	clients map[*SSEClient]struct{}
}

// New creates a Notifier.
func New(b *bus.Bus, log *slog.Logger) *Notifier {
	return &Notifier{
		bus:     b,
		log:     log,
		clients: make(map[*SSEClient]struct{}),
	}
}

// Run subscribes to all bus topics. Blocks until ctx is cancelled.
func (n *Notifier) Run(ctx context.Context) {
	topics := []bus.Topic{
		bus.TopicImageUpdated,
		bus.TopicContainerUnhealthy,
		bus.TopicUpdateApplied,
		bus.TopicUpdateSkipped,
		bus.TopicRollbackDone,
	}

	var wg sync.WaitGroup
	for _, t := range topics {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.runTopic(ctx, t)
		}()
	}
	wg.Wait()
}

func (n *Notifier) runTopic(ctx context.Context, topic bus.Topic) {
	ch, unsub := n.bus.Subscribe(topic)
	defer unsub()

	n.log.Info("notifier subscribed", "topic", topic)

	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ch:
			n.dispatch(ev)
		}
	}
}

// dispatch logs the event and pushes it to all connected SSE clients.
func (n *Notifier) dispatch(ev bus.Event) {
	data, err := json.Marshal(map[string]any{
		"topic":   ev.Topic,
		"payload": ev.Payload,
	})
	if err != nil {
		n.log.Error("notifier marshal error", "err", err)
		return
	}

	n.log.Info("event", "topic", ev.Topic, "payload", string(data))

	// Push to all connected SSE clients.
	n.mu.RLock()
	defer n.mu.RUnlock()
	for client := range n.clients {
		select {
		case client.ch <- string(data):
		default:
			// slow client — drop
		}
	}
}

// AddSSEClient registers a new browser SSE connection.
// Returns a receive channel and a cleanup function.
func (n *Notifier) AddSSEClient() (<-chan string, func()) {
	c := &SSEClient{ch: make(chan string, 32)}

	n.mu.Lock()
	n.clients[c] = struct{}{}
	n.mu.Unlock()

	remove := func() {
		n.mu.Lock()
		delete(n.clients, c)
		close(c.ch)
		n.mu.Unlock()
	}

	return c.ch, remove
}
