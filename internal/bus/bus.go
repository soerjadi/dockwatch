// Package bus provides an in-memory pub/sub event bus backed by Go channels.
// No external broker (Kafka, MQTT, Redis) is required — all message passing
// happens inside the single process via typed topics and buffered channels.
package bus

import (
	"sync"
)

// Topic is a named event channel.
type Topic string

const (
	TopicImageUpdated      Topic = "image.updated"
	TopicContainerUnhealthy Topic = "container.unhealthy"
	TopicUpdateApplied     Topic = "update.applied"
	TopicUpdateSkipped     Topic = "update.skipped"
	TopicRollbackDone      Topic = "rollback.done"
)

// Event carries a topic and an arbitrary payload.
type Event struct {
	Topic   Topic
	Payload any
}

// subscriber holds a channel and a unique ID.
type subscriber struct {
	id string
	ch chan Event
}

// Bus is the central in-memory pub/sub router.
// Producers call Publish; consumers call Subscribe.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[Topic][]*subscriber
	counter     int
}

// New creates a ready-to-use Bus.
func New() *Bus {
	return &Bus{
		subscribers: make(map[Topic][]*subscriber),
	}
}

// Subscribe registers interest in a topic and returns a receive-only channel
// and an unsubscribe function. The channel is buffered to 64 events; slow
// consumers will drop events rather than block the publisher.
func (b *Bus) Subscribe(topic Topic) (<-chan Event, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.counter++
	id := string(topic) + ":" + itoa(b.counter)
	sub := &subscriber{id: id, ch: make(chan Event, 64)}
	b.subscribers[topic] = append(b.subscribers[topic], sub)

	unsub := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		subs := b.subscribers[topic]
		for i, s := range subs {
			if s.id == id {
				close(s.ch)
				b.subscribers[topic] = append(subs[:i], subs[i+1:]...)
				return
			}
		}
	}

	return sub.ch, unsub
}

// Publish broadcasts an event to all subscribers of the given topic.
// Non-blocking: if a subscriber's buffer is full the event is dropped for
// that subscriber only.
func (b *Bus) Publish(topic Topic, payload any) {
	b.mu.RLock()
	subs := b.subscribers[topic]
	b.mu.RUnlock()

	ev := Event{Topic: topic, Payload: payload}
	for _, s := range subs {
		select {
		case s.ch <- ev:
		default:
			// subscriber too slow — drop rather than block
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
