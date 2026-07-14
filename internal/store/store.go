// Package store holds in-memory runtime state:
//   - current and previous image digests per container (ring buffer)
//   - container metadata snapshot
//
// No persistence layer — state is rebuilt from Docker on startup.
package store

import (
	"sync"
	"time"
)

const digestHistorySize = 5

// DigestEntry is one snapshot of a container's image digest.
type DigestEntry struct {
	Digest    string
	Image     string
	RecordedAt time.Time
}

// ContainerState tracks everything dockwatch knows about a single container.
type ContainerState struct {
	ID      string
	Name    string
	Image   string
	Labels  map[string]string

	// ring buffer of the last N digests; index 0 = most recent
	digestHistory []DigestEntry
	mu            sync.RWMutex
}

// CurrentDigest returns the latest known digest, or "" if unknown.
func (c *ContainerState) CurrentDigest() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.digestHistory) == 0 {
		return ""
	}
	return c.digestHistory[0].Digest
}

// PreviousDigest returns the digest before the last update, or "" if only
// one entry exists.
func (c *ContainerState) PreviousDigest() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.digestHistory) < 2 {
		return ""
	}
	return c.digestHistory[1].Digest
}

// PushDigest prepends a new digest entry to the ring buffer.
func (c *ContainerState) PushDigest(image, digest string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := DigestEntry{Digest: digest, Image: image, RecordedAt: time.Now()}
	c.digestHistory = append([]DigestEntry{entry}, c.digestHistory...)
	if len(c.digestHistory) > digestHistorySize {
		c.digestHistory = c.digestHistory[:digestHistorySize]
	}
}

// Store is the central in-memory registry of container states.
type Store struct {
	mu         sync.RWMutex
	containers map[string]*ContainerState // keyed by container ID
}

// New returns an empty Store.
func New() *Store {
	return &Store{containers: make(map[string]*ContainerState)}
}

// Upsert creates or updates the state for a container.
func (s *Store) Upsert(id, name, image string, labels map[string]string) *ContainerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	cs, ok := s.containers[id]
	if !ok {
		cs = &ContainerState{ID: id, Name: name, Image: image, Labels: labels}
		s.containers[id] = cs
	} else {
		cs.Name = name
		cs.Image = image
		cs.Labels = labels
	}
	return cs
}

// Get returns the state for a container by ID, or nil if not tracked.
func (s *Store) Get(id string) *ContainerState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.containers[id]
}

// GetByName returns the state for a container by name, or nil if not tracked.
func (s *Store) GetByName(name string) *ContainerState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, cs := range s.containers {
		if cs.Name == name {
			return cs
		}
	}
	return nil
}

// Rekey moves a container's state from oldID to newID, preserving its digest
// history. Recreating a container (image update or rollback) produces a new
// container ID; without re-keying, the ring buffer that powers rollback would
// be orphaned under the old ID. Returns the moved state, or nil if oldID was
// not tracked.
func (s *Store) Rekey(oldID, newID string) *ContainerState {
	s.mu.Lock()
	defer s.mu.Unlock()
	cs, ok := s.containers[oldID]
	if !ok {
		return nil
	}
	delete(s.containers, oldID)
	cs.ID = newID
	s.containers[newID] = cs
	return cs
}

// Remove deletes a container from the store.
func (s *Store) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.containers, id)
}

// All returns a snapshot of all tracked container states.
func (s *Store) All() []*ContainerState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*ContainerState, 0, len(s.containers))
	for _, cs := range s.containers {
		out = append(out, cs)
	}
	return out
}
