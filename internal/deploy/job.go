// Package deploy owns the DeployJob type and JobRegistry.
// It wraps what the executor does with an observable identity — every deploy
// gets a UUID, a status, and a streamed log so callers can track progress.
package deploy

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

// JobStatus represents the lifecycle state of a single deploy.
type JobStatus string

const (
	StatusQueued  JobStatus = "queued"
	StatusRunning JobStatus = "running"
	StatusSuccess JobStatus = "success"
	StatusFailed  JobStatus = "failed"
)

// maxLogLines is the upper bound on in-memory log lines per job.
const maxLogLines = 500

// LogLine is the unit of captured output from docker commands.
type LogLine struct {
	Seq    int       `json:"seq"`
	Ts     time.Time `json:"ts"`
	Source string    `json:"source"` // "stdout" | "stderr"
	Text   string    `json:"text"`
}

// DeployJob is the observable handle to a single deploy lifecycle.
// All exported fields and methods are safe for concurrent access.
type DeployJob struct {
	ID            string
	ContainerName string
	Image         string
	TriggerSource string // "webhook" | "poll" | "manual"

	// LogCh receives each log line as it arrives. It is closed exactly once
	// when the job reaches a terminal state (success or failed), satisfying AC#4.
	// Callers must not send to LogCh; use AppendLog instead.
	LogCh chan LogLine

	mu      sync.Mutex // single lock guards all mutable state AND channel operations
	status  JobStatus
	started time.Time
	ended   *time.Time
	logs    []LogLine
	seq     int
	chDone  bool      // true once LogCh has been closed
	once    sync.Once // belt-and-suspenders: close exactly once
}

// NewJob creates a DeployJob in the Queued state.
func NewJob(containerName, image, triggerSource string) *DeployJob {
	return &DeployJob{
		ID:            newUUID(),
		ContainerName: containerName,
		Image:         image,
		TriggerSource: triggerSource,
		status:        StatusQueued,
		started:       time.Now(),
		LogCh:         make(chan LogLine, 256),
	}
}

// AppendLog records one log line. The line is appended to the in-memory log
// buffer (capped at maxLogLines) and forwarded non-blocking to LogCh.
// The mutex is held for the entire operation so the channel send and the
// channel close (in Transition) are mutually exclusive — no data race.
// AC#3: lines appear in Logs within 500 ms of being emitted.
func (j *DeployJob) AppendLog(source, text string) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.seq++
	line := LogLine{
		Seq:    j.seq,
		Ts:     time.Now(),
		Source: source,
		Text:   text,
	}
	if len(j.logs) < maxLogLines {
		j.logs = append(j.logs, line)
	}

	// Only send when the channel is still open.
	if !j.chDone {
		select {
		case j.LogCh <- line:
		default:
			// buffer full — drop to avoid blocking; log is still in j.logs
		}
	}
}

// Transition moves the job to a new status. For terminal states (success, failed)
// it sets EndedAt and closes LogCh exactly once, satisfying AC#4.
// Calling Transition after the job is already terminal is a no-op (idempotent).
func (j *DeployJob) Transition(status JobStatus) {
	j.mu.Lock()
	// Guard: don't re-transition if already terminal.
	if j.status == StatusSuccess || j.status == StatusFailed {
		j.mu.Unlock()
		return
	}
	j.status = status
	if status == StatusSuccess || status == StatusFailed {
		now := time.Now()
		j.ended = &now
		j.chDone = true
		// Close the channel while holding the lock so no AppendLog goroutine
		// can be mid-send. sync.Once provides extra safety for concurrent
		// Transition calls from different goroutines.
		j.once.Do(func() { close(j.LogCh) })
	}
	j.mu.Unlock()
}

// Snapshot is a consistent, immutable view of the job's current state.
type Snapshot struct {
	ID            string     `json:"id"`
	ContainerName string     `json:"container"`
	Image         string     `json:"image"`
	TriggerSource string     `json:"trigger_source"`
	Status        JobStatus  `json:"status"`
	StartedAt     time.Time  `json:"started_at"`
	EndedAt       *time.Time `json:"ended_at,omitempty"`
	LogCount      int        `json:"log_count"`
}

// Snap returns a read-consistent snapshot of the job for API responses.
func (j *DeployJob) Snap() Snapshot {
	j.mu.Lock()
	defer j.mu.Unlock()
	return Snapshot{
		ID:            j.ID,
		ContainerName: j.ContainerName,
		Image:         j.Image,
		TriggerSource: j.TriggerSource,
		Status:        j.status,
		StartedAt:     j.started,
		EndedAt:       j.ended,
		LogCount:      len(j.logs),
	}
}

// Logs returns a defensive copy of the captured log lines.
func (j *DeployJob) Logs() []LogLine {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]LogLine, len(j.logs))
	copy(out, j.logs)
	return out
}

// newUUID generates a random UUID v4 via crypto/rand (no external deps).
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
