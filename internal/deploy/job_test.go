package deploy_test

import (
	"sync"
	"testing"
	"time"

	"github.com/soerjadi/dockwatch/internal/deploy"
)

// TestJobTransitionClosesLogCh verifies AC#4: LogCh is closed on every exit path.
func TestJobTransitionClosesLogCh(t *testing.T) {
	t.Parallel()

	for _, status := range []deploy.JobStatus{deploy.StatusSuccess, deploy.StatusFailed} {
		status := status
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()
			j := deploy.NewJob("web", "nginx:1.0", "webhook")
			j.Transition(deploy.StatusRunning)
			j.Transition(status)

			// LogCh must be closed — reading from a closed channel returns immediately.
			select {
			case _, ok := <-j.LogCh:
				if ok {
					t.Fatalf("expected LogCh to be closed after %s, got a value", status)
				}
				// ok == false means channel is closed ✓
			case <-time.After(time.Second):
				t.Fatalf("LogCh not closed within 1s after transitioning to %s", status)
			}
		})
	}
}

// TestJobTransitionIdempotent ensures double-closing doesn't panic.
func TestJobTransitionIdempotent(t *testing.T) {
	t.Parallel()
	j := deploy.NewJob("db", "postgres:15", "poll")
	j.Transition(deploy.StatusRunning)
	j.Transition(deploy.StatusSuccess)
	// second call must not panic
	j.Transition(deploy.StatusSuccess)
	j.Transition(deploy.StatusFailed)
}

// TestAppendLogCappedAt500 verifies that in-memory logs are bounded.
func TestAppendLogCappedAt500(t *testing.T) {
	t.Parallel()
	j := deploy.NewJob("svc", "img:v1", "manual")
	for i := 0; i < 600; i++ {
		j.AppendLog("stdout", "line")
	}
	snap := j.Snap()
	if snap.LogCount > 500 {
		t.Fatalf("expected log count ≤ 500, got %d", snap.LogCount)
	}
}

// TestIsRunningLifecycle covers AC#5.
func TestIsRunningLifecycle(t *testing.T) {
	t.Parallel()
	r := deploy.NewRegistry()
	j := deploy.NewJob("api", "myapp:latest", "webhook")

	if r.IsRunning("api") {
		t.Fatal("IsRunning must be false before Register")
	}
	_ = r.Register(j)
	if !r.IsRunning("api") {
		t.Fatal("IsRunning must be true after Register")
	}
	r.Finish(j.ID)
	if r.IsRunning("api") {
		t.Fatal("IsRunning must be false after Finish")
	}
}

// TestJobRace exercises AppendLog and Transition from many goroutines to
// satisfy AC#6 (go test -race passes).
func TestJobRace(t *testing.T) {
	t.Parallel()
	j := deploy.NewJob("race-svc", "img:v1", "poll")
	j.Transition(deploy.StatusRunning)

	var wg sync.WaitGroup

	// Drain LogCh in background so AppendLog never blocks.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range j.LogCh { //nolint:revive
		}
	}()

	// 50 goroutines writing logs concurrently.
	writers := 50
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(n int) {
			defer wg.Done()
			for k := 0; k < 20; k++ {
				j.AppendLog("stdout", "line")
			}
		}(i)
	}

	// Wait for writers then terminate.
	writeDone := make(chan struct{})
	go func() {
		// Let writers finish before closing
		time.Sleep(50 * time.Millisecond)
		j.Transition(deploy.StatusSuccess)
		close(writeDone)
	}()

	<-writeDone
	wg.Wait()
}

// TestRegistryRace exercises concurrent Register/Get/IsRunning/Finish.
func TestRegistryRace(t *testing.T) {
	t.Parallel()
	r := deploy.NewRegistry()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			j := deploy.NewJob("svc", "img:v1", "poll")
			_ = r.Register(j)
			_ = r.IsRunning("svc")
			_, _ = r.Get(j.ID)
			r.Finish(j.ID)
		}(i)
	}
	wg.Wait()
}
