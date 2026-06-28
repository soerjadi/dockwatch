package deploy

import "sync"

// JobRegistry is a thread-safe store of in-flight and recently completed
// DeployJobs. It maintains two indexes:
//
//   - jobs[id]           → fast lookup by UUID (for status queries)
//   - byContainer[name]  → tracks the currently running job for a container
//     (removed by Finish so IsRunning reflects live state)
type JobRegistry struct {
	mu          sync.RWMutex
	jobs        map[string]*DeployJob
	byContainer map[string]string // containerName → jobID
}

// NewRegistry creates an empty JobRegistry.
func NewRegistry() *JobRegistry {
	return &JobRegistry{
		jobs:        make(map[string]*DeployJob),
		byContainer: make(map[string]string),
	}
}

// Register adds j to both indexes. Returns an error if a job for this
// container is already running (caller should check IsRunning first).
func (r *JobRegistry) Register(j *DeployJob) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs[j.ID] = j
	r.byContainer[j.ContainerName] = j.ID
	return nil
}

// Get returns the DeployJob for the given UUID, or (nil, false) if unknown.
func (r *JobRegistry) Get(id string) (*DeployJob, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	j, ok := r.jobs[id]
	return j, ok
}

// IsRunning returns true if a deploy for containerName is currently registered
// (i.e. between Register and Finish). Satisfies AC#5.
func (r *JobRegistry) IsRunning(containerName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.byContainer[containerName]
	return ok
}

// Finish removes containerName from the active index. The job record is kept
// in jobs so that status queries can still resolve it after completion.
// Finish is idempotent.
func (r *JobRegistry) Finish(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if j, ok := r.jobs[id]; ok {
		delete(r.byContainer, j.ContainerName)
	}
}
