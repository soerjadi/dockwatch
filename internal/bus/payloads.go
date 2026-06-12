package bus

import "time"

// ImageUpdatedPayload is published on TopicImageUpdated when a newer image
// digest is detected for a watched container.
type ImageUpdatedPayload struct {
	ContainerID   string
	ContainerName string
	Image         string
	OldDigest     string
	NewDigest     string
	DetectedAt    time.Time
}

// ContainerUnhealthyPayload is published on TopicContainerUnhealthy when a
// container fails its health check after an update was applied.
type ContainerUnhealthyPayload struct {
	ContainerID   string
	ContainerName string
	Image         string
	AppliedDigest string
	PrevDigest    string
	FailedAt      time.Time
}

// UpdateAppliedPayload is published on TopicUpdateApplied after a container
// has been successfully restarted with the new image.
type UpdateAppliedPayload struct {
	ContainerID   string
	ContainerName string
	Image         string
	OldDigest     string
	NewDigest     string
	AppliedAt     time.Time
}

// UpdateSkippedPayload is published on TopicUpdateSkipped when a semver
// rule prevents the automatic update.
type UpdateSkippedPayload struct {
	ContainerID   string
	ContainerName string
	Image         string
	NewDigest     string
	Reason        string
	SkippedAt     time.Time
}

// RollbackDonePayload is published on TopicRollbackDone after a rollback
// to the previous image has completed.
type RollbackDonePayload struct {
	ContainerID   string
	ContainerName string
	Image         string
	RestoredDigest string
	RolledBackAt  time.Time
}
