// Package compose detects Docker Compose-managed containers from their labels.
//
// dockwatch treats compose-managed containers as notify-only — it detects
// image updates and publishes events, but never auto-applies updates or edits
// compose files. Updates must be applied externally:
//
//	docker compose pull <service>
//	docker compose up -d --no-deps <service>
package compose

import "strings"

const (
	labelProject     = "com.docker.compose.project"
	labelService     = "com.docker.compose.service"
	labelConfigFiles = "com.docker.compose.project.config_files"
	labelWorkingDir  = "com.docker.compose.project.working_dir"
)

// Info holds Docker Compose metadata extracted from container labels.
type Info struct {
	Project     string
	Service     string
	ConfigFiles []string
	WorkingDir  string
}

// FromLabels extracts compose metadata from container labels.
// Returns nil if the container is not managed by Docker Compose.
func FromLabels(labels map[string]string) *Info {
	project := labels[labelProject]
	if project == "" {
		return nil
	}
	var files []string
	if raw := labels[labelConfigFiles]; raw != "" {
		for _, f := range strings.Split(raw, ",") {
			if f = strings.TrimSpace(f); f != "" {
				files = append(files, f)
			}
		}
	}
	return &Info{
		Project:     project,
		Service:     labels[labelService],
		ConfigFiles: files,
		WorkingDir:  labels[labelWorkingDir],
	}
}
