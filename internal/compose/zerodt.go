package compose

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/soerjadi/dockwatch/internal/dockerclient"
)

// ZeroDTConfig controls the zero-downtime update behaviour.
type ZeroDTConfig struct {
	Timeout time.Duration // how long to wait for new container to become healthy
}

// ZeroDowntimeUpdate performs a rolling update for a compose service:
//  1. Record the IDs of the currently-running containers for the service (the "old" set)
//  2. Patch the compose file with the new tag
//  3. Scale up to 2 instances (--no-recreate keeps the old ones running)
//  4. Wait until the newest container is healthy
//  5. On success: stop+remove each old container directly via the Docker API
//  6. On timeout/crash: restore the compose file and scale back to 1
//
// Requires a reverse proxy (Traefik auto-discovers via Docker labels; Caddy/Nginx
// need manual routing config). Two containers cannot share the same host port.
func ZeroDowntimeUpdate(
	ctx context.Context,
	info *Info,
	newTag string,
	histDir string,
	cfg ZeroDTConfig,
	docker dockerclient.Scoped,
	log *slog.Logger,
) (backupPath string, err error) {
	if len(info.ConfigFiles) == 0 {
		return "", fmt.Errorf("zero-downtime: no config files")
	}
	configFile := info.ConfigFiles[0]

	// Snapshot the old container IDs before we touch anything.
	oldIDs, err := currentContainerIDs(ctx, info, docker)
	if err != nil {
		return "", fmt.Errorf("zero-downtime: list current containers: %w", err)
	}

	backupPath, err = BackupFile(configFile, info.Service, histDir)
	if err != nil {
		return "", err
	}

	if err := UpdateServiceImage(configFile, info.Service, newTag); err != nil {
		return backupPath, err
	}

	scaledUp := false
	restore := func() {
		log.Warn("zero-downtime: rolling back", "service", info.Service)
		if restoreErr := copyFile(backupPath, configFile); restoreErr != nil {
			log.Error("zero-downtime: failed to restore compose file", "err", restoreErr)
		}
		if scaledUp {
			_ = scaleCompose(ctx, info, 1, true, log)
		}
	}

	if err := scaleCompose(ctx, info, 2, true, log); err != nil {
		restore()
		return backupPath, fmt.Errorf("zero-downtime: scale up failed: %w", err)
	}
	scaledUp = true

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	deadline := time.Now().Add(timeout)

	log.Info("zero-downtime: waiting for new container to become healthy",
		"service", info.Service, "timeout", timeout)

	for time.Now().Before(deadline) {
		newID, healthy, checkErr := findNewestHealthy(ctx, info, docker)
		if checkErr != nil {
			log.Warn("zero-downtime: health probe error", "err", checkErr)
		} else if healthy {
			log.Info("zero-downtime: healthy — removing old instance(s)",
				"service", info.Service, "new_id", newID[:min(12, len(newID))])
			if err := removeContainers(ctx, oldIDs, docker, log); err != nil {
				return backupPath, fmt.Errorf("zero-downtime: remove old containers: %w", err)
			}
			return backupPath, nil
		}

		select {
		case <-ctx.Done():
			restore()
			return backupPath, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}

	restore()
	return backupPath, fmt.Errorf("zero-downtime: new container did not become healthy within %s", timeout)
}

// currentContainerIDs returns the IDs of all running containers for the service.
func currentContainerIDs(ctx context.Context, info *Info, docker dockerclient.Scoped) ([]string, error) {
	containers, err := docker.ListContainers(ctx)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, c := range containers {
		if c.Labels["com.docker.compose.project"] != info.Project {
			continue
		}
		if c.Labels["com.docker.compose.service"] != info.Service {
			continue
		}
		ids = append(ids, c.ID)
	}
	return ids, nil
}

// removeContainers stops and removes each container by ID.
func removeContainers(ctx context.Context, ids []string, docker dockerclient.Scoped, log *slog.Logger) error {
	for _, id := range ids {
		short := id[:min(12, len(id))]
		if err := docker.StopContainer(ctx, id); err != nil {
			log.Warn("zero-downtime: stop old container failed", "id", short, "err", err)
		}
		if err := docker.RemoveContainer(ctx, id); err != nil {
			return fmt.Errorf("remove %s: %w", short, err)
		}
		log.Info("zero-downtime: old container removed", "id", short)
	}
	return nil
}

func scaleCompose(ctx context.Context, info *Info, n int, noRecreate bool, log *slog.Logger) error {
	scale := fmt.Sprintf("%s=%d", info.Service, n)
	args := []string{"compose", "-f", info.ConfigFiles[0], "up", "-d", "--pull", "always", "--scale", scale}
	if noRecreate {
		args = append(args, "--no-recreate")
	}
	args = append(args, info.Service)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = info.WorkingDir
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		log.Error("scale compose failed", "args", strings.Join(args, " "), "output", out.String(), "err", err)
		return err
	}
	return nil
}

// findNewestHealthy returns the ID and health status of the newest container
// for the compose service (identified by creation timestamp).
func findNewestHealthy(ctx context.Context, info *Info, docker dockerclient.Scoped) (id string, healthy bool, err error) {
	containers, err := docker.ListContainers(ctx)
	if err != nil {
		return "", false, err
	}

	var newestID string
	var newestCreated int64

	for _, c := range containers {
		if c.Labels["com.docker.compose.project"] != info.Project {
			continue
		}
		if c.Labels["com.docker.compose.service"] != info.Service {
			continue
		}
		if c.Created > newestCreated {
			newestCreated = c.Created
			newestID = c.ID
		}
	}

	if newestID == "" {
		return "", false, fmt.Errorf("no containers found for service %s", info.Service)
	}

	detail, err := docker.InspectContainer(ctx, newestID)
	if err != nil {
		return newestID, false, err
	}

	// No HEALTHCHECK defined → treat as healthy once running
	if detail.State.Health == nil {
		return newestID, detail.State.Running, nil
	}
	return newestID, detail.State.Health.Status == "healthy", nil
}

func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0o644)
}
