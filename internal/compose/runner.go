package compose

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
)

// UpService runs "docker compose -f <configFile> up -d --no-deps <service>"
// from workingDir, streaming output to log. Returns an error on non-zero exit.
func UpService(ctx context.Context, workingDir, configFile, service string, log *slog.Logger) error {
	args := []string{"compose", "-f", configFile, "up", "-d", "--no-deps", service}
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = workingDir

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		log.Error("compose up failed",
			"service", service,
			"output", strings.TrimSpace(out.String()),
			"err", err,
		)
		return fmt.Errorf("docker compose up: %w", err)
	}
	log.Info("compose up completed", "service", service, "output", strings.TrimSpace(out.String()))
	return nil
}
