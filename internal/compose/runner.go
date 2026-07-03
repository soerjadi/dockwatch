package compose

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"sync"

	"github.com/soerjadi/dockwatch/internal/deploy"
)

// UpService runs "docker compose -f <configFile> up -d --no-deps <service>"
// from workingDir. When job is non-nil, stdout and stderr are streamed into
// job.AppendLog() line-by-line (AC#3: lines appear within 500 ms of emission).
// Passing nil for job is safe — output is discarded (legacy behaviour).
func UpService(ctx context.Context, workingDir, configFile, service string, log *slog.Logger, job *deploy.DeployJob) error {
	args := []string{"compose", "-f", configFile, "up", "-d", "--pull", "always", "--no-deps", service}
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Dir = workingDir

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("compose up stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("compose up stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("compose up start: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go streamPipe(&wg, bufio.NewScanner(stdout), "stdout", log, job)
	go streamPipe(&wg, bufio.NewScanner(stderr), "stderr", log, job)
	wg.Wait()

	if err := cmd.Wait(); err != nil {
		log.Error("compose up failed", "service", service, "err", err)
		return fmt.Errorf("docker compose up: %w", err)
	}
	log.Info("compose up completed", "service", service)
	return nil
}

// streamPipe drains a pipe via scanner, logging each line and optionally
// forwarding it to the DeployJob. Safe to call with a nil job.
func streamPipe(wg *sync.WaitGroup, scanner *bufio.Scanner, source string, log *slog.Logger, job *deploy.DeployJob) {
	defer wg.Done()
	for scanner.Scan() {
		line := scanner.Text()
		log.Debug("compose "+source, "line", line)
		if job != nil {
			job.AppendLog(source, line)
		}
	}
}
