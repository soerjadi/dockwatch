// dockwatch-agent — headless agent that runs on remote hosts and connects
// outbound to a dockwatch controller.
//
// The agent establishes a WebSocket connection to the controller, authenticates
// with a shared token, then listens for commands (update/rollback) and streams
// output back. Outbound-only connection means no firewall ports need to be opened
// on the agent host.
//
// Configuration via environment variables:
//
//	DOCKWATCH_CONTROLLER_URL  — e.g. "wss://dockwatch.example.com/agent/connect"
//	DOCKWATCH_AGENT_TOKEN     — shared secret matching the controller's token
//	DOCKWATCH_LOG_LEVEL       — "debug" | "info" | "warn" | "error" (default: "info")
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"

	"github.com/soerjadi/dockwatch/internal/agentproto"
)

func main() {
	controllerURL := mustEnv("DOCKWATCH_CONTROLLER_URL")
	token := os.Getenv("DOCKWATCH_AGENT_TOKEN")
	logLevel := os.Getenv("DOCKWATCH_LOG_LEVEL")

	level := slog.LevelInfo
	switch logLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	hostname, _ := os.Hostname()
	log.Info("dockwatch-agent starting", "hostname", hostname, "controller", controllerURL)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Reconnect loop with exponential backoff
	backoff := 2 * time.Second
	for {
		if err := runSession(ctx, controllerURL, token, hostname, log); err != nil {
			if ctx.Err() != nil {
				log.Info("agent shutting down")
				return
			}
			log.Warn("session ended, reconnecting", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 60*time.Second {
				backoff *= 2
			}
		} else {
			backoff = 2 * time.Second
		}
	}
}

func runSession(ctx context.Context, url, token, hostname string, log *slog.Logger) error {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()

	// Authenticate
	reg := agentproto.RegisterMsg{
		Type:     agentproto.TypeRegister,
		Hostname: hostname,
		Token:    token,
	}
	if err := wsjson.Write(ctx, conn, reg); err != nil {
		return fmt.Errorf("register: %w", err)
	}
	log.Info("registered with controller", "hostname", hostname)

	// Message loop
	for {
		var raw json.RawMessage
		if err := wsjson.Read(ctx, conn, &raw); err != nil {
			return fmt.Errorf("read: %w", err)
		}

		var base struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &base); err != nil {
			continue
		}

		switch base.Type {
		case agentproto.TypePing:
			_ = wsjson.Write(ctx, conn, agentproto.PongMsg{Type: agentproto.TypePong})
		case agentproto.TypeCommand:
			var cmd agentproto.CommandMsg
			if err := json.Unmarshal(raw, &cmd); err != nil {
				continue
			}
			go handleCommand(ctx, conn, cmd, log)
		}
	}
}

func handleCommand(ctx context.Context, conn *websocket.Conn, cmd agentproto.CommandMsg, log *slog.Logger) {
	log.Info("received command", "id", cmd.ID, "action", cmd.Action, "service", cmd.Service)

	send := func(msg any) {
		_ = wsjson.Write(ctx, conn, msg)
	}

	args, err := buildArgs(cmd)
	if err != nil {
		send(agentproto.DoneMsg{
			Type:     agentproto.TypeDone,
			ID:       cmd.ID,
			ExitCode: 1,
			Error:    err.Error(),
		})
		return
	}

	c := exec.CommandContext(ctx, args[0], args[1:]...)
	var buf bytes.Buffer
	c.Stdout = &lineWriter{id: cmd.ID, conn: conn, ctx: ctx, buf: &buf}
	c.Stderr = c.Stdout

	exitCode := 0
	runErr := ""
	if err := c.Run(); err != nil {
		exitCode = 1
		runErr = err.Error()
	}

	send(agentproto.DoneMsg{
		Type:     agentproto.TypeDone,
		ID:       cmd.ID,
		ExitCode: exitCode,
		Error:    runErr,
	})
}

func buildArgs(cmd agentproto.CommandMsg) ([]string, error) {
	switch cmd.Action {
	case "update":
		if cmd.Service == "" {
			return nil, fmt.Errorf("service is required for update")
		}
		// docker compose pull <service> && docker compose up -d --no-deps <service>
		return []string{"sh", "-c",
			fmt.Sprintf("docker compose pull %s && docker compose up -d --no-deps %s",
				cmd.Service, cmd.Service)}, nil
	case "rollback":
		if cmd.Service == "" {
			return nil, fmt.Errorf("service is required for rollback")
		}
		return []string{"docker", "compose", "up", "-d", "--no-deps", cmd.Service}, nil
	default:
		return nil, fmt.Errorf("unknown action: %s", cmd.Action)
	}
}

// lineWriter writes command output line-by-line as OutputMsg over WebSocket.
type lineWriter struct {
	id   string
	conn *websocket.Conn
	ctx  context.Context
	buf  *bytes.Buffer
}

func (lw *lineWriter) Write(p []byte) (int, error) {
	lw.buf.Write(p)
	for {
		line, rest, found := strings.Cut(lw.buf.String(), "\n")
		if !found {
			break
		}
		lw.buf.Reset()
		lw.buf.WriteString(rest)
		_ = wsjson.Write(lw.ctx, lw.conn, agentproto.OutputMsg{
			Type: agentproto.TypeOutput,
			ID:   lw.id,
			Line: strings.TrimRight(line, "\r"),
		})
	}
	return len(p), nil
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "fatal: %s is required\n", key)
		os.Exit(1)
	}
	return v
}
