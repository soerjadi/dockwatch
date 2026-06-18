// Package agentserver implements the controller-side WebSocket hub that
// accepts inbound connections from remote dockwatch agents.
package agentserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"

	"github.com/soerjadi/dockwatch/internal/agentproto"
)

// AgentConn represents a connected remote agent.
type AgentConn struct {
	Hostname    string
	ConnectedAt time.Time
	conn        *websocket.Conn
	send        chan any // outbound messages
}

// Server manages agent WebSocket connections.
type Server struct {
	token string
	log   *slog.Logger

	mu     sync.RWMutex
	agents map[string]*AgentConn // keyed by hostname
}

// New creates an agent Server that authenticates connections using token.
func New(token string, log *slog.Logger) *Server {
	return &Server{
		token:  token,
		log:    log,
		agents: make(map[string]*AgentConn),
	}
}

// ServeHTTP upgrades the HTTP request to a WebSocket connection and starts
// the agent session. Register this at GET /agent/connect.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: false,
	})
	if err != nil {
		s.log.Warn("agent: websocket upgrade failed", "err", err)
		return
	}

	ctx := r.Context()

	// First message must be a RegisterMsg
	var reg agentproto.RegisterMsg
	if err := wsjson.Read(ctx, conn, &reg); err != nil {
		s.log.Warn("agent: failed to read register message", "err", err)
		_ = conn.Close(websocket.StatusPolicyViolation, "expected register")
		return
	}
	if reg.Type != agentproto.TypeRegister {
		_ = conn.Close(websocket.StatusPolicyViolation, "expected register")
		return
	}
	if s.token != "" && reg.Token != s.token {
		s.log.Warn("agent: authentication failed", "hostname", reg.Hostname)
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid token")
		return
	}

	ac := &AgentConn{
		Hostname:    reg.Hostname,
		ConnectedAt: time.Now(),
		conn:        conn,
		send:        make(chan any, 32),
	}
	s.register(ac)
	defer s.unregister(ac.Hostname)

	s.log.Info("agent connected", "hostname", ac.Hostname)

	go s.writeLoop(ctx, ac)
	s.readLoop(ctx, ac)
}

func (s *Server) writeLoop(ctx context.Context, ac *AgentConn) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = wsjson.Write(ctx, ac.conn, agentproto.PingMsg{Type: agentproto.TypePing})
		case msg, ok := <-ac.send:
			if !ok {
				return
			}
			if err := wsjson.Write(ctx, ac.conn, msg); err != nil {
				s.log.Warn("agent: write error", "hostname", ac.Hostname, "err", err)
				return
			}
		}
	}
}

func (s *Server) readLoop(ctx context.Context, ac *AgentConn) {
	for {
		var raw json.RawMessage
		if err := wsjson.Read(ctx, ac.conn, &raw); err != nil {
			s.log.Info("agent disconnected", "hostname", ac.Hostname, "err", err)
			return
		}
		var base struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &base); err != nil {
			continue
		}
		switch base.Type {
		case agentproto.TypePong:
			// keepalive — ignore
		case agentproto.TypeOutput:
			var msg agentproto.OutputMsg
			if err := json.Unmarshal(raw, &msg); err == nil {
				s.log.Info("agent output", "hostname", ac.Hostname, "cmd", msg.ID, "line", msg.Line)
			}
		case agentproto.TypeDone:
			var msg agentproto.DoneMsg
			if err := json.Unmarshal(raw, &msg); err == nil {
				s.log.Info("agent command done", "hostname", ac.Hostname, "cmd", msg.ID,
					"exit_code", msg.ExitCode, "error", msg.Error)
			}
		}
	}
}

// Dispatch sends a command to the named agent. Returns an error if the agent
// is not connected.
func (s *Server) Dispatch(hostname string, cmd agentproto.CommandMsg) error {
	s.mu.RLock()
	ac, ok := s.agents[hostname]
	s.mu.RUnlock()
	if !ok {
		return &AgentNotFoundError{Hostname: hostname}
	}
	select {
	case ac.send <- cmd:
		return nil
	default:
		return &AgentNotFoundError{Hostname: hostname}
	}
}

// ListAgents returns a snapshot of all currently connected agents.
func (s *Server) ListAgents() []AgentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]AgentInfo, 0, len(s.agents))
	for _, ac := range s.agents {
		out = append(out, AgentInfo{
			Hostname:    ac.Hostname,
			ConnectedAt: ac.ConnectedAt,
		})
	}
	return out
}

// AgentInfo is the public view of a connected agent.
type AgentInfo struct {
	Hostname    string    `json:"hostname"`
	ConnectedAt time.Time `json:"connectedAt"`
}

// AgentNotFoundError is returned when dispatching to an unknown hostname.
type AgentNotFoundError struct {
	Hostname string
}

func (e *AgentNotFoundError) Error() string {
	return "agent not connected: " + e.Hostname
}

func (s *Server) register(ac *AgentConn) {
	s.mu.Lock()
	s.agents[ac.Hostname] = ac
	s.mu.Unlock()
}

func (s *Server) unregister(hostname string) {
	s.mu.Lock()
	delete(s.agents, hostname)
	s.mu.Unlock()
}
