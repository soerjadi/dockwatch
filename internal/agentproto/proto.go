// Package agentproto defines the JSON message types exchanged over the
// WebSocket connection between the dockwatch controller and remote agents.
package agentproto

const (
	TypeRegister = "register" // Agent → Controller: announce presence
	TypeCommand  = "command"  // Controller → Agent: execute a command
	TypeOutput   = "output"   // Agent → Controller: stdout/stderr line
	TypeDone     = "done"     // Agent → Controller: command finished
	TypePing     = "ping"     // Controller → Agent: keepalive
	TypePong     = "pong"     // Agent → Controller: keepalive reply
)

// RegisterMsg is sent by the agent immediately after connecting.
type RegisterMsg struct {
	Type     string `json:"type"`     // TypeRegister
	Hostname string `json:"hostname"` // the agent's hostname
	Token    string `json:"token"`    // shared pre-auth token
}

// CommandMsg is sent by the controller to trigger an action on the agent.
type CommandMsg struct {
	Type    string `json:"type"`    // TypeCommand
	ID      string `json:"id"`      // unique command ID (UUID)
	Action  string `json:"action"`  // "update" | "rollback"
	Service string `json:"service"` // compose service name
	Image   string `json:"image"`   // target image (for update)
}

// OutputMsg carries a single line of stdout/stderr from the agent.
type OutputMsg struct {
	Type string `json:"type"` // TypeOutput
	ID   string `json:"id"`   // command ID this belongs to
	Line string `json:"line"` // output line
}

// DoneMsg signals that a command has finished.
type DoneMsg struct {
	Type     string `json:"type"`     // TypeDone
	ID       string `json:"id"`       // command ID
	ExitCode int    `json:"exitCode"` // 0 = success
	Error    string `json:"error,omitempty"`
}

// PingMsg / PongMsg are keepalive messages with no payload beyond the type.
type PingMsg struct {
	Type string `json:"type"` // TypePing
}

type PongMsg struct {
	Type string `json:"type"` // TypePong
}
