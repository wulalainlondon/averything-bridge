package backend

import "everything-go/internal/session"

// PMConfiguration is supplied by the trusted Bridge, never by a chat message.
type PMConfiguration struct {
	Instructions, MCPURL, Token, RuntimeDir string
	Tools                                   []map[string]any
	Worker                                  bool
	Sandbox                                 string
	RequestID                               string
}
type PMProvider interface {
	PMConfiguration(sessionID string) (*PMConfiguration, error)
}
type CollaborationWorkerProvider interface {
	IsReadOnlyCollaborationWorker(sessionID string) (bool, error)
}
type TurnAdmission interface {
	// The release function keeps takeover and dispatch ordered through the point
	// at which the executor accepts a turn. It must always be called.
	AdmitTurn(*session.Session, string, string) (string, func(), error)
}
