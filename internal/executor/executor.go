// Package executor implements backend-adapter helpers on top of the neutral
// backend contract. This is the architectural pivot that unlocks the three test
// configurations:
//
//	GoExecutor      → spawns the CLI + parses streams in Go      (config 2: pure Go)
//	PythonExecutor  → forwards to a Python worker over a socket   (config 3: Go + Python, the "P3" hybrid)
//
// The connection core (hub/client/router) is written ONCE against this
// interface; swapping the Executor swaps the entire backend implementation
// without touching connection, routing, or session management.
package executor

import (
	"context"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// Sink is re-exported for existing backend implementations. The source of truth
// is backend.Sink; executor adds muxing/reliability around that contract.
type Sink = backend.Sink

// Executor is re-exported for existing backend implementations. The source of
// truth is backend.Executor.
type Executor = backend.Executor

// UsageProvider is an optional backend capability: report quota/usage windows.
// Claude (claude.ai OAuth via bun) and Codex (app-server rate limits) implement
// it; Ollama does not. get_usage skips backends that don't.
type UsageProvider interface {
	FetchUsage(ctx context.Context) (*backend.UsageReport, error)
}

// ProcInspector is an optional capability: report / kill the live subprocess
// backing a session, for get_tasks and kill_task. Only the Claude backend
// (one subprocess per session) implements it.
type ProcInspector interface {
	PID(s *session.Session) (int, bool)
	KillProc(s *session.Session) bool
}

// InteractionResponder is an optional capability: pause a turn on an
// AskUserQuestion tool_use and accept the user's answer back to resume it. Only
// the Claude backend implements it. RespondUserInput reports whether the id
// matched a pending interaction.
type InteractionResponder interface {
	RespondUserInput(id string, answers map[string]any, cancelled bool) bool
	PendingInteractions(sessionID string) []backend.UserInputPayload
}

// RuntimeDiagnosticsProvider exposes cached, non-blocking health details for
// backend runtimes. Implementations must not spawn processes or perform network
// I/O from this method; status_result is expected to remain cheap.
type RuntimeDiagnosticsProvider interface {
	RuntimeDiagnostics() map[string]any
}
