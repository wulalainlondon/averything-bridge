package backend

import (
	"context"
	"everything-go/internal/session"
	"everything-go/internal/toolenv"
)

// ToolEnvironmentExecutor is optional and deliberately separate from turns,
// compact/maintenance completion, and unrestricted config mutation APIs.
type ToolEnvironmentExecutor interface {
	InspectToolEnvironment(context.Context, *session.Session, bool) (toolenv.Snapshot, error)
	RepairToolEnvironment(context.Context, *session.Session, string, string) (toolenv.Result, error)
}
