package executor

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/toolenv"
)

func (m *Mux) InspectToolEnvironment(ctx context.Context, s *session.Session, force bool) (toolenv.Snapshot, error) {
	p, ok := m.pick(s).(backend.ToolEnvironmentExecutor)
	if !ok {
		return toolenv.Snapshot{}, toolenv.Error("backend_unsupported")
	}
	return p.InspectToolEnvironment(ctx, s, force)
}

func (m *Mux) RepairToolEnvironment(ctx context.Context, s *session.Session, action, generation string) (toolenv.Result, error) {
	p, ok := m.pick(s).(backend.ToolEnvironmentExecutor)
	if !ok {
		return toolenv.Result{}, toolenv.Error("backend_unsupported")
	}
	return p.RepairToolEnvironment(ctx, s, action, generation)
}
