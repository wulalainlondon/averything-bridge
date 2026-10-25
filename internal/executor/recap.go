package executor

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/recap"
	"everything-go/internal/session"
)

func (m *Mux) SessionRecap(ctx context.Context, s *session.Session, generate, force bool) (recap.Result, error) {
	p, ok := m.pick(s).(backend.RecapExecutor)
	if !ok {
		return recap.Result{}, errors.New("backend_unsupported")
	}
	return p.SessionRecap(ctx, s, generate, force)
}
