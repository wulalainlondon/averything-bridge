package executor

import (
	"context"
	"fmt"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// CatalogDefinitions overlays runtime catalogs on the bridge's static
// fallback registry. A backend failure only leaves that backend on fallback.
func (m *Mux) CatalogDefinitions(ctx context.Context, base []backend.Definition) []backend.Definition {
	out := append([]backend.Definition(nil), base...)
	for i := range out {
		e, ok := m.byBackend[out[i].ID]
		if !ok {
			continue
		}
		cp, ok := e.(backend.CatalogProvider)
		if !ok {
			continue
		}
		if d, err := cp.Catalog(ctx); err == nil {
			// Keep configured transport capabilities authoritative.
			d.Capabilities = out[i].Capabilities
			out[i] = d
		}
	}
	return out
}

// Mux routes Executor calls to a per-backend implementation based on
// Session.Backend (claude / codex / ollama / ...). This is how the single Go
// connection core supports multiple AI runtimes simultaneously, mirroring the
// Python bridge's backend registry.
type Mux struct {
	admission backend.TurnAdmission
	byBackend map[string]Executor
	def       Executor
	terminal  *TerminalSink
}

func (m *Mux) SetTurnAdmission(p backend.TurnAdmission) { m.admission = p }

func (m *Mux) ObserveNativeLifecycle(s *session.Session, turnID, phase string) {
	if observer, ok := m.pick(s).(backend.NativeTurnObserver); ok {
		observer.ObserveNativeLifecycle(s, turnID, phase)
	}
}

func NewMux(byBackend map[string]Executor, def Executor) *Mux {
	return &Mux{byBackend: byBackend, def: def}
}

func NewReliableMux(byBackend map[string]Executor, def Executor, terminal *TerminalSink) *Mux {
	return &Mux{byBackend: byBackend, def: def, terminal: terminal}
}

func (m *Mux) pick(s *session.Session) Executor {
	if e, ok := m.byBackend[s.Backend()]; ok {
		return e
	}
	return m.def
}

func (m *Mux) Send(ctx context.Context, s *session.Session, reqID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	if m.admission != nil {
		updated, release, err := m.admission.AdmitTurn(s, reqID, content)
		if err != nil {
			if m.terminal != nil {
				m.terminal.Emit(backend.NewError(s.ID, reqID, "pm_policy_rejected", err.Error()))
			}
			return err
		}
		defer release()
		content = updated
	}
	e := m.pick(s)
	if m.terminal == nil {
		return e.Send(ctx, s, reqID, content, images, files)
	}
	return sendReliable(ctx, e, m.terminal, s, reqID, content, images, files)
}
func (m *Mux) Stop(ctx context.Context, s *session.Session) error  { return m.pick(s).Stop(ctx, s) }
func (m *Mux) Clear(ctx context.Context, s *session.Session) error { return m.pick(s).Clear(ctx, s) }
func (m *Mux) Close(ctx context.Context, s *session.Session) error { return m.pick(s).Close(ctx, s) }

func (m *Mux) Steer(ctx context.Context, s *session.Session, clientUserMessageID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) (backend.SteerResult, error) {
	steerer, ok := m.pick(s).(backend.SteeringExecutor)
	if !ok {
		return backend.SteerResult{}, backend.ErrUnsupportedSteer
	}
	return steerer.Steer(ctx, s, clientUserMessageID, content, images, files)
}

func (m *Mux) SetGoal(ctx context.Context, s *session.Session, objective, status string, tokenBudget *int) error {
	gc, ok := m.pick(s).(backend.GoalController)
	if !ok {
		return backend.ErrUnsupportedGoal
	}
	return gc.SetGoal(ctx, s, objective, status, tokenBudget)
}

func (m *Mux) GetGoal(ctx context.Context, s *session.Session) error {
	gc, ok := m.pick(s).(backend.GoalController)
	if !ok {
		return backend.ErrUnsupportedGoal
	}
	return gc.GetGoal(ctx, s)
}

func (m *Mux) ClearGoal(ctx context.Context, s *session.Session) error {
	gc, ok := m.pick(s).(backend.GoalController)
	if !ok {
		return backend.ErrUnsupportedGoal
	}
	return gc.ClearGoal(ctx, s)
}

func (m *Mux) ReleaseSession(ctx context.Context, s *session.Session) error {
	controller, ok := m.pick(s).(backend.SessionControlExecutor)
	if !ok {
		return nil
	}
	return controller.ReleaseSession(ctx, s)
}

func (m *Mux) ClaimSession(ctx context.Context, s *session.Session) error {
	controller, ok := m.pick(s).(backend.SessionControlExecutor)
	if !ok {
		return nil
	}
	return controller.ClaimSession(ctx, s)
}

func (m *Mux) UpdateSessionSettings(ctx context.Context, s *session.Session) error {
	updater, ok := m.pick(s).(interface {
		UpdateSessionSettings(context.Context, *session.Session) error
	})
	if !ok {
		return nil
	}
	return updater.UpdateSessionSettings(ctx, s)
}

// ProviderFor returns the history provider backing this session's backend, if any.
func (m *Mux) ProviderFor(s *session.Session) (backend.HistoryProvider, bool) {
	hp, ok := m.pick(s).(backend.HistoryProvider)
	return hp, ok
}

// FinalAnswerForSession provides the exact terminal answer for Bridge-managed
// delegation. It never substitutes streaming commentary for a final answer.
func (m *Mux) FinalAnswerForSession(s *session.Session, requestID string) (string, bool, error) {
	reader, ok := m.pick(s).(interface {
		FinalAnswerForRequest(*session.Session, string) (string, bool, error)
	})
	if !ok {
		return "", false, fmt.Errorf("final answer unavailable for backend %s", s.Backend())
	}
	return reader.FinalAnswerForRequest(s, requestID)
}

// AllProviders returns the distinct history providers across all backends,
// for aggregating resumable sessions.
func (m *Mux) AllProviders() []backend.HistoryProvider {
	seen := map[Executor]bool{}
	var out []backend.HistoryProvider
	for _, e := range m.byBackend {
		if seen[e] {
			continue
		}
		seen[e] = true
		if hp, ok := e.(backend.HistoryProvider); ok {
			out = append(out, hp)
		}
	}
	return out
}

// PID delegates to the session's backend if it can inspect subprocesses.
func (m *Mux) PID(s *session.Session) (int, bool) {
	if pi, ok := m.pick(s).(ProcInspector); ok {
		return pi.PID(s)
	}
	return 0, false
}

// KillProc delegates to the session's backend if it can kill its subprocess.
func (m *Mux) KillProc(s *session.Session) bool {
	if pi, ok := m.pick(s).(ProcInspector); ok {
		return pi.KillProc(s)
	}
	return false
}

// UsageFor returns the UsageProvider for this session's backend, if any.
func (m *Mux) UsageFor(s *session.Session) (UsageProvider, bool) {
	up, ok := m.pick(s).(UsageProvider)
	return up, ok
}

// AllUsageProviders returns the distinct usage providers across all backends.
func (m *Mux) AllUsageProviders() []UsageProvider {
	seen := map[Executor]bool{}
	var out []UsageProvider
	for _, e := range m.byBackend {
		if seen[e] {
			continue
		}
		seen[e] = true
		if up, ok := e.(UsageProvider); ok {
			out = append(out, up)
		}
	}
	return out
}

// RuntimeDiagnostics aggregates distinct backend diagnostics without coupling
// the core status endpoint to Codex or any other concrete runtime.
func (m *Mux) RuntimeDiagnostics() map[string]any {
	seen := map[Executor]bool{}
	out := map[string]any{}
	for _, e := range m.byBackend {
		if seen[e] {
			continue
		}
		seen[e] = true
		if provider, ok := e.(RuntimeDiagnosticsProvider); ok {
			for key, value := range provider.RuntimeDiagnostics() {
				out[key] = value
			}
		}
	}
	return out
}

func (m *Mux) MaintenanceRecords() []backend.Maintenance {
	var records []backend.Maintenance
	for _, e := range m.byBackend {
		if p, ok := e.(backend.MaintenanceProvider); ok {
			records = append(records, p.MaintenanceRecords()...)
		}
	}
	return records
}
func (m *Mux) ReconcileMaintenance(ctx context.Context, id string, release bool) (backend.Maintenance, error) {
	for _, e := range m.byBackend {
		if p, ok := e.(backend.MaintenanceProvider); ok {
			for _, r := range p.MaintenanceRecords() {
				if r.SessionID == id {
					return p.ReconcileMaintenance(ctx, id, release)
				}
			}
		}
	}
	return backend.Maintenance{}, fmt.Errorf("no maintenance record")
}

// RespondUserInput tries each interaction-capable backend until one owns the id.
// The answer carries no session, so the backend matches by request_id/tool_use_id.
func (m *Mux) RespondUserInput(id string, answers map[string]any, cancelled bool) bool {
	seen := map[Executor]bool{}
	for _, e := range m.byBackend {
		if seen[e] {
			continue
		}
		seen[e] = true
		if ir, ok := e.(InteractionResponder); ok {
			if ir.RespondUserInput(id, answers, cancelled) {
				return true
			}
		}
	}
	return false
}

// PendingInteractions aggregates open interactions across all backends.
func (m *Mux) PendingInteractions(sessionID string) []backend.UserInputPayload {
	seen := map[Executor]bool{}
	out := []backend.UserInputPayload{}
	for _, e := range m.byBackend {
		if seen[e] {
			continue
		}
		seen[e] = true
		if ir, ok := e.(InteractionResponder); ok {
			out = append(out, ir.PendingInteractions(sessionID)...)
		}
	}
	return out
}
