package core

import (
	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// Conservative read projection, never a repair/rebind of historical facts.
func (h *Hub) nativeCorrelationKnown(s *session.Session, thread, request, turn string) bool {
	if s.Backend() != backend.Codex {
		return false // these native turn tuples are Codex evidence, not Claude tokens
	}
	if hr, ok := h.exec.(historyRouter); ok {
		if p, ok := hr.ProviderFor(s); ok {
			if verifier, ok := p.(interface {
				ValidateNativeAcceptance(*session.Session, string, string, string) error
			}); ok {
				return verifier.ValidateNativeAcceptance(s, thread, request, turn) == nil
			}
		}
	}
	return false
}

func (h *Hub) nativeSealKnown(s *session.Session, saved map[string]any, request, thread, turn string) bool {
	exec, ok := saved["execution"].(map[string]any)
	if !ok {
		return false
	}
	if exec["backend"] != "codex" {
		return exec["backend"] == "claude"
	}
	if s.Backend() != backend.Codex {
		return false
	}
	return ok && saved["instance_id"] == h.cfg.InstanceID && saved["session_id"] == s.ID && saved["request_id"] == request && exec["backend"] == "codex" && exec["native_thread_id"] == thread && exec["native_turn_id"] == turn && h.nativeCorrelationKnown(s, thread, request, turn)
}

func (h *Hub) frozenTaskNativeKnown(task map[string]any) bool {
	target, _ := task["target"].(map[string]any)
	if turn, ok := target["native_turn_id"].(string); ok && turn != "" {
		sessionID, _ := target["session_id"].(string)
		request, _ := target["request_id"].(string)
		thread, _ := target["native_thread_id"].(string)
		s, found := h.registry.Get(sessionID)
		if !found || !h.nativeCorrelationKnown(s, thread, request, turn) {
			return false
		}
	}
	if anchor, ok := task["result_anchor"].(map[string]any); ok {
		if execution, ok := anchor["execution"].(map[string]any); ok && execution["backend"] == "codex" {
			sessionID, _ := anchor["session_id"].(string)
			request, _ := anchor["request_id"].(string)
			thread, _ := execution["native_thread_id"].(string)
			turn, _ := execution["native_turn_id"].(string)
			s, found := h.registry.Get(sessionID)
			if !found || anchor["instance_id"] != h.cfg.InstanceID || thread == "" || turn == "" || request == "" || !h.nativeCorrelationKnown(s, thread, request, turn) {
				return false
			}
			if id, ok := target["request_id"].(string); ok && id != request {
				return false
			}
		}
	}
	return true
}
