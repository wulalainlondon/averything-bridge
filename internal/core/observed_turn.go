package core

import (
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
)

// Native activity has presentation state, not ownership of the Session worker.
// Return only an accepted terminal event for the usual delivery pipeline.
func (h *Hub) observedTurnEvent(turn backend.ObservedTurn) (any, bool) {
	if turn.SessionID == "" || turn.RequestID == "" {
		return nil, false
	}
	s, ok := h.registry.Get(turn.SessionID)
	if !ok {
		return nil, false
	}
	if owned := s.ActiveQueuedID(); owned != "" && owned != turn.RequestID {
		return nil, false
	}
	if turn.Phase == "running" {
		views := h.runtimes.Snapshot("", []string{turn.SessionID})
		if len(views) == 1 && views[0].ActiveRequestID == turn.RequestID {
			// A delayed file observation cannot revive a turn already finished
			// by its RPC event, or reset an active turn's progress stage.
			if views[0].Phase == "completed" || views[0].Phase == "failed" || runtimePhaseActive(views[0].Phase) {
				return nil, false
			}
		}
		h.updateRuntime(turn.SessionID, "running", turn.RequestID, s.QueueLen(), "", "")
		h.updateRuntimeProgress(turn.SessionID, turn.RequestID, "thinking", "")
		return nil, false
	}
	views := h.runtimes.Snapshot("", []string{turn.SessionID})
	if len(views) != 1 || views[0].ActiveRequestID != turn.RequestID || !runtimePhaseActive(views[0].Phase) {
		return nil, false
	}
	switch turn.Phase {
	case "completed":
		return protocol.NewDone(turn.SessionID, turn.RequestID), true
	case "interrupted":
		return protocol.NewStopped(turn.SessionID, turn.RequestID), true
	case "failed":
		return backend.NewError(turn.SessionID, turn.RequestID, turn.ErrorCode, turn.Message), true
	default:
		return nil, false
	}
}

func runtimePhaseActive(phase string) bool {
	return phase == "queued" || phase == "running" || phase == "waiting" || phase == "stopping"
}
