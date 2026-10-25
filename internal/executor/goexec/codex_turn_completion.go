package goexec

import (
	"encoding/json"
	"errors"

	"everything-go/internal/recovery"
)

type codexTurnResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Error  struct {
		Message string          `json:"message"`
		Info    json.RawMessage `json:"codexErrorInfo"`
	} `json:"error"`
}

type codexTurnTerminal struct {
	ID, Status, Message, ErrorCode string
	Failure                        recovery.Failure
	NativeWillRetry                *bool
}

func codexTerminalStatus(status string) bool {
	return status == "completed" || status == "failed" || status == "interrupted"
}

// completeOwnedTurn correlates and settles under one lock. During submission,
// notifications may precede the RPC response (or be delayed from an older turn).
// Only the accepted response establishes which buffered terminal can end work.
func (st *codexState) completeOwnedTurn(terminal codexTurnTerminal) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.turnActive || terminal.ID == "" || !codexTerminalStatus(terminal.Status) {
		return
	}
	for _, retired := range st.retiredTurns {
		if retired == terminal.ID {
			return
		}
	}
	if st.turnStartPending || st.currentTurnID == "" {
		if st.pendingTerminals == nil {
			st.pendingTerminals = make(map[string]codexTurnTerminal)
		}
		if _, exists := st.pendingTerminals[terminal.ID]; exists || len(st.pendingTerminals) < 16 {
			st.pendingTerminals[terminal.ID] = terminal
		}
		return
	}
	if terminal.ID != st.currentTurnID {
		return
	}
	st.applyTurnTerminalLocked(terminal)
}

func (st *codexState) applyTurnTerminalLocked(terminal codexTurnTerminal) {
	switch terminal.Status {
	case "interrupted":
		if st.inactivityStopReason != "" && !st.stopping {
			st.turnErrorCode = codexInactivityTimeoutCode
			st.finishTurnLocked(st.inactivityStopReason)
		} else {
			st.finishTurnLocked("stopped")
		}
	case "failed":
		if terminal.Failure.Category == "" {
			terminal.Failure = recovery.ClassifyCodex(nil, terminal.Message)
		}
		st.turnErrorCode = firstNonEmpty(terminal.ErrorCode, terminal.Failure.ErrorCode())
		st.turnFailure = &codexFailureDetail{Failure: terminal.Failure, Acceptance: recovery.Accepted, TurnID: terminal.ID, Terminal: true, NativeWillRetry: terminal.NativeWillRetry}
		st.finishTurnLocked(firstNonEmpty(terminal.Message, "turn failed"))
	case "completed":
		st.finishTurnLocked("")
	}
}

func (c *Codex) confirmOwnedTurnSubmission(st *codexState, threadID, requestID string, turn codexTurnResponse, onAccepted ...func()) error {
	st.mu.Lock()
	var pending []json.RawMessage
	var terminal codexTurnTerminal
	settle, confirmed := false, false
	defer func() {
		st.mu.Unlock()
		if !confirmed {
			return
		}
		// Publish the immutable ACK fact outside state/history locks. Hub admission
		// may hold its canonical queue mutex while revalidating native callers.
		// A later cancel is late acceptance, not grounds to erase this past fact.
		for _, publish := range onAccepted {
			publish()
		}
		for _, raw := range pending {
			var n struct {
				Params struct{ ThreadID, TurnID string } `json:"params"`
			}
			if json.Unmarshal(raw, &n) != nil || n.Params.ThreadID != threadID || n.Params.TurnID != turn.ID {
				continue
			}
			st.mu.Lock()
			current := st.turnActive && st.reqID == requestID && st.threadID == threadID && st.currentTurnID == turn.ID && st.submissionEpoch == c.rpc.transportEpoch.Load()
			st.mu.Unlock()
			if current {
				c.dispatch(raw)
			}
		}
		if settle {
			st.mu.Lock()
			if st.turnActive && st.reqID == requestID && st.threadID == threadID && st.currentTurnID == turn.ID {
				st.applyTurnTerminalLocked(terminal)
			}
			st.mu.Unlock()
		}
	}()
	natural := turn.Status == "completed" || st.pendingTerminals[turn.ID].Status == "completed"
	if !st.turnActive || (st.stopping && !natural) || st.reqID != requestID || st.threadID != threadID {
		return &codexAcceptanceError{Reason: "native_current_request_changed"}
	}
	if !st.startAdmission.sourceKnown() {
		return &codexAcceptanceError{Reason: "native_source_changed"}
	}
	if st.submissionEpoch != 0 && st.submissionEpoch != c.rpc.transportEpoch.Load() {
		return &codexAcceptanceError{Reason: "native_source_changed"}
	}
	if c.dataDir == "" && len(onAccepted) > 0 {
		return &codexAcceptanceError{Reason: "native_identity_unavailable"}
	}
	// Hold the current-request/cancel boundary while checking durable ownership.
	// A failed association is not an accepted effect and cannot settle this waiter.
	if c.dataDir != "" {
		if err := c.rememberAcceptedTurnRequest(threadID, turn.ID, requestID); err != nil {
			if errors.Is(err, errCodexTurnOwnerConflict) {
				return &codexAcceptanceError{Reason: "native_acceptance_conflict"}
			}
			return &codexAcceptanceError{Reason: "native_identity_unavailable"}
		}
	}
	if !st.startAdmission.sourceKnown() {
		return &codexAcceptanceError{Reason: "native_source_changed"}
	}
	if st.submissionEpoch != 0 && st.submissionEpoch != c.rpc.transportEpoch.Load() {
		return &codexAcceptanceError{Reason: "native_source_changed"}
	}
	st.currentTurnID = turn.ID
	st.confirmedRequestID, st.confirmedTurnID = requestID, turn.ID
	st.turnStartPending = false
	if st.observedTurnID == turn.ID {
		st.observedTurnID, st.observedRequestID = "", ""
	}
	var exists bool
	terminal, exists = st.pendingTerminals[turn.ID]
	st.pendingTerminals = nil
	pending = st.pendingNotifications
	st.pendingNotifications = nil
	st.pendingNotificationBytes = 0
	if codexTerminalStatus(turn.Status) {
		// The response itself may contain the already-terminal accepted turn.
		terminal = codexTurnTerminal{ID: turn.ID, Status: turn.Status, Message: turn.Error.Message,
			ErrorCode: codexErrorCode(turn.Error.Info, turn.Error.Message), Failure: recovery.ClassifyCodex(turn.Error.Info, turn.Error.Message)}
		exists = true
	}
	settle, confirmed = exists, true
	return nil
}
