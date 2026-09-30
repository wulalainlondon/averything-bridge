package goexec

import (
	"encoding/json"
	"log"
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
		st.turnErrorCode = terminal.ErrorCode
		st.finishTurnLocked(firstNonEmpty(terminal.Message, "turn failed"))
	case "completed":
		st.finishTurnLocked("")
	}
}

func (c *Codex) confirmOwnedTurnSubmission(st *codexState, threadID, requestID string, turn codexTurnResponse) {
	st.mu.Lock()
	current := st.turnActive && st.reqID == requestID && st.threadID == threadID
	st.mu.Unlock()
	if !current {
		return
	}
	if c.dataDir != "" {
		if err := c.rememberAcceptedTurnRequest(threadID, turn.ID, requestID); err != nil {
			// An accepted submission must never be retried because metadata failed.
			log.Printf("[codex] could not persist accepted turn identity: %v", err)
		}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.turnActive || st.reqID != requestID || st.threadID != threadID {
		return
	}
	st.currentTurnID = turn.ID
	st.turnStartPending = false
	if st.observedTurnID == turn.ID {
		st.observedTurnID, st.observedRequestID = "", ""
	}
	terminal, exists := st.pendingTerminals[turn.ID]
	st.pendingTerminals = nil
	if codexTerminalStatus(turn.Status) {
		// The response itself may contain the already-terminal accepted turn.
		terminal = codexTurnTerminal{ID: turn.ID, Status: turn.Status, Message: turn.Error.Message,
			ErrorCode: codexErrorCode(turn.Error.Info, turn.Error.Message)}
		exists = true
	}
	if exists {
		st.applyTurnTerminalLocked(terminal)
	}
}
