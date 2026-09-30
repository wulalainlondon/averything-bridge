package goexec

import (
	"log"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// Keep native observations separate from reqID/turnActive/turnDone, which belong
// exclusively to Send's runTurn waiter. An external terminal must not wake it.
func (st *codexState) retireTurnLocked(turnID string) {
	if turnID == "" {
		return
	}
	st.retiredTurns = append(st.retiredTurns, turnID)
	if len(st.retiredTurns) > 128 {
		st.retiredTurns = st.retiredTurns[len(st.retiredTurns)-128:]
	}
}

func (c *Codex) ObserveNativeLifecycle(s *session.Session, turnID, phase string) {
	if s.ResumeID() == "" || turnID == "" {
		return
	}
	st := c.state(s.ID)
	st.mu.Lock()
	owned, compacting, threadID := st.turnActive, st.compactActive, st.threadID
	st.mu.Unlock()
	if owned {
		if !compacting && threadID == s.ResumeID() && codexTerminalStatus(phase) {
			// A committed native terminal is an independent completion source for
			// this exact owned turn, not an external event or a queue-release ACK.
			st.completeOwnedTurn(codexTurnTerminal{ID: turnID, Status: phase})
		}
		return
	}
	if compacting || s.IsStreaming() {
		return
	}
	if phase == "running" {
		c.observeNativeTurn(s, st, "turn/started", turnID)
	} else {
		c.finishObservedTurn(s, st, turnID, phase, "", "")
	}
}

func (c *Codex) observeNativeTurn(s *session.Session, st *codexState, method, turnID string) (string, bool) {
	st.mu.Lock()
	for _, retired := range st.retiredTurns {
		if turnID != "" && retired == turnID {
			st.mu.Unlock()
			return "", false
		}
	}
	if st.turnActive || st.compactActive {
		requestID := st.reqID
		accept := st.turnStartPending || st.currentTurnID == "" || turnID == "" || st.currentTurnID == turnID || st.compactActive
		st.mu.Unlock()
		return requestID, accept
	}
	started := false
	// Only live, turn-correlated activity can repair a missed turn/started.
	// History reads, token usage and uncorrelated messages never start a turn.
	live := method == "turn/started" || strings.HasPrefix(method, "item/") || method == "turn/plan/updated" || method == "turn/diff/updated"
	if live && turnID != "" && turnID != st.observedTurnID {
		if st.observedTurnID != "" {
			if method != "turn/started" {
				st.mu.Unlock()
				return "", false
			}
			st.retireTurnLocked(st.observedTurnID)
		}
		st.observedTurnID = turnID
		st.observedRequestID = "codex_external_" + turnID
		// A reattached live turn may already have an exact durable Bridge
		// association. Preserve it instead of splitting history into two IDs.
		if c.dataDir != "" {
			c.historyRequestMu.Lock()
			journal, err := c.loadTurnRequests(firstNonEmpty(st.threadID, s.ResumeID()))
			c.historyRequestMu.Unlock()
			if err == nil && journal.Requests[turnID] != "" {
				st.observedRequestID = journal.Requests[turnID]
			}
		}
		started = true
	}
	requestID, threadID := st.reqID, firstNonEmpty(st.threadID, s.ResumeID())
	if st.observedTurnID != "" {
		if turnID != "" && turnID != st.observedTurnID {
			st.mu.Unlock()
			return "", false
		}
		requestID = st.observedRequestID
	}
	st.mu.Unlock()
	if started {
		if c.dataDir != "" {
			if err := c.rememberTurnRequest(threadID, turnID, requestID); err != nil {
				log.Printf("[codex] persist observed turn identity: %v", err)
			}
		}
		c.sink.Emit(backend.ObservedTurn{SessionID: s.ID, RequestID: requestID, Phase: "running"})
	}
	return requestID, true
}

func (c *Codex) finishObservedTurn(s *session.Session, st *codexState, turnID, phase, code, message string) bool {
	st.mu.Lock()
	if st.observedTurnID == "" || st.turnActive || st.compactActive || (turnID != "" && turnID != st.observedTurnID) {
		st.mu.Unlock()
		return false
	}
	requestID := st.observedRequestID
	st.retireTurnLocked(st.observedTurnID)
	st.observedTurnID, st.observedRequestID = "", ""
	st.mu.Unlock()
	c.sink.Emit(backend.ObservedTurn{SessionID: s.ID, RequestID: requestID, Phase: phase, ErrorCode: code, Message: message})
	return true
}
