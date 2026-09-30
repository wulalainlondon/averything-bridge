package goexec

import (
	"fmt"
	"log"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

const codexInactivityTimeoutCode = "codex_inactivity_timeout"

// ticks is injectable so tests can cross hours of logical time without sleeping
// or changing production thresholds. There is deliberately no total-turn timer.
func (c *Codex) waitForCodexTurn(s *session.Session, st *codexState, done <-chan struct{}, ticks <-chan time.Time) {
	for {
		select {
		case <-done:
			return
		case now, ok := <-ticks:
			if !ok {
				ticks = nil
				continue
			}
			c.checkCodexTurnLiveness(s, st, now)
		}
	}
}

func (c *Codex) checkCodexTurnLiveness(s *session.Session, st *codexState, now time.Time) {
	waitingForInput := c.hasPendingInteraction(s.ID)
	st.mu.Lock()
	if !st.turnActive || st.stopping || st.inactivityStopRequested {
		st.mu.Unlock()
		return
	}
	if waitingForInput {
		st.lastEventAt = now
		st.stallWarned = false
		st.mu.Unlock()
		return
	}
	lastEventAt := st.lastEventAt
	action := codexLivenessActionAt(now, lastEventAt, st.stallWarned, false, c.stallWarnAfter, c.stallAbortAfter)
	requestID, threadID, turnID := st.reqID, st.threadID, st.currentTurnID
	if action == codexLivenessWarn {
		st.stallWarned = true
	}
	if action == codexLivenessAbort {
		// Reserve the reason before sending the RPC: the correlated interrupted
		// notification may arrive before the RPC ACK. Do not prematurely mark
		// the turn finished or let queued work run if cancellation is unconfirmed.
		st.inactivityStopRequested = true
		st.inactivityStopReason = fmt.Sprintf("Codex 連續 %s 未收到進度事件，因無回應逾時而停止。已完成的內容保留，未自動重送。", c.stallAbortAfter)
	}
	st.mu.Unlock()
	switch action {
	case codexLivenessWarn:
		c.sink.Emit(backend.NewSessionWarning(s.ID, fmt.Sprintf("Codex 已連續 %s 未收到進度事件，仍在等待；若達 %s 無回應，將要求停止。持續有進度的長任務沒有總時間上限。", c.stallWarnAfter, c.stallAbortAfter)))
	case codexLivenessAbort:
		log.Printf("[codex] inactivity_timeout session=%s request=%s thread=%s turn=%s last_event_at=%s idle=%s", s.ID, requestID, threadID, turnID, lastEventAt.Format(time.RFC3339Nano), now.Sub(lastEventAt).Round(time.Second))
		if err := c.interruptCodexTurn(threadID, turnID); err != nil {
			c.sink.Emit(backend.NewSessionWarning(s.ID, "Codex 無回應，已要求停止但執行端尚未確認。未將對話標為完成，也未重送或啟動下一個排隊工作；仍可手動停止。"))
		}
	}
}
