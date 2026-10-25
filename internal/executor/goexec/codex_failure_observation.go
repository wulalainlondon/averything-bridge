package goexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/recovery"
	"everything-go/internal/session"
)

type codexFailureDetail struct {
	Failure         recovery.Failure
	Acceptance      recovery.Acceptance
	IngressRejected bool
	TurnID          string
	Terminal        bool
	NativeWillRetry *bool
}

func codexSubmissionFailure(err error) *codexFailureDetail {
	var correlation *codexAcceptanceError
	if errors.As(err, &correlation) {
		f := correlation.Failure
		if f.Category == "" {
			f.Category = recovery.Unknown
		}
		f.Source = correlation.Reason
		return &codexFailureDetail{Failure: f, Acceptance: recovery.AcceptanceUnknown}
	}
	d := &codexFailureDetail{Failure: recovery.ClassifyCodex(nil, err.Error()), Acceptance: recovery.AcceptanceUnknown}
	if d.Failure.Source == "unknown" {
		d.Failure.Source = "submission_error"
	}
	var response *rpcResponseError
	if errors.As(err, &response) {
		var rpcError struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		if json.Unmarshal([]byte(response.raw), &rpcError) == nil {
			d.Failure, d.IngressRejected = recovery.ClassifyIngress(rpcError.Code, rpcError.Message)
			if d.IngressRejected {
				d.Acceptance = recovery.Rejected
			}
			if !d.IngressRejected {
				classified := recovery.ClassifyCodex(nil, rpcError.Message)
				if classified.Category != recovery.Unknown {
					d.Failure = classified
				}
			}
		}
	}
	var timeout *rpcTimeoutError
	if errors.As(err, &timeout) {
		d.Failure = recovery.Failure{Category: recovery.TransportUncertain, Source: "rpc_timeout"}
	}
	return d
}

func (c *Codex) recordCodexFailure(sessionID, requestID, threadID, turnID string, failure recovery.Failure, context recovery.Context) {
	decision := recovery.Decide(failure, context)
	if c.dataDir == "" {
		return
	}
	c.failureJournalMu.Lock()
	if c.failureJournal == nil {
		c.failureJournal = &recovery.Journal{Path: filepath.Join(c.dataDir, "codex_failure_observations.json")}
	}
	journal := c.failureJournal
	c.failureJournalMu.Unlock()
	if err := journal.Record(recovery.Observation{At: time.Now().UnixMilli(), SessionID: sessionID, RequestID: requestID,
		ThreadID: threadID, TurnID: turnID, Failure: failure, Context: context, Decision: decision}); err != nil {
		// Persistence failure must not cause a replay or obscure the terminal.
		log.Printf("[codex] failure observation could not be saved: %v", err)
	}
}

func (c *Codex) observeCodexRetryHint(s *session.Session, st *codexState, threadID, turnID string, failure recovery.Failure, willRetry *bool) {
	st.mu.Lock()
	owned := st.turnActive && !st.turnStartPending && turnID != "" && st.currentTurnID == turnID
	observed := !st.turnActive && !st.compactActive && turnID != "" && st.observedTurnID == turnID
	if !owned && !observed {
		st.mu.Unlock()
		return
	}
	requestID := st.reqID
	if observed {
		requestID = st.observedRequestID
	}
	retry := "unknown"
	if willRetry != nil {
		retry = fmt.Sprint(*willRetry)
	}
	key := requestID + ":" + turnID + ":" + string(failure.Category) + ":" + retry
	if st.failureNoticeKey == key {
		st.mu.Unlock()
		return
	}
	st.failureNoticeKey = key
	st.mu.Unlock()
	context := recovery.Context{Owned: owned, OrdinaryChat: owned && !codexPinnedRequest(requestID),
		Acceptance: recovery.Accepted, NativeWillRetry: willRetry}
	c.recordCodexFailure(s.ID, requestID, threadID, turnID, failure, context)
	message := "收到上游錯誤，正在等待原工作狀態確認；不會另行重送。"
	if willRetry != nil && *willRetry {
		message = "模型端正在重試；Bridge 持續觀察原工作。"
	}
	if recovery.Decide(failure, context).Action == "requires_user" {
		message = failure.PublicMessage(message)
	}
	// Keep the existing stage vocabulary for older React/Swift clients.
	// The journal rejects stage regression from thinking/composing to
	// waiting_model. Native retries remain part of the accepted model turn.
	c.sink.Emit(backend.NewTurnProgress(s.ID, requestID, "thinking", message))
}
