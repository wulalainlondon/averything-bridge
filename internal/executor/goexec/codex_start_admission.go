package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/recovery"
	"everything-go/internal/session"
	"time"
)

// Fixed, non-secret correlation reasons. They are observations, never permission
// to replay or proof that a provider capacity hint caused the original failure.
type codexAcceptanceError struct {
	Reason  string
	Failure recovery.Failure
}

func (e *codexAcceptanceError) Error() string { return e.Reason }
func (e *codexAcceptanceError) Unwrap() error {
	if e.Reason == "native_submission_busy" {
		return backend.ErrNativeSubmissionBusy
	}
	return nil
}

// Read metadata before dequeuing after a restart. No resume, settings changes,
// history load or input RPC is allowed here. Send still performs its complete
// admission after the actor starts, including a second idle/source check.
func (c *Codex) QueuedSubmissionReady(ctx context.Context, s *session.Session, requestID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	st := c.state(s.ID)
	st.mu.Lock()
	threadID, busy := st.threadID, st.turnActive || st.compactActive
	st.mu.Unlock()
	if busy {
		return backend.ErrNativeSubmissionBusy
	}
	if threadID == "" {
		threadID = s.ResumeID()
	}
	if threadID == "" {
		return nil
	}
	if err := c.ensureServer(); err != nil {
		return err
	}
	epoch := c.rpc.transportEpoch.Load()
	raw, err := c.rpc.requestAtEpoch("thread/read", map[string]any{"threadId": threadID, "includeTurns": false}, 5*time.Second, &epoch)
	if err != nil {
		// An unloaded thread is not a busy turn. Let Send resume the exact
		// existing thread (or publish its terminal failure if no rollout exists).
		// Keep this local to admission: unloaded is not evidence for stale-thread
		// recovery, a replacement thread, or a replay of previously accepted input.
		if isQueuedThreadUnloaded(err, threadID) {
			if ctx.Err() != nil || c.rpc.transportEpoch.Load() != epoch {
				return &codexAcceptanceError{Reason: "native_idle_unverified"}
			}
			return nil
		}
		if isStaleThreadError(err) {
			return nil
		} // Existing Send recovery owns this case.
		return err
	}
	var response struct {
		Thread struct {
			ID     string `json:"id"`
			Status struct {
				Type string `json:"type"`
			} `json:"status"`
		} `json:"thread"`
	}
	if len(raw) > 64<<10 || json.Unmarshal(raw, &response) != nil || response.Thread.ID != threadID || ctx.Err() != nil || c.rpc.transportEpoch.Load() != epoch {
		return &codexAcceptanceError{Reason: "native_idle_unverified"}
	}
	if response.Thread.Status.Type == "active" {
		return backend.ErrNativeSubmissionBusy
	}
	// notLoaded/systemError need the existing resume/terminal proof in Send;
	// this read only prevents a known-active thread from being dequeued.
	return nil
}

// Accept only the provider's exact RPC error for the requested thread.
// Transport failures, malformed replies and another thread's error stay queued.
func isQueuedThreadUnloaded(err error, threadID string) bool {
	if err == nil || threadID == "" {
		return false
	}
	var reply struct {
		Message string `json:"message"`
	}
	return json.Unmarshal([]byte(err.Error()), &reply) == nil &&
		reply.Message == "thread not loaded: "+threadID
}

func codexCorrelationReason(err error) string {
	var e *codexAcceptanceError
	if errors.As(err, &e) {
		return e.Reason
	}
	return "native_acceptance_unknown"
}

type codexStartAdmission struct {
	ThreadID, RequestID             string
	Epoch, Boundary, ConfigRevision uint64
	Source                          *session.Session
}

// Use the existing public 0.160 thread/read metadata (no history). Restart
// quarantine or a local idle flag cannot certify an idle provider thread.
func (c *Codex) readStartAdmission(s *session.Session, st *codexState, requestID string) (*codexStartAdmission, error) {
	st.mu.Lock()
	threadID, active, boundary := st.threadID, (st.turnActive && !(st.turnStartPending && st.reqID == requestID && st.currentTurnID == "")) || st.compactActive, st.nativeBoundary
	st.mu.Unlock()
	if active {
		return nil, &codexAcceptanceError{Reason: "native_submission_busy"}
	}
	if threadID == "" {
		return nil, &codexAcceptanceError{Reason: "native_acceptance_unknown"}
	}
	epoch := c.rpc.transportEpoch.Load()
	revision := s.Snapshot().ConfigRevision
	raw, err := c.rpc.requestAtEpoch("thread/read", map[string]any{"threadId": threadID, "includeTurns": false}, 15*time.Second, &epoch)
	if err != nil || len(raw) > 64<<10 {
		return nil, &codexAcceptanceError{Reason: "native_idle_unverified"}
	}
	var response struct {
		Thread struct {
			ID     string `json:"id"`
			Status struct {
				Type string `json:"type"`
			} `json:"status"`
		} `json:"thread"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Thread.ID != threadID {
		return nil, &codexAcceptanceError{Reason: "native_idle_unverified"}
	}
	if response.Thread.Status.Type == "active" {
		return nil, &codexAcceptanceError{Reason: "native_submission_busy"}
	}
	// Codex keeps systemError after a failed turn until the next turn starts.
	// It is recoverable, but an error notification can precede the terminal
	// event. Confirm the newest native turn ended without loading its items.
	if response.Thread.Status.Type == "systemError" {
		if !c.systemErrorTurnEnded(threadID, epoch) {
			return nil, &codexAcceptanceError{Reason: "native_idle_unverified"}
		}
	} else if response.Thread.Status.Type != "idle" {
		return nil, &codexAcceptanceError{Reason: "native_idle_unverified"}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.threadID != threadID || (st.turnActive && !(st.turnStartPending && st.reqID == requestID && st.currentTurnID == "")) || st.compactActive || st.nativeBoundary != boundary || c.rpc.transportEpoch.Load() != epoch || s.ResumeID() != threadID || s.Snapshot().ConfigRevision != revision {
		return nil, &codexAcceptanceError{Reason: "native_source_changed"}
	}
	return &codexStartAdmission{ThreadID: threadID, RequestID: requestID, Epoch: epoch, Boundary: boundary, ConfigRevision: revision, Source: s}, nil
}

func (c *Codex) systemErrorTurnEnded(threadID string, epoch uint64) bool {
	raw, err := c.rpc.requestAtEpoch("thread/turns/list", map[string]any{
		"threadId": threadID, "limit": 1, "sortDirection": "desc", "itemsView": "notLoaded",
	}, 15*time.Second, &epoch)
	if err != nil || len(raw) > 64<<10 {
		return false
	}
	var response struct {
		Data []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &response) != nil || len(response.Data) != 1 || response.Data[0].ID == "" {
		return false
	}
	switch response.Data[0].Status {
	case "completed", "interrupted", "failed":
		return true
	default:
		return false
	}
}

func (a *codexStartAdmission) sourceKnown() bool {
	if a == nil {
		return true
	}
	if a.Source == nil {
		return false
	}
	s := a.Source.Snapshot()
	return s.ResumeID == a.ThreadID && s.ConfigRevision == a.ConfigRevision && s.State != session.Closed && !s.Hidden
}
