package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/protocol"
	"everything-go/internal/recovery"
)

func dispatchRecoveryError(c *Codex, thread, turn string, retry *bool, info, message string) {
	params := map[string]any{"threadId": thread, "turnId": turn, "error": map[string]any{"message": message, "codexErrorInfo": info}}
	if retry != nil {
		params["willRetry"] = *retry
	}
	b, _ := json.Marshal(map[string]any{"method": "error", "params": params})
	c.dispatch(b)
}

func failureObservations(t *testing.T, c *Codex) []recovery.Observation {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(c.dataDir, "codex_failure_observations.json"))
	if err != nil {
		t.Fatal(err)
	}
	var records []recovery.Observation
	if err := json.Unmarshal(b, &records); err != nil {
		t.Fatal(err)
	}
	return records
}

func TestCodexNativeRetryAndMissingHintStayLiveWithoutReplay(t *testing.T) {
	yes := true
	for _, retry := range []*bool{nil, &yes} {
		t.Run(fmt.Sprint(retry != nil), func(t *testing.T) {
			c, _, st, w, sink := livenessFixture(t)
			dispatchRecoveryError(c, "root", "turn-1", retry, "serverOverloaded", "provider secret")
			dispatchRecoveryError(c, "root", "turn-1", retry, "serverOverloaded", "provider secret")
			if !st.turnActive || st.turnErr != "" || len(w.methods) != 0 {
				t.Fatal("retry hint ended/replayed work")
			}
			records := failureObservations(t, c)
			if len(records) != 1 || records[0].Context.Acceptance != recovery.Accepted || records[0].Context.Terminal || records[0].Decision.RetryEligible {
				t.Fatalf("bad hint observation: %+v", records)
			}
			action := "reconcile"
			if retry != nil {
				action = "observe_upstream"
			}
			if records[0].Decision.Action != action {
				t.Fatal(records[0].Decision)
			}
			if sink.count(func(e any) bool {
				p, ok := e.(protocol.TurnProgress)
				return ok && p.Stage == "thinking" && !strings.Contains(p.Message, "provider secret")
			}) != 1 {
				t.Fatal("missing/duplicate retry status")
			}
			completeLivenessTurn(c, "completed")
			if st.turnActive || st.turnErr != "" {
				t.Fatal("retry status prevented natural completion")
			}
		})
	}
}

func TestCodexUncorrelatedErrorsAndChildRetriesDoNotAffectParent(t *testing.T) {
	yes, no := true, false
	c, s, st, w, sink := livenessFixture(t)
	lastActivity := st.lastEventAt
	for _, turn := range []string{"old-turn", ""} {
		dispatchRecoveryError(c, "root", turn, &no, "serverOverloaded", "old failure")
	}
	if !st.lastEventAt.Equal(lastActivity) {
		t.Fatal("unrelated errors renewed the current inactivity clock")
	}
	c.threadToSession["child"] = s
	st.agents["child"] = &codexAgent{id: "child"}
	for _, retry := range []*bool{nil, &yes} {
		dispatchRecoveryError(c, "child", "child-turn", retry, "serverOverloaded", "child retry")
	}
	if !st.turnActive || st.turnErr != "" || st.agents["child"].endMS != nil || len(w.methods) != 0 {
		t.Fatal("unrelated/child error changed parent")
	}
	if sink.count(func(e any) bool { _, ok := e.(protocol.TurnProgress); return ok }) != 0 {
		t.Fatal("unrelated retry changed parent status")
	}
	if _, err := os.Stat(filepath.Join(c.dataDir, "codex_failure_observations.json")); !os.IsNotExist(err) {
		t.Fatal("unrelated error attributed to current request")
	}
}

func TestCodexEarlyRetryHintCannotBindOldTurn(t *testing.T) {
	yes := true
	c, s, st, w, sink := livenessFixture(t)
	st.currentTurnID = ""
	w.reply = func(string, json.RawMessage) (any, error) {
		dispatchRecoveryError(c, "root", "old-turn", &yes, "serverOverloaded", "old retry")
		return map[string]any{"turn": map[string]string{"id": "joined-turn", "status": "inProgress"}}, nil
	}
	if err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID); err != nil {
		t.Fatal(err)
	}
	if !st.turnActive || st.currentTurnID != "joined-turn" {
		t.Fatal("retry hint rebound owned turn")
	}
	if sink.count(func(e any) bool { _, ok := e.(protocol.TurnProgress); return ok }) != 0 {
		t.Fatal("early unconfirmed retry displayed for new work")
	}
}

func TestCodexBlockedRetryHintRequiresUserWithoutInventingTerminal(t *testing.T) {
	yes := true
	for _, tc := range []struct {
		info, message string
		category      recovery.Category
	}{
		{"misalignmentPolicyViolation", "provider denied", recovery.PolicyBlocked},
		{"other", "Fatal error: application network permission was revoked", recovery.PermissionRevoked},
		{"usageLimitExceeded", "usage exhausted", recovery.UsageExhausted},
	} {
		t.Run(string(tc.category), func(t *testing.T) {
			c, _, st, w, _ := livenessFixture(t)
			dispatchRecoveryError(c, "root", "turn-1", &yes, tc.info, tc.message)
			if !st.turnActive || st.turnErr != "" || len(w.methods) != 0 {
				t.Fatal("retry hint manufactured a terminal or replay")
			}
			observation := failureObservations(t, c)[0]
			if observation.Context.Terminal || observation.Decision.Action != "requires_user" || observation.Decision.Reason != string(tc.category) {
				t.Fatal("blocking cause ignored", observation)
			}
			// Only the upstream's explicit terminal releases the original turn.
			b, _ := json.Marshal(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "root", "turn": map[string]any{"id": "turn-1", "status": "failed", "error": map[string]any{"codexErrorInfo": tc.info, "message": tc.message}}}})
			c.dispatch(b)
			if st.turnActive || st.turnErrorCode != observation.Failure.ErrorCode() {
				t.Fatal("upstream terminal lost")
			}
		})
	}
}

type recoveryRPCWriter struct {
	c       *Codex
	methods []string
	before  func()
	code    int
	message string
}

func (w *recoveryRPCWriter) Write(b []byte) (int, error) {
	var r struct {
		ID     json.RawMessage `json:"id"`
		Method string          `json:"method"`
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return 0, err
	}
	w.methods = append(w.methods, r.Method)
	if w.before != nil {
		w.before()
	}
	response, _ := json.Marshal(map[string]any{"id": r.ID, "error": map[string]any{"code": w.code, "message": w.message}})
	w.c.rpc.dispatchResponse(response)
	return len(b), nil
}

func TestCodexIngressRejectionIsObservedOnceAndNeverAutomaticallyResent(t *testing.T) {
	for _, contradiction := range []bool{false, true} {
		t.Run(fmt.Sprint(contradiction), func(t *testing.T) {
			c, s, st, _, sink := livenessFixture(t)
			st.currentTurnID = ""
			w := &recoveryRPCWriter{c: c, code: -32001, message: "Server overloaded; retry later."}
			if contradiction {
				w.before = func() {
					c.dispatch(json.RawMessage(`{"method":"turn/started","params":{"threadId":"root","turn":{"id":"unconfirmed-turn"}}}`))
				}
			}
			c.rpc.setWriter(w)
			c.runTurn(s, st, "root", nil, st.turnDone, "")
			if len(w.methods) != 1 || w.methods[0] != "turn/start" {
				t.Fatal("replayed or probed model", w.methods)
			}
			if (!contradiction && sink.count(func(e any) bool { err, ok := e.(protocol.Error); return ok && err.Code == "model_capacity" }) != 1) || (contradiction && sink.count(func(e any) bool { _, ok := e.(backend.NativeSubmissionUncertain); return ok }) != 1) {
				t.Fatal("logical terminal missing/duplicated")
			}
			records := failureObservations(t, c)
			if len(records) != 1 || records[0].Context.Terminal || records[0].Decision.Mode != "observe_only" {
				t.Fatal(records)
			}
			if records[0].Decision.RetryEligible == contradiction {
				t.Fatal("contradictory proof allowed retry", records)
			}
			if contradiction && records[0].Context.Acceptance != recovery.AcceptanceUnknown {
				t.Fatal("live activity ignored")
			}
		})
	}
}

func TestCodexAcceptedFailuresAreClassifiedWithoutReplayingInput(t *testing.T) {
	for _, tc := range []struct {
		info, message, code string
		category            recovery.Category
	}{
		{"serverOverloaded", "provider secret", "model_capacity", recovery.ModelCapacity},
		{"rate_limit_exceeded", "provider secret", "temporary_rate_limit", recovery.TemporaryRateLimit},
		{"usageLimitExceeded", "provider secret", "usage_exhausted", recovery.UsageExhausted},
		{"other", "Fatal error: application network permission was revoked", "application_permission_revoked", recovery.PermissionRevoked},
	} {
		t.Run(tc.code, func(t *testing.T) {
			c, s, st, w, sink := livenessFixture(t)
			w.reply = func(string, json.RawMessage) (any, error) {
				dispatchRecoveryError(c, "root", "turn-1", func() *bool { v := false; return &v }(), tc.info, tc.message)
				return map[string]any{"turn": map[string]string{"id": "turn-1", "status": "inProgress"}}, nil
			}
			c.runTurn(s, st, "root", nil, st.turnDone, "")
			if len(w.methods) != 1 {
				t.Fatal("accepted input was replayed", w.methods)
			}
			if sink.count(func(e any) bool {
				err, ok := e.(protocol.Error)
				return ok && err.Code == tc.code && !strings.Contains(err.Message, "provider secret")
			}) != 1 {
				t.Fatal("failure was collapsed or leaked")
			}
			records := failureObservations(t, c)
			if len(records) != 1 || records[0].Failure.Category != tc.category || records[0].TurnID != "turn-1" || records[0].Context.Acceptance != recovery.Accepted || !records[0].Context.Terminal || records[0].Decision.RetryEligible {
				t.Fatalf("bad accepted failure receipt: %+v", records)
			}
			b, _ := json.Marshal(records)
			if strings.Contains(string(b), "provider secret") {
				t.Fatal("raw diagnostic retained")
			}
		})
	}
}

func TestCodexTimeoutAndGenericTransportErrorsCannotProveRejection(t *testing.T) {
	for _, err := range []error{
		fmt.Errorf("wrapped: %w", &rpcTimeoutError{Name: "codex", Method: "turn/start", Timeout: time.Second}),
		fmt.Errorf("writer closed after partial send; HTTP 503"),
		&rpcResponseError{raw: `{"code":-32000,"message":"Server overloaded; retry later."}`},
	} {
		d := codexSubmissionFailure(err)
		if d.Acceptance != recovery.AcceptanceUnknown || d.IngressRejected {
			t.Fatalf("invented rejection: %+v", d)
		}
		decision := recovery.Decide(d.Failure, recovery.Context{Owned: true, OrdinaryChat: true, Acceptance: d.Acceptance})
		if decision.Action != "reconcile" || decision.RetryEligible {
			t.Fatal(decision)
		}
	}
}

func TestCodexObservationFailureDoesNotMaskTerminalOrTriggerReplay(t *testing.T) {
	c, s, st, w, sink := livenessFixture(t)
	path := filepath.Join(c.dataDir, "codex_failure_observations.json")
	if err := os.WriteFile(path, []byte("corrupt evidence"), 0600); err != nil {
		t.Fatal(err)
	}
	w.reply = func(string, json.RawMessage) (any, error) {
		return map[string]any{"turn": map[string]any{"id": "turn-1", "status": "failed", "error": map[string]any{"message": "busy", "codexErrorInfo": "serverOverloaded"}}}, nil
	}
	c.runTurn(s, st, "root", nil, st.turnDone, "")
	if len(w.methods) != 1 || sink.count(func(e any) bool { _, ok := e.(backend.NativeSubmissionUncertain); return ok }) != 1 {
		t.Fatal("diagnostic failure changed execution")
	}
	b, _ := os.ReadFile(path)
	if string(b) != "corrupt evidence" {
		t.Fatal("damaged evidence overwritten")
	}
}

func TestCodexExplicitNoRetryEvidenceIsRetainedThroughTerminal(t *testing.T) {
	no := false
	c, s, st, w, _ := livenessFixture(t)
	w.reply = func(string, json.RawMessage) (any, error) {
		dispatchRecoveryError(c, "root", "turn-1", &no, "serverOverloaded", "busy")
		return map[string]any{"turn": map[string]string{"id": "turn-1", "status": "inProgress"}}, nil
	}
	c.runTurn(s, st, "root", nil, st.turnDone, "")
	records := failureObservations(t, c)
	if len(records) != 1 || records[0].Context.NativeWillRetry == nil || *records[0].Context.NativeWillRetry || !records[0].Context.Terminal {
		t.Fatal("false hint was collapsed to missing", records)
	}
}
