package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/recovery"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestNativeCorrelationSourceChangedBeforeACKDoesNotPublish(t *testing.T) {
	c, s, st, w, sink := livenessFixture(t)
	st.turnActive = false
	st.currentTurnID = ""
	w.reply = func(string, json.RawMessage) (any, error) {
		return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}, nil
	}
	a, err := c.readStartAdmission(s, st, "request-1")
	if err != nil {
		t.Fatal(err)
	}
	st.startAdmission = a
	st.turnActive = true
	st.turnStartPending = true
	w.reply = func(string, json.RawMessage) (any, error) {
		s.SetResumeID("different-source")
		return map[string]any{"turn": map[string]string{"id": "fresh", "status": "inProgress"}}, nil
	}
	err = c.startTurn("root", nil, s.Snapshot(), "", st.reqID)
	if err == nil || codexCorrelationReason(err) != "native_source_changed" || sink.count(func(e any) bool { _, ok := e.(backend.NativeTaskAccepted); return ok }) != 0 {
		t.Fatal("sourcechanged ACK accepted", err)
	}
	j, _ := c.loadTurnRequests("root")
	if len(j.Requests) != 0 {
		t.Fatal("sourcechanged ownership persisted")
	}
}

func TestNativeCorrelationPendingBuffersBoundedAndExactOwnStreamOnly(t *testing.T) {
	c, s, st, w, sink := livenessFixture(t)
	st.currentTurnID = ""
	w.reply = func(string, json.RawMessage) (any, error) {
		for i := 0; i < 100; i++ {
			turn := "foreign"
			if i%2 == 0 {
				turn = "fresh"
			}
			raw, _ := json.Marshal(map[string]any{"method": "item/agentMessage/delta", "params": map[string]any{"threadId": "root", "turnId": turn, "delta": strings.Repeat("x", 1024), "phase": "final"}})
			c.dispatch(raw)
		}
		if len(st.pendingNotifications) > 64 || st.pendingNotificationBytes > 256<<10 {
			t.Fatal("unbounded buffer")
		}
		if sink.count(func(e any) bool { _, ok := e.(protocol.TextChunk); return ok }) != 0 {
			t.Fatal("preACK payload published")
		}
		return map[string]any{"turn": map[string]string{"id": "fresh", "status": "inProgress"}}, nil
	}
	if err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID); err != nil {
		t.Fatal(err)
	}
	if sink.count(func(e any) bool { _, ok := e.(protocol.TextChunk); return ok }) != 32 || len(st.accumulatedText) != 32*1024 {
		t.Fatal("foreign frames or own stream lost")
	}
}

func TestNativeCorrelationAcceptedPublisherDoesNotHoldStateOrHistoryLocks(t *testing.T) {
	c, _, st, _, _ := livenessFixture(t)
	st.currentTurnID = ""
	if err := c.confirmOwnedTurnSubmission(st, "root", st.reqID, codexTurnResponse{ID: "fresh", Status: "inProgress"}, func() { st.mu.Lock(); st.mu.Unlock(); c.historyRequestMu.Lock(); c.historyRequestMu.Unlock() }); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCorrelationForeignOwnerNeverAcceptedOrSettled(t *testing.T) {
	for _, status := range []string{"inProgress", "failed", "completed", "interrupted"} {
		for _, early := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/early=%v", status, early), func(t *testing.T) {
				c, s, st, w, sink := livenessFixture(t)
				st.currentTurnID = ""
				if err := c.rememberTurnRequest("root", "old-turn", "old-real-request"); err != nil {
					t.Fatal(err)
				}
				before, _ := os.ReadFile(c.turnRequestsPath("root"))
				w.reply = func(method string, _ json.RawMessage) (any, error) {
					if method != "turn/start" {
						t.Fatal(method)
					}
					c.dispatch(json.RawMessage(`{"method":"turn/started","params":{"threadId":"root","turn":{"id":"old-turn"}}}`))
					c.dispatch(json.RawMessage(`{"method":"item/agentMessage/delta","params":{"threadId":"root","turnId":"old-turn","delta":"old final","phase":"final"}}`))
					if _, err := c.sessionControlCaller(s, "old-turn", "tool"); err == nil {
						t.Fatal("pending tool acquired caller")
					}
					if early {
						dispatchCompletion(c, "old-turn", "completed")
					}
					return map[string]any{"turn": map[string]any{"id": "old-turn", "status": status, "error": map[string]any{"codexErrorInfo": "serverOverloaded"}}}, nil
				}
				err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID)
				if err == nil || codexSubmissionFailure(err).Acceptance != recovery.AcceptanceUnknown {
					t.Fatal("false acceptance", err)
				}
				if st.currentTurnID != "" || !st.turnActive {
					t.Fatal("foreign terminal settled/rebound new worker")
				}
				if sink.count(func(e any) bool { _, ok := e.(backend.NativeTaskAccepted); return ok }) != 0 || sink.count(func(e any) bool { _, ok := e.(protocol.TextChunk); return ok }) != 0 {
					t.Fatal("foreign evidence published")
				}
				after, _ := os.ReadFile(c.turnRequestsPath("root"))
				if string(before) != string(after) {
					t.Fatal("real owner rewritten")
				}
				st.finish("unknown")
				c.ObserveNativeLifecycle(s, "old-turn", "completed")
				if sink.count(func(e any) bool { x, ok := e.(backend.ObservedTurn); return ok && x.RequestID == "request-1" }) != 0 {
					t.Fatal("foreign final assigned new request")
				}
			})
		}
	}
}

func TestNativeCorrelationMissingFailedAndTransportChangedACKNoReplay(t *testing.T) {
	for _, kind := range []string{"missing", "status-missing", "failed", "interrupted", "epoch", "cancel", "request", "map-corrupt"} {
		t.Run(kind, func(t *testing.T) {
			c, s, st, w, sink := livenessFixture(t)
			st.currentTurnID = ""
			if kind == "map-corrupt" {
				if err := os.MkdirAll(c.dataDir+"/codex-turn-requests", 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(c.turnRequestsPath("root"), []byte("bad original journal"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				if method != "turn/start" {
					t.Fatal("second model send", method)
				}
				turn := map[string]any{"id": "fresh", "status": "inProgress"}
				switch kind {
				case "missing":
					return map[string]any{}, nil
				case "status-missing":
					delete(turn, "status")
				case "failed", "interrupted":
					turn["status"] = kind
					turn["error"] = map[string]any{"codexErrorInfo": "serverOverloaded", "message": "not exported"}
				case "epoch":
					c.rpc.transportEpoch.Add(1)
				case "cancel":
					st.mu.Lock()
					st.stopping = true
					st.mu.Unlock()
				case "request":
					st.mu.Lock()
					st.reqID = "other-real-request"
					st.mu.Unlock()
				}
				return map[string]any{"turn": turn}, nil
			}
			err := c.startTurn("root", nil, s.Snapshot(), "", "request-1")
			if err == nil {
				t.Fatal("unproven ACK accepted")
			}
			d := codexSubmissionFailure(err)
			if d.Acceptance != recovery.AcceptanceUnknown || d.IngressRejected || recovery.Decide(d.Failure, recovery.Context{Acceptance: d.Acceptance}).RetryEligible {
				t.Fatal("unknown permits replay", d)
			}
			if len(w.methods) != 1 || st.currentTurnID != "" || sink.count(func(e any) bool { _, ok := e.(backend.NativeTaskAccepted); return ok }) != 0 {
				t.Fatal("false accepted or extra send")
			}
		})
	}
}

func TestNativeCorrelationSameRequestAndExactExternalPromotion(t *testing.T) {
	for _, owner := range []string{"request-1", "codex_external_fresh", "codex_external_wrong"} {
		t.Run(owner, func(t *testing.T) {
			c, s, st, w, sink := livenessFixture(t)
			st.currentTurnID = ""
			if err := c.rememberTurnRequest("root", "fresh", owner); err != nil {
				t.Fatal(err)
			}
			w.reply = func(string, json.RawMessage) (any, error) {
				return map[string]any{"turn": map[string]string{"id": "fresh", "status": "inProgress"}}, nil
			}
			err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID)
			journal, _ := c.loadTurnRequests("root")
			if owner == "codex_external_wrong" {
				if err == nil || journal.Requests["fresh"] != owner {
					t.Fatal("foreign synthetic promoted")
				}
				return
			}
			if err != nil || journal.Requests["fresh"] != "request-1" || sink.count(func(e any) bool { _, ok := e.(backend.NativeTaskAccepted); return ok }) != 1 {
				t.Fatal("valid ownership lost", err)
			}
		})
	}
}

func TestNativeCorrelationRecoveryIdleGateAndGenerationNoSecondSend(t *testing.T) {
	for _, kind := range []string{"active", "notLoaded", "systemError", "missing", "wrong-thread", "epoch", "local-active", "idle"} {
		t.Run(kind, func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			st.turnActive = false
			st.currentTurnID = ""
			st.observedTurnID = "old-original-turn"
			st.observedRequestID = "old-real-request"
			if kind == "local-active" {
				st.turnActive = true
			}
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				if kind == "systemError" && method == "thread/turns/list" {
					return map[string]any{"data": []any{}}, nil
				}
				if method != "thread/read" {
					t.Fatal("model sent before idle", method)
				}
				id, status := "root", kind
				if kind == "wrong-thread" {
					id = "other"
					status = "idle"
				}
				if kind == "epoch" {
					c.rpc.transportEpoch.Add(1)
					status = "idle"
				}
				return map[string]any{"thread": map[string]any{"id": id, "status": map[string]string{"type": status}}}, nil
			}
			a, err := c.readStartAdmission(s, st, "request-new")
			if kind == "idle" {
				if err != nil || a == nil {
					t.Fatal(err)
				}
				st.startAdmission = a
				st.turnActive = true
				st.turnStartPending = true
				st.reqID = "request-new"
				c.rpc.transportEpoch.Add(1)
				if err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID); err == nil {
					t.Fatal("old idle epoch reused")
				}
			}
			if kind != "idle" && err == nil {
				t.Fatal("unproven idle admitted")
			}
			maxReads := 1
			if kind == "systemError" {
				maxReads = 2
			}
			if len(w.methods) > maxReads || st.observedTurnID != "old-original-turn" || st.observedRequestID != "old-real-request" {
				t.Fatal("old activity modified or extra RPC")
			}
		})
	}
}

func TestNativeCorrelationExplicitSteerStrictACKSharesOriginalOwner(t *testing.T) {
	for _, kind := range []string{"valid", "empty", "wrong", "epoch", "owner-changed"} {
		t.Run(kind, func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			if err := c.rememberTurnRequest("root", "turn-1", "request-1"); err != nil {
				t.Fatal(err)
			}
			w.reply = func(method string, params json.RawMessage) (any, error) {
				if method != "turn/steer" {
					t.Fatal(method)
				}
				var p map[string]any
				json.Unmarshal(params, &p)
				if p["expectedTurnId"] != "turn-1" || p["clientUserMessageId"] != "supplement" {
					t.Fatal(p)
				}
				if kind == "epoch" {
					c.rpc.transportEpoch.Add(1)
				}
				if kind == "owner-changed" {
					st.mu.Lock()
					st.reqID = "other"
					st.mu.Unlock()
				}
				if kind == "empty" {
					return map[string]any{}, nil
				}
				if kind == "wrong" {
					return map[string]string{"turnId": "other"}, nil
				}
				return map[string]string{"turnId": "turn-1"}, nil
			}
			result, err := c.steerActiveTurn(s, "supplement", "goal amendment", nil, nil)
			if kind == "valid" {
				if err != nil || result.RequestID != "request-1" || result.TurnID != "turn-1" {
					t.Fatal(result, err)
				}
			} else if err == nil {
				t.Fatal("unproven steer success")
			}
			j, _ := c.loadTurnRequests("root")
			if j.Requests["turn-1"] != "request-1" || len(w.methods) != 1 {
				t.Fatal("steer rebound owner or replayed")
			}
		})
	}
}
