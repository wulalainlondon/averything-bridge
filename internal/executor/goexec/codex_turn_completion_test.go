package goexec

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
)

func dispatchCompletion(c *Codex, id, status string) {
	c.dispatch(json.RawMessage(fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":"root","turn":{"id":%q,"status":%q}}}`, id, status)))
}

func TestCodexStartResponseBindsTurnWithoutStartedNotification(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	st.currentTurnID = ""
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		if method != "turn/start" {
			t.Fatalf("unexpected RPC %s", method)
		}
		return map[string]any{"turn": map[string]string{"id": "joined-turn", "status": "inProgress"}}, nil
	}
	if err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID); err != nil {
		t.Fatal(err)
	}
	if st.currentTurnID != "joined-turn" {
		t.Fatalf("start response did not bind native turn: %q", st.currentTurnID)
	}
	dispatchCompletion(c, "old-turn", "completed")
	if !st.turnActive {
		t.Fatal("old completion released the joined turn")
	}
	dispatchCompletion(c, "joined-turn", "completed")
	if st.turnActive || st.turnErr != "" {
		t.Fatal("joined turn did not finish", st.turnErr)
	}
}

func TestCodexCompletionBeforeStartResponseWaitsForMatchingAck(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(fmt.Sprint(started), func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			st.currentTurnID = ""
			w.reply = func(_ string, _ json.RawMessage) (any, error) {
				// A delayed previous start/terminal cannot establish ownership while
				// the submission response is still outstanding.
				c.dispatch(json.RawMessage(`{"method":"turn/started","params":{"threadId":"root","turn":{"id":"old-turn"}}}`))
				dispatchCompletion(c, "old-turn", "completed")
				if started {
					c.dispatch(json.RawMessage(`{"method":"turn/started","params":{"threadId":"root","turn":{"id":"joined-turn"}}}`))
				}
				dispatchCompletion(c, "joined-turn", "completed")
				if !st.turnActive {
					t.Fatal("unconfirmed terminal released the worker")
				}
				return map[string]any{"turn": map[string]string{"id": "joined-turn", "status": "inProgress"}}, nil
			}
			if err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID); err != nil {
				t.Fatal(err)
			}
			select {
			case <-st.turnDone:
			case <-time.After(time.Second):
				t.Fatal("early matching completion was lost")
			}
			if st.turnActive || st.turnErr != "" {
				t.Fatal("matching completion failed", st.turnErr)
			}
		})
	}
}

func TestCodexAcceptedJoinPromotesOnlyExternalHistoryIdentity(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	st.currentTurnID = ""
	st.observedTurnID, st.observedRequestID = "joined-turn", "codex_external_joined-turn"
	if err := c.rememberTurnRequest("root", "joined-turn", st.observedRequestID); err != nil {
		t.Fatal(err)
	}
	w.reply = func(string, json.RawMessage) (any, error) {
		return map[string]any{"turn": map[string]string{"id": "joined-turn"}}, nil
	}
	if err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID); err != nil {
		t.Fatal(err)
	}
	journal, err := c.loadTurnRequests("root")
	if err != nil || journal.Requests["joined-turn"] != "request-1" {
		t.Fatalf("accepted join retained external identity: %+v %v", journal, err)
	}
	if st.observedTurnID != "" {
		t.Fatal("accepted owned turn retained an external observer")
	}
}

func TestCodexNativeFileTerminalSettlesOnlyConfirmedOwnedTurn(t *testing.T) {
	for _, phase := range []string{"completed", "interrupted", "failed"} {
		t.Run(phase, func(t *testing.T) {
			c, s, st, _, sink := livenessFixture(t)
			started := make(chan struct{})
			s.SubmitNamed(st.reqID, func() { close(started) })
			<-started
			t.Cleanup(s.EndTurn)
			c.ObserveNativeLifecycle(s, "old-turn", phase)
			c.ObserveNativeLifecycle(s, "turn-1", "running")
			if !st.turnActive {
				t.Fatal("unrelated/running file activity ended owned work")
			}
			c.ObserveNativeLifecycle(s, "turn-1", phase)
			if st.turnActive {
				t.Fatal("matching native terminal was ignored while session streamed")
			}
			if (phase == "completed") != (st.turnErr == "") {
				t.Fatalf("wrong terminal result: %s %q", phase, st.turnErr)
			}
			if sink.count(func(e any) bool { _, ok := e.(backend.ObservedTurn); return ok }) != 0 {
				t.Fatal("file fallback emitted external ownership for owned work")
			}
			c.ObserveNativeLifecycle(s, "turn-1", phase)
			if sink.count(func(e any) bool { _, ok := e.(backend.ObservedTurn); return ok }) != 0 {
				t.Fatal("duplicate file terminal resurrected observation")
			}
		})
	}
}

func TestCodexStartResponseTerminalAndEarlyFailurePreserveStatus(t *testing.T) {
	for _, early := range []bool{false, true} {
		for _, status := range []string{"completed", "failed", "interrupted"} {
			t.Run(fmt.Sprintf("early=%t/%s", early, status), func(t *testing.T) {
				c, s, st, w, _ := livenessFixture(t)
				st.currentTurnID = ""
				w.reply = func(string, json.RawMessage) (any, error) {
					turn := map[string]any{"id": "joined-turn", "status": status, "error": map[string]any{"message": "model unavailable", "codexErrorInfo": "server_overloaded"}}
					if early {
						raw, _ := json.Marshal(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "root", "turn": turn}})
						c.dispatch(raw)
						turn = map[string]any{"id": "joined-turn", "status": "inProgress"}
					}
					return map[string]any{"turn": turn}, nil
				}
				if err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID); err != nil {
					t.Fatal(err)
				}
				if st.turnActive {
					t.Fatal("accepted terminal did not finish")
				}
				switch status {
				case "completed":
					if st.turnErr != "" {
						t.Fatal(st.turnErr)
					}
				case "failed":
					if st.turnErr != "model unavailable" || st.turnErrorCode != backend.ErrTurn {
						t.Fatalf("lost failure %s/%s", st.turnErr, st.turnErrorCode)
					}
				case "interrupted":
					if st.turnErr != "stopped" {
						t.Fatal("interrupt treated as success", st.turnErr)
					}
				}
			})
		}
	}
}

func TestCodexNativeTerminalBeforeAckIsCorrelatedAfterAck(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	st.currentTurnID = ""
	w.reply = func(string, json.RawMessage) (any, error) {
		c.ObserveNativeLifecycle(s, "old-turn", "completed")
		c.ObserveNativeLifecycle(s, "joined-turn", "completed")
		if !st.turnActive {
			t.Fatal("uncorrelated file completion released work")
		}
		return map[string]any{"turn": map[string]string{"id": "joined-turn"}}, nil
	}
	if err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID); err != nil {
		t.Fatal(err)
	}
	if st.turnActive || st.turnErr != "" {
		t.Fatal("matching file terminal was lost")
	}
}

func TestCodexLateStartAckCannotMutateDifferentRequestOrThread(t *testing.T) {
	for _, changedThread := range []bool{false, true} {
		c, _, st, _, _ := livenessFixture(t)
		if changedThread {
			st.threadID = "new-root"
		} else {
			st.reqID = "new-request"
		}
		c.confirmOwnedTurnSubmission(st, "root", "request-1", codexTurnResponse{ID: "old-turn", Status: "completed"})
		if !st.turnActive || st.currentTurnID != "turn-1" {
			t.Fatal("stale response ended/rebound current work")
		}
		journal, err := c.loadTurnRequests("root")
		if err != nil || len(journal.Requests) != 0 {
			t.Fatal("stale response wrote request identity")
		}
	}
}

func TestCodexJoinedTurnRunEmitsOneOwnedDoneAfterFileFallback(t *testing.T) {
	c, s, st, w, sink := livenessFixture(t)
	st.currentTurnID = ""
	done := st.turnDone
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		switch method {
		case "turn/start":
			c.ObserveNativeLifecycle(s, "joined-turn", "completed")
			return map[string]any{"turn": map[string]string{"id": "joined-turn"}}, nil
		case "thread/goal/get":
			return map[string]any{}, nil
		default:
			t.Fatalf("unexpected RPC %s", method)
			return nil, nil
		}
	}
	c.runTurn(s, st, "root", nil, done, "")
	dispatchCompletion(c, "joined-turn", "completed")
	c.ObserveNativeLifecycle(s, "joined-turn", "completed")
	if sink.count(func(e any) bool {
		d, ok := e.(protocol.Done)
		return ok && d.SessionID == s.ID && d.RequestID == "request-1"
	}) != 1 {
		t.Fatal("owned Done was lost or duplicated")
	}
	if len(w.methods) != 2 {
		t.Fatal("completion triggered extra submissions", w.methods)
	}
}

func TestCodexCorrelatedErrorBeforeAckCannotFailDifferentAcceptedTurn(t *testing.T) {
	for _, failedID := range []string{"old-turn", "joined-turn"} {
		t.Run(failedID, func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			st.currentTurnID = ""
			w.reply = func(string, json.RawMessage) (any, error) {
				c.dispatch(json.RawMessage(fmt.Sprintf(`{"method":"error","params":{"threadId":"root","turnId":%q,"willRetry":false,"error":{"message":"native failure"}}}`, failedID)))
				if !st.turnActive {
					t.Fatal("error ended work before accepted turn was confirmed")
				}
				return map[string]any{"turn": map[string]string{"id": "joined-turn"}}, nil
			}
			if err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID); err != nil {
				t.Fatal(err)
			}
			if failedID == "old-turn" {
				if !st.turnActive || st.turnErr != "" {
					t.Fatal("old error failed the accepted turn")
				}
				dispatchCompletion(c, "joined-turn", "completed")
			} else if st.turnActive || st.turnErr != "native failure" || st.turnErrorCode != backend.ErrTurn {
				t.Fatal("matching error was lost", st.turnErr, st.turnErrorCode)
			}
		})
	}
}
