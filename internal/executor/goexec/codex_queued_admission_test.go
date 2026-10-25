package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/protocol"
	"reflect"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"github.com/coder/websocket"
)

func TestQueuedAdmissionReadsMetadataOnlyAndNeverResumesOrStarts(t *testing.T) {
	for _, status := range []string{"active", "idle", "notLoaded", "systemError", "wrong-thread", "invalid", "epoch", "transport", "local-active", "compact", "unloaded", "foreign-unloaded", "unloaded-epoch", "unloaded-cancelled"} {
		t.Run(status, func(t *testing.T) {
			c, s, st, w, sink := livenessFixture(t)
			st.turnActive = false
			st.currentTurnID = ""
			c.appServerMode = "daemon"
			c.remoteConn = &websocket.Conn{} // Readiness uses only the fake RPC writer.
			if status == "local-active" {
				st.turnActive = true
			}
			if status == "compact" {
				st.compactActive = true
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			w.reply = func(method string, p json.RawMessage) (any, error) {
				if method != "thread/read" || string(p) != `{"includeTurns":false,"threadId":"root"}` {
					t.Fatal("unexpected mutation/history", method, string(p))
				}
				if strings.HasPrefix(status, "unloaded") || status == "foreign-unloaded" {
					id := "root"
					if status == "foreign-unloaded" {
						id = "foreign"
					}
					if status == "unloaded-epoch" {
						c.rpc.transportEpoch.Add(1)
					}
					if status == "unloaded-cancelled" {
						cancel()
					}
					return nil, errors.New("thread not loaded: " + id)
				}
				if status == "transport" {
					return nil, errors.New("unavailable")
				}
				if status == "invalid" {
					return map[string]any{}, nil
				}
				if status == "epoch" {
					c.rpc.transportEpoch.Add(1)
				}
				id := "root"
				if status == "wrong-thread" {
					id = "foreign"
				}
				return map[string]any{"thread": map[string]any{"id": id, "status": map[string]string{"type": status}}}, nil
			}
			err := c.QueuedSubmissionReady(ctx, s, "new")
			busy := status == "active" || status == "local-active" || status == "compact"
			if busy && !errors.Is(err, backend.ErrNativeSubmissionBusy) {
				t.Fatal("busy not retained", err)
			}
			if (status == "idle" || status == "notLoaded" || status == "systemError" || status == "unloaded") && err != nil {
				t.Fatal(err)
			}
			if (status == "transport" || status == "invalid" || status == "wrong-thread" || status == "epoch" || status == "foreign-unloaded" || status == "unloaded-epoch" || status == "unloaded-cancelled") && err == nil {
				t.Fatal("unverified read admitted")
			}
			if sink.count(func(any) bool { return true }) != 0 || st.reqID != "request-1" {
				t.Fatal("read reassigned original work")
			}
		})
	}
}

// Exercise the gate and the original pinned Send path together. A missing
// rollout must terminate the same request without starting or forking a thread.
func TestQueuedUnloadedControllerThreadFailsOriginalRequestWithoutFork(t *testing.T) {
	c, s, st, w, sink := livenessFixture(t)
	c.appServerMode = "daemon"
	c.remoteConn = &websocket.Conn{}
	st.threadID, st.currentTurnID, st.reqID = "", "", ""
	st.turnActive = false
	request := "scjob_original"
	w.reply = func(method string, raw json.RawMessage) (any, error) {
		var params map[string]any
		if err := json.Unmarshal(raw, &params); err != nil {
			t.Fatal(err)
		}
		if params["threadId"] != "root" {
			t.Fatal("wrong original thread", params)
		}
		switch method {
		case "thread/read":
			if params["includeTurns"] != false {
				t.Fatal("history loaded")
			}
			return nil, errors.New("thread not loaded: root")
		case "thread/resume":
			if params["excludeTurns"] != true || params["sandbox"] != nil || params["approvalPolicy"] != nil {
				t.Fatal("resume changed history or permissions", params)
			}
			return nil, errors.New("no rollout found for thread id root")
		default:
			t.Fatal("unexpected input, retry, or fork", method)
			return nil, errors.New("unexpected method")
		}
	}
	if err := c.QueuedSubmissionReady(context.Background(), s, request); err != nil {
		t.Fatal("unloaded thread stuck at queue gate", err)
	}
	if err := c.Send(context.Background(), s, request, "original instruction", nil, nil); err == nil {
		t.Fatal("missing original thread treated as started")
	}
	if !reflect.DeepEqual(w.methods, []string{"thread/read", "thread/resume"}) || s.ResumeID() != "root" {
		t.Fatal("request retried or thread changed", w.methods, s.ResumeID())
	}
	if sink.count(func(value any) bool {
		e, ok := value.(protocol.Error)
		return ok && e.SessionID == s.ID && e.RequestID == request && strings.Contains(e.Message, "no rollout found")
	}) != 1 {
		t.Fatal("original request missing terminal error")
	}
}

func TestQueuedUnloadedControllerThreadResumesSameThread(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	c.appServerMode = "daemon"
	c.remoteConn = &websocket.Conn{}
	st.threadID, st.currentTurnID, st.reqID = "", "", ""
	st.turnActive = false
	w.reply = func(method string, raw json.RawMessage) (any, error) {
		var params map[string]any
		if err := json.Unmarshal(raw, &params); err != nil {
			t.Fatal(err)
		}
		if params["threadId"] != "root" {
			t.Fatal("wrong thread", params)
		}
		switch method {
		case "thread/read":
			if len(w.methods) == 1 {
				return nil, errors.New("thread not loaded: root")
			}
			return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "idle"}}}, nil
		case "thread/resume":
			if params["excludeTurns"] != true {
				t.Fatal("history loaded")
			}
			return map[string]any{"thread": map[string]string{"id": "root"}}, nil
		default:
			t.Fatal("unexpected native input or fork", method)
			return nil, errors.New("unexpected method")
		}
	}
	if err := c.QueuedSubmissionReady(context.Background(), s, "scjob_original"); err != nil {
		t.Fatal(err)
	}
	if id, err := c.resumeExactControllerThread(s); err != nil || id != "root" {
		t.Fatal(id, err)
	}
	if _, err := c.readStartAdmission(s, st, "scjob_original"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(w.methods, []string{"thread/read", "thread/resume", "thread/read"}) || s.ResumeID() != "root" {
		t.Fatal("original resume sequence changed", w.methods)
	}
}
