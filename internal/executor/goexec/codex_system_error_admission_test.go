package goexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func TestCodexSystemErrorAdmitsNewRequestOnlyAfterLatestTerminal(t *testing.T) {
	for _, status := range []string{"failed", "completed", "interrupted"} {
		t.Run(status, func(t *testing.T) {
			c, s, st, w, sink := livenessFixture(t)
			st.turnActive = false
			st.currentTurnID = ""
			if err := c.rememberTurnRequest("root", "old-turn", "old-request"); err != nil {
				t.Fatal(err)
			}
			w.reply = func(method string, raw json.RawMessage) (any, error) {
				var params map[string]any
				if err := json.Unmarshal(raw, &params); err != nil {
					t.Fatal(err)
				}
				switch method {
				case "thread/read":
					if params["threadId"] != "root" || params["includeTurns"] != false {
						t.Fatal("incorrect bounded thread read", params)
					}
					return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "systemError"}}}, nil
				case "thread/turns/list":
					if params["threadId"] != "root" || params["limit"] != float64(1) || params["sortDirection"] != "desc" || params["itemsView"] != "notLoaded" {
						t.Fatal("history content or older terminal requested", params)
					}
					return map[string]any{"data": []any{map[string]string{"id": "old-turn", "status": status}}}, nil
				case "turn/start":
					if params["threadId"] != "root" {
						t.Fatal("original thread changed", params)
					}
					return map[string]any{"turn": map[string]string{"id": "new-turn", "status": "inProgress"}}, nil
				default:
					t.Fatal("unexpected effect", method)
					return nil, nil
				}
			}
			admission, err := c.readStartAdmission(s, st, "new-request")
			if err != nil || admission == nil {
				t.Fatal("terminal systemError blocked a new user request", err)
			}
			st.startAdmission = admission
			st.reqID, st.turnActive, st.turnStartPending = "new-request", true, true
			if err := c.startTurn("root", nil, s.Snapshot(), "", st.reqID); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(w.methods, []string{"thread/read", "thread/turns/list", "turn/start"}) {
				t.Fatal("unexpected replay or extra submission", w.methods)
			}
			journal, err := c.loadTurnRequests("root")
			if err != nil || journal.Requests["old-turn"] != "old-request" || journal.Requests["new-turn"] != "new-request" {
				t.Fatal("original request ownership changed", journal, err)
			}
			if sink.count(func(e any) bool { _, ok := e.(backend.NativeTaskAccepted); return ok }) != 1 {
				t.Fatal("new native acceptance not published exactly once")
			}
		})
	}
}

// This probe reads only the authorized thread's status and latest turn header.
// It does not resume, subscribe, submit a turn or change production ownership.
func TestCodexSystemErrorLiveReadOnlyAdmission(t *testing.T) {
	threadID := os.Getenv("EVERYTHING_GO_SYSTEM_ERROR_PROBE_THREAD")
	if threadID == "" {
		t.Skip("set EVERYTHING_GO_SYSTEM_ERROR_PROBE_THREAD to the authorized failed thread")
	}
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	c.dataDir = t.TempDir()
	if err := c.ConnectExistingDaemon(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.startMu.Lock()
		defer c.startMu.Unlock()
		if err := c.stopServerLocked(); err != nil {
			t.Error(err)
		}
	})
	s := session.NewRegistry().Create("repair-read-only-probe", "read-only probe", t.TempDir(), "codex", "", "read-only", threadID)
	st := c.state(s.ID)
	st.threadID = threadID
	admission, err := c.readStartAdmission(s, st, "probe-not-submitted")
	if err != nil || admission == nil || admission.ThreadID != threadID || admission.RequestID != "probe-not-submitted" {
		t.Fatal("real failed thread cannot pass repaired admission", admission, err)
	}
	if sink.count(func(e any) bool { _, ok := e.(backend.NativeTaskAccepted); return ok }) != 0 || st.turnActive {
		t.Fatal("read-only probe submitted work")
	}
	t.Logf("read-only admission verified for original native thread %s; no new turn submitted", threadID)
}

func TestCodexSystemErrorRejectsUnverifiedTerminalAndSourceChanges(t *testing.T) {
	for _, kind := range []string{"inProgress", "unknown", "empty", "missing-id", "multiple", "rpc-error", "malformed", "oversized", "epoch", "boundary", "local-active", "compact-active", "config", "thread"} {
		t.Run(kind, func(t *testing.T) {
			c, s, st, w, _ := livenessFixture(t)
			st.turnActive = false
			st.currentTurnID = ""
			w.reply = func(method string, _ json.RawMessage) (any, error) {
				if method == "thread/read" {
					return map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "systemError"}}}, nil
				}
				if method != "thread/turns/list" {
					t.Fatal("model submitted before terminal proof", method)
				}
				row := map[string]string{"id": "old-turn", "status": "failed"}
				switch kind {
				case "inProgress", "unknown":
					row["status"] = kind
				case "empty":
					return map[string]any{"data": []any{}}, nil
				case "missing-id":
					row["id"] = ""
				case "multiple":
					return map[string]any{"data": []any{row, row}}, nil
				case "rpc-error":
					return nil, errors.New("unsupported pagination")
				case "malformed":
					return map[string]any{"data": "invalid"}, nil
				case "oversized":
					row["id"] = fmt.Sprintf("%070000d", 1)
				case "epoch":
					c.rpc.transportEpoch.Add(1)
				case "boundary":
					st.nativeBoundary++
				case "local-active":
					st.turnActive = true
				case "compact-active":
					st.compactActive = true
				case "config":
					s.SetConfigRevision(s.Snapshot().ConfigRevision + 1)
				case "thread":
					st.threadID = "different-thread"
				}
				return map[string]any{"data": []any{row}}, nil
			}
			admission, err := c.readStartAdmission(s, st, "new-request")
			if err == nil || admission != nil {
				t.Fatal("unverified terminal/source admitted", admission, err)
			}
			if len(w.methods) != 2 {
				t.Fatal("unexpected effect", w.methods)
			}
		})
	}
}
