package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type fixture struct {
	config                config
	sessions              []sessionView
	runtimes              []runtimeView
	states                map[string]string
	goals                 map[string]string
	queued                int
	streamingOverride     *int
	ackSession, ackStatus string
	mu                    sync.Mutex
	methods               []string
	messages              []map[string]string
}

func testFixture(t *testing.T) *fixture {
	t.Helper()
	dir, err := os.MkdirTemp("/private/tmp", "idle-watch-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	f := &fixture{
		config:   config{DataDir: dir, SessionsFile: filepath.Join(dir, "sessions.json"), Socket: filepath.Join(dir, "native.sock"), Target: "root", TargetThread: "root-thread", Authority: "fixture-authority", Job: "fixture", StateFile: filepath.Join(dir, "watch.json"), Poll: 5 * time.Second, Quiet: 30 * time.Second, Notify: true},
		sessions: []sessionView{{ID: "root", Name: "Bridge", Backend: "codex"}, {ID: "work", Name: "Work", Backend: "codex"}},
		runtimes: []runtimeView{{ID: "root", Phase: "completed", Terminal: "completed", Request: "root-request"}, {ID: "work", Phase: "completed", Terminal: "completed", Request: "work-request"}},
		states:   map[string]string{"root-thread": "idle", "work-thread": "idle"}, goals: map[string]string{}, ackSession: "root", ackStatus: "queued",
	}
	write := func(path string, value any) {
		raw, _ := json.Marshal(value)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(f.config.SessionsFile, map[string]savedSession{"root": {Name: "Bridge", Backend: "codex", ResumeID: "root-thread"}, "work": {Name: "Work", Backend: "codex", ResumeID: "work-thread"}})
	write(filepath.Join(dir, "goal_snapshots.json"), map[string]any{"items": map[string]any{}})
	write(filepath.Join(dir, "pairing.json"), map[string]any{"devices": []any{map[string]string{"token": "fixture-token", "device_id": "actual-phone"}}})
	db, err := sql.Open("sqlite", filepath.Join(dir, "message_queue.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE queue_commands(session_id TEXT, request_id TEXT, state TEXT)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var cmd map[string]string
			var envelope struct {
				Type   string `json:"type"`
				Device string `json:"device_id"`
			}
			_ = json.Unmarshal(raw, &envelope)
			switch envelope.Type {
			case "hello":
				if envelope.Device == "actual-phone" {
					t.Error("would evict a phone")
				}
				writeWS(r.Context(), conn, map[string]any{"type": "hello_ack", "instance_id": "fixture-authority", "locked_to_me": true})
			case "request_status":
				count := 0
				for _, s := range f.sessions {
					if s.Streaming {
						count++
					}
				}
				if f.streamingOverride != nil {
					count = *f.streamingOverride
				}
				writeWS(r.Context(), conn, map[string]any{"type": "status_result", "status": map[string]int{"sessions_total": len(f.sessions), "sessions_streaming": count, "queued_commands": f.queued}})
			case "request_sessions_list":
				writeWS(r.Context(), conn, map[string]any{"type": "sessions_list", "sessions": f.sessions})
			case "request_runtime_snapshot":
				writeWS(r.Context(), conn, map[string]any{"type": "session_runtime_snapshot", "items": f.runtimes})
			case "message":
				_ = json.Unmarshal(raw, &cmd)
				f.mu.Lock()
				f.messages = append(f.messages, cmd)
				f.mu.Unlock()
				writeWS(r.Context(), conn, map[string]string{"type": "message_ack", "session_id": f.ackSession, "request_id": cmd["request_id"], "status": f.ackStatus})
			default:
				t.Error("unexpected Bridge command", envelope.Type)
			}
		}
	}))
	t.Cleanup(bridge.Close)
	f.config.Bridge = "ws" + strings.TrimPrefix(bridge.URL, "http")
	listener, err := net.Listen("unix", f.config.Socket)
	if err != nil {
		t.Fatal(err)
	}
	nativeServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var cmd struct {
				ID     int    `json:"id"`
				Method string `json:"method"`
				Params struct {
					Thread       string `json:"threadId"`
					IncludeTurns bool   `json:"includeTurns"`
				} `json:"params"`
			}
			_ = json.Unmarshal(raw, &cmd)
			f.mu.Lock()
			f.methods = append(f.methods, cmd.Method)
			f.mu.Unlock()
			result := any(map[string]any{})
			switch cmd.Method {
			case "initialized":
				continue
			case "initialize":
			case "thread/loaded/list":
				result = map[string]any{"data": []string{"root-thread", "work-thread"}, "nextCursor": nil}
			case "thread/read":
				if cmd.Params.IncludeTurns {
					t.Error("read private transcript")
				}
				result = map[string]any{"thread": map[string]any{"id": cmd.Params.Thread, "status": map[string]string{"type": f.states[cmd.Params.Thread]}}}
			case "thread/goal/get":
				result = map[string]any{"goal": nil}
				if goal := f.goals[cmd.Params.Thread]; goal != "" {
					result = map[string]any{"goal": map[string]string{"threadId": cmd.Params.Thread, "status": goal}}
				}
			default:
				t.Error("unexpected native method", cmd.Method)
			}
			writeWS(r.Context(), conn, map[string]any{"id": cmd.ID, "result": result})
		}
	})}
	go nativeServer.Serve(listener)
	t.Cleanup(func() { nativeServer.Close() })
	return f
}

func TestInspectFailClosedAndChecksGoalsQueueAndNative(t *testing.T) {
	for _, test := range []struct {
		name                    string
		change                  func(*fixture)
		wantBusy, wantAttention bool
	}{
		{"completed", func(*fixture) {}, false, false},
		{"target-still-running", func(f *fixture) { f.states["root-thread"] = "active" }, true, false},
		{"bridge-false-complete-native-active", func(f *fixture) { f.states["work-thread"] = "active" }, true, false},
		{"active-goal-between-turns", func(f *fixture) { f.goals["work-thread"] = "active" }, true, false},
		{"queued-command", func(f *fixture) { f.queued = 1 }, true, false},
		{"failure-not-completion", func(f *fixture) { f.runtimes[1].Phase = "failed"; f.runtimes[1].Terminal = "failed" }, false, true},
		{"paused-goal-not-completion", func(f *fixture) { f.goals["work-thread"] = "paused" }, false, true},
		{"native-system-error-not-completion", func(f *fixture) { f.states["work-thread"] = "systemError" }, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := testFixture(t)
			test.change(f)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			seen := map[string]tracked{"work": {Name: "Work", Request: "work-request"}}
			busy, attention, err := inspect(ctx, f.config, seen)
			if err != nil || (len(busy) > 0) != test.wantBusy || (len(attention) > 0) != test.wantAttention {
				t.Fatal(busy, attention, err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.messages) != 0 {
				t.Fatal("inspect notified")
			}
			for _, method := range f.methods {
				if method != "initialize" && method != "initialized" && method != "thread/loaded/list" && method != "thread/read" && method != "thread/goal/get" {
					t.Fatal("native write", method)
				}
			}
		})
	}
}

func TestUnknownNativeStateAndIncompleteRuntimeCoverageFailClosed(t *testing.T) {
	f := testFixture(t)
	f.states["work-thread"] = "unexpected"
	if _, _, err := inspect(context.Background(), f.config, map[string]tracked{}); err == nil {
		t.Fatal("accepted unknown native state")
	}
	f.states["work-thread"] = "idle"
	f.runtimes = f.runtimes[:1]
	if _, _, err := inspect(context.Background(), f.config, map[string]tracked{}); err == nil {
		t.Fatal("accepted incomplete coverage")
	}
}

func TestUnknownIdentityCannotNotify(t *testing.T) {
	f := testFixture(t)
	f.config.TargetThread = "different-thread"
	_, _, err := inspect(context.Background(), f.config, map[string]tracked{})
	if err == nil || err.Error() != "target_thread_changed" {
		t.Fatal(err)
	}
}

func TestDurableUncertainCommandsNeedAttentionAndDatabaseIsNotMutated(t *testing.T) {
	f := testFixture(t)
	db, err := sql.Open("sqlite", filepath.Join(f.config.DataDir, "message_queue.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("INSERT INTO queue_commands VALUES('work','old','uncertain')")
	if err != nil {
		t.Fatal(err)
	}
	busy, attention, err := inspect(context.Background(), f.config, map[string]tracked{})
	if err != nil || len(busy) > 0 || len(attention) != 1 {
		t.Fatal(busy, attention, err)
	}
	var state string
	db.QueryRow("SELECT state FROM queue_commands").Scan(&state)
	if state != "uncertain" {
		t.Fatal("modified queue")
	}
}

func TestNotifyRequiresExactRecipientAndDurableReceipt(t *testing.T) {
	for _, status := range []string{"queued", "failed"} {
		t.Run(status, func(t *testing.T) {
			f := testFixture(t)
			f.ackStatus = status
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := notify(ctx, f.config, "idle-watch-fixture", "one scoped notification")
			if (err == nil) != (status == "queued") {
				t.Fatal(err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.messages) != 1 || f.messages[0]["session_id"] != "root" || f.messages[0]["request_id"] != "idle-watch-fixture" {
				t.Fatal(f.messages)
			}
		})
	}
	f := testFixture(t)
	f.ackSession = "work"
	if err := notify(context.Background(), f.config, "idle-watch-fixture", "test"); err == nil {
		t.Fatal("accepted another recipient")
	}
}

func TestNativeWritesAreForbiddenBeforeAnyConnectionUse(t *testing.T) {
	for _, method := range []string{"turn/start", "thread/resume", "thread/fork", "turn/interrupt", "thread/goal/set"} {
		if _, err := (&native{}).call(context.Background(), method, nil); err == nil {
			t.Fatal(method)
		}
	}
}

func TestOneShotCheckpointAndCancellation(t *testing.T) {
	f := testFixture(t)
	r := report{Job: f.config.Job, Target: f.config.Target, Phase: "notified", Tracked: map[string]tracked{}}
	if err := saveReport(f.config.StateFile, r); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), f.config); err != nil {
		t.Fatal(err)
	}
	if info, _ := os.Stat(f.config.StateFile); info.Mode().Perm() != 0600 {
		t.Fatal("checkpoint not private")
	}
	r.Phase = "waiting"
	saveReport(f.config.StateFile, r)
	os.WriteFile(f.config.StateFile+".cancel", []byte("cancel"), 0600)
	if err := run(context.Background(), f.config); err != nil {
		t.Fatal(err)
	}
	readJSON(f.config.StateFile, &r)
	if r.Phase != "cancelled" {
		t.Fatal(r)
	}
}

func TestRejectsRemoteEndpointsAndNotificationWithoutAuthorization(t *testing.T) {
	f := testFixture(t)
	if err := validateConfig(f.config); err != nil {
		t.Fatal(err)
	}
	f.config.Bridge = "ws://example.com/"
	if validateConfig(f.config) == nil {
		t.Fatal("accepted remote")
	}
	f.config.Bridge = "ws://127.0.0.1:8766/"
	f.config.Notify = false
	if validateConfig(f.config) == nil {
		t.Fatal("notification without explicit authorization")
	}
}
