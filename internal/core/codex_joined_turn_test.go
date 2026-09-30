package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/executor/goexec"
	"everything-go/internal/messagequeue"
	"github.com/coder/websocket"
)

// Real executor, RPC transport, Hub, queue and phone frames; only the native
// daemon is deterministic. No model call or production daemon is involved.
func TestCodexJoinedTurnSettlesDurableQueueAndPhoneRuntime(t *testing.T) {
	socketDir, err := os.MkdirTemp("/tmp", "bridge-turn-qa-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "daemon.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_HOME", t.TempDir())
	t.Setenv("EVERYTHING_GO_CODEX_APP_SERVER_MODE", "daemon")
	t.Setenv("EVERYTHING_GO_CODEX_APP_SERVER_SOCKET", socket)
	var starts atomic.Int32
	finishSecond := make(chan struct{})
	var finishOnce sync.Once
	finish := func() { finishOnce.Do(func() { close(finishSecond) }) }
	t.Cleanup(finish)
	var connected sync.Mutex
	var peer *websocket.Conn
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		connected.Lock()
		peer = conn
		connected.Unlock()
		defer conn.CloseNow()
		write := func(value any) bool {
			raw, _ := json.Marshal(value)
			return conn.Write(r.Context(), websocket.MessageText, raw) == nil
		}
		terminal := func(id string) bool {
			return write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "root", "turn": map[string]string{"id": id, "status": "completed"}}})
		}
		text := func(id, delta string) bool {
			return write(map[string]any{"method": "item/agentMessage/delta", "params": map[string]string{"threadId": "root", "turnId": id, "delta": delta, "phase": "final_answer"}})
		}
		for {
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var request struct {
				ID     *int   `json:"id"`
				Method string `json:"method"`
			}
			if json.Unmarshal(raw, &request) != nil || request.ID == nil {
				continue
			}
			result := any(map[string]any{})
			switch request.Method {
			case "thread/resume":
				result = map[string]any{"thread": map[string]any{"id": "root", "status": map[string]string{"type": "active"}}}
			case "turn/start":
				n := starts.Add(1)
				id := "joined-turn"
				if n > 1 {
					id = "next-turn"
				}
				if n == 1 {
					// Joining a running desktop turn produces no turn/started;
					// completion can even precede the submission response.
					if !text(id, "部署已完成") || !terminal("old-turn") || !terminal(id) {
						return
					}
				}
				result = map[string]any{"turn": map[string]string{"id": id, "status": "inProgress"}}
				if !write(map[string]any{"id": *request.ID, "result": result}) {
					return
				}
				if n > 1 {
					if !terminal("joined-turn") || !text(id, "new work") {
						return
					}
					<-finishSecond
					if !terminal(id) {
						return
					}
				}
				continue
			}
			if !write(map[string]any{"id": *request.ID, "result": result}) {
				return
			}
		}
	})}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		finish()
		connected.Lock()
		if peer != nil {
			_ = peer.CloseNow()
		}
		connected.Unlock()
		_ = server.Close()
		<-serverDone
	})

	h, _ := newTestHub(t)
	data := t.TempDir()
	codex := goexec.NewCodex(h, "/usr/bin/true")
	codex.SetDataDir(data)
	h.SetExecutor(codex)
	s := h.registry.Create("s1", "joined", t.TempDir(), "codex", "", "", "root")
	t.Cleanup(func() { _ = codex.Close(context.Background(), s); s.Close() })
	// Preserve the existing observer attribution until the accepted join.
	sum := sha256.Sum256([]byte("root"))
	journalDir := filepath.Join(data, "codex-turn-requests")
	if err := os.MkdirAll(journalDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(journalDir, hex.EncodeToString(sum[:])+".json"), []byte(`{"version":1,"thread_id":"root","requests":{"joined-turn":"codex_external_joined-turn"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	phone := newTestClient(h)
	enqueueTestMessage(t, h, phone, "phone-join")
	done := waitForType(t, phone, "done")
	if done["request_id"] != "phone-join" {
		t.Fatalf("wrong completion identity: %+v", done)
	}
	expectState(t, h, "phone-join", messagequeue.Completed)
	view := h.runtimeSnapshot("phone").Items[0]
	if view.Phase != "completed" || view.ActiveRequestID != "phone-join" || s.IsStreaming() || h.sessionSummaries()[0].IsStreaming {
		t.Fatalf("phone/runtime retained streaming after native completion: %+v", view)
	}

	enqueueTestMessage(t, h, phone, "phone-next")
	_ = waitForType(t, phone, "text_chunk")
	expectState(t, h, "phone-next", messagequeue.Running)
	if view := h.runtimeSnapshot("phone").Items[0]; view.Phase != "running" || view.ActiveRequestID != "phone-next" {
		t.Fatalf("late old terminal ended new runtime: %+v", view)
	}
	finish()
	done = waitForType(t, phone, "done")
	if done["request_id"] != "phone-next" {
		t.Fatalf("wrong next completion: %+v", done)
	}
	expectState(t, h, "phone-next", messagequeue.Completed)
	if starts.Load() != 2 {
		t.Fatal("messages were retried", starts.Load())
	}
	deadline := time.Now().Add(time.Second)
	for s.ActiveQueuedID() != "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.ActiveQueuedID() != "" || s.IsStreaming() {
		t.Fatal("completion did not release the worker")
	}
}
