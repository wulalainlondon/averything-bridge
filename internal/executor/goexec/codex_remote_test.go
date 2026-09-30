package goexec

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func TestRemoteTransportReadsLargeThreadResumeResponse(t *testing.T) {
	socketDir, err := os.MkdirTemp("/tmp", "everything-go-ws-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socketPath := filepath.Join(socketDir, "app-server.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatal(err)
	}

	methods := make(chan string, 32)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		for {
			_, payload, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var request struct {
				ID     *int   `json:"id"`
				Method string `json:"method"`
			}
			if err := json.Unmarshal(payload, &request); err != nil || request.ID == nil {
				continue
			}
			methods <- request.Method
			if request.Method == "test/disconnect" {
				_ = conn.Close(websocket.StatusPolicyViolation, "forced test disconnect")
				return
			}
			result := any(map[string]any{})
			if request.Method == "thread/resume" {
				// Well above coder/websocket's 32 KiB default. This models the
				// single-message history response returned for a mature thread.
				result = map[string]any{
					"thread": map[string]any{
						"id":      "large-thread",
						"history": strings.Repeat("h", 2*1024*1024),
					},
				}
			}
			response, err := json.Marshal(map[string]any{"id": *request.ID, "result": result})
			if err != nil {
				return
			}
			if err := conn.Write(r.Context(), websocket.MessageText, response); err != nil {
				return
			}
		}
	})}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Shutdown(context.Background())
		if err := <-serverDone; err != nil && err != http.ErrServerClosed {
			t.Errorf("server shutdown: %v", err)
		}
	})

	codexHome := t.TempDir()
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	c.appServerSocket = socketPath
	c.remoteReconnect = false
	if err := c.startRemoteServerLocked(codexHome); err != nil {
		t.Fatalf("connect remote transport: %v", err)
	}
	t.Cleanup(func() {
		c.startMu.Lock()
		defer c.startMu.Unlock()
		if err := c.stopServerLocked(); err != nil {
			t.Errorf("stop remote transport: %v", err)
		}
	})

	raw, err := c.rpcCall("thread/resume", map[string]any{"threadId": "large-thread"}, 5*time.Second)
	if err != nil {
		t.Fatalf("large thread/resume response: %v", err)
	}
	if len(raw) < 2*1024*1024 {
		t.Fatalf("resume result length = %d, want at least 2 MiB", len(raw))
	}
	var resumeResult struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(raw, &resumeResult); err != nil {
		t.Fatalf("decode resume result: %v", err)
	}
	if resumeResult.Thread.ID != "large-thread" {
		t.Fatalf("resume thread id = %q, want large-thread", resumeResult.Thread.ID)
	}

	// The successful large read must leave the shared connection usable.
	if _, err := c.rpcCall("thread/goal/get", map[string]any{"threadId": "large-thread"}, 5*time.Second); err != nil {
		t.Fatalf("request after large response: %v", err)
	}
	for len(methods) > 0 {
		<-methods
	}
	reg := session.NewRegistry()
	desktopOwned := reg.Create("desktop-owned", "desktop", t.TempDir(), backend.Codex, "", "", "desktop-thread")
	if err := c.GetGoal(context.Background(), desktopOwned); err != nil {
		t.Fatalf("read goal on desktop-owned thread: %v", err)
	}
	if method := <-methods; method != "thread/goal/get" {
		t.Fatalf("desktop-owned goal used %q, want direct thread/goal/get", method)
	}
	select {
	case method := <-methods:
		t.Fatalf("desktop-owned goal unexpectedly issued second RPC %q", method)
	default:
	}
	st := c.state(desktopOwned.ID)
	st.threadID = "desktop-thread"
	c.threadToSession["desktop-thread"] = desktopOwned
	if err := c.ReleaseSession(context.Background(), desktopOwned); err != nil {
		t.Fatalf("release shared-daemon thread: %v", err)
	}
	if method := <-methods; method != "thread/unsubscribe" {
		t.Fatalf("release used %q, want thread/unsubscribe", method)
	}
	st.mu.Lock()
	loadedThread := st.threadID
	st.mu.Unlock()
	if loadedThread != "" || c.threadToSession["desktop-thread"] != nil {
		t.Fatalf("release retained local thread routing: state=%q route=%v", loadedThread, c.threadToSession["desktop-thread"])
	}

	_, err = c.rpcCall("test/disconnect", nil, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "app-server connection lost") ||
		!strings.Contains(err.Error(), "forced test disconnect") {
		t.Fatalf("disconnect error = %v, want preserved transport reason", err)
	}

	// Guard the production ceiling: it must remain above the observed 22 MiB
	// rollout and finite rather than disabling memory protection.
	if codexRemoteReadLimit < 32*1024*1024 || codexRemoteReadLimit > 1024*1024*1024 {
		t.Fatalf("unexpected remote read limit: %d", codexRemoteReadLimit)
	}
}
