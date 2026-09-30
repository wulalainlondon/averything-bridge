package goexec

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func recapServer(t *testing.T, mode string) (string, func() []map[string]any) {
	t.Helper()
	dir, err := os.MkdirTemp("/private/tmp", "recap-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "daemon.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var calls []map[string]any
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		send := func(value any) { raw, _ := json.Marshal(value); conn.Write(r.Context(), websocket.MessageText, raw) }
		for {
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var req map[string]any
			if json.Unmarshal(raw, &req) != nil {
				return
			}
			mu.Lock()
			calls = append(calls, req)
			mu.Unlock()
			method, _ := req["method"].(string)
			if method == "" || method == "initialized" {
				continue
			}
			result := any(map[string]any{})
			switch method {
			case "config/read":
				result = map[string]any{"config": map[string]any{"mcp_servers": map[string]any{"danger": map[string]string{"token": "SECRET_DO_NOT_FORWARD"}}, "plugins": map[string]any{"example/plugin": map[string]bool{"enabled": true}}}}
			case "thread/start":
				result = map[string]any{"thread": map[string]string{"id": "recap-thread"}}
			case "turn/start":
				result = map[string]any{"turn": map[string]string{"id": "recap-turn"}}
				// Deliberately deliver completed notifications before the RPC reply.
				// Real app-server can do this for very short turns.
				send(map[string]any{"method": "item/started", "params": map[string]any{"threadId": "recap-thread", "item": map[string]string{"type": "userMessage"}}})
				if mode == "tool" {
					send(map[string]any{"id": "approval/string-id", "method": "item/commandExecution/requestApproval", "params": map[string]string{"threadId": "recap-thread"}})
				} else {
					send(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "other-thread", "item": map[string]string{"type": "agentMessage", "text": "WRONG_THREAD"}}})
					text := `{"summary":"APK built; installation validation is unfinished","next_action":"Bring Note20 online"}`
					if mode == "invalid" {
						text = "not JSON"
					}
					send(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "recap-thread", "item": map[string]string{"type": "agentMessage", "phase": "final_answer", "text": text}}})
					status := "completed"
					if mode == "failed" {
						status = "failed"
					}
					send(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "recap-thread", "turn": map[string]string{"id": "recap-turn", "status": status}}})
				}
			}
			send(map[string]any{"id": req["id"], "result": result})
		}
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	return path, func() []map[string]any { mu.Lock(); defer mu.Unlock(); return append([]map[string]any(nil), calls...) }
}

func TestCodexRecapUsesEphemeralSharedDaemonClientAndNoParentMutation(t *testing.T) {
	path, calls := recapServer(t, "ok")
	c := NewCodex(&capSink{}, "codex")
	c.appServerSocket = path
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g, err := c.generateRecap(ctx, "gpt-6.1-sol", "user: Add phone model refresh\nassistant: APK built; deployment pending")
	if err != nil || !strings.Contains(g.Summary, "unfinished") || g.NextAction == nil {
		t.Fatal(g, err)
	}
	for _, req := range calls() {
		method, _ := req["method"].(string)
		if method == "thread/resume" || method == "thread/fork" || method == "turn/steer" {
			t.Fatal("mutated parent", method)
		}
		if method == "thread/start" {
			p := req["params"].(map[string]any)
			config := p["config"].(map[string]any)
			servers := config["mcp_servers"].(map[string]any)
			features := config["features"].(map[string]any)
			if p["ephemeral"] != true || p["sandbox"] != "read-only" || p["approvalPolicy"] != "never" || servers["danger"].(map[string]any)["enabled"] != false || features["shell_tool"] != false || features["plugins"] != false || len(p["environments"].([]any)) != 0 {
				t.Fatal("unsafe recap configuration", p)
			}
		}
		if method == "turn/start" {
			p := req["params"].(map[string]any)
			if p["threadId"] != "recap-thread" || p["outputSchema"] == nil {
				t.Fatal(p)
			}
			raw, _ := json.Marshal(p)
			if strings.Contains(string(raw), "SECRET_DO_NOT_FORWARD") {
				t.Fatal("leaked config secret")
			}
		}
	}
}

func TestCodexRecapRejectsToolsInvalidJSONAndFailedTurns(t *testing.T) {
	for _, mode := range []string{"tool", "invalid", "failed"} {
		t.Run(mode, func(t *testing.T) {
			path, _ := recapServer(t, mode)
			c := NewCodex(&capSink{}, "codex")
			c.appServerSocket = path
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := c.generateRecap(ctx, "", "user: Summarize only"); err == nil {
				t.Fatal("accepted", mode)
			}
		})
	}
}

// One explicit, bounded model request; never resumes the active conversation.
func TestCodexRecapLive(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_TEST_CODEX_RECAP_LIVE") != "1" {
		t.Skip("opt-in model inference")
	}
	c := NewCodex(&capSink{}, "codex")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	g, err := c.generateRecap(ctx, "gpt-6.1-sol", "user: Fix phone model refresh and send an APK to Note20.\nassistant: The refresh fix is implemented and tests passed. Signed APK 1.3.51 was built. Note20 is offline; transfer failed. The model picker is not yet validated on that phone.\nuser: Tell me the status.")
	if err != nil || !g.Valid() {
		t.Fatal("live recap", g, err)
	}
	t.Logf("summary=%s next=%v", g.Summary, g.NextAction)
}
