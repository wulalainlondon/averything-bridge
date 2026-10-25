package goexec

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func healthTestCodex(t *testing.T) *Codex {
	t.Helper()
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.dataDir = t.TempDir()
	c.health.probeTimeout = 30 * time.Millisecond
	t.Cleanup(func() { c.health.mu.Lock(); c.health.blocked = false; c.health.mu.Unlock() })
	return c
}
func healthStatus(c *Codex) string {
	return c.RuntimeDiagnostics()["codex"].(map[string]any)["health_status"].(string)
}

// Real Unix WebSocket transport; no Codex installation, model calls or daemon
// mutations. Missing replies emulate a live initialize path with stuck reads.
func healthServer(t *testing.T, handler func(string, string) (any, bool)) (string, *atomic.Int32) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "codex-health-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "daemon.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	count := &atomic.Int32{}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		count.Add(1)
		defer conn.CloseNow()
		for {
			_, raw, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			var req struct {
				ID     int    `json:"id"`
				Method string `json:"method"`
				Params struct {
					ThreadID string `json:"threadId"`
				} `json:"params"`
			}
			if json.Unmarshal(raw, &req) != nil {
				return
			}
			if req.ID == 0 {
				continue
			}
			result, reply := handler(req.Method, req.Params.ThreadID)
			if !reply {
				continue
			}
			payload, _ := json.Marshal(map[string]any{"id": req.ID, "result": result})
			if conn.Write(r.Context(), websocket.MessageText, payload) != nil {
				return
			}
		}
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return path, count
}
func TestHealthProbeUsesIndependentReadConnections(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		t.Run(fmt.Sprint(healthy), func(t *testing.T) {
			c := healthTestCodex(t)
			var mu sync.Mutex
			methods := []string{}
			path, connections := healthServer(t, func(method, id string) (any, bool) {
				mu.Lock()
				methods = append(methods, method)
				mu.Unlock()
				switch method {
				case "initialize":
					return map[string]any{}, true
				case "thread/list":
					return map[string]any{"data": []map[string]any{{"id": "a"}, {"id": "b"}}}, true
				case "thread/read":
					return map[string]any{"thread": map[string]any{"id": id, "status": map[string]any{"type": "idle"}}}, healthy
				}
				return nil, false
			})
			c.appServerSocket = path
			result := c.probeDaemonHealth("")
			if !result.Initialized || connections.Load() != 3 {
				t.Fatalf("probe=%+v connections=%d", result, connections.Load())
			}
			if healthy && result.Reads != 2 {
				t.Fatalf("%+v", result)
			}
			if !healthy && result.ReadTimeouts != 2 {
				t.Fatalf("%+v", result)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, method := range methods {
				if method != "initialize" && method != "thread/list" && method != "thread/read" {
					t.Fatalf("mutating probe: %s", method)
				}
			}
		})
	}
}
func TestHealthProbeUsesKnownThreadsWhenListUnavailable(t *testing.T) {
	c := healthTestCodex(t)
	c.threadToSession["a"] = nil
	c.threadToSession["b"] = nil
	path, _ := healthServer(t, func(method, id string) (any, bool) { return map[string]any{}, method == "initialize" })
	c.appServerSocket = path
	result := c.probeDaemonHealth("a")
	if result.ReadTimeouts != 2 {
		t.Fatalf("%+v", result)
	}
}
func TestHealthProbeSingleThreadIsInsufficientForRestart(t *testing.T) {
	c := healthTestCodex(t)
	path, _ := healthServer(t, func(method, id string) (any, bool) {
		if method == "initialize" {
			return map[string]any{}, true
		}
		if method == "thread/list" {
			return map[string]any{"data": []map[string]any{{"id": "a"}, {"id": "a"}}}, true
		}
		return nil, false
	})
	c.appServerSocket = path
	result := c.probeDaemonHealth("a")
	if result.ReadTimeouts != 1 {
		t.Fatalf("%+v", result)
	}
}
func TestHealthRecoveryRequiresRepeatedEvidenceAndOwner(t *testing.T) {
	for _, owner := range []bool{false, true} {
		t.Run(fmt.Sprint(owner), func(t *testing.T) {
			t.Setenv("EVERYTHING_GO_CODEX_DAEMON_RECOVERY_OWNER", fmt.Sprint(owner))
			c := healthTestCodex(t)
			restarts := 0
			c.health.probe = func(string) codexProbeResult {
				if restarts > 0 {
					return codexProbeResult{Initialized: true, Reads: 2}
				}
				return codexProbeResult{Initialized: true, ReadTimeouts: 2}
			}
			c.health.recover = func() error { restarts++; return nil }
			c.checkHealth("a")
			if restarts != 0 || !c.health.blocked {
				t.Fatal("first failure must only isolate")
			}
			c.checkHealth("a")
			if owner {
				if restarts != 1 || c.health.blocked || healthStatus(c) != "ready" {
					t.Fatal("owner recovery was not verified")
				}
			} else if restarts != 0 || healthStatus(c) != "restart_required" {
				t.Fatal("shared client restarted daemon")
			}
		})
	}
}
func TestHealthDoesNotRestartOnPartialFailureOrActiveWork(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_DAEMON_RECOVERY_OWNER", "true")
	for _, test := range []struct {
		name   string
		result codexProbeResult
		active bool
	}{
		{"one thread", codexProbeResult{Initialized: true, ReadTimeouts: 1}, false},
		{"some healthy", codexProbeResult{Initialized: true, ReadTimeouts: 1, Reads: 1}, false},
		{"transport", codexProbeResult{}, false},
		{"active", codexProbeResult{Initialized: true, ReadTimeouts: 2}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := healthTestCodex(t)
			if test.active {
				c.state("a").turnActive = true
			}
			c.health.probe = func(string) codexProbeResult { return test.result }
			c.health.recover = func() error { t.Error("unsafe restart"); return nil }
			c.checkHealth("a")
			c.checkHealth("a")
		})
	}
}

type healthCountingWriter struct{ writes atomic.Int32 }

func (w *healthCountingWriter) Write(p []byte) (int, error) { w.writes.Add(1); return len(p), nil }
func TestTimeoutTriggersSingleProbeAndNeverReplaysMutation(t *testing.T) {
	c := healthTestCodex(t)
	writer := &healthCountingWriter{}
	c.rpc.setWriter(writer)
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c.health.probe = func(string) codexProbeResult { close(started); <-release; return codexProbeResult{Reads: 1} }
	_, err := c.rpcCall("turn/start", map[string]any{"threadId": "a"}, time.Millisecond)
	var timeout *rpcTimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("not typed timeout: %v", err)
	}
	<-started
	if _, err = c.rpcCall("thread/start", nil, time.Millisecond); err == nil {
		t.Fatal("circuit admitted mutation")
	}
	if writer.writes.Load() != 1 || c.rpc.pendingCount() != 0 {
		t.Fatal("request replayed or leaked")
	}
	if healthGatedMethod("turn/interrupt") || healthGatedMethod("thread/unsubscribe") {
		t.Fatal("recovery blocked cancellation")
	}
	close(release)
	go func() {
		for {
			c.health.mu.Lock()
			checking := c.health.checking
			c.health.mu.Unlock()
			if !checking {
				close(finished)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("probe did not complete")
	}
	if writer.writes.Load() != 1 {
		t.Fatal("original mutation replayed")
	}
}
func TestRecoveryLockAndCooldown(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_DAEMON_RECOVERY_OWNER", "true")
	c := healthTestCodex(t)
	home := t.TempDir()
	c.sessionsRoot = filepath.Join(home, "sessions")
	dir := filepath.Join(home, "app-server-daemon")
	_ = os.MkdirAll(dir, 0700)
	unlock, err := lockCodexRecovery(filepath.Join(dir, "bridge-recovery.lock"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockCodexRecovery(filepath.Join(dir, "bridge-recovery.lock")); err == nil {
		t.Fatal("concurrent owner admitted")
	}
	unlock()
	_ = os.WriteFile(filepath.Join(dir, "bridge-recovery-at"), []byte("attempt"), 0600)
	if err := c.recoverUnhealthyDaemon(); err == nil {
		t.Fatal("cooldown ignored")
	}
}
func TestHealthDiagnosticsPreserveVersionAndExcludePayloads(t *testing.T) {
	c := healthTestCodex(t)
	c.runtimeDiagnostics = map[string]any{"codex": map[string]any{"status": "restart_required", "running_version": "old"}}
	c.setHealthDiagnostics("ready", codexProbeResult{Reads: 2}, "checked")
	fields := c.RuntimeDiagnostics()["codex"].(map[string]any)
	if fields["status"] != "restart_required" || fields["health_status"] != "ready" {
		t.Fatal(fields)
	}
	c.captureHealthEvidence("ready", codexProbeResult{Reads: 2}, "checked")
	info, err := os.Stat(filepath.Join(c.dataDir, "diagnostics", "codex-health.json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("snapshot permissions: %v %v", info, err)
	}
}

func TestHealthLiveReadOnly(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_RUN_CODEX_HEALTH_PROBE") != "1" {
		t.Skip("read-only live probe opt-in")
	}
	c := healthTestCodex(t)
	c.health.probeTimeout = 4 * time.Second
	result := c.probeDaemonHealth("")
	t.Logf("read-only probe: %+v", result)
	if !result.Initialized || result.Reads < 1 {
		t.Fatal("daemon not ready")
	}
}

func TestRecoveryCommandRunsOnceAndWritesCooldown(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_DAEMON_RECOVERY_OWNER", "true")
	c := healthTestCodex(t)
	home, err := os.MkdirTemp("/tmp", "health-owner-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	c.sessionsRoot = filepath.Join(home, "sessions")
	c.health.threadID = "a"
	c.threadToSession["a"] = nil
	c.threadToSession["b"] = nil
	socket, _ := healthServer(t, func(method, id string) (any, bool) { return map[string]any{}, method == "initialize" })
	socketDir := filepath.Join(home, "app-server-control")
	if err := os.MkdirAll(socketDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(socket, filepath.Join(socketDir, "app-server-control.sock")); err != nil {
		t.Fatal(err)
	}
	c.codexBin = filepath.Join(home, "fake-codex")
	script := "#!/bin/sh\nif [ \"$3\" = restart ]; then echo restart >> \"$CODEX_HOME/restarts\"; exit 0; fi\necho '{\"status\":\"running\",\"appServerVersion\":\"test\",\"managedCodexVersion\":\"test\"}'\n"
	if err := os.WriteFile(c.codexBin, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := c.recoverUnhealthyDaemon(); err != nil {
		t.Fatal(err)
	}
	if err := c.recoverUnhealthyDaemon(); err == nil {
		t.Fatal("second restart escaped cooldown")
	}
	raw, err := os.ReadFile(filepath.Join(home, "restarts"))
	if err != nil || string(raw) != "restart\n" {
		t.Fatalf("restart count: %q %v", raw, err)
	}
}
