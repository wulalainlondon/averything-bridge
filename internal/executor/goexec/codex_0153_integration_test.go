package goexec

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// TestCodex0153IsolatedDaemonIntegration is an opt-in real-Codex compatibility
// gate. It uses a temporary CODEX_HOME and a fresh thread, never a production
// rollout. The test proves turn/resume, daemon restart recovery, and the v2
// structured request_user_input response used by Codex 0.153+.
func TestCodex0153IsolatedDaemonIntegration(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_RUN_CODEX_0153_INTEGRATION") != "1" {
		t.Skip("set EVERYTHING_GO_RUN_CODEX_0153_INTEGRATION=1")
	}
	realHome := os.Getenv("CODEX_HOME")
	if realHome == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			t.Fatal(err)
		}
		realHome = filepath.Join(userHome, ".codex")
	}
	auth, err := os.ReadFile(filepath.Join(realHome, "auth.json"))
	if err != nil {
		t.Fatalf("read existing Codex authentication: %v", err)
	}
	isolatedHome, err := os.MkdirTemp("/tmp", "c153-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(isolatedHome) })
	if err := os.WriteFile(filepath.Join(isolatedHome, "auth.json"), auth, 0o600); err != nil {
		t.Fatal(err)
	}
	managedBinary, err := filepath.EvalSymlinks(filepath.Join(realHome, "packages", "standalone", "current", "codex"))
	if err != nil {
		t.Fatalf("resolve managed Codex binary: %v", err)
	}
	isolatedManagedDir := filepath.Join(isolatedHome, "packages", "standalone", "current")
	if err := os.MkdirAll(isolatedManagedDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(managedBinary, filepath.Join(isolatedManagedDir, "codex")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_HOME", isolatedHome)
	t.Setenv("EVERYTHING_GO_CODEX_SESSIONS_DIR", "")

	runDaemon := func(action string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "codex", "app-server", "daemon", action)
		cmd.Env = append(os.Environ(), "CODEX_HOME="+isolatedHome)
		return cmd.CombinedOutput()
	}
	t.Cleanup(func() {
		_, _ = runDaemon("stop")
	})

	sink := &capSink{}
	c := NewCodex(sink, "codex")
	c.appServerMode = "daemon"
	t.Cleanup(func() {
		c.remoteReconnect = false
		c.startMu.Lock()
		_ = c.stopServerLocked()
		c.startMu.Unlock()
	})
	if err := c.ensureServer(); err != nil {
		t.Fatalf("start isolated 0.153 daemon: %v", err)
	}
	diagnostics := c.RuntimeDiagnostics()
	codexRuntime, _ := diagnostics["codex"].(map[string]any)
	if codexRuntime["status"] != "ready" || codexRuntime["running_version"] != "0.153.0" {
		t.Fatalf("unexpected isolated daemon version: %+v", diagnostics)
	}

	reg := session.NewRegistry()
	workspace := t.TempDir()
	s := reg.Create("codex-0153-isolated", "Codex 0.153 isolated QA", workspace, backend.Codex, codexDefaultModel, "read-only", "")
	s.SetEffort("low")
	if err := c.Send(context.Background(), s, "turn-1", "Reply with exactly CODEX_0153_TURN_OK", nil, nil); err != nil {
		t.Fatal(err)
	}
	waitForCodexDone(t, sink, 2*time.Minute)
	threadID := s.ResumeID()
	if threadID == "" {
		t.Fatal("first turn did not persist a thread id")
	}

	if output, err := runDaemon("restart"); err != nil {
		t.Fatalf("restart isolated daemon: %v (%s)", err, strings.TrimSpace(string(output)))
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		st := c.state(s.ID)
		st.mu.Lock()
		invalidated := st.threadID == ""
		st.mu.Unlock()
		if invalidated {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.ensureServer(); err == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err := c.Send(context.Background(), s, "turn-2", "Reply with exactly CODEX_0153_RESUME_OK", nil, nil); err != nil {
		t.Fatalf("turn after daemon restart: %v", err)
	}
	waitForCodexDone(t, sink, 2*time.Minute)
	if s.ResumeID() != threadID {
		t.Fatalf("daemon restart changed thread id: got %q want %q", s.ResumeID(), threadID)
	}
	if _, err := c.Catalog(context.Background()); err != nil {
		t.Fatalf("load 0.153 model/collaboration catalog: %v", err)
	}
	planMode := "plan"
	s.ApplyCodexSettings(nil, &planMode, nil)
	if err := c.UpdateSessionSettings(context.Background(), s); err != nil {
		t.Fatalf("enable Plan collaboration mode: %v", err)
	}

	beforeDone := sink.count(func(event any) bool {
		_, ok := event.(backend.Done)
		return ok
	})
	if err := c.Send(context.Background(), s, "turn-3", "Use request_user_input_async now to ask one short non-secret question. Do not answer the question yourself and do not complete the turn until the answer arrives.", nil, nil); err != nil {
		t.Fatal(err)
	}
	var pending backend.UserInputPayload
	deadline = time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		items := c.PendingInteractions(s.ID)
		if len(items) > 0 {
			pending = items[0]
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pending.RequestID == "" || len(pending.Questions) == 0 {
		t.Fatal("0.153 did not produce a structured user-input request")
	}
	answers := map[string]any{pending.Questions[0].QuestionID: "continue"}
	if !c.RespondUserInput(pending.RequestID, answers, false) {
		t.Fatal("structured user-input response was not accepted")
	}
	deadline = time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if sink.count(func(event any) bool {
			_, ok := event.(backend.Done)
			return ok
		}) > beforeDone {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if sink.count(func(event any) bool {
		_, ok := event.(backend.Done)
		return ok
	}) <= beforeDone {
		t.Fatal("turn did not complete after structured user-input response")
	}

	encoded, _ := json.Marshal(map[string]any{
		"thread_id": threadID, "runtime": codexRuntime, "interaction_kind": pending.Kind,
	})
	t.Log(string(encoded))
}
