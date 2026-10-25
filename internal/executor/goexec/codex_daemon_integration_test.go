package goexec

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// Opt-in real-Codex integration. It creates a fresh isolated thread and proves
// two independent Bridge proxy clients can resume/use it through one daemon.
func TestCodexSharedDaemonIntegration(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_RUN_CODEX_DAEMON_INTEGRATION") != "1" {
		t.Skip("set EVERYTHING_GO_RUN_CODEX_DAEMON_INTEGRATION=1")
	}
	cwd := t.TempDir()
	sinkA, sinkB := &capSink{}, &capSink{}
	a := NewCodex(sinkA, "codex")
	b := NewCodex(sinkB, "codex")
	a.appServerMode, b.appServerMode = "daemon", "daemon"
	for _, c := range []*Codex{a, b} {
		if err := c.ensureServer(); err != nil {
			t.Fatal(err)
		}
		defer func(c *Codex) {
			c.startMu.Lock()
			_ = c.stopServerLocked()
			c.startMu.Unlock()
		}(c)
	}

	regA, regB := session.NewRegistry(), session.NewRegistry()
	sa := regA.Create("bridge-a", "daemon integration", cwd, backend.Codex, codexDefaultModel, "workspace-write", "")
	if err := a.ensureThread(sa, a.state(sa.ID)); err != nil {
		t.Fatalf("client A thread/start: %v", err)
	}
	threadID := sa.ResumeID()
	if threadID == "" {
		t.Fatal("client A did not receive a thread id")
	}
	// Commit one isolated turn so the new thread has a durable rollout that a
	// second client can resume.
	if err := a.Send(context.Background(), sa, "seed-turn", "Reply exactly SHARED_DAEMON_SEED", nil, nil); err != nil {
		t.Fatal(err)
	}
	waitForCodexDone(t, sinkA, 2*time.Minute)
	if err := a.ReleaseSession(context.Background(), sa); err != nil {
		t.Fatalf("client A release before desktop handoff: %v", err)
	}

	sb := regB.Create("bridge-b", "daemon integration", cwd, backend.Codex, codexDefaultModel, "workspace-write", threadID)
	if err := b.ensureThread(sb, b.state(sb.ID)); err != nil {
		t.Fatalf("client B resume through same daemon: %v", err)
	}
	if sb.ResumeID() != threadID {
		t.Fatalf("client B resumed %q, want %q", sb.ResumeID(), threadID)
	}

	// Submit from B and require a complete streamed turn. A is also attached; a
	// text event on A records whether this CLI version fans notifications out to
	// every attached client (reported separately, not assumed).
	if err := b.Send(context.Background(), sb, "daemon-turn", "Reply exactly SHARED_DAEMON_OK", nil, nil); err != nil {
		t.Fatal(err)
	}
	waitForCodexDone(t, sinkB, 2*time.Minute)
	if err := b.ReleaseSession(context.Background(), sb); err != nil {
		t.Fatalf("client B release before reclaim: %v", err)
	}
	if err := a.ClaimSession(context.Background(), sa); err != nil {
		t.Fatalf("client A reclaim: %v", err)
	}
	if err := a.Send(context.Background(), sa, "reclaim-turn", "Reply exactly SHARED_DAEMON_RECLAIMED", nil, nil); err != nil {
		t.Fatal(err)
	}
	waitForCodexDone(t, sinkA, 2*time.Minute)
	t.Logf("thread_id=%s client_a_text_events=%d client_b_text_events=%d", threadID,
		sinkA.count(func(e any) bool { _, ok := e.(protocol.TextChunk); return ok }),
		sinkB.count(func(e any) bool { _, ok := e.(protocol.TextChunk); return ok }))
	if output := os.Getenv("EVERYTHING_GO_CODEX_TEST_THREAD_FILE"); output != "" {
		if err := os.WriteFile(filepath.Clean(output), []byte(threadID+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// Opt-in protocol probe for the lightweight shared-daemon attach path.
// excludeTurns keeps thread/resume from serializing the full rollout while
// preserving the subscription needed for streaming and terminal events.
func TestCodexSharedDaemonResumeWithoutTurnsThenTurnIntegration(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_RUN_CODEX_DAEMON_INTEGRATION") != "1" {
		t.Skip("set EVERYTHING_GO_RUN_CODEX_DAEMON_INTEGRATION=1")
	}
	cwd := t.TempDir()
	sinkA, sinkB := &capSink{}, &capSink{}
	a := NewCodex(sinkA, "codex")
	b := NewCodex(sinkB, "codex")
	a.appServerMode, b.appServerMode = "daemon", "daemon"
	for _, c := range []*Codex{a, b} {
		if err := c.ensureServer(); err != nil {
			t.Fatal(err)
		}
		defer func(c *Codex) {
			c.startMu.Lock()
			_ = c.stopServerLocked()
			c.startMu.Unlock()
		}(c)
	}

	regA := session.NewRegistry()
	sa := regA.Create("resume-seed", "daemon resume seed", cwd, backend.Codex, codexDefaultModel, "workspace-write", "")
	if err := a.Send(context.Background(), sa, "seed", "Reply exactly RESUME_WITHOUT_TURNS_SEED", nil, nil); err != nil {
		t.Fatal(err)
	}
	waitForCodexDone(t, sinkA, 2*time.Minute)
	threadID := sa.ResumeID()
	if err := a.ReleaseSession(context.Background(), sa); err != nil {
		t.Fatalf("release seed client: %v", err)
	}

	regB := session.NewRegistry()
	sb := regB.Create("resume-client", "daemon resume client", cwd, backend.Codex, codexDefaultModel, "workspace-write", threadID)
	raw, err := b.rpcCall("thread/resume", map[string]any{"threadId": threadID, "excludeTurns": true}, 15*time.Second)
	if err != nil {
		t.Fatalf("thread/resume without turns: %v", err)
	}
	if got := extractThreadID(raw, threadID); got != threadID {
		t.Fatalf("thread/resume id=%q, want %q", got, threadID)
	}
	st := b.state(sb.ID)
	st.threadID = threadID
	b.threadToSession[threadID] = sb
	if err := b.Send(context.Background(), sb, "resume-turn", "Reply exactly RESUME_WITHOUT_TURNS_OK", nil, nil); err != nil {
		t.Fatalf("turn after lightweight resume: %v", err)
	}
	waitForCodexDone(t, sinkB, 2*time.Minute)
}

// Opt-in read-only probe for a thread currently attached to another daemon
// client (typically a desktop TUI). It never resumes, starts, or mutates the
// thread; it verifies which metadata RPCs are safe across client connections.
func TestCodexSharedDaemonReadsDesktopOwnedThread(t *testing.T) {
	threadID := os.Getenv("EVERYTHING_GO_CODEX_DESKTOP_THREAD_ID")
	if threadID == "" {
		t.Skip("set EVERYTHING_GO_CODEX_DESKTOP_THREAD_ID to an attached desktop thread")
	}
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	if err := c.ensureServer(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.startMu.Lock()
		_ = c.stopServerLocked()
		c.startMu.Unlock()
	})
	if _, err := c.rpcCall("thread/read", map[string]any{"threadId": threadID, "includeTurns": false}, 15*time.Second); err != nil {
		t.Fatalf("thread/read on desktop-owned thread: %v", err)
	}
	reg := session.NewRegistry()
	s := reg.Create("desktop-read-probe", "desktop read probe", t.TempDir(), backend.Codex, codexDefaultModel, "workspace-write", threadID)
	if err := c.GetGoal(context.Background(), s); err != nil {
		t.Fatalf("GetGoal on desktop-owned thread: %v", err)
	}
}

// Opt-in non-turn probe for the exact attach operation used by Bridge. It may
// briefly subscribe this client to the thread, then immediately unsubscribes;
// it never starts, interrupts, archives, or deletes a turn.
func TestCodexSharedDaemonResumesDesktopOwnedThreadWithoutTurns(t *testing.T) {
	threadID := os.Getenv("EVERYTHING_GO_CODEX_DESKTOP_THREAD_ID")
	if threadID == "" {
		t.Skip("set EVERYTHING_GO_CODEX_DESKTOP_THREAD_ID to an attached desktop thread")
	}
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	if err := c.ensureServer(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.startMu.Lock()
		_ = c.stopServerLocked()
		c.startMu.Unlock()
	})
	started := time.Now()
	raw, err := c.rpcCall("thread/resume", map[string]any{"threadId": threadID, "excludeTurns": true}, 15*time.Second)
	if err != nil {
		t.Fatalf("thread/resume without turns: %v", err)
	}
	if got := extractThreadID(raw, threadID); got != threadID {
		t.Fatalf("thread/resume id=%q, want %q", got, threadID)
	}
	if len(raw) > 1024*1024 {
		t.Fatalf("metadata-only resume response=%d bytes, want <=1 MiB", len(raw))
	}
	if _, err := c.rpcCall("thread/unsubscribe", map[string]any{"threadId": threadID}, 15*time.Second); err != nil {
		t.Fatalf("thread/unsubscribe: %v", err)
	}
	t.Logf("thread_id=%s response_bytes=%d elapsed=%s", threadID, len(raw), time.Since(started))
}

func waitForCodexDone(t *testing.T, sink *capSink, timeout time.Duration) {
	t.Helper()
	before := sink.count(func(e any) bool { _, ok := e.(protocol.Done); return ok })
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sink.count(func(e any) bool { _, ok := e.(protocol.Done); return ok }) > before {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("Codex client did not receive turn completion")
}
