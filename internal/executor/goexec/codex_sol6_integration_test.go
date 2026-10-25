package goexec

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// Opt-in real model check. Uses a private stdio app-server, copied credentials,
// a temporary workspace and a new read-only turn. Never restarts the shared
// daemon, attaches a user's thread, or changes their model defaults.
func TestCodexSol6IsolatedIntegration(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_RUN_CODEX_SOL6_INTEGRATION") != "1" {
		t.Skip("set EVERYTHING_GO_RUN_CODEX_SOL6_INTEGRATION=1")
	}
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	auth, err := os.ReadFile(c.authPath)
	if err != nil {
		t.Fatal(err)
	}
	home, workspace := t.TempDir(), t.TempDir()
	c.sessionsRoot = filepath.Join(home, "sessions")
	c.indexPath = filepath.Join(home, "session_index.jsonl")
	c.authPath = filepath.Join(home, "auth.json")
	c.dataDir = t.TempDir()
	c.appServerMode = "stdio"
	c.appServerSocket = ""
	c.remoteReconnect = false
	if err := os.WriteFile(c.authPath, auth, 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.startMu.Lock(); defer c.startMu.Unlock(); _ = c.stopServerLocked() })
	catalog, err := c.Catalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, model := range catalog.Models {
		if model.ID == "gpt-6-sol" {
			found = true
		}
	}
	if !found {
		t.Fatal("authenticated CLI does not advertise gpt-6-sol")
	}
	s := session.NewRegistry().Create("sol6-isolated-qa", "Sol 6 isolated QA", workspace, backend.Codex, "gpt-6-sol", "read-only", "")
	s.SetEffort("low")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := c.Send(ctx, s, "sol6-smoke", "Reply with exactly SOL6_BRIDGE_OK. Do not use any tools.", nil, nil); err != nil {
		t.Fatal(err)
	}
	for sink.count(func(event any) bool { _, ok := event.(backend.Done); return ok }) == 0 {
		if sink.count(func(event any) bool { _, ok := event.(backend.Error); return ok }) > 0 {
			t.Fatal("Codex emitted an error during the isolated Sol 6 turn")
		}
		select {
		case <-ctx.Done():
			t.Fatal("isolated Sol 6 turn timed out")
		case <-time.After(100 * time.Millisecond):
		}
	}
	var text strings.Builder
	sink.mu.Lock()
	for _, event := range sink.events {
		if chunk, ok := event.(backend.TextChunk); ok {
			text.WriteString(chunk.Content)
		}
	}
	sink.mu.Unlock()
	if strings.TrimSpace(text.String()) != "SOL6_BRIDGE_OK" {
		t.Fatalf("unexpected model response: %q", text.String())
	}
	if s.Snapshot().Model != "gpt-6-sol" || s.ResumeID() == "" {
		t.Fatalf("wrong model or missing thread: model=%s", s.Snapshot().Model)
	}
	t.Log("authenticated catalog, explicit gpt-6-sol selection, streamed response and done verified")
}
