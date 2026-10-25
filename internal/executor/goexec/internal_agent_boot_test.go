package goexec

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"path/filepath"
	"testing"
)

func TestVisibilityBootRegistrationDoesNotLaunchOrBorrowScope(t *testing.T) {
	c := NewCodex(&capSink{}, "forbidden-model-binary")
	c.sessionsRoot = filepath.Join(t.TempDir(), "sessions")
	c.appServerMode = "daemon"
	c.appServerSocket = filepath.Join(t.TempDir(), "missing.sock")
	s := session.NewRegistry().Create("original", "S", t.TempDir(), backend.Codex, "", "", "original-root")
	c.RegisterExistingDaemonObservation()
	first := codexVisibilityRPC{c: c, s: s, authority: "original-authority"}.Scope()
	c.RegisterExistingDaemonObservation()
	second := codexVisibilityRPC{c: c, s: s, authority: "original-authority"}.Scope()
	if first.Epoch == second.Epoch || first.Epoch == "" || second.Epoch == "" {
		t.Fatal("boot epoch was reused")
	}
	out := c.ObserveInternalAgents(context.Background(), s, "original-authority")
	if out.Availability != "unsupported" || out.ObservedAt != "" || c.proc != nil || c.remoteConn != nil || c.state(s.ID).turnActive {
		t.Fatal("registration or unavailable read launched provider or claimed freshness")
	}
}
