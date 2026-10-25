package goexec

import (
	"testing"

	"everything-go/internal/backend"
)

func TestCollaborationWorkerPreflightAndExecutionHaveDifferentHardSandboxes(t *testing.T) {
	for _, sandbox := range []string{"read-only", "workspace-write"} {
		t.Run(sandbox, func(t *testing.T) {
			c := NewCodex(&capSink{}, "codex")
			c.SetPMProvider(codexPMFixtureProvider{p: &backend.PMConfiguration{Worker: true, Sandbox: sandbox, RuntimeDir: t.TempDir(), RequestID: "request"}})
			params := map[string]any{"collaborationMode": "unrestricted", "approvalPolicy": "on-request"}
			if err := c.applyPMTurnPolicy("pm_test", params); err != nil {
				t.Fatal(err)
			}
			policy, ok := params["sandboxPolicy"].(map[string]any)
			if !ok {
				t.Fatal("worker sandbox policy missing", params)
			}
			want := "readOnly"
			if sandbox == "workspace-write" {
				want = "workspaceWrite"
			}
			if policy["type"] != want || params["approvalPolicy"] != "never" || params["collaborationMode"] != nil {
				t.Fatal("worker policy drift", params)
			}
		})
	}
}

func TestCollaborationWorkerRejectsUnknownSandbox(t *testing.T) {
	for _, sandbox := range []string{"", "danger-full-access", "unknown"} {
		c := NewCodex(&capSink{}, "codex")
		c.SetPMProvider(codexPMFixtureProvider{p: &backend.PMConfiguration{Worker: true, Sandbox: sandbox}})
		if err := c.applyPMTurnPolicy("pm_test", map[string]any{}); err == nil {
			t.Fatalf("unsafe sandbox %q accepted", sandbox)
		}
	}
}
