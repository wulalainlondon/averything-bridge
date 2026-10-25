package core

import (
	"encoding/json"
	"everything-go/internal/executor/goexec"
	"everything-go/internal/history"
	"everything-go/internal/session"
	"os"
	"testing"
)

func TestInteractionProbeInspectOriginalFinal(t *testing.T) {
	path := os.Getenv("BRIDGE_INTERACTION_INSPECT_INPUT")
	if path == "" {
		t.Skip("exact original isolated probe identity required")
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var in struct{ SessionID, RequestID, Workspace, ThreadID, Report string }
	if json.Unmarshal(raw, &in) != nil || in.ThreadID == "" || in.RequestID == "" {
		t.Fatal("exact original identity required")
	}
	c := goexec.NewCodex(nil, "codex")
	c.SetDataDir(in.Workspace)
	reg := session.NewRegistry()
	s := reg.Create(in.SessionID, "Original isolated probe read", in.Workspace, "codex", "", "read-only", in.ThreadID)
	final, found, e := c.FinalAnswerForRequest(s, in.RequestID)
	observed, _ := c.LoadHistory(in.ThreadID, history.Opts{Limit: 20, Scoped: true})
	out, _ := json.MarshalIndent(map[string]any{"session_id": in.SessionID, "request_id": in.RequestID, "thread_id": in.ThreadID, "found": found, "native_final": final, "error": func() string {
		if e != nil {
			return e.Error()
		}
		return ""
	}(), "read_only": true, "provider_history": observed}, "", "  ")
	if e = os.WriteFile(in.Report, out, 0600); e != nil {
		t.Fatal(e)
	}
}
