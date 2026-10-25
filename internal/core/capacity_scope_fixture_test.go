package core

import (
	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"path/filepath"
	"testing"
)

type capacityScopeCapabilityFixture struct{ *fakeExec }

func (*capacityScopeCapabilityFixture) TaskAPICapabilities() []taskapi.ProviderCapability {
	result := []taskapi.ProviderCapability{}
	for _, p := range []string{"codex", "claude"} {
		c := taskapi.UnloadedCapability(p, "fixture-only", "Synthetic registration; no provider call")
		c.Lifecycle = "registered"
		model := "gpt-6.1-sol"
		if p == "claude" {
			model = "sonnet"
		}
		c.Models = []any{map[string]any{"model": model, "efforts": []string{"high"}}}
		result = append(result, c)
	}
	return result
}

func capacityControllerFixture(t *testing.T) (*Hub, *fakeExec, backend.SessionControlCaller, *session.Session) {
	root := t.TempDir()
	h := NewHub(session.NewRegistry(), Config{InstanceID: "i1", RootDir: root, DataDir: root}, governance.NewPairing(filepath.Join(root, "pair.json")), 0)
	t.Cleanup(func() {
		for _, s := range h.registry.List() {
			s.CloseAndWait()
		}
		h.messageQueue.Close()
		h.delegations.Close()
		h.dispatches.Close()
	})
	fe := &fakeExec{sink: h}
	h.SetExecutor(fe)
	parent := h.registry.Create("controller", "Synthetic parent", root, backend.Codex, "", "workspace-write", "parent-thread")
	target := h.registry.Create("target", "Synthetic target", root, backend.Codex, "", "read-only", "target-thread")
	c := newTestClient(h)
	h.voiceClients = map[*Client]*clientVoiceCall{c: {session: parent, voiceID: "synthetic-voice"}}
	caller := backend.SessionControlCaller{Parent: parent, RequestID: "synthetic-request", TurnID: "synthetic-turn", ToolCallID: "synthetic-call", VoiceID: "synthetic-voice"}
	return h, fe, caller, target
}
