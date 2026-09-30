package goexec

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// Opt-in installed-app-server probe. It opens a fresh isolated thread without
// starting a model turn, proving the runtime accepts the delegation catalog.
func TestCodexDelegationToolCatalogIntegration(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_RUN_DELEGATION_INTEGRATION") != "1" {
		t.Skip("set EVERYTHING_GO_RUN_DELEGATION_INTEGRATION=1")
	}
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.SetDataDir(t.TempDir())
	c.SetDelegationProvider(delegationFixtureProvider{calls: make(chan backend.DelegationSpec, 1)})
	if err := c.ensureServer(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		c.startMu.Lock()
		_ = c.stopServerLocked()
		c.startMu.Unlock()
	}()
	s := session.NewRegistry().Create("probe-parent", "Delegation catalog probe", t.TempDir(), backend.Codex, "", "read-only", "")
	if err := c.ensureThread(s, c.state(s.ID)); err != nil {
		t.Fatalf("installed app-server rejected delegation tool catalog: %v", err)
	}
	if s.ResumeID() == "" {
		t.Fatal("app-server returned no thread")
	}
}

type delegationFixtureProvider struct{ calls chan backend.DelegationSpec }

func (p delegationFixtureProvider) DelegateSession(_ *session.Session, _, _ string, spec backend.DelegationSpec) (backend.DelegationReceipt, error) {
	p.calls <- spec
	return backend.DelegationReceipt{ID: "dg_test", ChildSessionID: "child", ChildRequestID: "work"}, nil
}
func (delegationFixtureProvider) ReadDelegationResult(_ *session.Session, id string, offset, limit int) (backend.DelegationResultPage, error) {
	return backend.DelegationResultPage{ID: id, Text: "sealed result", NextOffset: offset, HasMore: false}, nil
}

func TestCodexDelegationToolIsScopedToLiveParentTurn(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	provider := delegationFixtureProvider{calls: make(chan backend.DelegationSpec, 2)}
	c.SetDelegationProvider(provider)
	s := session.NewRegistry().Create("parent", "Parent", t.TempDir(), backend.Codex, "", "workspace-write", "")
	c.threadToSession["native-thread"] = s
	params := map[string]any{"model": "gpt-6-sol"}
	c.applyDelegationThreadTools(s, params)
	if _, ok := params["dynamicTools"]; !ok {
		t.Fatal("ordinary parent has no delegation tool")
	}
	child := session.NewRegistry().Create("s_dg_child", "Child", t.TempDir(), backend.Codex, "", "workspace-write", "")
	childParams := map[string]any{}
	c.applyDelegationThreadTools(child, childParams)
	if _, ok := childParams["dynamicTools"]; ok {
		t.Fatal("delegated child could recursively delegate")
	}
	st := c.state(s.ID)
	st.mu.Lock()
	st.reqID, st.currentTurnID, st.turnActive = "parent-request", "native-turn", true
	st.mu.Unlock()
	writer := &rpcCaptureWriter{writes: make(chan []byte, 12)}
	c.rpc.setWriter(writer)
	call := func(turnID string) map[string]any {
		return map[string]any{"threadId": "native-thread", "namespace": "bridge_sessions", "tool": "delegate_session",
			"callId": "tool-1", "turnId": turnID, "arguments": map[string]any{"name": "Check", "instruction": "Verify one claim"}}
	}
	for _, tc := range []struct {
		turnID string
		wantOK bool
	}{{"wrong-turn", false}, {"native-turn", true}} {
		raw, _ := json.Marshal(call(tc.turnID))
		c.handleServerRequest(1, "item/tool/call", raw)
		deadline := time.After(2 * time.Second)
		for {
			select {
			case response := <-writer.writes:
				if !strings.Contains(string(response), `"contentItems"`) {
					continue
				}
				if gotOK := strings.Contains(string(response), `"success":true`); gotOK != tc.wantOK {
					t.Fatalf("turn %s response=%s", tc.turnID, response)
				}
				goto received
			case <-deadline:
				t.Fatalf("no tool response for %s", tc.turnID)
			}
		}
	received:
	}
	select {
	case got := <-provider.calls:
		if got.Instruction != "Verify one claim" {
			t.Fatalf("wrong instruction: %+v", got)
		}
	default:
		t.Fatal("provider not called for active turn")
	}
	select {
	case <-provider.calls:
		t.Fatal("stale turn reached provider")
	default:
	}
	readCall := map[string]any{"threadId": "native-thread", "namespace": "bridge_sessions", "tool": "read_result",
		"callId": "tool-read", "turnId": "native-turn", "arguments": map[string]any{"delegation_id": "dg_test", "offset": 0, "limit": 100}}
	raw, _ := json.Marshal(readCall)
	c.handleServerRequest(2, "item/tool/call", raw)
	select {
	case response := <-writer.writes:
		if !strings.Contains(string(response), "sealed result") || !strings.Contains(string(response), `"success":true`) {
			t.Fatalf("read_result response=%s", response)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read_result did not respond")
	}
}
