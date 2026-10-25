package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

type fixedTaskProfile struct {
	root, model, effort string
}

func (p *fixedTaskProfile) TaskWorkerScope(s *session.Session) (*taskapi.ChildScope, error) {
	active := s.Snapshot()
	if active.Model != p.model || active.Effort != p.effort || active.Sandbox != "read-only" {
		return nil, errors.New("delegation_child_profile_changed")
	}
	return &taskapi.ChildScope{Roots: []string{p.root}, Operations: []string{"read"}, Sandbox: "read-only", Network: "deny", MaxChildren: 0}, nil
}
func (p *fixedTaskProfile) TaskTools(s *session.Session) ([]map[string]any, error) {
	if _, err := p.TaskWorkerScope(s); err != nil {
		return nil, err
	}
	return []map[string]any{{"name": "task_capabilities", "inputSchema": map[string]any{"type": "object"}}, {"name": "task_read_input", "inputSchema": map[string]any{"type": "object"}}}, nil
}
func (*fixedTaskProfile) ExecuteTask(context.Context, backend.TaskCaller, []byte) taskapi.Response {
	return taskapi.Response{}
}

func TestFunctionalColdResumeKeepsOriginalTaskProfile(t *testing.T) {
	root := t.TempDir()
	p := &fixedTaskProfile{root: root, model: "gpt-6.1-sol", effort: "high"}
	c := NewCodex(&capSink{}, "unused")
	c.taskProvider = p
	c.appServerMode = "daemon"
	c.runtimeDiagnostics = map[string]any{"codex": map[string]any{"running_version": "0.162.1"}}
	s := session.NewRegistry().Create("owned-cold", "fixture", root, backend.Codex, p.model, "read-only", "original-native-thread")
	s.SetEffort(p.effort)
	st := c.state(s.ID)
	writer := &rpcCaptureWriter{writes: make(chan []byte, 4)}
	c.rpc.setWriter(writer)
	done := make(chan error, 1)
	go func() { done <- c.ensureThread(s, st) }()
	for _, method := range []string{"config/read", "thread/resume"} {
		var frame struct {
			ID     int
			Method string
			Params map[string]any
		}
		select {
		case raw := <-writer.writes:
			if err := json.Unmarshal(raw, &frame); err != nil || frame.Method != method {
				t.Fatalf("unexpected RPC %s: %s %v", method, raw, err)
			}
		case <-time.After(time.Second):
			t.Fatal("missing retained resume RPC", method)
		}
		var result any = map[string]any{"config": map[string]any{"mcp_servers": map[string]any{}}}
		if method == "thread/resume" {
			if _, unsupported := frame.Params["dynamicTools"]; unsupported {
				t.Fatal("0.162.1 resume does not support a dynamicTools override")
			}
			// The protocol returns stored thread defaults. The already accepted
			// task turn has its own immutable model/effort, applied by turn/start.
			result = map[string]any{"thread": map[string]any{"id": s.ResumeID()}, "model": "stored-model", "reasoningEffort": "medium"}
		}
		reply, _ := json.Marshal(map[string]any{"id": frame.ID, "result": result})
		c.rpc.dispatchResponse(reply)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("resume did not finish")
	}
	if tools, err := p.TaskTools(s); err != nil || len(tools) != 2 {
		t.Fatalf("resume metadata disabled the original task toolset: snapshot=%+v error=%v", s.Snapshot(), err)
	}
	params := c.codexTurnParamsForSession(s.ResumeID(), nil, s.Snapshot(), "")
	if params["model"] != p.model || params["effort"] != p.effort || s.ResumeID() != "original-native-thread" {
		t.Fatal("turn no longer carries its accepted profile", params)
	}
}
