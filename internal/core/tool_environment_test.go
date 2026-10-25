package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/clientproto"
	"everything-go/internal/session"
	"everything-go/internal/toolenv"
	"sync/atomic"
	"testing"
	"time"
)

type toolEnvExec struct {
	*fakeExec
	repairs atomic.Int32
}

func (e *toolEnvExec) InspectToolEnvironment(_ context.Context, s *session.Session, _ bool) (toolenv.Snapshot, error) {
	return toolenv.Snapshot{ThreadID: s.ResumeID(), Generation: "g1", CheckedAt: time.Now().UnixMilli(), Revision: 1, State: "listed_unverified", Evidence: "inventory", Services: []toolenv.Service{}, ReloadAllowed: true, ForkAllowed: true}, nil
}
func (e *toolEnvExec) RepairToolEnvironment(ctx context.Context, s *session.Session, _, _ string) (toolenv.Result, error) {
	e.repairs.Add(1)
	v, _ := e.InspectToolEnvironment(ctx, s, true)
	return toolenv.Result{Snapshot: v}, nil
}
func TestToolRepairIndependentFromChatAndDurablyDeduplicated(t *testing.T) {
	h, f := newControlTestHub(t, t.TempDir())
	e := &toolEnvExec{fakeExec: f}
	h.SetExecutor(e)
	c := newTestClient(h)
	c.deviceID = "phone"
	s := h.registry.Create("s1", "test", "/work", "codex", "", "", "t1")
	route(h, c, `{"type":"prepare_tool_environment_repair","session_id":"s1","tool_environment":{"action":"reload"}}`)
	event := waitForType(t, c, "tool_environment_snapshot")
	p := event["plan"].(map[string]any)
	request := map[string]any{"type": "apply_tool_environment_repair", "session_id": "s1", "tool_environment": map[string]any{"action": "reload", "repair_token": p["repair_token"], "operation_id": "op1", "maintenance_confirmed": true}}
	b, _ := json.Marshal(request)
	route(h, c, string(b))
	for {
		ev := waitForType(t, c, "tool_environment_operation")
		if ev["operation"].(map[string]any)["phase"] == "completed_unverified" {
			break
		}
	}
	if s.IsStreaming() || s.ResumeID() != "t1" {
		t.Fatal("changed chat runtime")
	}
	route(h, c, string(b))
	waitForType(t, c, "tool_environment_operation")
	if e.repairs.Load() != 1 {
		t.Fatal("duplicate reload")
	}
}
func TestToolEnvironmentRequiresPairedDevice(t *testing.T) {
	h, f := newControlTestHub(t, t.TempDir())
	h.SetExecutor(&toolEnvExec{fakeExec: f})
	c := newTestClient(h)
	h.registry.Create("s1", "test", "/work", "codex", "", "", "t1")
	route(h, c, `{"type":"request_tool_environment","session_id":"s1"}`)
	e := waitForType(t, c, "tool_environment_snapshot")
	if e["error_code"] != "pairing_required" {
		t.Fatal(e)
	}
}
func TestToolMaintenanceDoesNotBlockStopOrOtherBackends(t *testing.T) {
	h, _ := newControlTestHub(t, t.TempDir())
	c := newTestClient(h)
	h.toolRepairRunning.Store(true)
	h.registry.Create("codex", "test", "/work", "codex", "", "", "t1")
	h.registry.Create("claude", "test", "/work", "claude", "", "", "t2")
	for _, v := range []struct {
		session, kind string
		blocked       bool
	}{{"codex", "message", true}, {"codex", "stop", false}, {"claude", "message", false}, {"codex", "request_tool_environment", false}} {
		if got := h.rejectToolMaintenanceWrite(c, clientproto.Command{Kind: v.kind, SessionID: v.session}); got != v.blocked {
			t.Fatal(v, got)
		}
	}
}

func TestToolMaintenanceRejectionCannotTerminateUnrelatedChat(t *testing.T) {
	h, _ := newControlTestHub(t, t.TempDir())
	c := newTestClient(h)
	h.toolRepairRunning.Store(true)
	h.registry.Create("s1", "test", "/work", "codex", "", "", "t1")
	h.rejectToolMaintenanceWrite(c, clientproto.Command{Kind: "message", SessionID: "s1", RequestID: "new-request"})
	e := waitForType(t, c, "error")
	if e["request_id"] != "new-request" {
		t.Fatal(e)
	}
	h.rejectToolMaintenanceWrite(c, clientproto.Command{Kind: "switch_session_config", SessionID: "s1", MutationID: "settings-1"})
	e = waitForType(t, c, "session_config_result")
	if e["accepted"] != false {
		t.Fatal(e)
	}
	h.rejectToolMaintenanceWrite(c, clientproto.Command{Kind: "codex_goal_set", SessionID: "s1"})
	waitForType(t, c, "session_warning")
}
