package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"testing"
)

func TestLateCompactTerminalCannotFinishNewPendingTurn(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "test", t.TempDir(), "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "thread-1"
	st.turnActive = true
	st.turnDone = make(chan struct{})
	c.threadToSession[st.threadID] = s
	c.dispatch(json.RawMessage(`{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"old-compact","status":"completed"}}}`))
	select {
	case <-st.turnDone:
		t.Fatal("late compact ended a newly submitted turn")
	default:
	}
}

func TestMaintenanceRestartCorrelatesTerminal(t *testing.T) {
	dir := t.TempDir()
	logs := t.TempDir()
	path := filepath.Join(logs, "rollout-thread-1.jsonl")
	if err := os.WriteFile(path, []byte(`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"old"}}
{"type":"event_msg","payload":{"type":"task_complete","turn_id":"compact-1"}}
`), 0600); err != nil {
		t.Fatal(err)
	}
	c := NewCodex(&capSink{}, "codex")
	c.SetDataDir(dir)
	r := backend.Maintenance{SessionID: "s1", RequestID: "r1", OperationID: "op1", ThreadID: "thread-1", TurnID: "compact-1", State: "running"}
	c.maintenanceMu.Lock()
	c.maintenance["s1"] = r
	err := c.saveMaintenanceLocked()
	c.maintenanceMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	restored := NewCodex(&capSink{}, "codex")
	restored.SetDataDir(dir)
	if len(restored.MaintenanceRecords()) != 1 {
		t.Fatal("lost maintenance across restart")
	}
	restoredRecord := restored.MaintenanceRecords()[0]
	state, err := compactTerminal(context.Background(), path, &restoredRecord)
	if err != nil || state != "completed" {
		t.Fatal(state, err)
	}
	r.TurnID = "missing"
	state, err = compactTerminal(context.Background(), path, &r)
	if err != nil || state != "" {
		t.Fatal("unrelated completion accepted", state, err)
	}
	restored.updateMaintenance("s1", "completed", "", "", "stale-operation")
	if restored.MaintenanceRecords()[0].State != "running" {
		t.Fatal("stale generation released newer hold")
	}
}
func TestMaintenanceFailedTerminalIsNotSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	os.WriteFile(path, []byte(`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"compact-1","error":{"message":"failed"}}}`), 0600)
	state, err := compactTerminal(context.Background(), path, &backend.Maintenance{TurnID: "compact-1"})
	if err != nil || state != "failed" {
		t.Fatal(state, err)
	}
}
