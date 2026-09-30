package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type recoveringMaintenanceExec struct {
	*fakeExec
	record       backend.Maintenance
	confirmCalls atomic.Int32
}

func (e *recoveringMaintenanceExec) MaintenanceRecords() []backend.Maintenance {
	return []backend.Maintenance{e.record}
}
func (e *recoveringMaintenanceExec) ReconcileMaintenance(_ context.Context, _ string, release bool) (backend.Maintenance, error) {
	if release {
		e.confirmCalls.Add(1)
	}
	return e.record, nil
}

func TestStaleConfirmationDoesNotReleaseAnotherOperation(t *testing.T) {
	h, base := newTestHub(t)
	p := &recoveringMaintenanceExec{fakeExec: base, record: backend.Maintenance{SessionID: "s1", OperationID: "new", State: "failed"}}
	h.exec = p
	h.reconcileMaintenance("s1", true, "old")
	h.reconcileMaintenance("s1", true)
	if p.confirmCalls.Load() != 0 {
		t.Fatal("stale or missing operation ID was accepted")
	}
	h.reconcileMaintenance("s1", true, "new")
	if p.confirmCalls.Load() != 1 {
		t.Fatal("matching confirmation missing")
	}
}
func TestRestartRestoresHoldBeforeRebuildingQueuedCallbacks(t *testing.T) {
	dir := t.TempDir()
	q, err := messagequeue.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if _, _, err = q.Enqueue(messagequeue.Entry{SessionID: "s1", RequestID: id, Content: id, FileNames: []string{}, Payload: []byte(`{"content":"` + id + `"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err = q.Transition("s1", "a", []messagequeue.State{messagequeue.Queued}, messagequeue.Running, "", "", ""); err != nil {
		t.Fatal(err)
	}
	q.Close()
	reg := session.NewRegistry()
	s := reg.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "")
	h := NewHub(reg, Config{DataDir: dir}, governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	defer h.messageQueue.Close()
	started := make(chan string, 1)
	record := backend.Maintenance{SessionID: s.ID, RequestID: "a", State: "running", OperationID: "op1"}
	fe := &recoveringMaintenanceExec{fakeExec: &fakeExec{sink: h}, record: record}
	fe.onSend = func(s *session.Session, id, _ string) { started <- id; h.Emit(protocol.NewDone(s.ID, id)) }
	h.SetExecutor(fe)
	select {
	case <-started:
		t.Fatal("restart ran queue before maintenance reconciliation")
	case <-time.After(30 * time.Millisecond):
	}
	expectState(t, h, "a", messagequeue.Uncertain)
	expectState(t, h, "b", messagequeue.Queued)
	record.State = "completed"
	h.Emit(record)
	select {
	case id := <-started:
		if id != "b" {
			t.Fatal(id)
		}
	case <-time.After(time.Second):
		t.Fatal("late completion failed to release recovered queue")
	}
	expectState(t, h, "a", messagequeue.Completed)
}

func TestMaintenanceHoldRetainsQueueUntilConfirmed(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	s := h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "")
	started := make(chan string, 2)
	fe.onSend = func(s *session.Session, id, _ string) { started <- id; h.Emit(protocol.NewDone(s.ID, id)) }
	hold := backend.Maintenance{SessionID: s.ID, State: "unknown", OperationID: "op1"}
	h.Emit(hold)
	enqueueTestMessage(t, h, c, "b")
	select {
	case <-started:
		t.Fatal("unconfirmed maintenance let queued work run")
	case <-time.After(30 * time.Millisecond):
	}
	hold.State = "failed"
	h.Emit(hold)
	select {
	case <-started:
		t.Fatal("failed maintenance auto released queue")
	case <-time.After(30 * time.Millisecond):
	}
	hold.State = "released"
	h.Emit(hold)
	select {
	case id := <-started:
		if id != "b" {
			t.Fatal(id)
		}
	case <-time.After(time.Second):
		t.Fatal("confirmed queue did not resume")
	}
}
