package core

import (
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"testing"
)

func TestObservedBusyTurnKeepsItsRuntimeAndAllowsQueuedSteering(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s1", "observed", t.TempDir(), backend.Codex, "", "", "original")
	defer s.CloseAndWait()
	e := &admissionExec{fakeExec: fe, checks: make(chan struct{}, 10)}
	e.busy.Store(true)
	h.SetExecutor(e)
	c := sharedReadClient(t, h, "owner")
	h.Emit(backend.ObservedTurn{SessionID: s.ID, RequestID: "native-observed", Phase: "running"})
	enqueueTestMessage(t, h, c, "waiting")
	<-e.checks
	view := h.runtimeSnapshot("phone").Items[0]
	if view.ActiveRequestID != "native-observed" || view.Phase != "running" {
		t.Fatalf("queue replaced native active owner: %+v", view)
	}
	fe.onSteer = func(_ *session.Session, id, content string) (backend.SteerResult, error) {
		return backend.SteerResult{StrictACK: true, TurnID: "native-turn", RequestID: "native-observed"}, nil
	}
	route(h, c, `{"type":"promote_queued_message","session_id":"s1","request_id":"waiting"}`)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "accepted" {
		t.Fatal(result)
	}
	expectState(t, h, "waiting", messagequeue.Steered)
	if e.sends.Load() != 0 || s.ActiveQueuedID() != "" {
		t.Fatal("created a new worker send")
	}
}

func TestObservedSteeringRejectionRestoresExactWaitingInput(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s1", "observed", t.TempDir(), backend.Codex, "", "", "original")
	defer s.CloseAndWait()
	e := &admissionExec{fakeExec: fe, checks: make(chan struct{}, 10)}
	e.busy.Store(true)
	h.SetExecutor(e)
	c := sharedReadClient(t, h, "owner")
	h.Emit(backend.ObservedTurn{SessionID: s.ID, RequestID: "native-observed", Phase: "running"})
	enqueueTestMessage(t, h, c, "waiting")
	<-e.checks
	fe.onSteer = func(_ *session.Session, id, content string) (backend.SteerResult, error) {
		return backend.SteerResult{}, backend.ErrNoActiveTurn
	}
	route(h, c, `{"type":"promote_queued_message","session_id":"s1","request_id":"waiting"}`)
	if r := waitForType(t, c, "queue_action_result"); r["status"] != "retained" {
		t.Fatal(r)
	}
	expectState(t, h, "waiting", messagequeue.Queued)
	if e.sends.Load() != 0 || s.ActiveQueuedID() != "" || s.QueueLen() != 1 {
		t.Fatal("rejection replayed or lost input")
	}
}
