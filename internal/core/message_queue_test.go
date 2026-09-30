package core

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/governance"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/runtimejournal"
	"everything-go/internal/session"
)

func TestConfirmedTerminalReconcilesUncertainQueueItem(t *testing.T) {
	h, _ := newTestHub(t)
	if _, _, err := h.messageQueue.Enqueue(messagequeue.Entry{
		SessionID: "s1", RequestID: "a", Content: "a", Payload: []byte(`{"content":"a"}`),
	}); err != nil {
		t.Fatal(err)
	}
	for _, transition := range []struct {
		from messagequeue.State
		to   messagequeue.State
	}{
		{messagequeue.Queued, messagequeue.Running},
		{messagequeue.Running, messagequeue.Uncertain},
	} {
		if _, changed, err := h.messageQueue.Transition("s1", "a", []messagequeue.State{transition.from}, transition.to, "", "", ""); err != nil || !changed {
			t.Fatalf("transition to %s: changed=%v err=%v", transition.to, changed, err)
		}
	}
	h.finishQueuedMessage(runtimejournal.View{SessionID: "s1", ActiveRequestID: "other", LastTerminal: "completed"})
	expectState(t, h, "a", messagequeue.Uncertain)
	h.finishQueuedMessage(runtimejournal.View{SessionID: "s1", ActiveRequestID: "a", LastTerminal: "completed"})
	expectState(t, h, "a", messagequeue.Completed)
}

func enqueueTestMessage(t *testing.T, h *Hub, c *Client, id string) {
	t.Helper()
	route(h, c, `{"type":"message","session_id":"s1","request_id":"`+id+`","content":"`+id+`"}`)
	waitForType(t, c, "message_ack")
}
func expectState(t *testing.T, h *Hub, id string, want messagequeue.State) {
	t.Helper()
	e, found, err := h.messageQueue.Get("s1", id)
	if err != nil || !found || e.State != want {
		t.Fatalf("request %s: want %s got %+v, err=%v", id, want, e, err)
	}
}
func TestQueuedPromotionIsAtomicAndDeduplicatedAcrossLateAck(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	s := h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "")
	starts := make(chan string, 4)
	release := make(chan struct{})
	steerRelease := make(chan struct{})
	steerCalled := make(chan struct{})
	var calls atomic.Int32
	fe.onSend = func(s *session.Session, id, content string) {
		starts <- id
		if id == "a" {
			<-release
		}
		h.Emit(protocol.NewDone(s.ID, id))
	}
	fe.onSteer = func(_ *session.Session, id, content string) (backend.SteerResult, error) {
		calls.Add(1)
		if id != "b" || content != "b" {
			t.Errorf("promotion must use the stored message: %s %s", id, content)
		}
		close(steerCalled)
		<-steerRelease
		return backend.SteerResult{TurnID: "t1", RequestID: "a"}, nil
	}
	enqueueTestMessage(t, h, c, "a")
	if <-starts != "a" {
		t.Fatal("first turn")
	}
	enqueueTestMessage(t, h, c, "b")
	enqueueTestMessage(t, h, c, "c")
	enqueueTestMessage(t, h, c, "d")
	route(h, c, `{"type":"promote_queued_message","session_id":"s1","request_id":"b","content":"must not replace original"}`)
	<-steerCalled
	route(h, c, `{"type":"promote_queued_message","session_id":"s1","request_id":"b"}`)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "pending" {
		t.Fatal(result)
	}
	route(h, c, `{"type":"cancel_queued_message","session_id":"s1","request_id":"d"}`)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "accepted" {
		t.Fatal(result)
	}
	expectState(t, h, "d", messagequeue.Cancelled)
	close(release) // Current turn finishes before steering acknowledgement.
	select {
	case next := <-starts:
		t.Fatalf("queue ran during reservation: %s", next)
	case <-time.After(30 * time.Millisecond):
	}
	close(steerRelease)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "accepted" {
		t.Fatal(result)
	}
	select {
	case next := <-starts:
		if next != "c" {
			t.Fatalf("steered message executed twice: %s", next)
		}
	case <-time.After(time.Second):
		t.Fatal("remaining queue did not resume")
	}
	expectState(t, h, "b", messagequeue.Steered)
	route(h, c, `{"type":"promote_queued_message","session_id":"s1","request_id":"b"}`)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "accepted" {
		t.Fatal(result)
	}
	if calls.Load() != 1 {
		t.Fatalf("duplicate steering calls: %d", calls.Load())
	}
	if s.QueueLen() != 0 {
		t.Fatalf("unexpected queue length: %d", s.QueueLen())
	}
}
func TestRejectedPromotionRestoresOriginalFIFO(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "")
	starts := make(chan string, 4)
	release := make(chan struct{})
	fe.onSend = func(s *session.Session, id, content string) {
		starts <- id
		if id == "a" {
			<-release
		}
		h.Emit(protocol.NewDone(s.ID, id))
	}
	fe.onSteer = func(_ *session.Session, _, _ string) (backend.SteerResult, error) {
		return backend.SteerResult{}, backend.ErrSteerRejected
	}
	enqueueTestMessage(t, h, c, "a")
	<-starts
	enqueueTestMessage(t, h, c, "b")
	enqueueTestMessage(t, h, c, "c")
	route(h, c, `{"type":"promote_queued_message","session_id":"s1","request_id":"c"}`)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "retained" {
		t.Fatal(result)
	}
	expectState(t, h, "c", messagequeue.Queued)
	close(release)
	for _, want := range []string{"b", "c"} {
		select {
		case got := <-starts:
			if got != want {
				t.Fatalf("got %s want %s", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("queue stalled")
		}
	}
}
func TestUncertainPromotionDoesNotAutomaticallyResend(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "")
	starts := make(chan string, 4)
	release := make(chan struct{})
	var calls atomic.Int32
	fe.onSend = func(s *session.Session, id, content string) {
		starts <- id
		if id == "a" {
			<-release
		}
		h.Emit(protocol.NewDone(s.ID, id))
	}
	fe.onSteer = func(_ *session.Session, _, _ string) (backend.SteerResult, error) {
		calls.Add(1)
		return backend.SteerResult{}, errors.New("ack lost")
	}
	enqueueTestMessage(t, h, c, "a")
	<-starts
	enqueueTestMessage(t, h, c, "b")
	enqueueTestMessage(t, h, c, "c")
	route(h, c, `{"type":"promote_queued_message","session_id":"s1","request_id":"b"}`)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "uncertain" {
		t.Fatal(result)
	}
	route(h, c, `{"type":"promote_queued_message","session_id":"s1","request_id":"b"}`)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "uncertain" {
		t.Fatal(result)
	}
	close(release)
	select {
	case got := <-starts:
		if got != "c" {
			t.Fatalf("unknown result resent: %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("queue stalled")
	}
	if calls.Load() != 1 {
		t.Fatal("uncertain operation retried")
	}
	expectState(t, h, "b", messagequeue.Uncertain)
	route(h, c, `{"type":"cancel_queued_message","session_id":"s1","request_id":"b"}`)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "accepted" {
		t.Fatal(result)
	}
	expectState(t, h, "b", messagequeue.Cancelled)
}
func TestCancelQueuedMessageRemovesOnlyWaitingItem(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	s := h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "")
	starts := make(chan string, 3)
	release := make(chan struct{})
	fe.onSend = func(s *session.Session, id, content string) {
		starts <- id
		if id == "a" {
			<-release
		}
		h.Emit(protocol.NewDone(s.ID, id))
	}
	enqueueTestMessage(t, h, c, "a")
	<-starts
	enqueueTestMessage(t, h, c, "b")
	enqueueTestMessage(t, h, c, "c")
	route(h, c, `{"type":"cancel_queued_message","session_id":"s1","request_id":"a"}`)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "rejected" {
		t.Fatal(result)
	}
	route(h, c, `{"type":"cancel_queued_message","session_id":"s1","request_id":"b"}`)
	if result := waitForType(t, c, "queue_action_result"); result["status"] != "accepted" {
		t.Fatal(result)
	}
	if s.QueueLen() != 1 {
		t.Fatalf("cancel did not update queue length: %d", s.QueueLen())
	}
	close(release)
	select {
	case got := <-starts:
		if got != "c" {
			t.Fatalf("cancelled item ran: %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("queue stalled")
	}
}

func TestQueuedMessageRecoversWithItsDurableSession(t *testing.T) {
	dir := t.TempDir()
	registry := session.NewRegistry()
	registry.AttachStore(session.NewStore(filepath.Join(dir, "sessions.json")))
	old := NewHub(registry, Config{InstanceID: "i1", DataDir: dir}, governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	old.SetExecutor(&fakeExec{sink: old})
	s := registry.Create("s1", "Recovery QA", dir, backend.Codex, "", "", "")
	gate := make(chan struct{})
	active := make(chan struct{})
	s.Submit(func() { close(active); <-gate })
	<-active
	client := newTestClient(old)
	enqueueTestMessage(t, old, client, "waiting")
	s.Close()
	close(gate)
	old.messageQueue.Close()
	restoredRegistry := session.NewRegistry()
	restoredRegistry.AttachStore(session.NewStore(filepath.Join(dir, "sessions.json")))
	if _, found := restoredRegistry.Get("s1"); !found {
		t.Fatal("ACK did not persist Session identity")
	}
	restored := NewHub(restoredRegistry, Config{InstanceID: "i1", DataDir: dir}, governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	defer restored.messageQueue.Close()
	receiver := newTestClient(restored)
	var sends atomic.Int32
	executor := &fakeExec{sink: restored, onSend: func(s *session.Session, id, content string) { sends.Add(1); restored.Emit(protocol.NewDone(s.ID, id)) }}
	restored.SetExecutor(executor)
	waitForType(t, receiver, "done")
	expectState(t, restored, "waiting", messagequeue.Completed)
	if sends.Load() != 1 {
		t.Fatalf("restored message executed %d times", sends.Load())
	}
	enqueueTestMessage(t, restored, receiver, "waiting")
	if sends.Load() != 1 {
		t.Fatal("lost-ACK retry duplicated a restored message")
	}
}
