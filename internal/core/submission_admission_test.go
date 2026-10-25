package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type admissionExec struct {
	*fakeExec
	busy   atomic.Bool
	race   atomic.Bool
	checks chan struct{}
	sends  atomic.Int32
}

func (e *admissionExec) QueuedSubmissionReady(context.Context, *session.Session, string) error {
	select {
	case e.checks <- struct{}{}:
	default:
	}
	if e.busy.Load() {
		return backend.ErrNativeSubmissionBusy
	}
	return nil
}
func (e *admissionExec) Send(ctx context.Context, s *session.Session, id, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	if e.race.CompareAndSwap(true, false) {
		e.busy.Store(true)
		return backend.ErrNativeSubmissionBusy
	}
	e.sends.Add(1)
	return e.fakeExec.Send(ctx, s, id, content, images, files)
}

func TestQueuedNativeBusyAcrossRestartWaitsAndRemainsCancellable(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "original")
	defer s.CloseAndWait()
	e := &admissionExec{fakeExec: fe, checks: make(chan struct{}, 10)}
	e.busy.Store(true)
	h.SetExecutor(e)
	// Preserve an already accepted but uncertain pre-restart request exactly.
	_, _, _ = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: "old", Payload: []byte(`{"content":"old"}`)})
	_, _, _ = h.messageQueue.Transition(s.ID, "old", []messagequeue.State{messagequeue.Queued}, messagequeue.Uncertain, "restarted", "", "")
	c := sharedReadClient(t, h, "owner")
	enqueueTestMessage(t, h, c, "waiting")
	<-e.checks
	expectState(t, h, "waiting", messagequeue.Queued)
	expectState(t, h, "old", messagequeue.Uncertain)
	if e.sends.Load() != 0 || s.ActiveQueuedID() != "" {
		t.Fatal("busy native started a new turn")
	}
	route(h, c, `{"type":"cancel_queued_message","session_id":"s1","request_id":"waiting"}`)
	if waitForType(t, c, "queue_action_result")["status"] != "accepted" {
		t.Fatal("waiting input cannot cancel")
	}
	expectState(t, h, "waiting", messagequeue.Cancelled)
	e.busy.Store(false)
	fe.onSend = func(s *session.Session, id, _ string) { h.Emit(protocol.NewDone(s.ID, id)) }
	enqueueTestMessage(t, h, c, "next")
	waitForType(t, c, "done")
	if e.sends.Load() != 1 {
		t.Fatal("old or cancelled request replayed")
	}
	expectState(t, h, "old", messagequeue.Uncertain)
}

func TestKnownBusyRaceReturnsOriginalInputToFIFOWithoutFailure(t *testing.T) {
	h, fe := newTestHub(t)
	s := h.registry.Create("s1", "race", t.TempDir(), backend.Codex, "", "", "original")
	defer s.CloseAndWait()
	e := &admissionExec{fakeExec: fe, checks: make(chan struct{}, 20)}
	e.race.Store(true)
	h.SetExecutor(e)
	c := newTestClient(h)
	// enqueueTestMessage waits for a later message_ack and may consume an
	// earlier done frame. Observe the native submission order independently.
	starts := make(chan string, 3)
	fe.onSend = func(s *session.Session, id, _ string) { starts <- id; h.Emit(protocol.NewDone(s.ID, id)) }
	enqueueTestMessage(t, h, c, "a")
	deadline := time.After(2 * time.Second)
	for {
		r, _, _ := h.messageQueue.Get(s.ID, "a")
		if r.State == messagequeue.Queued && r.Message == "native_submission_busy" {
			break
		}
		select {
		case <-deadline:
			t.Fatal("not deferred", r)
		case <-time.After(time.Millisecond):
		}
	}
	enqueueTestMessage(t, h, c, "b")
	if e.sends.Load() != 0 {
		t.Fatal("busy input was submitted")
	}
	e.busy.Store(false)
	// Wake the original actor gate by appending another ordinary input.
	enqueueTestMessage(t, h, c, "c")
	for _, id := range []string{"a", "b", "c"} {
		select {
		case got := <-starts:
			if got != id {
				t.Fatal("FIFO changed", got, id)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("native input was not submitted", id)
		}
	}
	if e.sends.Load() != 3 {
		t.Fatal("duplicate native submission")
	}
}
