package core

import (
	"errors"
	"testing"
	"time"

	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func TestStopAcknowledgementDoesNotReleaseNextQueuedTurn(t *testing.T) {
	h, backend := newTestHub(t)
	client := newTestClient(h)
	started := make(chan string, 4)
	stopped := make(chan struct{}, 1)
	backend.onSend = func(s *session.Session, id, _ string) {
		started <- id
		h.Emit(protocol.NewTextChunk(s.ID, id, "active"))
	}
	backend.onStop = func(*session.Session) error { stopped <- struct{}{}; return nil }
	route(h, client, `{"type":"new_session","session_id":"s1","backend":"codex"}`)
	waitForType(t, client, "session_created")
	route(h, client, `{"type":"message","session_id":"s1","request_id":"r1","content":"long work"}`)
	if <-started != "r1" {
		t.Fatal("first run")
	}
	waitForType(t, client, "text_chunk")
	route(h, client, `{"type":"message","session_id":"s1","request_id":"r2","content":"next work"}`)
	route(h, client, `{"type":"stop","session_id":"s1"}`)
	<-stopped
	current, _ := h.registry.Get("s1")
	select {
	case id := <-started:
		t.Fatalf("ACK started %s before confirmed stop", id)
	case <-time.After(40 * time.Millisecond):
	}
	if current.State() != session.Stopping || current.ActiveQueuedID() != "r1" {
		t.Fatal("stop ACK released ownership")
	}
	h.Emit(protocol.NewStopped("s1", "r1"))
	select {
	case id := <-started:
		if id != "r2" {
			t.Fatal(id)
		}
	case <-time.After(time.Second):
		t.Fatal("confirmed stop did not release next turn")
	}
	h.Emit(protocol.NewDone("s1", "r2"))
}
func TestFailedStopKeepsOwnershipAndAllowsRetry(t *testing.T) {
	h, backend := newTestHub(t)
	client := newTestClient(h)
	backend.onSend = func(s *session.Session, id, _ string) { h.Emit(protocol.NewTextChunk(s.ID, id, "active")) }
	backend.onStop = func(*session.Session) error { return errors.New("interrupt transport unavailable") }
	route(h, client, `{"type":"new_session","session_id":"s1","backend":"codex"}`)
	waitForType(t, client, "session_created")
	route(h, client, `{"type":"message","session_id":"s1","request_id":"r1","content":"long work"}`)
	waitForType(t, client, "text_chunk")
	route(h, client, `{"type":"stop","session_id":"s1"}`)
	waitForType(t, client, "session_warning")
	current, _ := h.registry.Get("s1")
	if current.State() != session.Streaming || current.ActiveQueuedID() != "r1" {
		t.Fatal("failed stop released original run")
	}
	views := h.runtimes.Snapshot("", []string{"s1"})
	if len(views) != 1 || views[0].Phase != "running" || views[0].ActiveRequestID != "r1" {
		t.Fatal("failed stop left false stopping/terminal state", views)
	}
	backend.onStop = nil
	route(h, client, `{"type":"stop","session_id":"s1"}`)
	waitForType(t, client, "stopped")
	waitState(t, current, session.Idle)
}
