package core

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
)

type asyncInteractionFixture struct {
	*fakeExec
	scans atomic.Int32
}

func (f *asyncInteractionFixture) PrepareAsyncReply(id string, _ map[string]any, _ bool) (string, string, bool, error) {
	if id != "ui_async_fixture" {
		return "", "", false, nil
	}
	return "s1", "<send_user_message_question_reply>\n[{\"questionItemId\":\"fixture\",\"answer\":\"fixture\",\"question\":\"fixture\"}]\n</send_user_message_question_reply>", true, nil
}
func (f *asyncInteractionFixture) ReconcileAsyncQuestions(*session.Session) { f.scans.Add(1) }
func (f *asyncInteractionFixture) PendingInteractions(string) []backend.UserInputPayload {
	return []backend.UserInputPayload{}
}
func (f *asyncInteractionFixture) RespondUserInput(string, map[string]any, bool) bool { return false }

func TestNativeAsyncReplyEntersDurableQueueOnceWithoutEarlyResolution(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	h.registry.Create("s1", "native", t.TempDir(), backend.Codex, "", "read-only", "native-thread")
	started := make(chan string, 2)
	fe.onSend = func(s *session.Session, id, content string) { started <- content }
	f := &asyncInteractionFixture{fakeExec: fe}
	h.SetExecutor(f)
	route(h, c, `{"type":"user_input_response","request_id":"ui_async_fixture","answers":{"fixture":"fixture"}}`)
	ack := waitForType(t, c, "message_ack")
	if ack["request_id"] != "ui_async_fixture" || ack["session_id"] != "s1" {
		t.Fatal(ack)
	}
	select {
	case text := <-started:
		if text == "" {
			t.Fatal("native reply lost")
		}
	case <-time.After(time.Second):
		t.Fatal("not dispatched")
	}
	e, found, err := h.messageQueue.Get("s1", "ui_async_fixture")
	if err != nil || !found || e.State != messagequeue.Running {
		t.Fatalf("no durable ownership: %+v %v", e, err)
	}
	var payload queuedPayload
	_ = json.Unmarshal(e.Payload, &payload)
	if payload.Content == "" {
		t.Fatal("correlated reply not persisted")
	}
	route(h, c, `{"type":"user_input_response","request_id":"ui_async_fixture","answers":{"fixture":"fixture"}}`)
	waitForType(t, c, "message_ack")
	select {
	case <-started:
		t.Fatal("duplicate reply started twice")
	default:
	}
	for len(c.send) > 0 {
		var event map[string]any
		_ = json.Unmarshal(<-c.send, &event)
		if event["type"] == "interaction_resolved" {
			t.Fatal("queue receipt falsely resolved native question")
		}
	}
}

func TestNativeAsyncPendingSnapshotReconcilesOnlyRequestedSession(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	h.registry.Create("s1", "one", t.TempDir(), backend.Codex, "", "", "one")
	h.registry.Create("s2", "two", t.TempDir(), backend.Codex, "", "", "two")
	f := &asyncInteractionFixture{fakeExec: fe}
	h.SetExecutor(f)
	route(h, c, `{"type":"pending_interactions_list","session_id":"s1"}`)
	event := waitForType(t, c, "pending_interactions_list")
	if f.scans.Load() != 1 || event["scope_session_id"] != "s1" || event["snapshot_all"] == true {
		t.Fatal(event, f.scans.Load())
	}
}

func TestNativeAsyncReplyDoesNotBypassToolMaintenance(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	h.registry.Create("s1", "native", t.TempDir(), backend.Codex, "", "read-only", "native-thread")
	h.SetExecutor(&asyncInteractionFixture{fakeExec: fe})
	h.toolRepairRunning.Store(true)
	route(h, c, `{"type":"user_input_response","request_id":"ui_async_fixture","answers":{}}`)
	if event := waitForType(t, c, "error"); event["code"] != "session_busy" {
		t.Fatal(event)
	}
	if _, found, _ := h.messageQueue.Get("s1", "ui_async_fixture"); found {
		t.Fatal("reply bypassed maintenance gate")
	}
}
