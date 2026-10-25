package core

import (
	"context"
	"encoding/json"
	ic "everything-go/contracts/task-api/interactions-v5"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/taskapi"
	ti "everything-go/internal/taskinteraction"
	"strings"
	"testing"
	"time"
)

func interactionCall(t *testing.T, h *Hub, s *session.Session, request, call string, input map[string]any) taskapi.Response {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": ic.Version, "operation": "interactions", "correlation_id": call, "idempotency_key": call, "input": input})
	caller := backend.TaskCaller{Session: s, RequestID: request, ThreadID: s.ResumeID(), TurnID: "native-" + request, CallID: call, ProviderVersion: "0.162.1", Validate: func() bool { return true }}
	result := h.ExecuteTask(context.Background(), caller, raw)
	encoded, _ := json.Marshal(result)
	if e := ic.ValidateResponse(encoded); e != nil {
		t.Fatalf("interaction response contract: %v; %s", e, encoded)
	}
	return result
}
func waitActor(t *testing.T, s *session.Session, id string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for s.ActiveQueuedID() != id {
		if time.Now().After(deadline) {
			t.Fatal("actor not started", s.ActiveQueuedID(), id)
		}
		time.Sleep(time.Millisecond)
	}
}
func fixtureInteractions(t *testing.T) (*Hub, *session.Session, *session.Session, string) {
	t.Helper()
	h, fe, caller, child := controllerFixture(t)
	parent := caller.Parent
	store, err := ti.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.interactions = store
	t.Cleanup(func() { child.CloseAndWait(); parent.CloseAndWait(); h.interactions.Close() })
	_, err = h.dispatches.SetGrant(context.Background(), parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	// The synthetic executor waits; model calls below use the exact admitted IDs.
	fe.onSend = func(*session.Session, string, string) {}
	value, err := h.ControlSession(context.Background(), caller, backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: child.ID, ExpectedThreadID: child.ResumeID(), Content: "synthetic child task"})
	if err != nil {
		t.Fatal(err)
	}
	r := value.(sessiondispatch.Record)
	waitActor(t, child, r.RequestID)
	return h, parent, child, r.RequestID
}
func resultRecord(t *testing.T, response taskapi.Response) ti.Record {
	t.Helper()
	if !response.OK {
		t.Fatal(response.Error)
	}
	b, _ := json.Marshal(response.Result)
	var r ti.Record
	if e := json.Unmarshal(b, &r); e != nil {
		t.Fatal(e)
	}
	return r
}
func TestInteractionOriginalParentAnswerAndChildAcknowledgement(t *testing.T) {
	h, parent, child, request := fixtureInteractions(t)
	r := resultRecord(t, interactionCall(t, h, child, request, "ask-once", map[string]any{"action": "ask_parent", "question": "Which test fixture?", "kind": "task_clarification", "blocking": false}))
	if r.State != "pending_parent" || r.Binding.ChildRequest != request {
		t.Fatal(r)
	}
	h.reconcileInteractions(context.Background())
	wake := "tiwake_" + r.ID
	waitActor(t, parent, wake)
	get := interactionCall(t, h, parent, wake, "parent-get", map[string]any{"action": "get", "interaction_id": r.ID})
	if !get.OK {
		t.Fatal(get)
	}
	raw, _ := json.Marshal(get.Result)
	if strings.Contains(string(raw), "ack_token") {
		t.Fatal("parent received child proof")
	}
	answer := interactionCall(t, h, parent, wake, "answer-once", map[string]any{"action": "answer", "interaction_id": r.ID, "expected_revision": 1, "answer": "fixture-alpha"})
	if !answer.OK {
		t.Fatal(answer.Error)
	}
	childGet := interactionCall(t, h, child, request, "child-get", map[string]any{"action": "get", "interaction_id": r.ID})
	if !childGet.OK {
		t.Fatal(childGet.Error)
	}
	m := childGet.Result.(map[string]any)
	if m["answer"] != "fixture-alpha" || m["answer_actor"] != "parent_agent" || m["human_authorization"] != false {
		t.Fatal(m)
	}
	acknowledged := resultRecord(t, interactionCall(t, h, child, request, "child-ack", map[string]any{"action": "ack", "interaction_id": r.ID, "ack_token": m["ack_token"]}))
	if acknowledged.State != "resolved" || !strings.Contains(acknowledged.AcknowledgedBy, request) {
		t.Fatal(acknowledged)
	}
	again := resultRecord(t, interactionCall(t, h, parent, wake, "answer-once", map[string]any{"action": "answer", "interaction_id": r.ID, "expected_revision": 1, "answer": "fixture-alpha"}))
	if again.State != "resolved" || again.ID != r.ID {
		t.Fatal("duplicate answer changed identity")
	}
	h.Emit(protocol.NewDone(parent.ID, wake))
	h.Emit(protocol.NewDone(child.ID, request))
}
func TestInteractionHumanDecisionAndBindingChangeRefuseAgent(t *testing.T) {
	h, parent, child, request := fixtureInteractions(t)
	r := resultRecord(t, interactionCall(t, h, child, request, "ask-human", map[string]any{"action": "ask_parent", "question": "Need your decision", "kind": "human_decision", "blocking": false}))
	h.reconcileInteractions(context.Background())
	wake := "tiwake_" + r.ID
	waitActor(t, parent, wake)
	bad := interactionCall(t, h, parent, wake, "parent-answer", map[string]any{"action": "answer", "interaction_id": r.ID, "expected_revision": 1, "answer": "approved"})
	if bad.OK || bad.Error.Code != "human_answer_required" {
		t.Fatal("agent granted authority", bad)
	}
	child.SetResumeID("replacement")
	bad = interactionCall(t, h, parent, wake, "parent-read-after-change", map[string]any{"action": "get", "interaction_id": r.ID})
	if bad.OK {
		t.Fatal("stale child binding readable")
	}
	h.reconcileInteractions(context.Background())
	stored, _ := h.interactions.Get(context.Background(), r.ID)
	if stored.State != "orphaned" {
		t.Fatal(stored)
	}
}
func TestInteractionBusyParentReadableWithoutSecondWriter(t *testing.T) {
	h, parent, child, request := fixtureInteractions(t)
	parent.SubmitNamed("parent-already-running", func() {})
	waitActor(t, parent, "parent-already-running")
	r := resultRecord(t, interactionCall(t, h, child, request, "ask", map[string]any{"action": "ask_parent", "question": "Fixture?", "kind": "task_clarification", "blocking": true, "wait_ms": 1}))
	if !h.TaskInteractionWaiting(child.ID) {
		t.Fatal("blocking ask not visible to liveness")
	}
	h.reconcileInteractions(context.Background())
	stored, _ := h.interactions.Get(context.Background(), r.ID)
	if stored.Notification != "deferred_parent_busy" || parent.QueueLen() != 0 || parent.ActiveQueuedID() != "parent-already-running" {
		t.Fatal("parent writer disturbed", stored)
	}
	res := interactionCall(t, h, parent, "parent-already-running", "join", map[string]any{"action": "wait"})
	if !res.OK || res.Result.(map[string]any)["needs_input"] != true {
		t.Fatal("join did not yield needs_input", res)
	}
}
func TestInteractionLegacyParentUsesOriginalTaskSchema(t *testing.T) {
	h, parent, child, request := fixtureInteractions(t)
	r := resultRecord(t, interactionCall(t, h, child, request, "ask", map[string]any{"action": "ask_parent", "question": "Fixture?", "kind": "task_clarification", "blocking": false}))
	h.reconcileInteractions(context.Background())
	wake := "tiwake_" + r.ID
	waitActor(t, parent, wake)
	body, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": "1.0.0-rc.1", "operation": "get", "correlation_id": "legacy", "input": map[string]any{"task_id": "ti5:" + r.ID}})
	response := h.ExecuteTask(context.Background(), backend.TaskCaller{Session: parent, RequestID: wake, ThreadID: parent.ResumeID(), TurnID: "native", CallID: "legacy", ProviderVersion: "0.162.1", Validate: func() bool { return true }}, body)
	if !response.OK {
		t.Fatal(response)
	}
	encoded, _ := json.Marshal(response)
	if response.Version != ic.Version || response.RequestVersion == nil || *response.RequestVersion != "1.0.0-rc.1" || ic.ValidateResponse(encoded) != nil {
		t.Fatal("legacy adapter must explicitly return the new version family", string(encoded))
	}
}
