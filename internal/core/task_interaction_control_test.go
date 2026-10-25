package core

import (
	"context"
	"everything-go/internal/clientproto"
	"everything-go/internal/session"
	ti "everything-go/internal/taskinteraction"
	"testing"
)

func TestInteractionControlPrefixCannotBeForgedByClient(t *testing.T) {
	h, parent, child, request := fixtureInteractions(t)
	q := resultRecord(t, interactionCall(t, h, child, request, "ask", map[string]any{"action": "ask_parent", "question": "Fixture?", "kind": "task_clarification", "blocking": false}))
	id := "tiwake_" + q.ID
	if h.enqueueChatMessage(newTestClient(h), clientproto.Command{Kind: "message", SessionID: parent.ID, RequestID: id, Content: "untrusted replacement", MessagePurpose: "control"}) {
		t.Fatal("public client used reserved control lane")
	}
	if _, found, _ := h.messageQueue.Get(parent.ID, id); found {
		t.Fatal("forged control was persisted")
	}
	h.reconcileInteractions(context.Background())
	waitActor(t, parent, id)
}
func TestInteractionCompletedDelegatedChildUsesControlLaneWithoutNewWorkRound(t *testing.T) {
	h, p, f := continuityFixture(t)
	_, r := continuityDelegate(t, h, p, "worker", "original final")
	child, _ := h.registry.Get(r.ChildSessionID)
	b, e := h.interactionBinding(context.Background(), child.ID, r.ChildRequestID)
	if e != nil {
		t.Fatal(e)
	}
	q, _, e := h.interactions.Create(context.Background(), ti.Record{Binding: b, ProducerRequest: r.ChildRequestID, OriginKey: "fixture-after-final", IntentHash: "fixture", Question: "Fixture?", Kind: "task_clarification"})
	if e != nil {
		t.Fatal(e)
	}
	q, e = h.interactions.Answer(context.Background(), q.ID, "parent", "answer", "hash", 1, "parent_agent", "parent", "alpha")
	if e != nil {
		t.Fatal(e)
	}
	f.onSend = func(*session.Session, string, string) {}
	before, e := h.delegations.CountsForParent(context.Background(), p.ID)
	if e != nil {
		t.Fatal(e)
	}
	h.resumeAnsweredInteraction(context.Background(), q)
	waitActor(t, child, "tianswer_"+q.ID)
	if e = h.delegationExecutionGuard(context.Background(), child, "tianswer_"+q.ID, true); e != nil {
		t.Fatal(e)
	}
	if e = h.delegationExecutionGuard(context.Background(), child, "unrelated-work", true); e == nil {
		t.Fatal("control lane enabled arbitrary worker input")
	}
	after, e := h.delegations.CountsForParent(context.Background(), p.ID)
	if e != nil || before != after {
		t.Fatal("control delivery altered work-round capacity", before, after, e)
	}
	// Changing the immutable original configuration invalidates a stored control.
	child.SetResumeID("replacement")
	if _, e = h.interactionControlGuard(context.Background(), child, "tianswer_"+q.ID); e == nil {
		t.Fatal("control adopted replacement thread")
	}
}
