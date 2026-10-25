package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/sessiondispatch"
	"strings"
	"testing"
)

func TestInteractionAnswerAfterProducerFinalUsesOnePinnedContinuation(t *testing.T) {
	h, parent, child, request := fixtureInteractions(t)
	r := resultRecord(t, interactionCall(t, h, child, request, "ask", map[string]any{"action": "ask_parent", "question": "Fixture?", "kind": "task_clarification", "blocking": false}))
	h.Emit(protocol.NewDone(child.ID, request))
	h.reconcileInteractions(context.Background())
	wake := "tiwake_" + r.ID
	waitActor(t, parent, wake)
	resultRecord(t, interactionCall(t, h, parent, wake, "answer", map[string]any{"action": "answer", "interaction_id": r.ID, "expected_revision": 1, "answer": "alpha"}))
	h.reconcileInteractions(context.Background())
	resumed := "tianswer_" + r.ID
	waitActor(t, child, resumed)
	got := interactionCall(t, h, child, resumed, "get", map[string]any{"action": "get", "interaction_id": r.ID})
	if !got.OK {
		t.Fatal(got)
	}
	token := got.Result.(map[string]any)["ack_token"]
	ack := resultRecord(t, interactionCall(t, h, child, resumed, "ack", map[string]any{"action": "ack", "interaction_id": r.ID, "ack_token": token}))
	if ack.State != "resolved" || !strings.HasPrefix(ack.AcknowledgedBy, resumed) {
		t.Fatal(ack)
	}
	duplicate := resultRecord(t, interactionCall(t, h, child, resumed, "ack-again", map[string]any{"action": "ack", "interaction_id": r.ID, "ack_token": token}))
	if duplicate.Version != ack.Version {
		t.Fatal("duplicate ACK changed receipt version")
	}
	h.Emit(protocol.NewDone(child.ID, resumed))
	h.reconcileInteractions(context.Background())
	if child.QueueLen() != 0 {
		t.Fatal("answer continuation replayed")
	}
}
func TestInteractionNativeReplyIsNotModelConsumption(t *testing.T) {
	h, _, child, request := fixtureInteractions(t)
	h.ObserveTaskQuestion(context.Background(), backend.TaskQuestionEvidence{Session: child, RequestID: request, ThreadID: child.ResumeID(), CallID: "native", TurnID: "turn", Payload: protocol.UserInputRequestPayload{RequestID: "ui_original", SessionID: child.ID, Questions: []protocol.UserInputQuestion{{Text: "Human decision"}}}})
	h.Emit(protocol.NewInteractionResolved("ui_original", child.ID, "resolved"))
	rows, e := h.interactions.List(context.Background(), child.ID, 100)
	if e != nil || len(rows) != 1 || rows[0].State != "native_reply_written" || rows[0].AcknowledgedBy != "" {
		t.Fatal(rows, e)
	}
	h.reconcileInteractions(context.Background())
}

func TestInteractionMultiLevelOriginalLauncherAndMissingLineage(t *testing.T) {
	h, parent, child, request := fixtureInteractions(t)
	grand := h.registry.Create("grand", "Grand", t.TempDir(), backend.Codex, "", "danger-full-access", "grand-thread")
	t.Cleanup(func() { grand.CloseAndWait() })
	q := resultRecord(t, interactionCall(t, h, child, request, "ask", map[string]any{"action": "ask_parent", "question": "Need ancestor answer", "kind": "task_clarification", "blocking": false}))
	h.reconcileInteractions(context.Background())
	wake := "tiwake_" + q.ID
	waitActor(t, parent, wake)
	result := interactionCall(t, h, parent, wake, "missing-upstream", map[string]any{"action": "ask_parent", "question": "Forward this", "kind": "task_clarification", "blocking": false})
	if result.OK {
		t.Fatal("invented upstream lineage accepted")
	}
	_, err := h.dispatches.SetGrant(context.Background(), grand.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	record := sessiondispatch.Record{ID: "upstream", IntentHash: "upstream-hash", ParentID: grand.ID, ParentThreadID: grand.ResumeID(), ParentConfigKnown: true, ParentConfigRevision: grand.SettingsSnapshot().ConfigRevision, ParentExecutionRequestID: "grand-producer", OriginRequestID: "grand-producer", SessionID: parent.ID, ThreadID: parent.ResumeID(), ConfigRevision: parent.SettingsSnapshot().ConfigRevision, InstanceID: h.cfg.InstanceID, RequestID: q.Binding.ParentRequest, State: "completed"}
	if _, err = h.dispatches.Create(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	upper := resultRecord(t, interactionCall(t, h, parent, wake, "ask-upstream", map[string]any{"action": "ask_parent", "question": "Forward this", "kind": "task_clarification", "blocking": false}))
	if upper.SourceInteraction != q.ID || len(upper.Route) != 2 || upper.Binding.Parent != grand.ID {
		t.Fatal(upper)
	}
	h.reconcileInteractions(context.Background())
	grandWake := "tiwake_" + upper.ID
	waitActor(t, grand, grandWake)
	resultRecord(t, interactionCall(t, h, grand, grandWake, "ancestor-answer", map[string]any{"action": "answer", "interaction_id": upper.ID, "expected_revision": 1, "answer": "alpha"}))
	get := interactionCall(t, h, parent, wake, "upper-get", map[string]any{"action": "get", "interaction_id": upper.ID})
	if !get.OK {
		t.Fatal(get)
	}
	resultRecord(t, interactionCall(t, h, parent, wake, "upper-ack", map[string]any{"action": "ack", "interaction_id": upper.ID, "ack_token": get.Result.(map[string]any)["ack_token"]}))
	lower := resultRecord(t, interactionCall(t, h, parent, wake, "lower-answer", map[string]any{"action": "answer", "interaction_id": q.ID, "expected_revision": 1, "answer": "alpha"}))
	if lower.State != "answer_reserved" || lower.Actor != "parent_agent" {
		t.Fatal(lower)
	}
}

func TestInteractionFreshDelegationBindsOnlyNativeAcceptance(t *testing.T) {
	h, p, _ := continuityFixture(t)
	_, r := continuityDelegate(t, h, p, "fresh-child", "fixture")
	child, _ := h.registry.Get(r.ChildSessionID)
	expected, _, _, err := h.delegations.ExecutionBinding(context.Background(), r.ID)
	if err != nil || expected != "" {
		t.Fatal("fixture must retain empty pre-creation thread", expected, err)
	}
	binding, err := h.interactionBinding(context.Background(), child.ID, r.ChildRequestID)
	if err != nil || binding.ChildThread != child.ResumeID() {
		t.Fatal("observed native thread not bound", binding, err)
	}
	child.SetResumeID("replacement")
	if _, err = h.interactionBinding(context.Background(), child.ID, r.ChildRequestID); err == nil {
		t.Fatal("later thread adopted")
	}
}

func TestInteractionHumanAuthorizationPayloadStaysAtOriginalQuestion(t *testing.T) {
	h, _, child, request := fixtureInteractions(t)
	for _, kind := range []string{"mcp_url", "dynamic_tool"} {
		h.ObserveTaskQuestion(context.Background(), backend.TaskQuestionEvidence{Session: child, RequestID: request, ThreadID: child.ResumeID(), TurnID: "turn", CallID: kind, Payload: protocol.UserInputRequestPayload{RequestID: "native_" + kind, SessionID: child.ID, Kind: kind, Questions: []protocol.UserInputQuestion{{Text: "private-fixture-credential-and-url"}}}})
	}
	rows, err := h.interactions.List(context.Background(), child.ID, 100)
	if err != nil || len(rows) != 2 {
		t.Fatal(err)
	}
	for _, q := range rows {
		raw, err := json.Marshal(interactionView(q, false))
		if err != nil || strings.Contains(string(raw), "private-fixture") || !q.HumanOnly || q.State != "pending_human" {
			t.Fatal("sensitive payload forwarded", err)
		}
	}
}
