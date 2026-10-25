package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"fmt"
	"testing"
)

func TestCallbackStrictSteerFollowsOnlyOriginalExecution(t *testing.T) {
	h, c, s, e := callbackFixture(t)
	ctx := context.Background()
	root := taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Running)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: root.RequestID, ThreadID: s.ResumeID(), TurnID: "exact-turn"})
	supplement := taskEntry(t, h, s.ID, "r_supplement", "phone-a", messagequeue.Queued)
	if err := h.armAdmittedCallback(ctx, c, s, supplement.RequestID); err != nil {
		t.Fatal(err)
	}
	supplement, _, _ = h.messageQueue.Transition(s.ID, supplement.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Steered, "", root.RequestID, "exact-turn", true)
	h.reconcileTaskCallbacks(ctx, s.ID, supplement.RequestID)
	rows, _ := h.callbacks.List(ctx)
	if rows[0].Report != nil {
		t.Fatal("steer ACK became completion")
	}
	h.messageQueue.Transition(s.ID, root.RequestID, []messagequeue.State{messagequeue.Running}, messagequeue.Completed, "", "", "")
	e.finalFound = false
	h.reconcileTaskCallbacks(ctx, s.ID, root.RequestID)
	rows, _ = h.callbacks.List(ctx)
	if rows[0].Report != nil {
		t.Fatal("missing final became report")
	}
	e.finalFound = true
	h.reconcileTaskCallbacks(ctx, s.ID, root.RequestID)
	rows, _ = h.callbacks.List(ctx)
	if rows[0].Report == nil || rows[0].Report.ExecutionRequest != root.RequestID || rows[0].Request != supplement.RequestID || rows[0].Report.Text != e.final {
		t.Fatal(rows)
	}
	if !h.callbackReportCurrent(ctx, rows[0]) {
		t.Fatal("exact report rejected")
	}
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: root.RequestID, ThreadID: s.ResumeID(), TurnID: "foreign"})
	if h.callbackReportCurrent(ctx, rows[0]) {
		t.Fatal("foreign execution accepted")
	}
}
func TestCallbackRootAndSteeredDemandSealOneAutomaticInvitation(t *testing.T) {
	h, c, s, _ := callbackFixture(t)
	ctx := context.Background()
	root := taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Completed)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: root.RequestID, ThreadID: s.ResumeID(), TurnID: "turn"})
	taskEntry(t, h, s.ID, "r_supplement", "phone-a", messagequeue.Queued)
	h.messageQueue.Transition(s.ID, "r_supplement", []messagequeue.State{messagequeue.Queued}, messagequeue.Steered, "", root.RequestID, "turn", true)
	for _, id := range []string{root.RequestID, "r_supplement"} {
		if e := h.armAdmittedCallback(ctx, c, s, id); e != nil {
			t.Fatal(e)
		}
	}
	h.reconcileTaskCallbacks(ctx, s.ID, root.RequestID)
	rows, _ := h.callbacks.List(ctx)
	pending := 0
	for _, r := range rows {
		if r.Report == nil {
			t.Fatal("unsealed supplement", r)
		}
		if r.Invite.State == "delivery_pending" {
			pending++
		}
	}
	if len(rows) != 2 || pending != 1 {
		t.Fatal("duplicate result invitation", rows)
	}
}
func TestCallbackUnprovenSteerCannotSeal(t *testing.T) {
	h, c, s, _ := callbackFixture(t)
	ctx := context.Background()
	taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Completed)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: "r_callbacktask", ThreadID: s.ResumeID(), TurnID: "turn"})
	taskEntry(t, h, s.ID, "r_unproven", "phone-a", messagequeue.Queued)
	h.messageQueue.Transition(s.ID, "r_unproven", []messagequeue.State{messagequeue.Queued}, messagequeue.Steered, "", "r_callbacktask", "turn")
	if e := h.armAdmittedCallback(ctx, c, s, "r_unproven"); e != nil {
		t.Fatal(e)
	}
	h.reconcileTaskCallbacks(ctx, s.ID, "r_callbacktask")
	rows, _ := h.callbacks.List(ctx)
	if rows[0].Report != nil {
		t.Fatal("unproven receipt became report")
	}
}

func TestCallbackSteerCannotRebindFrozenConfiguration(t *testing.T) {
	h, c, s, _ := callbackFixture(t)
	ctx := context.Background()
	taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Completed)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: "r_callbacktask", ThreadID: s.ResumeID(), TurnID: "turn"})
	raw, _ := json.Marshal(queuedPayload{MessagePurpose: "instruction", OwnerDevice: "phone-a", ExpectedTarget: &dispatchTargetExpectation{ThreadID: s.ResumeID(), Revision: s.SettingsSnapshot().ConfigRevision + 1}})
	h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: "r_wrongconfig", Content: "supplement", Payload: raw})
	h.messageQueue.Transition(s.ID, "r_wrongconfig", []messagequeue.State{messagequeue.Queued}, messagequeue.Steered, "", "r_callbacktask", "turn", true)
	if e := h.armAdmittedCallback(ctx, c, s, "r_wrongconfig"); e != nil {
		t.Fatal(e)
	}
	h.reconcileTaskCallbacks(ctx, s.ID, "r_callbacktask")
	rows, _ := h.callbacks.List(ctx)
	if rows[0].Report != nil {
		t.Fatal("frozen configuration rebound", rows)
	}
}

func TestCallbackSourcesExplicitActiveSurvivesLongQueue(t *testing.T) {
	h, c, s, _ := callbackFixture(t)
	taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Running)
	for i := 0; i < 25; i++ {
		taskEntry(t, h, s.ID, fmt.Sprintf("r_queued_%02d", i), "phone-a", messagequeue.Queued)
	}
	got := callbackAction(t, h, c, callbackInput{Action: "sources", Operation: "sources_active_longqueue", Session: s.ID, Request: "r_callbacktask"})
	rows, _ := got["tasks"].([]any)
	found := false
	for _, row := range rows {
		value, _ := row.(map[string]any)
		if value["task_request_id"] == "r_callbacktask" {
			found = true
		}
	}
	if got["accepted"] != true || !found {
		t.Fatal("active hidden by queued followups", got)
	}
}
