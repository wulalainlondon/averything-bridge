package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"testing"
	"time"
)

func TestIndependentQAFormalSteerCollectsExactExecutionSeal(t *testing.T) {
	h, p, _ := continuityFixture(t)
	f := &capacityAppendExecutor{&capacityScopeCapabilityFixture{&fakeExec{sink: h}}}
	started := make(chan string, 2)
	f.onSend = func(s *session.Session, req, _ string) {
		s.SetResumeID("synthetic-formal-thread")
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-formal-turn"})
		started <- req
	}
	h.SetExecutor(f)
	ctx := context.Background()
	binding := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + p.ID, SourceSessionID: p.ID, SourceResumeID: p.ResumeID(), SourceConfigKnown: true, SourceConfigRevision: p.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool", InvocationGeneration: "synthetic-formal-continuity", SourceRequestID: p.ActiveQueuedID(), ToolCallID: "synthetic-create-formal", Transport: "native_dynamic_tool"}
	bound, e := taskapi.BindCaller(ctx, apiVerifier{func() (taskapi.VerifiedContext, error) { return binding, nil }}, taskapi.Invocation{})
	if e != nil {
		t.Fatal(e)
	}
	root := p.Cwd()
	input := map[string]any{"goal": "Synthetic formal task", "instruction": "check", "new_worker": map[string]any{"name": "Formal owner", "profile": map[string]string{"backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}}, "scope": map[string]any{"workspace_roots": []string{root}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "workspace": map[string]any{"cwd": root, "input_artifact_ids": []any{}}, "acceptance": []any{}, "dependencies": []any{}}
	raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-formal", "operation": "create_dispatch", "idempotency_key": "synthetic-formal-task", "input": input})
	response := h.taskService.Execute(ctx, bound, raw)
	if !response.OK {
		t.Fatal(response.Error)
	}
	receipt := response.Result.(map[string]any)
	intent, e := h.delegations.TaskJournal().Task(ctx, binding.StableScopeID, 0, receipt["task_id"].(string))
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("formal task did not start")
	}
	original := capacityRecordFor(t, h, intent.NativeID)
	child, _ := h.registry.Get(original.ChildSessionID)

	f.onSteer = func(_ *session.Session, _, _ string) (backend.SteerResult, error) {
		return backend.SteerResult{RequestID: original.ChildRequestID, TurnID: "synthetic-formal-turn", StrictACK: true}, nil
	}
	in := continuityInput(original, p, h, "supplement")
	in.Mode = "steer"
	in.ActiveRequestID = original.ChildRequestID
	got, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "qa-formal-steer", in)
	if e != nil {
		t.Fatal(e)
	}
	id := got.(map[string]any)["delegation_id"].(string)
	supplement := capacityRecordFor(t, h, id)
	if e = h.reconcileDelegation(ctx, supplement); e != nil {
		t.Fatal(e)
	}
	correctionWaitState(t, h, child.ID, supplement.ChildRequestID, messagequeue.Steered)
	text := "exact original active final including supplement"
	digest := sha256.Sum256([]byte(text))
	hash := hex.EncodeToString(digest[:])
	anchor, _ := json.Marshal(map[string]any{"instance_id": h.cfg.InstanceID, "session_id": child.ID, "request_id": original.ChildRequestID, "source_message_id": "synthetic-final-message", "execution": map[string]string{"backend": "codex", "native_thread_id": child.ResumeID(), "native_turn_id": "synthetic-formal-turn"}})
	if _, e = h.messageQueue.SealTask(ctx, messagequeue.TaskSeal{ID: "seal_" + hash, SessionID: child.ID, RequestID: original.ChildRequestID, Hash: hash, Text: text, Anchor: anchor}); e != nil {
		t.Fatal(e)
	}
	h.Emit(protocol.NewDone(child.ID, original.ChildRequestID))
	projected, e := h.apiTask(ctx, intent)
	if e != nil || projected["seal_id"] != "seal_"+hash {
		t.Fatal("fixture original exact seal unavailable", projected, e)
	}
	if e = h.reconcileDelegation(ctx, original); e != nil {
		t.Fatal("original collection", e)
	}
	if e = h.reconcileDelegation(ctx, capacityRecordFor(t, h, id)); e != nil {
		counts, _ := h.delegations.CountsForParent(ctx, p.ID)
		t.Fatalf("formal steer failed exact execution seal collection: %v; supplement=%+v; capacity=%+v", e, capacityRecordFor(t, h, id), counts)
	}
	final := capacityRecordFor(t, h, id)
	if final.State != "terminal" || final.Result != text {
		t.Fatal("supplement final not collected", final)
	}

	// Simulate acknowledged delivery only in this isolated store fixture, then
	// reopen the durable store and ensure exact round results survive unchanged.
	for _, record := range []delegation.Record{original, final} {
		if err := h.delegations.MarkDeliveryQueued(ctx, record.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := h.delegations.MarkDelivered(ctx, record.ParentSessionID, record.ParentRequestID); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := delegation.Open(h.cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, record := range []delegation.Record{original, final} {
		stored, found, err := reopened.ByID(ctx, record.ID)
		if err != nil || !found || stored.Result != text || stored.DeliveryState != "delivered" {
			t.Fatal("reopen lost result or delivery", stored, err)
		}
	}
	counts, err := reopened.CountsForParent(ctx, p.ID)
	if err != nil || counts != (delegation.Counts{}) {
		t.Fatal("reservation not drained", counts, err)
	}
	if err := h.reconcileDelegation(ctx, capacityRecordFor(t, h, final.ID)); err != nil {
		t.Fatal("delivered reconcile", err)
	}
	if got := capacityRecordFor(t, h, final.ID); got.DeliveryState != "delivered" || got.Result != text {
		t.Fatal("replayed result changed", got)
	}
}

func TestIndependentQAWrongOwnerVerdictRefused(t *testing.T) {
	h, p, _ := continuityFixture(t)
	owner, _ := continuityDelegate(t, h, p, "qa-owner", "owner-final")
	qa, _ := continuityDelegate(t, h, p, "qa-reviewer", `{"owner_round_id":"foreign-round","verdict":"PASS"}`)
	if _, e := h.BindDelegationQA(p, p.ActiveQueuedID(), "qa-bind", owner.ID, qa.ID); e != nil {
		t.Fatal(e)
	}
	if _, e := h.RecordDelegationReview(p, p.ActiveQueuedID(), "qa-wrong-result", owner.ID, qa.ID); e == nil {
		t.Fatal("foreign owner result accepted")
	}
	_, v, e := h.delegations.QAForRound(context.Background(), owner.ID)
	if e != nil || v != "" {
		t.Fatal("refusal persisted verdict", v, e)
	}
}

func TestIndependentQADistinctSessionsSharingNativeThreadRefused(t *testing.T) {
	h, p, f := continuityFixture(t)
	f.onSend = func(s *session.Session, req, content string) {
		s.SetResumeID("synthetic-shared-thread")
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-turn-" + req})
		f.mu.Lock()
		f.finals[req] = content
		f.mu.Unlock()
		h.Emit(protocol.NewDone(s.ID, req))
	}
	owner, _ := continuityDelegate(t, h, p, "qa-owner", "owner-final")
	qa, _ := continuityDelegate(t, h, p, "qa-reviewer", "qa-final")
	if owner.ChildSessionID == qa.ChildSessionID {
		t.Fatal("fixture sessions not distinct")
	}
	if _, e := h.BindDelegationQA(p, p.ActiveQueuedID(), "qa-shared-native", owner.ID, qa.ID); e == nil {
		t.Fatal("same native thread accepted as independent QA")
	}
}

func TestIndependentQACorrectionCannotReuseOldFinal(t *testing.T) {
	h, p, f := continuityFixture(t)
	_, old := continuityDelegate(t, h, p, "owner", "old-final")
	f.onSend = func(s *session.Session, req, _ string) {
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-turn-" + req})
		h.Emit(protocol.NewDone(s.ID, req))
	}
	got, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "qa-new-correction", continuityInput(old, p, h, "new work"))
	if e != nil {
		t.Fatal(e)
	}
	id := got.(map[string]any)["delegation_id"].(string)
	newRound := continuityWait(t, h, id, "collecting")
	for i := 0; i < 3; i++ {
		if e = h.reconcileDelegation(context.Background(), capacityRecordFor(t, h, id)); e != nil {
			t.Fatal(e)
		}
	}
	if r := capacityRecordFor(t, h, id); r.State != "collecting" || r.Result != "" {
		t.Fatal("old final settled new correction", r)
	}
	f.mu.Lock()
	f.finals[newRound.ChildRequestID] = "exact-new-final"
	f.mu.Unlock()
	final := continuityWait(t, h, id, "terminal")
	if final.Result != "exact-new-final" || capacityRecordFor(t, h, old.ID).Result != "old-final" {
		t.Fatal("round result confused", final)
	}
}
