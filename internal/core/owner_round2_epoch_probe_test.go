package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"fmt"
	"testing"
	"time"
)

// Entirely isolated synthetic fixture. Completion/delivery marks below prepare
// an already-settled pair; they are not live ledger edits or delivery evidence.
func ownerRound2FormalPair(t *testing.T) (*Hub, *session.Session, *capacityAppendExecutor, taskapi.IntentRecord, delegation.Record, delegation.Record) {
	t.Helper()
	h, p, _ := continuityFixture(t)
	f := &capacityAppendExecutor{&capacityScopeCapabilityFixture{&fakeExec{sink: h}}}
	started := make(chan string, 4)
	f.onSend = func(s *session.Session, req, _ string) {
		if s.ID == p.ID {
			h.Emit(protocol.NewDone(s.ID, req))
			return
		}
		if s.ResumeID() == "" {
			s.SetResumeID("synthetic-thread-" + s.ID)
		}
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-turn-" + req})
		started <- req
	}
	h.SetExecutor(f)
	ctx := context.Background()
	binding := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + p.ID, SourceSessionID: p.ID, SourceResumeID: p.ResumeID(), SourceConfigKnown: true, SourceConfigRevision: p.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool", InvocationGeneration: "owner-round2-fixture", SourceRequestID: p.ActiveQueuedID(), ToolCallID: "fixture-formal-create", Transport: "native_dynamic_tool"}
	bound, e := taskapi.BindCaller(ctx, apiVerifier{func() (taskapi.VerifiedContext, error) { return binding, nil }}, taskapi.Invocation{})
	if e != nil {
		t.Fatal(e)
	}
	input := map[string]any{"goal": "synthetic formal owner", "instruction": "check", "new_worker": map[string]any{"name": "Formal owner", "profile": map[string]string{"backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}}, "scope": map[string]any{"workspace_roots": []string{p.Cwd()}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "workspace": map[string]any{"cwd": p.Cwd(), "input_artifact_ids": []any{}}, "acceptance": []any{}, "dependencies": []any{}}
	raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "fixture-create", "operation": "create_dispatch", "idempotency_key": "owner-round2-formal-create", "input": input})
	response := h.taskService.Execute(ctx, bound, raw)
	if !response.OK {
		t.Fatal(response.Error)
	}
	intent, e := h.delegations.TaskJournal().Task(ctx, binding.StableScopeID, 0, response.Result.(map[string]any)["task_id"].(string))
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("formal owner did not start")
	}
	owner := capacityRecordFor(t, h, intent.NativeID)
	qaReceipt, e := h.DelegateSession(p, p.ActiveQueuedID(), "fixture-qa-create", backend.DelegationSpec{Name: "Independent QA", Instruction: "review"})
	if e != nil {
		t.Fatal(e)
	}
	if e = h.reconcileDelegation(ctx, capacityRecordFor(t, h, qaReceipt.ID)); e != nil {
		t.Fatal(e)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("QA did not start")
	}
	qa := capacityRecordFor(t, h, qaReceipt.ID)
	for _, r := range []delegation.Record{owner, qa} {
		h.Emit(protocol.NewDone(r.ChildSessionID, r.ChildRequestID))
		if _, e = h.delegations.Complete(ctx, r.ChildSessionID, r.ChildRequestID, "completed", "immutable fixture final", "", nil); e != nil {
			t.Fatal(e)
		}
		if e = h.delegations.MarkDeliveryQueued(ctx, r.ID); e != nil {
			t.Fatal(e)
		}
		if _, e = h.delegations.MarkDelivered(ctx, r.ParentSessionID, r.ParentRequestID); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = h.BindDelegationQA(p, p.ActiveQueuedID(), "fixture-bind", owner.ID, qa.ID); e != nil {
		t.Fatal(e)
	}
	p.EndTurn()
	old := p.SettingsSnapshot().ConfigRevision
	if _, e = p.SetFutureConfiguration(session.ConfigurationFrom(p.SettingsSnapshot()), nil); e != nil {
		t.Fatal(e)
	}
	continuityParentTurn(t, p, "r_fixture_reauthorization")
	os, _ := h.registry.Get(owner.ChildSessionID)
	qs, _ := h.registry.Get(qa.ChildSessionID)
	args := delegationReauthorization{Owner: owner.ID, QA: qa.ID, Instance: h.cfg.InstanceID, SourceThread: p.ResumeID(), From: old, To: old + 1, OwnerThread: os.ResumeID(), QAThread: qs.ResumeID(), OwnerRevision: os.SettingsSnapshot().ConfigRevision, QARevision: qs.SettingsSnapshot().ConfigRevision}
	continuityCompatData(t, continuityCompatCall(t, h, p, "append", "owner-round2-fixture-reauth", "reauthorize_pair", args))
	return h, p, f, intent, owner, qa
}
func TestOwnerRound2FormalAppendUsesApprovedSourceEpoch(t *testing.T) {
	h, p, _, intent, owner, _ := ownerRound2FormalPair(t)
	ctx := context.Background()
	child, _ := h.registry.Get(owner.ChildSessionID)
	binding := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + p.ID, SourceSessionID: p.ID, SourceResumeID: p.ResumeID(), SourceConfigKnown: true, SourceConfigRevision: p.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool", InvocationGeneration: "owner-round2-fixture", SourceRequestID: p.ActiveQueuedID(), ToolCallID: "fixture-formal-append", Transport: "native_dynamic_tool"}
	bound, e := taskapi.BindCaller(ctx, apiVerifier{func() (taskapi.VerifiedContext, error) { return binding, nil }}, taskapi.Invocation{})
	if e != nil {
		t.Fatal(e)
	}
	task, e := h.apiTask(ctx, intent)
	if e != nil {
		t.Fatal(e)
	}
	input := map[string]any{"task_id": intent.TaskID, "expected_revision": task["revision"], "expected_previous_round_id": owner.ID, "target": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": child.ID, "expected_native_thread_id": child.ResumeID(), "expected_config_revision": child.SettingsSnapshot().ConfigRevision}, "content": "authorized same-session correction", "mode": "queue", "return_to_parent": true}
	raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "fixture-post-epoch-append", "operation": "append", "idempotency_key": "owner-round2-post-epoch-append", "input": input})
	result := h.taskService.Execute(ctx, bound, raw)
	if !result.OK {
		epoch, _, _ := h.delegations.SourceEpoch(ctx, owner.ID)
		t.Fatalf("approved formal append refused: %+v; epoch=%+v; parent_revision=%d", result.Error, epoch, p.SettingsSnapshot().ConfigRevision)
	}
	req := result.Result.(map[string]any)["effect_request_ids"].([]string)[0]
	round, found, e := h.delegations.ByChildRequest(ctx, child.ID, req)
	if e != nil || !found {
		t.Fatal(e)
	}
	_, rev, _, e := h.delegations.SourceBinding(ctx, round.ID)
	if e != nil || rev != p.SettingsSnapshot().ConfigRevision {
		t.Fatal("new round bound stale revision", rev, e)
	}
	_, oldRev, _, e := h.delegations.SourceBinding(ctx, owner.ID)
	if e != nil || oldRev == rev {
		t.Fatal("original binding overwritten", oldRev, rev, e)
	}
}
func TestOwnerRound2FormalReturnUsesApprovedEpochWithoutMutatingMetadata(t *testing.T) {
	h, p, _, _, owner, _ := ownerRound2FormalPair(t)
	ctx := context.Background()
	child, _ := h.registry.Get(owner.ChildSessionID)
	// Reserve a new formal round through the core store with the verified epoch,
	// isolating delivery validation from the separate original-append admission bug.
	root, meta, _, e := h.continuityBase(p, owner.ID)
	if e != nil {
		t.Fatal(e)
	}
	rawBefore, _, e := h.delegations.APIChildMetadata(ctx, child.ID)
	if e != nil {
		t.Fatal(e)
	}
	next := delegation.Record{ID: "fixture-epoch-return", LogicalID: meta.LogicalID, PreviousID: owner.ID, Authority: h.cfg.InstanceID, ParentSessionID: p.ID, OriginRequestID: p.ActiveQueuedID(), ToolCallID: "fixture-return-only", ChildSessionID: child.ID, ChildRequestID: "r_fixture_epoch_return", ParentRequestID: "dgreturn_fixture_epoch_return", ChildName: root.ChildName, Cwd: root.Cwd, Instruction: "fixture", Model: root.Model, Effort: root.Effort, Sandbox: root.Sandbox, SourceThread: root.SourceThread, SourceRevision: root.SourceRevision, ExpectedThread: root.ExpectedThread, ExpectedRevision: root.ExpectedRevision, WorkspaceIdentity: root.WorkspaceIdentity}
	if e = h.delegations.CreateBounded(ctx, next); e != nil {
		t.Fatal(e)
	}
	if _, e = h.delegations.Complete(ctx, child.ID, next.ChildRequestID, "completed", "immutable new fixture final", "", nil); e != nil {
		t.Fatal(e)
	}
	next = capacityRecordFor(t, h, next.ID)
	p.EndTurn()
	if e = h.reconcileDelegation(ctx, next); e != nil {
		t.Fatal(e)
	}
	after := capacityRecordFor(t, h, next.ID)
	if after.DeliveryState == "failed" {
		t.Fatalf("approved formal return refused: state=%s error=%s original_metadata_revision remains old, current=%d", after.DeliveryState, after.DeliveryError, p.SettingsSnapshot().ConfigRevision)
	}
	// Parent was already released above; do not EndTurn a second time while
	// its new result-return actor is running. Reconcile only the exact receipt.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e = h.reconcileDelegation(ctx, capacityRecordFor(t, h, next.ID)); e != nil {
			t.Fatal(e)
		}
		if capacityRecordFor(t, h, next.ID).DeliveryState == "delivered" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if state := capacityRecordFor(t, h, next.ID); state.DeliveryState != "delivered" {
		entry, _, _ := h.messageQueue.Get(p.ID, next.ParentRequestID)
		t.Fatal("return not delivered", state, entry)
	}
	frozen, found, e := h.delegations.ReturnContext(ctx, capacityRecordFor(t, h, next.ID), root.SourceThread, root.SourceRevision)
	if e != nil || !found || frozen.Snapshot.Status != "available" {
		t.Fatal("new frozen return missing", frozen, found, e)
	}
	rawAfter, _, e := h.delegations.APIChildMetadata(ctx, child.ID)
	if e != nil || string(rawBefore) != string(rawAfter) {
		t.Fatal("original formal metadata rewritten")
	}
	for i := 0; i < 2; i++ {
		if e = h.reconcileDelegation(ctx, capacityRecordFor(t, h, next.ID)); e != nil {
			t.Fatal(e)
		}
	}
	reopened, e := delegation.Open(h.cfg.DataDir)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	result, found, e := reopened.ByID(ctx, next.ID)
	if e != nil || !found || result.Result != "immutable new fixture final" || result.DeliveryState != "delivered" {
		t.Fatal(fmt.Sprint(result), e)
	}
}

func TestOwnerRound2FormalEpochReturnRejectsFreshDriftAndMetadataAdoption(t *testing.T) {
	for _, kind := range []string{"metadata_new_revision", "metadata_foreign_thread", "subsequent_parent_drift", "child_profile_drift"} {
		t.Run(kind, func(t *testing.T) {
			h, p, _, _, owner, _ := ownerRound2FormalPair(t)
			ctx := context.Background()
			child, _ := h.registry.Get(owner.ChildSessionID)
			raw, _, e := h.delegations.APIChildMetadata(ctx, child.ID)
			if e != nil {
				t.Fatal(e)
			}
			var metadata apiMetadata
			if e = json.Unmarshal(raw, &metadata); e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "metadata_new_revision":
				metadata.SourceRevision = p.SettingsSnapshot().ConfigRevision
			case "metadata_foreign_thread":
				metadata.SourceThread = "synthetic-foreign-parent-thread"
			case "subsequent_parent_drift":
				snap := p.SettingsSnapshot()
				snap.ConfigRevision++
				p.RestoreFutureConfiguration(snap)
			case "child_profile_drift":
				snap := child.SettingsSnapshot()
				snap.Model = "foreign-model"
				child.RestoreFutureConfiguration(snap)
			}
			if kind == "child_profile_drift" {
				// Child drift blocks NEW execution; stored terminal return does not
				// renew execution authority (SC-QA2-001).
				if _, _, _, e = h.continuityBase(p, owner.ID); e == nil {
					t.Fatal("continuation admitted unapproved child drift")
				}
				if _, _, e = h.formalDelegationReturnSource(ctx, owner, p, metadata); e != nil {
					t.Fatal("old terminal source incorrectly requires executable child", e)
				}
			} else if _, _, e = h.formalDelegationReturnSource(ctx, owner, p, metadata); e == nil {
				t.Fatal("formal epoch admitted unapproved drift", kind)
			}
			after, _, e := h.delegations.APIChildMetadata(ctx, child.ID)
			if e != nil || string(raw) != string(after) {
				t.Fatal("refusal rewrote original metadata")
			}
		})
	}
}
