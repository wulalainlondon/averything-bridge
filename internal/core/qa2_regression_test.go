package core

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

// Task-scoped fixture only: real service admission, temporary stores/actors.
// Synthetic terminal preparation never represents a real sealed runtime final.
func qa2FormalFixture(t *testing.T, mode string) (*Hub, *session.Session, delegation.Record, *atomic.Int32) {
	t.Helper()
	h, caller := capacityHub(t, 1)
	p := caller.Parent
	handoffFixture(t, h, p)
	ctx := context.Background()
	sends := &atomic.Int32{}
	started := make(chan string, 4)
	f := &capacityAppendExecutor{&capacityScopeCapabilityFixture{&fakeExec{sink: h}}}
	f.onSend = func(s *session.Session, req, _ string) {
		if s.ID == p.ID {
			sends.Add(1)
			h.Emit(protocol.NewDone(s.ID, req))
			return
		}
		if mode == "failed_before_native" {
			h.Emit(backend.NewError(s.ID, req, "synthetic_pre_acceptance_failure", "fixture failure before native acceptance"))
		} else if mode == "completed" {
			s.SetResumeID("synthetic-qa2-thread-" + s.ID)
			emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-qa2-turn-" + req})
			h.Emit(protocol.NewDone(s.ID, req))
		}
		started <- req
	}
	h.SetExecutor(f)
	binding := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + p.ID, SourceSessionID: p.ID, SourceResumeID: p.ResumeID(), SourceConfigKnown: true, SourceConfigRevision: p.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool", InvocationGeneration: "qa2-isolated-fixture", SourceRequestID: caller.RequestID, Transport: "native_dynamic_tool"}
	bound, err := taskapi.BindCaller(ctx, apiVerifier{func() (taskapi.VerifiedContext, error) { return binding, nil }}, taskapi.Invocation{})
	if err != nil {
		t.Fatal(err)
	}
	input := map[string]any{"goal": "QA2 formal failure delivery", "instruction": "synthetic only", "new_worker": map[string]any{"name": "QA2 formal fixture", "profile": map[string]string{"backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}}, "scope": map[string]any{"workspace_roots": []string{p.Cwd()}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "workspace": map[string]any{"cwd": p.Cwd(), "input_artifact_ids": []any{}}, "acceptance": []any{}, "dependencies": []any{}}
	create := func(key string) delegation.Record {
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": key, "operation": "create_dispatch", "idempotency_key": key, "input": input})
		response := h.taskService.Execute(ctx, bound, raw)
		if !response.OK {
			t.Fatal(response.Error)
		}
		intent, e := h.delegations.TaskJournal().Task(ctx, binding.StableScopeID, 0, response.Result.(map[string]any)["task_id"].(string))
		if e != nil {
			t.Fatal(e)
		}
		return capacityRecordFor(t, h, intent.NativeID)
	}
	r := create("qa2-formal-first")
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("initial actor did not send")
	}
	if mode == "queued_cancel" || mode == "provision_failure" {
		r = create("qa2-formal-waiting")
		if r.State != "queued" {
			t.Fatal("fixture expected durable waiting round", r)
		}
		if mode == "queued_cancel" {
			if _, e := h.CancelWaitingDelegation(p, r.ID); e != nil {
				t.Fatal(e)
			}
		} else if e := h.delegations.MarkProvisionFailed(ctx, r.ID, "synthetic_provision_refusal_before_native"); e != nil {
			t.Fatal(e)
		}
	} else if mode == "completed" {
		if _, e := h.delegations.Complete(ctx, r.ChildSessionID, r.ChildRequestID, "completed", "immutable synthetic old final", "", nil); e != nil {
			t.Fatal(e)
		}
	}
	return h, p, capacityRecordFor(t, h, r.ID), sends
}

func qa2RequireParentReturn(t *testing.T, h *Hub, p *session.Session, r delegation.Record, sends *atomic.Int32) {
	t.Helper()
	ctx := context.Background()
	raw, _, e := h.delegations.APIChildMetadata(ctx, r.ChildSessionID)
	if e != nil {
		t.Fatal(e)
	}
	thread, revision, bound, e := h.delegations.SourceBinding(ctx, r.ID)
	if e != nil || !bound {
		t.Fatal(e)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e = h.reconcileDelegation(ctx, capacityRecordFor(t, h, r.ID)); e != nil {
			t.Fatal(e)
		}
		current := capacityRecordFor(t, h, r.ID)
		if current.DeliveryState == "failed" {
			proof, accepted, _ := h.messageQueue.ExactNativeAcceptance(r.ChildSessionID, r.ChildRequestID)
			t.Fatalf("original terminal return regressed: terminal=%s error=%s delivery=%s delivery_error=%s native_accepted=%v proof=%+v parent_sends=%d", current.TerminalStatus, current.Error, current.DeliveryState, current.DeliveryError, accepted, proof, sends.Load())
		}
		if current.DeliveryState == "delivered" {
			if current.Result != r.Result || current.Error != r.Error || current.TerminalStatus != r.TerminalStatus {
				t.Fatal("return changed original terminal evidence", current, r)
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	current := capacityRecordFor(t, h, r.ID)
	if current.DeliveryState != "delivered" || sends.Load() != 1 {
		t.Fatal("exact parent return not delivered once", current, sends.Load())
	}
	frozen, found, e := h.delegations.ReturnContext(ctx, current, thread, revision)
	if e != nil || !found || frozen.Snapshot.Status != "available" {
		t.Fatal("missing frozen handoff", found, frozen, e)
	}
	after, _, e := h.delegations.APIChildMetadata(ctx, r.ChildSessionID)
	if e != nil || string(after) != string(raw) {
		t.Fatal("return rewrote immutable metadata", e)
	}
	for i := 0; i < 2; i++ {
		if e = h.reconcileDelegation(ctx, capacityRecordFor(t, h, r.ID)); e != nil {
			t.Fatal(e)
		}
	}
	if sends.Load() != 1 {
		t.Fatal("return replayed")
	}
}

func TestQA2FormalFirstRoundWithoutNativeAcceptanceStillReturns(t *testing.T) {
	for _, mode := range []string{"failed_before_native", "queued_cancel", "provision_failure"} {
		t.Run(mode, func(t *testing.T) {
			h, p, r, sends := qa2FormalFixture(t, mode)
			if _, accepted, e := h.messageQueue.ExactNativeAcceptance(r.ChildSessionID, r.ChildRequestID); e != nil || accepted {
				t.Fatal("fixture unexpectedly has native acceptance", accepted, e)
			}
			if mode == "failed_before_native" {
				if e := h.reconcileDelegation(context.Background(), r); e != nil {
					t.Fatal(e)
				}
				r = capacityRecordFor(t, h, r.ID)
			}
			if r.State != "terminal" || (r.TerminalStatus != "failed" && r.TerminalStatus != "interrupted") {
				t.Fatal("fixture terminal evidence not ready", r)
			}
			qa2RequireParentReturn(t, h, p, r, sends)
		})
	}
}

func TestQA2FormalOldResultReturnsAfterChildClosedOrProfileDrift(t *testing.T) {
	for _, change := range []string{"closed", "profile"} {
		t.Run(change, func(t *testing.T) {
			h, p, r, sends := qa2FormalFixture(t, "completed")
			child, found := h.registry.Get(r.ChildSessionID)
			if !found {
				t.Fatal("fixture child missing")
			}
			if change == "closed" {
				child.CloseAndWait()
			} else {
				deadline := time.Now().Add(3 * time.Second)
				for {
					_, release, e := child.ReserveConfiguration(nil)
					if e == nil {
						snap := child.SettingsSnapshot()
						snap.Model = "synthetic-changed-model"
						child.RestoreFutureConfiguration(snap)
						release()
						break
					}
					if time.Now().After(deadline) {
						t.Fatal(e)
					}
					time.Sleep(time.Millisecond)
				}
			}
			qa2RequireParentReturn(t, h, p, r, sends)
		})
	}
}

func TestQA2TerminalReturnStillRejectsParentSourceDrift(t *testing.T) {
	for _, change := range []string{"thread", "revision", "closed"} {
		t.Run(change, func(t *testing.T) {
			h, p, r, sends := qa2FormalFixture(t, "provision_failure")
			ctx := context.Background()
			raw, _, e := h.delegations.APIChildMetadata(ctx, r.ChildSessionID)
			if e != nil {
				t.Fatal(e)
			}
			switch change {
			case "thread":
				p.SetResumeID("synthetic-foreign-parent")
			case "revision":
				p.SetConfigRevision(p.SettingsSnapshot().ConfigRevision + 1)
			case "closed":
				p.CloseAndWait()
			}
			if e = h.reconcileDelegation(ctx, r); e != nil {
				t.Fatal(e)
			}
			after := capacityRecordFor(t, h, r.ID)
			if after.DeliveryState != "failed" || sends.Load() != 0 || after.TerminalStatus != r.TerminalStatus || after.Result != r.Result || after.Error != r.Error {
				t.Fatal("source drift delivered or changed original result", after, sends.Load())
			}
			if _, found, e := h.messageQueue.Get(p.ID, r.ParentRequestID); e != nil || found {
				t.Fatal("source drift minted return receipt", found, e)
			}
			metadata, _, e := h.delegations.APIChildMetadata(ctx, r.ChildSessionID)
			if e != nil || string(metadata) != string(raw) {
				t.Fatal("source refusal changed original metadata")
			}
		})
	}
}
