package core

import (
	"context"
	"encoding/json"
	"fmt"

	"sync"
	"sync/atomic"
	"testing"
	"time"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

type capacityFinalFixture struct {
	*fakeExec
	ready atomic.Bool
}

func (f *capacityFinalFixture) FinalAnswerForSession(*session.Session, string) (string, bool, error) {
	return "synthetic sealed final", f.ready.Load(), nil
}
func capacityHub(t *testing.T, active int) (*Hub, backend.SessionControlCaller) {
	t.Helper()
	h, _, caller, _ := capacityControllerFixture(t)
	if err := h.delegations.ConfigureCapacity(context.Background(), delegation.CapacityConfig{Default: delegation.Limits{Active: active, Queued: 64, Delivery: 128}}); err != nil {
		t.Fatal(err)
	}
	return h, caller
}
func capacityDelegate(t *testing.T, h *Hub, caller backend.SessionControlCaller, id string) backend.DelegationReceipt {
	t.Helper()
	caller.ToolCallID = id
	r, e := h.DelegateVoiceSession(caller, backend.DelegationSpec{Name: id, Cwd: caller.Parent.Cwd(), Instruction: "Synthetic check"})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func capacityRecordFor(t *testing.T, h *Hub, id string) delegation.Record {
	t.Helper()
	r, found, e := h.delegations.ByID(context.Background(), id)
	if e != nil || !found {
		t.Fatal(found, e)
	}
	return r
}
func TestCapacityHubSixRealStartsSeventhWaitsAndLateFinalDoesNotHoldSlot(t *testing.T) {
	h, caller := capacityHub(t, 6)
	ctx := context.Background()
	started := make(chan string, 16)
	release := make(chan struct{})
	var wg sync.WaitGroup
	hold := make(chan struct{})
	caller.Parent.SubmitNamed("fixture-hold", func() { close(hold) })
	<-hold
	f := &capacityFinalFixture{fakeExec: &fakeExec{sink: h}}
	f.onSend = func(s *session.Session, req, _ string) {
		wg.Add(1)
		defer wg.Done()
		started <- req
		<-release
		h.Emit(protocol.NewDone(s.ID, req))
	}
	h.SetExecutor(f)
	records := []backend.DelegationReceipt{}
	for i := 0; i < 8; i++ {
		records = append(records, capacityDelegate(t, h, caller, fmt.Sprint(i)))
	}
	t.Cleanup(func() { close(release); wg.Wait(); caller.Parent.EndTurn() })
	if e := h.reconcileDelegations(ctx); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 6; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("reserved child did not start")
		}
	}
	select {
	case id := <-started:
		t.Fatal("seventh child exceeded capacity", id)
	default:
	}
	if _, found := h.registry.Get(records[6].ChildSessionID); found {
		t.Fatal("waiting task provisioned native child")
	}
	// Exact synthetic native terminal; final reader intentionally not ready.
	h.Emit(protocol.NewDone(records[0].ChildSessionID, records[0].ChildRequestID))
	if e := h.reconcileDelegations(ctx); e != nil {
		t.Fatal(e)
	}
	if got := capacityRecordFor(t, h, records[0].ID); got.State != "collecting" || got.Result != "" {
		t.Fatal(got)
	}
	select {
	case id := <-started:
		if id != records[6].ChildRequestID {
			t.Fatal(id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late seal retained execution slot")
	}
	if got := capacityRecordFor(t, h, records[7].ID); got.State != "queued" {
		t.Fatal(got)
	}
	f.ready.Store(true)
	if e := h.reconcileDelegations(ctx); e != nil {
		t.Fatal(e)
	}
	if got := capacityRecordFor(t, h, records[0].ID); got.State != "terminal" || got.Result != "synthetic sealed final" {
		t.Fatal(got)
	}
}
func TestCapacityHubStaleSnapshotCannotStartCancelledWork(t *testing.T) {
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	first := capacityDelegate(t, h, caller, "first")
	waiting := capacityDelegate(t, h, caller, "waiting")
	stale := capacityRecordFor(t, h, waiting.ID)
	if _, e := h.CancelWaitingDelegation(caller.Parent, waiting.ID); e != nil {
		t.Fatal(e)
	}
	h.delegations.MarkProvisionFailed(ctx, first.ID, "fixture-before-effect")
	if e := h.provisionDelegation(ctx, stale); e != nil {
		t.Fatal(e)
	}
	if _, found := h.registry.Get(waiting.ChildSessionID); found {
		t.Fatal("stale cancelled snapshot created child")
	}
	if _, found, e := h.messageQueue.Get(waiting.ChildSessionID, waiting.ChildRequestID); e != nil || found {
		t.Fatal(found, e)
	}
	if _, e := h.CancelWaitingDelegation(caller.Parent, waiting.ID); e != nil {
		t.Fatal("cancel not idempotent", e)
	}
	other := h.registry.Create("other", "Other", caller.Parent.Cwd(), backend.Codex, "", "workspace-write", "other-thread")
	if _, e := h.CancelWaitingDelegation(other, waiting.ID); e == nil {
		t.Fatal("foreign parent cancelled record")
	}
}
func TestCapacityHubQueuedAuthorityPreservedButChangedBindingFailsClosed(t *testing.T) {
	for _, change := range []string{"unchanged", "thread", "configuration"} {
		t.Run(change, func(t *testing.T) {
			h, caller := capacityHub(t, 1)
			ctx := context.Background()
			sent := make(chan struct{})
			h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, _ string) {
				h.Emit(protocol.NewDone(s.ID, req))
				close(sent)
			}})

			first := capacityDelegate(t, h, caller, "first")
			wait := capacityDelegate(t, h, caller, "wait")
			h.delegations.MarkProvisionFailed(ctx, first.ID, "fixture-before-effect")
			switch change {
			case "thread":
				caller.Parent.SetResumeID("changed-thread")
			case "configuration":
				caller.Parent.ApplyConfig(backend.Codex, "changed-model", "read-only")
			}
			e := h.provisionDelegation(ctx, capacityRecordFor(t, h, wait.ID))
			_, exists := h.registry.Get(wait.ChildSessionID)
			if change == "unchanged" {
				if e != nil || !exists {
					t.Fatal("accepted commitment lost for original binding", e, exists)
				}
				select {
				case <-sent:
				case <-time.After(2 * time.Second):
					t.Fatal("fixture Send did not complete")
				}

			} else {
				if e == nil || exists {
					t.Fatal("changed binding executed", e, exists)
				}
			}
		})
	}
}
func TestCapacityHubCapabilitiesMatchParentConfigurationAndFrozenSchemas(t *testing.T) {
	h, caller := capacityHub(t, 2)
	ctx := context.Background()
	for _, version := range []string{contract.Version} {
		result, e := h.Read(ctx, taskapi.AuthorizedCommand{Caller: taskapi.VerifiedContext{SourceSessionID: caller.Parent.ID}, Request: taskapi.Request{Version: version, Operation: "capabilities", Input: json.RawMessage(`{}`)}})
		if e != nil {
			t.Fatal(e)
		}
		limits := result.(map[string]any)["limits"].(map[string]int)
		if limits["max_pending_children"] != 2 {
			t.Fatal(limits)
		}
		compiler, e := contract.New()
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := json.Marshal(result)
		if e = compiler.Validate(raw, "Capabilities"); e != nil {
			t.Fatal(e)
		}
	}
	capacity, e := h.DelegationCapacity(caller.Parent)
	if e != nil || capacity["max_active"] != 2 || capacity["max_queued"] != 64 || capacity["max_delivery_reserved"] != 128 {
		t.Fatal(capacity, e)
	}
}

func TestCapacityHubActualAPIQueuedCancelExactReceiptAndReplay(t *testing.T) {
	if !apiOwnedAdmissionEnabled {
		t.Skip("owned admission deliberately disabled in compatible hold build")
	}
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	root := caller.Parent.Cwd()
	h.SetExecutor(&capacityScopeCapabilityFixture{fakeExec: &fakeExec{sink: h}})
	binding := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + caller.Parent.ID, SourceSessionID: caller.Parent.ID, SourceResumeID: caller.Parent.ResumeID(), SourceConfigKnown: true, SourceConfigRevision: caller.Parent.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool", InvocationGeneration: "synthetic-capacity", SourceRequestID: caller.RequestID, Transport: "native_dynamic_tool"}
	bound, e := taskapi.BindCaller(ctx, apiVerifier{func() (taskapi.VerifiedContext, error) { return binding, nil }}, taskapi.Invocation{})
	if e != nil {
		t.Fatal(e)
	}
	invoke := func(op, key string, input any) taskapi.Response {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-capacity", "operation": op, "idempotency_key": "capacity-" + key, "input": input})
		response := h.taskService.Execute(ctx, bound, raw)
		compiler, _ := contract.New()
		encoded, _ := json.Marshal(response)
		if e := compiler.Validate(encoded, "Response"); e != nil {
			t.Fatalf("invalid response %s: %v", encoded, e)
		}
		return response
	}
	input := map[string]any{"goal": "Synthetic capacity", "instruction": "Synthetic check", "new_worker": map[string]any{"name": "Child", "profile": map[string]string{"backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}}, "scope": map[string]any{"workspace_roots": []string{root}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "workspace": map[string]any{"cwd": root, "input_artifact_ids": []any{}}, "acceptance": []any{}, "dependencies": []any{}}
	first := invoke("create_dispatch", "first", input)
	if !first.OK {
		t.Fatalf("first response: %+v", first.Error)
	}
	waiting := invoke("create_dispatch", "waiting", input)
	if !waiting.OK {
		t.Fatal(waiting.Error)
	}
	receipt := waiting.Result.(map[string]any)
	taskID := receipt["task_id"].(string)
	intent, e := h.delegations.TaskJournal().Task(ctx, binding.StableScopeID, 0, taskID)
	if e != nil {
		t.Fatal(e)
	}
	if r := capacityRecordFor(t, h, intent.NativeID); r.State != "queued" {
		t.Fatal(r)
	}
	if _, found := h.registry.Get(intent.SessionID); found {
		t.Fatal("API wait created native session")
	}
	if axes := receipt["axes"].(map[string]string); axes["admission"] != "queued" || axes["execution"] != "not_started" {
		t.Fatal(axes)
	}
	replay := invoke("create_dispatch", "waiting", input)
	if !replay.OK || replay.Result.(map[string]any)["receipt_id"] != receipt["receipt_id"] {
		t.Fatal("accepted queue replay lost", replay)
	}
	cancelInput := map[string]any{"task_id": taskID, "command_receipt_id": receipt["receipt_id"], "expected_revision": receipt["task_revision"], "target": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": caller.Parent.ID, "expected_native_thread_id": caller.Parent.ResumeID(), "expected_config_revision": caller.Parent.SettingsSnapshot().ConfigRevision}, "mode": "queued"}
	cancelled := invoke("cancel", "cancel-waiting", cancelInput)
	if !cancelled.OK {
		t.Fatal(cancelled.Error)
	}
	again := invoke("cancel", "cancel-waiting", cancelInput)
	if !again.OK || again.Result.(map[string]any)["receipt_id"] != cancelled.Result.(map[string]any)["receipt_id"] {
		t.Fatal(again)
	}
	if axes := cancelled.Result.(map[string]any)["axes"].(map[string]string); axes["cancellation"] != "confirmed" || axes["execution"] != "not_started" {
		t.Fatal(axes)
	}
	if _, found, e := h.messageQueue.Get(intent.SessionID, intent.RequestID); e != nil || found {
		t.Fatal("cancel executed native request", found, e)
	}
}

type capacityAppendExecutor struct {
	*capacityScopeCapabilityFixture
}

func (*capacityAppendExecutor) ValidateExistingTaskScope(*session.Session, taskapi.ChildScope, string) error {
	return nil
}
func TestCapacityHubAPIAppendUsesReservationAndOriginalNativeThread(t *testing.T) {
	if !apiOwnedAdmissionEnabled {
		t.Skip("owned admission deliberately disabled in compatible hold build")
	}
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	root := caller.Parent.Cwd()
	started := make(chan string, 8)
	h.SetExecutor(&capacityAppendExecutor{&capacityScopeCapabilityFixture{&fakeExec{sink: h, onSend: func(s *session.Session, req, _ string) {
		s.SetResumeID("synthetic-child-thread")
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-turn-" + req})
		started <- req
	}}}})
	binding := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + caller.Parent.ID, SourceSessionID: caller.Parent.ID, SourceResumeID: caller.Parent.ResumeID(), SourceConfigKnown: true, SourceConfigRevision: caller.Parent.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool", InvocationGeneration: "synthetic-append-capacity", SourceRequestID: caller.RequestID, Transport: "native_dynamic_tool"}
	bound, e := taskapi.BindCaller(ctx, apiVerifier{func() (taskapi.VerifiedContext, error) { return binding, nil }}, taskapi.Invocation{})
	if e != nil {
		t.Fatal(e)
	}
	invoke := func(op, key string, input any) taskapi.Response {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-append", "operation": op, "idempotency_key": "capacity-" + key, "input": input})
		return h.taskService.Execute(ctx, bound, raw)
	}
	input := map[string]any{"goal": "Synthetic capacity", "instruction": "Synthetic check", "new_worker": map[string]any{"name": "Child", "profile": map[string]string{"backend": "codex", "model": "gpt-6.1-sol", "effort": "high"}}, "scope": map[string]any{"workspace_roots": []string{root}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "workspace": map[string]any{"cwd": root, "input_artifact_ids": []any{}}, "acceptance": []any{}, "dependencies": []any{}}
	created := invoke("create_dispatch", "original", input)
	if !created.OK {
		t.Fatal(created.Error)
	}
	receipt := created.Result.(map[string]any)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first not started")
	}
	taskID := receipt["task_id"].(string)
	intent, e := h.delegations.TaskJournal().Task(ctx, binding.StableScopeID, 0, taskID)
	if e != nil {
		t.Fatal(e)
	}
	task, e := h.apiTask(ctx, intent)
	if e != nil {
		t.Fatal(e)
	}
	child, ok := h.registry.Get(intent.SessionID)
	if !ok {
		t.Fatal("child missing")
	}
	appendInput := map[string]any{"task_id": taskID, "expected_revision": task["revision"], "target": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": child.ID, "expected_native_thread_id": child.ResumeID(), "expected_config_revision": child.SettingsSnapshot().ConfigRevision}, "mode": "queue", "content": "Synthetic follow-up"}
	appended := invoke("append", "follow-up", appendInput)
	if !appended.OK {
		t.Fatal(appended.Error)
	}
	appendReceipt := appended.Result.(map[string]any)
	request := appendReceipt["effect_request_ids"].([]string)[0]
	waiting, found, e := h.delegations.ByChildRequest(ctx, child.ID, request)
	if e != nil || !found || waiting.State != "queued" || waiting.DeliveryState != "not_requested" {
		t.Fatal(waiting, found, e)
	}
	if _, found, e := h.messageQueue.Get(child.ID, request); e != nil || found {
		t.Fatal("append bypassed capacity into message queue", found, e)
	}
	// The ordinary-message route cannot bypass the same reservation.
	client := newTestClient(h)
	if h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: child.ID, RequestID: "r_bypass_capacity", Content: "Synthetic bypass"}) {
		t.Fatal("bare child message bypassed capacity")
	}
	first := capacityRecordFor(t, h, intent.NativeID)
	if _, e := h.delegations.Complete(ctx, child.ID, first.ChildRequestID, "completed", "synthetic original final", "", nil); e != nil {
		t.Fatal(e)
	}
	child.EndTurn()
	if e := h.provisionDelegation(ctx, waiting); e != nil {
		t.Fatal(e)
	}
	select {
	case got := <-started:
		if got != request {
			t.Fatal("wrong continuation", got, request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("continuation did not start")
	}
	if child.ResumeID() != "synthetic-child-thread" {
		t.Fatal("native thread changed")
	}
	// Replaying the old append key cannot create a second queue record/turn.
	replay := invoke("append", "follow-up", appendInput)
	if !replay.OK || replay.Result.(map[string]any)["receipt_id"] != appendReceipt["receipt_id"] {
		t.Fatal(replay)
	}
	select {
	case duplicate := <-started:
		t.Fatal("duplicate continuation", duplicate)
	default:
	}
	child.EndTurn()
}

func TestCapacityHubConcurrentNativeSameOriginOnePromise(t *testing.T) {
	h, caller := capacityHub(t, 1)
	spec := backend.DelegationSpec{Name: "One intent", Cwd: caller.Parent.Cwd(), Instruction: "Synthetic immutable request"}
	var wg sync.WaitGroup
	receipts := make(chan backend.DelegationReceipt, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := h.DelegateVoiceSession(caller, spec)
			if e != nil {
				t.Error(e)
				return
			}
			receipts <- r
		}()
	}
	wg.Wait()
	close(receipts)
	id := ""
	n := 0
	for r := range receipts {
		n++
		if id == "" {
			id = r.ID
		}
		if id != r.ID {
			t.Fatal("duplicate promise", id, r.ID)
		}
	}
	if n != 32 {
		t.Fatal("missing receipt", n)
	}
	counts, e := h.delegations.CountsForParent(context.Background(), caller.Parent.ID)
	if e != nil || counts.Active != 1 || counts.Queued != 0 || counts.Reserved != 1 {
		t.Fatal(counts, e)
	}
	spec.Instruction = "Changed intent"
	if _, e := h.DelegateVoiceSession(caller, spec); e == nil {
		t.Fatal("same origin changed intent accepted")
	}
}

func TestCapacityHubActorAdmissionFailureDoesNotLeakSlot(t *testing.T) {
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	receipt := capacityDelegate(t, h, caller, "actor-failure")
	h.registry.Create(receipt.ChildSessionID, "Synthetic", caller.Parent.Cwd(), backend.Codex, "", "workspace-write", "")
	if _, _, e := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: receipt.ChildSessionID, RequestID: receipt.ChildRequestID, Content: "Synthetic", Payload: json.RawMessage(`{}`)}); e != nil {
		t.Fatal(e)
	}
	if _, _, e := h.messageQueue.Transition(receipt.ChildSessionID, receipt.ChildRequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Failed, "Queue is full or session is closed", "", ""); e != nil {
		t.Fatal(e)
	}
	if e := h.delegations.MarkRunning(ctx, receipt.ID); e != nil {
		t.Fatal(e)
	}
	if e := h.reconcileDelegations(ctx); e != nil {
		t.Fatal(e)
	}
	r := capacityRecordFor(t, h, receipt.ID)
	if r.State != "terminal" || r.TerminalStatus != "failed" || r.Error != "child_actor_admission_failed" {
		t.Fatal(r)
	}
	counts, e := h.delegations.CountsForParent(ctx, caller.Parent.ID)
	if e != nil || counts.Active != 0 {
		t.Fatal(counts, e)
	}
}
