package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

type continuityExec struct {
	*fakeExec
	mu     sync.Mutex
	finals map[string]string
	sends  map[string]int
}

func (f *continuityExec) FinalAnswerForSession(_ *session.Session, req string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.finals[req]
	return r, ok, nil
}
func continuityFixture(t *testing.T) (*Hub, *session.Session, *continuityExec) {
	t.Helper()
	h, c := capacityHub(t, 6)
	p := c.Parent
	f := &continuityExec{fakeExec: &fakeExec{sink: h}, finals: map[string]string{}, sends: map[string]int{}}
	f.onSend = func(s *session.Session, req, content string) {
		if s.ResumeID() == "" {
			s.SetResumeID("synthetic-thread-" + s.ID)
		}
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-turn-" + req})
		f.mu.Lock()
		f.finals[req] = content
		f.sends[req]++
		f.mu.Unlock()
		h.Emit(protocol.NewDone(s.ID, req))
	}
	h.SetExecutor(f)
	continuityParentTurn(t, p, "r_parent_one")
	handoffFixture(t, h, p)
	return h, p, f
}
func continuityParentTurn(t *testing.T, p *session.Session, req string) {
	t.Helper()
	started := make(chan struct{})
	if !p.SubmitNamed(req, func() { close(started) }) {
		t.Fatal("parent not admitted")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("parent not started")
	}
	t.Cleanup(func() { p.EndTurn() })
}
func continuityWait(t *testing.T, h *Hub, id, state string) delegation.Record {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r := capacityRecordFor(t, h, id)
		if e := h.reconcileDelegation(context.Background(), r); e != nil {
			t.Fatal(e)
		}
		r = capacityRecordFor(t, h, id)
		if r.State == state {
			return r
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("round %s never %s: %+v", id, state, capacityRecordFor(t, h, id))
	return delegation.Record{}
}
func continuityDelegate(t *testing.T, h *Hub, p *session.Session, call, final string) (backend.DelegationReceipt, delegation.Record) {
	t.Helper()
	r, e := h.DelegateSession(p, p.ActiveQueuedID(), call, backend.DelegationSpec{Name: call, Instruction: final})
	if e != nil {
		t.Fatal(e)
	}
	return r, continuityWait(t, h, r.ID, "terminal")
}
func continuityInput(r delegation.Record, p *session.Session, h *Hub, content string) backend.DelegationContinuation {
	child, _ := h.registry.Get(r.ChildSessionID)
	return backend.DelegationContinuation{DelegationID: r.ID, ExpectedPreviousID: r.ID, InstanceID: h.cfg.InstanceID, ExpectedThreadID: child.ResumeID(), ExpectedConfigRevision: child.SettingsSnapshot().ConfigRevision, Content: content, Mode: "queue"}
}
func continuityContinue(t *testing.T, h *Hub, p *session.Session, call string, in backend.DelegationContinuation) delegation.Record {
	t.Helper()
	receipt, e := h.ContinueDelegation(p, p.ActiveQueuedID(), call, in)
	if e != nil {
		t.Fatal(e)
	}
	return continuityWait(t, h, receipt.(map[string]any)["delegation_id"].(string), "terminal")
}
func continuityDeliver(t *testing.T, h *Hub, p *session.Session, records ...delegation.Record) {
	t.Helper()
	p.EndTurn()
	for _, r := range records {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if e := h.reconcileDelegation(context.Background(), capacityRecordFor(t, h, r.ID)); e != nil {
				t.Fatal(e)
			}
			if capacityRecordFor(t, h, r.ID).DeliveryState == "delivered" {
				break
			}
			time.Sleep(time.Millisecond)
		}
		got := capacityRecordFor(t, h, r.ID)
		if got.DeliveryState != "delivered" {
			t.Fatalf("return not delivered: %+v", got)
		}
		frozen, found, e := h.delegations.ReturnContext(context.Background(), got, p.ResumeID(), p.SettingsSnapshot().ConfigRevision)
		if e != nil || !found || frozen.Snapshot.Status != "available" || !strings.Contains(frozen.Content, got.Result) {
			t.Fatal("round handoff not frozen", frozen, found, e)
		}
	}
}
func TestContinuitySyntheticOwnerCompletedQAFailSameOwnerSameQAPass(t *testing.T) {
	h, p, f := continuityFixture(t)
	owner, first := continuityDelegate(t, h, p, "owner", "owner version one")
	failText := fmt.Sprintf(`{"owner_round_id":%q,"verdict":"FAIL"}`, owner.ID)
	qa, qafail := continuityDelegate(t, h, p, "independent-qa", failText)
	if _, e := h.BindDelegationQA(p, p.ActiveQueuedID(), "bind", owner.ID, qa.ID); e != nil {
		t.Fatal(e)
	}
	if _, e := h.RecordDelegationReview(p, p.ActiveQueuedID(), "review-fail", owner.ID, qa.ID); e != nil {
		t.Fatal(e)
	}
	ownerThread, _, _, _ := h.delegations.NativeThread(context.Background(), owner.ChildSessionID)
	qaThread, _, _, _ := h.delegations.NativeThread(context.Background(), qa.ChildSessionID)
	continuityDeliver(t, h, p, first, qafail)
	continuityParentTurn(t, p, "r_parent_correction")
	fixed := continuityContinue(t, h, p, "fix-owner", continuityInput(first, p, h, "owner version two"))
	passText := fmt.Sprintf(`{"owner_round_id":%q,"verdict":"PASS"}`, fixed.ID)
	checked := continuityContinue(t, h, p, "recheck-qa", continuityInput(qafail, p, h, passText))
	if _, e := h.RecordDelegationReview(p, p.ActiveQueuedID(), "review-pass", fixed.ID, checked.ID); e != nil {
		t.Fatal(e)
	}
	if fixed.ChildSessionID != owner.ChildSessionID || checked.ChildSessionID != qa.ChildSessionID || fixed.ChildRequestID == first.ChildRequestID || checked.ChildRequestID == qafail.ChildRequestID {
		t.Fatal("session replaced or request overwritten")
	}
	if child, _ := h.registry.Get(owner.ChildSessionID); child.ResumeID() != ownerThread {
		t.Fatal("owner thread changed")
	}
	if child, _ := h.registry.Get(qa.ChildSessionID); child.ResumeID() != qaThread {
		t.Fatal("QA thread changed")
	}
	continuityDeliver(t, h, p, fixed, checked)
	_, fail, e := h.delegations.QAForRound(context.Background(), first.ID)
	if e != nil || fail != "FAIL" {
		t.Fatal("FAIL overwritten", fail, e)
	}
	_, pass, e := h.delegations.QAForRound(context.Background(), fixed.ID)
	if e != nil || pass != "PASS" {
		t.Fatal(pass, e)
	}
	if got := capacityRecordFor(t, h, first.ID); got.Result != "owner version one" || got.ChildRequestID != first.ChildRequestID {
		t.Fatal("original result overwritten")
	}
	rounds, e := h.delegations.Rounds(context.Background(), owner.ID)
	if e != nil || len(rounds) != 2 || rounds[1].PreviousID != first.ID {
		t.Fatal(rounds, e)
	}
	counts, e := h.delegations.CountsForParent(context.Background(), p.ID)
	if e != nil || counts != (delegation.Counts{}) {
		t.Fatal("capacity leak", counts, e)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for req, n := range f.sends {
		if n != 1 {
			t.Fatal("duplicate native send", req, n)
		}
	}
	t.Logf("SYNTHETIC ONLY owner session=%s thread=%s QA session=%s thread=%s requests=%s,%s,%s,%s FAIL/PASS preserved; four frozen parent handoffs; capacity=%+v", owner.ChildSessionID, ownerThread, qa.ChildSessionID, qaThread, first.ChildRequestID, qafail.ChildRequestID, fixed.ChildRequestID, checked.ChildRequestID, counts)
}
func TestContinuityRejectsForeignDriftArchivedMissingAndSelfQA(t *testing.T) {
	for _, change := range []string{"foreign-parent", "foreign-authority", "parent-thread", "parent-config", "thread", "sandbox", "model", "effort", "backend", "closed", "hidden", "native-conflict", "missing-native"} {
		t.Run(change, func(t *testing.T) {
			h, p, _ := continuityFixture(t)
			_, r := continuityDelegate(t, h, p, "owner", "version one")
			in := continuityInput(r, p, h, "fix")
			child, _ := h.registry.Get(r.ChildSessionID)
			// Reserve the real configuration/dequeue boundary. A terminal event can
			// precede the actor's future-config restoration; an unreserved raw mutation
			// there can be legitimately restored before admission and is not stable drift.
			deadline := time.Now().Add(3 * time.Second)
			var release func()
			for time.Now().Before(deadline) {
				_, done, e := child.ReserveConfiguration(nil)
				if e == nil {
					release = done
					break
				}
				time.Sleep(time.Millisecond)
			}
			if release == nil {
				t.Fatal("child configuration boundary remained busy")
			}
			switch change {
			case "foreign-parent":
				p = h.registry.Create("foreign-parent", "Foreign", p.Cwd(), backend.Codex, "", "workspace-write", "foreign-thread")
				continuityParentTurn(t, p, "r_foreign")
			case "foreign-authority":
				in.InstanceID = "foreign-instance"
			case "parent-thread":
				p.SetResumeID("foreign-parent-thread")
			case "parent-config":
				p.SetConfigRevision(5)
			case "closed":
				child.CloseAndWait()
			case "hidden":
				hidden := true
				child.SetMeta(nil, &hidden)
			case "native-conflict":
				_ = h.messageQueue.RecordNativeAcceptance(child.ID, r.ChildRequestID, "foreign-native", "foreign-turn")
			case "missing-native": // A fresh unstarted delegation cannot manufacture a thread.
				receipt, e := h.DelegateSession(p, p.ActiveQueuedID(), "unstarted", backend.DelegationSpec{Name: "unstarted", Instruction: "check"})
				if e != nil {
					t.Fatal(e)
				}
				in.DelegationID = receipt.ID
				in.ExpectedPreviousID = receipt.ID
			default:
				correctionMutateChild(t, child, change)
			}
			release()
			before, e := h.delegations.CountsForParent(context.Background(), p.ID)
			if e != nil {
				t.Fatal(e)
			}
			if got, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "bad-fix", in); e == nil {
				t.Fatal("unsafe continuation accepted", got)
			}
			after, e := h.delegations.CountsForParent(context.Background(), p.ID)
			if e != nil || before != after {
				t.Fatal("refusal leaked reservation", before, after, e)
			}
		})
	}
	h, p, _ := continuityFixture(t)
	owner, _ := continuityDelegate(t, h, p, "owner", "version one")
	if _, e := h.BindDelegationQA(p, p.ActiveQueuedID(), "self-qa", owner.ID, owner.ID); e == nil {
		t.Fatal("self QA claimed independent")
	}
	worker, _ := h.registry.Get(owner.ChildSessionID)
	if _, e := h.ContinueDelegation(worker, "r_worker", "worker-call", backend.DelegationContinuation{}); e == nil {
		t.Fatal("worker gained global parent powers")
	}
}
func TestContinuityConcurrentDuplicateAndStaleRounds(t *testing.T) {
	for _, same := range []bool{true, false} {
		t.Run(fmt.Sprint(same), func(t *testing.T) {
			h, p, _ := continuityFixture(t)
			_, r := continuityDelegate(t, h, p, "owner", "one")
			in := continuityInput(r, p, h, "two")
			var wg sync.WaitGroup
			var successes atomic.Int32
			var mu sync.Mutex
			ids := map[string]bool{}
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					call := "same-call"
					if !same {
						call = fmt.Sprint("different-", i)
					}
					got, e := h.ContinueDelegation(p, p.ActiveQueuedID(), call, in)
					if e == nil {
						successes.Add(1)
						mu.Lock()
						ids[got.(map[string]any)["delegation_id"].(string)] = true
						mu.Unlock()
					}
				}(i)
			}
			wg.Wait()
			if len(ids) != 1 || (same && successes.Load() != 16) || (!same && successes.Load() != 1) {
				t.Fatal("duplicate or lost receipt", ids, successes.Load())
			}
			rounds, e := h.delegations.Rounds(context.Background(), r.ID)
			if e != nil || len(rounds) != 2 {
				t.Fatal(rounds, e)
			}
			if same {
				in.Content = "conflict"
				if _, e = h.ContinueDelegation(p, p.ActiveQueuedID(), "same-call", in); e == nil {
					t.Fatal("intent conflict accepted")
				}
			}
		})
	}
}
func TestContinuityRecoveredReceiptKeepsOriginalRoundReturnAndContext(t *testing.T) {
	h, p, f := continuityFixture(t)
	_, r := continuityDelegate(t, h, p, "owner", "one")
	in := continuityInput(r, p, h, "two")
	receipt, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "fix-once", in)
	if e != nil {
		t.Fatal(e)
	}
	id := receipt.(map[string]any)["delegation_id"].(string)
	// Original ACK exists but execution has not started. Reopen real stores and
	// registry, recover same request, retain the exact native thread and sandbox.
	request := p.ActiveQueuedID()
	correctionReopen(t, h)
	p, _ = h.registry.Get(p.ID)
	continuityParentTurn(t, p, request)
	retry, e := h.ContinueDelegation(p, request, "fix-once", in)
	if e != nil {
		t.Fatal(e)
	}
	if retry.(map[string]any)["delegation_id"] != id {
		t.Fatal("unknown acceptance reminted round")
	}
	h.resumeQueuedMessages()
	fixed := continuityWait(t, h, id, "terminal")
	continuityDeliver(t, h, p, r, fixed)
	if _, e := h.ContinueDelegation(p, request, "fix-once", in); e == nil {
		t.Fatal("inactive parent still mutated")
	}
	counts, e := h.delegations.CountsForParent(context.Background(), p.ID)
	if e != nil || counts.Reserved != 0 {
		t.Fatal(counts, e)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.sends[fixed.ChildRequestID] != 1 || f.sends[fixed.ParentRequestID] != 1 {
		t.Fatal("restart duplicate send", f.sends)
	}
}
func TestContinuityRoundMetadataDoesNotConflateExecutionQAAndDelivery(t *testing.T) {
	h, p, _ := continuityFixture(t)
	_, r := continuityDelegate(t, h, p, "owner", "one")
	result, e := h.DelegationRounds(p, r.ID, 0)
	if e != nil {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(result)
	for _, want := range []string{`"execution":"terminal"`, `"delivery":"pending"`, `"qa_verdict":""`, `"round_version":1`, `"dispatcher_session_id"`} {
		if !strings.Contains(string(raw), want) {
			t.Fatal(want, string(raw))
		}
	}
}

func TestContinuityExplicitSteerSharesActiveRoundNeverFallsBackOrReplays(t *testing.T) {
	for _, outcome := range []string{"accepted", "rejected", "unknown", "wrong-execution"} {
		t.Run(outcome, func(t *testing.T) {
			h, p, f := continuityFixture(t)
			var ownerReq string
			var started atomic.Bool
			f.onSend = func(s *session.Session, req, content string) {
				if s.ID == p.ID {
					h.Emit(protocol.NewDone(s.ID, req))
					return
				}
				if s.ResumeID() == "" {
					s.SetResumeID("synthetic-active-thread")
				}
				emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: s.ResumeID(), TurnID: "synthetic-active-turn"})
				f.mu.Lock()
				f.finals[req] = "synthetic final including supplement"
				f.sends[req]++
				f.mu.Unlock()
				ownerReq = req
				started.Store(true)
			}
			owner, e := h.DelegateSession(p, p.ActiveQueuedID(), "owner-active", backend.DelegationSpec{Name: "Owner", Instruction: "work"})
			if e != nil {
				t.Fatal(e)
			}
			if e = h.reconcileDelegation(context.Background(), capacityRecordFor(t, h, owner.ID)); e != nil {
				t.Fatal(e)
			}
			deadline := time.Now().Add(time.Second)
			for !started.Load() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if !started.Load() {
				t.Fatal("owner not active")
			}
			var rpcCalls atomic.Int32
			f.onSteer = func(_ *session.Session, req, content string) (backend.SteerResult, error) {
				rpcCalls.Add(1)
				switch outcome {
				case "rejected":
					return backend.SteerResult{}, backend.ErrSteerRejected
				case "unknown":
					return backend.SteerResult{}, fmt.Errorf("synthetic transport lost ACK")
				}
				turn := "synthetic-active-turn"
				if outcome == "wrong-execution" {
					turn = "foreign-turn"
				}
				return backend.SteerResult{RequestID: ownerReq, TurnID: turn, StrictACK: true}, nil
			}
			record := capacityRecordFor(t, h, owner.ID)
			in := continuityInput(record, p, h, "explicit supplement")
			in.Mode = "steer"
			in.ActiveRequestID = ownerReq
			got, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "steer-once", in)
			if e != nil {
				t.Fatal(e)
			}
			id := got.(map[string]any)["delegation_id"].(string)
			r := capacityRecordFor(t, h, id)
			if e = h.reconcileDelegation(context.Background(), r); e != nil && (outcome != "wrong-execution" || e.Error() != "delegation_steer_native_execution_conflict") {
				t.Fatal(e)
			}
			want := messagequeue.Steered
			if outcome == "rejected" {
				want = messagequeue.Queued
			}
			if outcome == "unknown" {
				want = messagequeue.Uncertain
			}
			correctionWaitState(t, h, r.ChildSessionID, r.ChildRequestID, want)
			counts, e := h.delegations.CountsForParent(context.Background(), p.ID)
			if e != nil || counts.Active != 1 || counts.Reserved != 2 {
				t.Fatal("steer double counted execution", counts, e)
			}
			for i := 0; i < 3; i++ {
				retry, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "steer-once", in)
				if e != nil || retry.(map[string]any)["delegation_id"] != id {
					t.Fatal(retry, e)
				}
				e = h.reconcileDelegation(context.Background(), capacityRecordFor(t, h, id))
				if outcome == "wrong-execution" {
					if e == nil {
						t.Fatal("foreign steer ACK claimed exact execution")
					}
				} else if e != nil {
					t.Fatal(e)
				}
			}
			if rpcCalls.Load() != 1 {
				t.Fatal("steer replayed", rpcCalls.Load())
			}
			child, _ := h.registry.Get(owner.ChildSessionID)
			h.Emit(protocol.NewDone(child.ID, ownerReq))
			if outcome == "accepted" {
				final := continuityWait(t, h, id, "terminal")
				if final.Result != "synthetic final including supplement" {
					t.Fatal(final)
				}
			} else if outcome == "rejected" {
				final := continuityWait(t, h, id, "terminal")
				if final.TerminalStatus != "failed" {
					t.Fatal("steer silently queued as turn", final)
				}
			} else if outcome == "unknown" {
				correctionReopen(t, h)
				p, _ = h.registry.Get(p.ID)
				h.resumeQueuedMessages()
				for i := 0; i < 3; i++ {
					if e = h.reconcileDelegation(context.Background(), capacityRecordFor(t, h, id)); e != nil {
						t.Fatal(e)
					}
				}
				entry, _, e := h.messageQueue.Get(child.ID, r.ChildRequestID)
				if e != nil || entry.State != messagequeue.Uncertain || rpcCalls.Load() != 1 {
					t.Fatal("unknown steer replay after restart", entry, rpcCalls.Load(), e)
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.sends[r.ChildRequestID] != 0 || f.sends[ownerReq] != 1 {
				t.Fatal("steer created another native turn", f.sends)
			}
		})
	}
}
func TestContinuityReadOriginalReceiptAfterChildDeleted(t *testing.T) {
	h, p, _ := continuityFixture(t)
	_, r := continuityDelegate(t, h, p, "owner", "one")
	in := continuityInput(r, p, h, "two")
	receipt, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "fix-once", in)
	if e != nil {
		t.Fatal(e)
	}
	h.registry.Delete(r.ChildSessionID)
	read, e := h.DelegationRounds(p, r.ID, 0)
	if e != nil || len(read.(map[string]any)["rounds"].([]any)) != 2 {
		t.Fatal("receipt inaccessible after delete", read, e)
	}
	retry, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "fix-once", in)
	if e != nil || retry.(map[string]any)["delegation_id"] != receipt.(map[string]any)["delegation_id"] {
		t.Fatal("accepted receipt disappeared", retry, e)
	}
	in.ExpectedPreviousID = receipt.(map[string]any)["delegation_id"].(string)
	if _, e = h.ContinueDelegation(p, p.ActiveQueuedID(), "new-after-delete", in); e == nil {
		t.Fatal("deleted worker revived")
	}
}

func TestContinuityAPINativeSteerRetainsFormalScopeButQueueUsesExistingAppend(t *testing.T) {
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
	beforeScope, e := h.TaskWorkerScope(child)
	if e != nil {
		t.Fatal(e)
	}
	queue := continuityInput(original, p, h, "queued fix")
	if _, e = h.ContinueDelegation(p, p.ActiveQueuedID(), "queue-native-rejected", queue); e == nil || e.Error() != "unsupported" {
		t.Fatal("duplicated formal append implementation", e)
	}
	roundsView, e := h.DelegationRounds(p, original.ID, 0)
	if e != nil || roundsView.(map[string]any)["continuation_availability"] != "available" {
		t.Fatal("misleading queue continuation availability", roundsView, e)
	}
	currentTarget := roundsView.(map[string]any)["current_target"].(map[string]any)
	if currentTarget["config_revision"] != child.SettingsSnapshot().ConfigRevision || currentTarget["native_thread_id"] != child.ResumeID() {
		t.Fatal("target configuration borrowed from source", currentTarget)
	}
	bad := queue
	bad.ExpectedConfigRevision++
	if _, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "synthetic-wrong-cfg", bad); e == nil || e.Error() != "delegation_child_binding_changed" {
		t.Fatal("wrong target revision admitted", e)
	}
	bad = queue
	bad.ExpectedPreviousID = "wrong-predecessor"
	if _, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "synthetic-wrong-round", bad); e == nil || e.Error() != "delegation_stale_predecessor" {
		t.Fatal("wrong predecessor admitted", e)
	}
	task, e := h.apiTask(ctx, intent)
	if e != nil {
		t.Fatal(e)
	}
	appendInput := map[string]any{"task_id": intent.TaskID, "expected_revision": task["revision"], "target": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": child.ID, "expected_native_thread_id": child.ResumeID(), "expected_config_revision": child.SettingsSnapshot().ConfigRevision}, "mode": "queue", "content": "queued formal fix", "return_to_parent": true, "expected_previous_round_id": original.ID}
	raw, _ = json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-formal", "operation": "append", "idempotency_key": "synthetic-formal-fix", "input": appendInput})
	binding.ToolCallID = "synthetic-append-formal"
	bound, e = taskapi.BindCaller(ctx, apiVerifier{func() (taskapi.VerifiedContext, error) { return binding, nil }}, taskapi.Invocation{})
	if e != nil {
		t.Fatal(e)
	}
	response = h.taskService.Execute(ctx, bound, raw)
	if !response.OK {
		t.Fatal(response.Error)
	}
	appended := response.Result.(map[string]any)
	request := appended["effect_request_ids"].([]string)[0]
	fix, found, e := h.delegations.ByChildRequest(ctx, child.ID, request)
	if e != nil || !found || fix.State != "queued" || fix.DeliveryState != "pending" {
		t.Fatal("append skipped reservation/handoff", fix, e)
	}
	// Exact API receipt replay preserves one round and one return reservation.
	replay := h.taskService.Execute(ctx, bound, raw)
	if !replay.OK || replay.Result.(map[string]any)["receipt_id"] != appended["receipt_id"] {
		t.Fatal(replay)
	}
	steer := queue
	steer.Mode = "steer"
	steer.ActiveRequestID = original.ChildRequestID
	steer.ExpectedPreviousID = fix.ID
	got, e := h.ContinueDelegation(p, p.ActiveQueuedID(), "formal-steer", steer)
	if e != nil {
		t.Fatal(e)
	}
	r := capacityRecordFor(t, h, got.(map[string]any)["delegation_id"].(string))
	meta, _, e := h.delegations.Round(ctx, r.ID)
	if e != nil || meta.Mode != "steer" || meta.ExecutionRequestID != original.ChildRequestID {
		t.Fatal(meta, e)
	}
	afterScope, e := h.TaskWorkerScope(child)
	a, _ := json.Marshal(beforeScope)
	b, _ := json.Marshal(afterScope)
	if e != nil || string(a) != string(b) || child.SettingsSnapshot().Sandbox != "read-only" || child.ResumeID() != "synthetic-formal-thread" {
		t.Fatal("formal grant/sandbox/thread expanded", e, string(a), string(b))
	}
	originalTask, e := h.apiTask(ctx, intent)
	if e != nil {
		t.Fatal(e)
	}
	continuity := originalTask["continuity"].(map[string]any)
	if continuity["current_round_version"] != uint64(3) {
		t.Fatal("API omitted native continuation round", continuity)
	}
	var otherRequest string
	for _, round := range continuity["rounds"].([]any) {
		m := round.(map[string]any)
		if m["delegation_id"] == fix.ID {
			otherRequest = m["request_id"].(string)
		}
	}
	if otherRequest != fix.ChildRequestID {
		t.Fatal("per-round receipt correlation absent")
	}
	// A native steer changes logical rounds even when command timestamps alias.
	// An old predecessor cannot admit an API queue correction against that round.
	appendInput["expected_revision"] = originalTask["revision"]
	appendInput["expected_previous_round_id"] = fix.ID
	raw, _ = json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-stale-round", "operation": "append", "idempotency_key": "synthetic-stale-round-key", "input": appendInput})
	stale := h.taskService.Execute(ctx, bound, raw)
	if stale.OK || stale.Error.Code != "stale_revision" || stale.Error.Acceptance != "known_none" {
		t.Fatal("native/API round CAS race", stale)
	}
}

func TestContinuityWorkersHaveNoGlobalTaskManagementGrant(t *testing.T) {
	h, p, _ := continuityFixture(t)
	_, r := continuityDelegate(t, h, p, "owner", "one")
	child, _ := h.registry.Get(r.ChildSessionID)
	tools, e := h.TaskTools(child)
	if e != nil {
		t.Fatal(e)
	}
	if len(tools) != 2 || tools[0]["name"] != "task_capabilities" || tools[1]["name"] != "task_interactions" {
		t.Fatal("legacy worker exposed global management", tools)
	}
	caller := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + child.ID, SourceSessionID: child.ID, SourceResumeID: child.ResumeID(), SourceConfigKnown: true, BindingKind: "native_tool"}
	raw, _ := json.Marshal(map[string]any{"task_id": "foreign-task", "expected_revision": 0, "content": "escape", "mode": "queue"})
	for _, op := range []string{"append", "create_dispatch", "snapshot", "list"} {
		if _, e = h.Authorize(context.Background(), caller, taskapi.Request{Operation: op, Input: raw}); e == nil {
			t.Fatal("worker got management", op)
		}
	}
	cap, e := h.Read(context.Background(), taskapi.AuthorizedCommand{Caller: caller, Request: taskapi.Request{Operation: "capabilities", Input: []byte(`{}`)}})
	if e != nil {
		t.Fatal(e)
	}
	dto := cap.(map[string]any)
	if len(dto["allowed_operations"].([]string)) != 1 || dto["limits"].(map[string]int)["max_pending_children"] != 0 {
		t.Fatal("misleading global capability", dto)
	}
}
