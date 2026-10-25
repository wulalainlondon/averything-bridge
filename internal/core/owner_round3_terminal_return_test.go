package core

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// Real temporary SQLite stores, actor queue and recovery. No live history,
// acceptance/seal fabrication or ledger SQL; all terminal evidence is synthetic.
func TestOwnerRound3FormalTerminalExistingReceiptReopenNeverReplays(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		name := "completed"
		if uncertain {
			name = "uncertain"
		}
		t.Run(name, func(t *testing.T) {
			h, p, r, _ := qa2FormalFixture(t, "provision_failure")
			ctx := context.Background()
			policy, doc := handoffFixture(t, h, p)
			thread, revision, _, e := h.delegations.SourceBinding(ctx, r.ID)
			if e != nil {
				t.Fatal(e)
			}
			frozen, e := h.delegationReturnContext(ctx, r, p, thread, revision)
			if e != nil {
				t.Fatal(e)
			}
			metadata, _, e := h.delegations.APIChildMetadata(ctx, r.ChildSessionID)
			if e != nil {
				t.Fatal(e)
			}
			release := p.HoldQueue()
			defer release()
			if !h.enqueueChatMessageExpected(newTestClient(h), clientproto.Command{Kind: "message", SessionID: p.ID, RequestID: r.ParentRequestID, Content: frozen.Content, MessagePurpose: "result_return"}, &dispatchTargetExpectation{ThreadID: thread, Revision: revision}) {
				t.Fatal("receipt refused")
			}
			before, found, e := h.messageQueue.Get(p.ID, r.ParentRequestID)
			if e != nil || !found {
				t.Fatal(found, e)
			}
			if capacityRecordFor(t, h, r.ID).DeliveryState != "pending" {
				t.Fatal("not crash gap")
			}
			doc.Revision++
			doc.Goal = "later handoff must not replace frozen terminal payload"
			handoffWrite(t, policy, doc)
			correctionReopen(t, h)
			p, _ = h.registry.Get(r.ParentSessionID)
			var sends, steers atomic.Int32
			h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, content string) {
				sends.Add(1)
				if s.ID != r.ParentSessionID || req != r.ParentRequestID || content != frozen.Content {
					t.Error("recovery changed original return identity/payload")
				}
				if uncertain {
					h.Emit(backend.NativeSubmissionUncertain{SessionID: s.ID, RequestID: req, Reason: "synthetic lost return ACK"})
				} else {
					h.Emit(protocol.NewDone(s.ID, req))
				}
			}, onSteer: func(_ *session.Session, _, _ string) (backend.SteerResult, error) {
				steers.Add(1)
				return backend.SteerResult{}, nil
			}})
			// At restart the existing receipt owns the payload; reconciliation cannot
			// mint a second request, read a later handoff or invoke child execution.
			if e = h.reconcileDelegation(ctx, capacityRecordFor(t, h, r.ID)); e != nil {
				t.Fatal(e)
			}
			prepared, _, e := h.messageQueue.Get(p.ID, r.ParentRequestID)
			if e != nil || prepared.Sequence != before.Sequence || prepared.PayloadHash != before.PayloadHash || !reflect.DeepEqual(prepared.Payload, before.Payload) {
				t.Fatal("reopen replaced existing queued payload", e)
			}
			h.resumeQueuedMessages()
			want := messagequeue.Completed
			if uncertain {
				want = messagequeue.Uncertain
			}
			correctionWaitState(t, h, r.ParentSessionID, r.ParentRequestID, want)
			var wg sync.WaitGroup
			for i := 0; i < 6; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := h.reconcileDelegation(ctx, r); err != nil {
						t.Error(err)
					}
				}()
			}
			wg.Wait()
			after, _, e := h.messageQueue.Get(p.ID, r.ParentRequestID)
			if e != nil || after.Sequence != before.Sequence || after.PayloadHash != before.PayloadHash || after.Content != before.Content || (uncertain && !reflect.DeepEqual(after.Payload, before.Payload)) {
				t.Fatal("original receipt changed", after, e)
			}
			expectedDelivery := "delivered"
			if uncertain {
				expectedDelivery = "failed"
			}
			terminal := capacityRecordFor(t, h, r.ID)
			if terminal.DeliveryState != expectedDelivery || terminal.Result != r.Result || terminal.Error != r.Error || terminal.TerminalStatus != r.TerminalStatus {
				t.Fatal("terminal evidence changed", terminal)
			}
			// A second real restart must neither repeat the return nor reinterpret
			// uncertain acceptance as a pre-execution refusal.
			correctionReopen(t, h)
			h.resumeQueuedMessages()
			for i := 0; i < 3; i++ {
				if e = h.reconcileDelegation(ctx, capacityRecordFor(t, h, r.ID)); e != nil {
					t.Fatal(e)
				}
			}
			if sends.Load() != 1 || steers.Load() != 0 {
				t.Fatal("duplicate Send/Steer/return", sends.Load(), steers.Load())
			}
			final, found, e := h.delegations.ReturnContext(ctx, r, thread, revision)
			if e != nil || !found || !reflect.DeepEqual(final, frozen) {
				t.Fatal("frozen payload reread or changed", final, e)
			}
			raw, _, e := h.delegations.APIChildMetadata(ctx, r.ChildSessionID)
			if e != nil || string(raw) != string(metadata) {
				t.Fatal("immutable metadata changed", e)
			}
			if _, accepted, e := h.messageQueue.ExactNativeAcceptance(r.ChildSessionID, r.ChildRequestID); e != nil || accepted {
				t.Fatal("fabricated acceptance", accepted, e)
			}
			if _, _, pinned, e := h.delegations.NativeThread(ctx, r.ChildSessionID); e != nil || pinned {
				t.Fatal("terminal return upgraded native binding", pinned, e)
			}
		})
	}
}

func TestOwnerRound3FormalTerminalSourceRejectsImmutableProvenanceDrift(t *testing.T) {
	for _, kind := range []string{"caller_parent", "caller_authority", "caller_thread", "caller_revision", "caller_request", "caller_call", "transport", "model", "sandbox", "workspace_identity", "canonical_root", "backend", "foreign_round"} {
		t.Run(kind, func(t *testing.T) {
			h, p, r, sends := qa2FormalFixture(t, "provision_failure")
			ctx := context.Background()
			raw, _, e := h.delegations.APIChildMetadata(ctx, r.ChildSessionID)
			if e != nil {
				t.Fatal(e)
			}
			var meta apiMetadata
			if e = json.Unmarshal(raw, &meta); e != nil {
				t.Fatal(e)
			}
			var input apiInput
			if e = json.Unmarshal(meta.Request.Input, &input); e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "caller_parent":
				meta.Caller.SourceSessionID = "foreign"
			case "caller_authority":
				meta.Caller.Authority = "foreign"
			case "caller_thread":
				meta.Caller.SourceResumeID = "foreign"
			case "caller_revision":
				meta.Caller.SourceConfigRevision++
			case "caller_request":
				meta.Caller.SourceRequestID = "foreign"
			case "caller_call":
				meta.Caller.ToolCallID = "foreign"
			case "transport":
				meta.Caller.Transport = "foreign"
			case "model":
				input.New.Profile.Model = "foreign"
			case "sandbox":
				input.Scope.Sandbox = "danger-full-access"
			case "workspace_identity":
				meta.RootIdentities[r.Cwd] = "foreign"
			case "canonical_root":
				meta.CanonicalRoots = append(meta.CanonicalRoots, "/synthetic/foreign")
			case "backend":
				input.New.Profile.Backend = "foreign"
			case "foreign_round":
				r.ParentSessionID = "foreign"
			}
			meta.Request.Input, _ = json.Marshal(input)
			if _, _, e = h.formalDelegationReturnSource(ctx, r, p, meta); e == nil {
				t.Fatal("accepted foreign immutable provenance", kind)
			}
			after, _, e := h.delegations.APIChildMetadata(ctx, r.ChildSessionID)
			if e != nil || string(after) != string(raw) || sends.Load() != 0 {
				t.Fatal("refusal changed metadata or sent", e)
			}
			if _, found, e := h.messageQueue.Get(p.ID, r.ParentRequestID); e != nil || found {
				t.Fatal("refusal minted return", found, e)
			}
		})
	}
}

func TestOwnerRound3FormalTerminalSourceRejectsUnapprovedEpochDigest(t *testing.T) {
	for _, kind := range []string{"digest", "authority", "thread"} {
		t.Run(kind, func(t *testing.T) {
			h, p, _, _, owner, qa := ownerRound2FormalPair(t)
			ctx := context.Background()
			// Append-only store API in an isolated adversarial fixture, never SQL or
			// mutation of an accepted epoch. The resolver must reject the next epoch.
			var pair [2]delegation.SourceEpoch
			for i, r := range []delegation.Record{owner, qa} {
				a, _, e := h.delegations.SourceEpoch(ctx, r.ID)
				if e != nil {
					t.Fatal(e)
				}
				a.FromRevision = a.ToRevision
				a.ToRevision++
				a.Epoch++
				a.Request = "r_synthetic_bad_epoch"
				a.Call = "bad-epoch"
				if kind == "digest" {
					a.GrantDigest = "foreign"
				}
				if kind == "authority" {
					a.Authority = "foreign"
				}
				if kind == "thread" {
					a.Thread = "foreign"
				}
				pair[i] = a
			}
			// Store itself refuses thread drift. Digest/authority are admission-level
			// guards and therefore independently exercised by the source resolver.
			e := h.delegations.ReauthorizePair(ctx, pair, func() error { return nil })
			if kind == "thread" {
				if e == nil {
					t.Fatal("store admitted foreign thread")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			p.SetConfigRevision(pair[0].ToRevision)
			raw, _, e := h.delegations.APIChildMetadata(ctx, owner.ChildSessionID)
			if e != nil {
				t.Fatal(e)
			}
			var meta apiMetadata
			if e = json.Unmarshal(raw, &meta); e != nil {
				t.Fatal(e)
			}
			if _, _, e = h.formalDelegationReturnSource(ctx, owner, p, meta); e == nil {
				t.Fatal("return adopted unapproved epoch", kind)
			}
			if _, _, _, e = h.continuityBase(p, owner.ID); e == nil {
				t.Fatal("new execution adopted unapproved epoch", kind)
			}
		})
	}
}
