package core

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func correctionWaitState(t *testing.T, h *Hub, sid, req string, want messagequeue.State) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		e, ok, err := h.messageQueue.Get(sid, req)
		if err != nil {
			t.Fatal(err)
		}
		if ok && e.State == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	e, _, _ := h.messageQueue.Get(sid, req)
	t.Fatalf("queue did not reach %s: %+v", want, e)
}

// A real close/reopen of both SQLite stores, new registry actors, and the real
// queue recovery path. No on-disk rows or queue payloads are edited.
func correctionReopen(t *testing.T, h *Hub) {
	t.Helper()
	snapshots := []session.Snapshot{}
	for _, s := range h.registry.List() {
		s.CloseAndWait()
		snap := s.SettingsSnapshot()
		snapshots = append(snapshots, snap)
	}
	if err := h.messageQueue.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.delegations.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	h.messageQueue, err = messagequeue.Open(h.cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.messageQueue.Recover(); err != nil {
		t.Fatal(err)
	}
	h.delegations, err = delegation.Open(h.cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	h.registry = session.NewRegistry()
	for _, snap := range snapshots {
		s := h.registry.Create(snap.ID, snap.Name, snap.Cwd, snap.Backend, snap.Model, snap.Sandbox, snap.ResumeID)
		s.SetEffort(snap.Effort)
		s.SetConfigRevision(snap.ConfigRevision)
		hidden := snap.Hidden
		s.SetMeta(nil, &hidden)
	}
}

func TestCorrectionReturnPinsSourceThroughRecoveryAndSend(t *testing.T) {
	for _, recoverQueue := range []bool{false, true} {
		for _, change := range []string{"none", "thread", "configuration"} {
			t.Run(change+map[bool]string{false: "", true: "-reopen"}[recoverQueue], func(t *testing.T) {
				h, caller := capacityHub(t, 1)
				ctx := context.Background()
				var sends atomic.Int32
				h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, _ string) { sends.Add(1); h.Emit(protocol.NewDone(s.ID, req)) }})
				receipt := capacityDelegate(t, h, caller, "correction-return")
				if _, err := h.delegations.Complete(ctx, receipt.ChildSessionID, receipt.ChildRequestID, "completed", "immutable child result", "", nil); err != nil {
					t.Fatal(err)
				}
				release := caller.Parent.HoldQueue()
				defer release()
				if err := h.reconcileDelegation(ctx, capacityRecordFor(t, h, receipt.ID)); err != nil {
					t.Fatal(err)
				}
				r := capacityRecordFor(t, h, receipt.ID)
				e, ok, err := h.messageQueue.Get(r.ParentSessionID, r.ParentRequestID)
				if err != nil || !ok {
					t.Fatal(ok, err)
				}
				var payload queuedPayload
				if err = json.Unmarshal(e.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				if payload.ExpectedTarget == nil || payload.ExpectedTarget.ThreadID != "parent-thread" || payload.ExpectedTarget.Revision != 0 {
					t.Fatal("source not pinned", payload.ExpectedTarget)
				}
				switch change {
				case "thread":
					caller.Parent.SetResumeID("foreign-parent")
				case "configuration":
					caller.Parent.SetConfigRevision(9)
				}
				if recoverQueue {
					correctionReopen(t, h)
					h.resumeQueuedMessages()
				} else {
					release()
				}
				want := messagequeue.Completed
				if change != "none" {
					want = messagequeue.Failed
				}
				correctionWaitState(t, h, r.ParentSessionID, r.ParentRequestID, want)
				if err = h.reconcileDelegation(ctx, capacityRecordFor(t, h, r.ID)); err != nil {
					t.Fatal(err)
				}
				final := capacityRecordFor(t, h, r.ID)
				if final.State != "terminal" || final.TerminalStatus != "completed" || final.Result != "immutable child result" {
					t.Fatal("delivery overwrote execution", final)
				}
				if change == "none" {
					if sends.Load() != 1 || final.DeliveryState != "delivered" {
						t.Fatal(sends.Load(), final)
					}
				} else if sends.Load() != 0 || final.DeliveryState != "failed" {
					t.Fatal("unsafe send or missing durable failure", sends.Load(), final)
				}
				for i := 0; i < 3; i++ {
					if err = h.reconcileDelegation(ctx, final); err != nil {
						t.Fatal(err)
					}
				}
				if change == "none" && sends.Load() != 1 {
					t.Fatal("return replayed")
				}
			})
		}
	}
}

func correctionMutateChild(t *testing.T, s *session.Session, change string) {
	t.Helper()
	switch change {
	case "thread":
		s.SetResumeID("foreign-child")
	case "sandbox":
		s.ApplyConfig("", "", "danger-full-access")
	case "backend":
		s.ApplyConfig(backend.Claude, "", "")
	case "model":
		s.ApplyConfig("", "foreign-model", "")
	case "effort":
		s.SetEffort("foreign-effort")
	case "revision":
		s.SetConfigRevision(9)
	case "hidden":
		hidden := true
		s.SetMeta(nil, &hidden)
	case "closed":
		s.Close()
	}
}

func TestCorrectionChildProfileDriftBeforeReuseAndSend(t *testing.T) {
	for _, boundary := range []string{"reuse", "held-send", "reopen-send"} {
		changes := []string{"thread", "sandbox", "backend", "model", "effort", "revision", "hidden"}
		if boundary == "reuse" {
			changes = append(changes, "cwd")
		}
		for _, change := range changes {
			t.Run(boundary+"/"+change, func(t *testing.T) {
				h, caller := capacityHub(t, 1)
				ctx := context.Background()
				var sends atomic.Int32
				h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, _ string) { sends.Add(1); h.Emit(protocol.NewDone(s.ID, req)) }})
				receipt := capacityDelegate(t, h, caller, "correction-child")
				r := capacityRecordFor(t, h, receipt.ID)
				cwd := r.Cwd
				if change == "cwd" {
					cwd = t.TempDir()
				}
				child := h.registry.Create(r.ChildSessionID, r.ChildName, cwd, backend.Codex, r.Model, r.Sandbox, "")
				child.SetEffort(r.Effort)
				if boundary == "reuse" {
					correctionMutateChild(t, child, change)
					if err := h.provisionDelegation(ctx, r); err == nil {
						t.Fatal("reused mismatched child")
					}
					final := capacityRecordFor(t, h, r.ID)
					if final.State != "terminal" || final.Error == "" {
						t.Fatal("missing durable provision failure", final)
					}
					if _, ok, err := h.messageQueue.Get(child.ID, r.ChildRequestID); err != nil || ok {
						t.Fatal("mismatched child queued", ok, err)
					}
				} else {
					release := child.HoldQueue()
					defer release()
					if err := h.provisionDelegation(ctx, r); err != nil {
						t.Fatal(err)
					}
					e, _, _ := h.messageQueue.Get(child.ID, r.ChildRequestID)
					var payload queuedPayload
					if err := json.Unmarshal(e.Payload, &payload); err != nil {
						t.Fatal(err)
					}
					if payload.ExpectedTarget == nil || payload.ExpectedTarget.ThreadID != "" || payload.ExpectedTarget.Revision != 0 {
						t.Fatal("empty initial thread not pinned", payload.ExpectedTarget)
					}
					correctionMutateChild(t, child, change)
					if boundary == "reopen-send" {
						correctionReopen(t, h)
						h.resumeQueuedMessages()
					} else {
						release()
					}
					correctionWaitState(t, h, r.ChildSessionID, r.ChildRequestID, messagequeue.Failed)
					if err := h.reconcileDelegation(ctx, capacityRecordFor(t, h, r.ID)); err != nil {
						t.Fatal(err)
					}
					final := capacityRecordFor(t, h, r.ID)
					if final.State != "terminal" || final.TerminalStatus != "failed" {
						t.Fatal(final)
					}
				}
				if sends.Load() != 0 {
					t.Fatal("profile drift reached Send", sends.Load())
				}
			})
		}
	}
}

func TestCorrectionLegacyReturnMissingBindingFailsClosed(t *testing.T) {
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	var sends atomic.Int32
	h.SetExecutor(&fakeExec{sink: h, onSend: func(*session.Session, string, string) { sends.Add(1) }})
	r := delegation.Record{ID: "legacy", ParentSessionID: caller.Parent.ID, ChildSessionID: "legacy-child", ChildRequestID: "legacy-request", ParentRequestID: "legacy-return", Cwd: caller.Parent.Cwd()}
	if err := h.delegations.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := h.delegations.Complete(ctx, r.ChildSessionID, r.ChildRequestID, "completed", "legacy preserved", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := h.reconcileDelegation(ctx, capacityRecordFor(t, h, r.ID)); err != nil {
		t.Fatal(err)
	}
	final := capacityRecordFor(t, h, r.ID)
	if final.DeliveryState != "failed" || final.Result != "legacy preserved" || sends.Load() != 0 {
		t.Fatal(final, sends.Load())
	}
}

func TestCorrectionUnknownNativeAcceptanceNotReplayedOrRebound(t *testing.T) {
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	var sends atomic.Int32
	h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, _ string) {
		sends.Add(1)
		s.SetResumeID("accepted-child")
		h.Emit(backend.NativeTaskAccepted{SessionID: s.ID, RequestID: req, ThreadID: "accepted-child", TurnID: "accepted-turn"})
		h.Emit(backend.NativeSubmissionUncertain{SessionID: s.ID, RequestID: req, Reason: "synthetic lost acknowledgement"})
	}})
	receipt := capacityDelegate(t, h, caller, "unknown-native")
	r := capacityRecordFor(t, h, receipt.ID)
	if err := h.provisionDelegation(ctx, r); err != nil {
		t.Fatal(err)
	}
	correctionWaitState(t, h, r.ChildSessionID, r.ChildRequestID, messagequeue.Uncertain)
	proof, found, err := h.messageQueue.ExactNativeAcceptance(r.ChildSessionID, r.ChildRequestID)
	if err != nil || !found || proof.ThreadID != "accepted-child" {
		t.Fatal(proof, found, err)
	}
	correctionReopen(t, h)
	h.resumeQueuedMessages()
	for i := 0; i < 3; i++ {
		if err = h.reconcileDelegation(ctx, capacityRecordFor(t, h, r.ID)); err != nil {
			t.Fatal(err)
		}
	}
	if sends.Load() != 1 || capacityRecordFor(t, h, r.ID).State != "running" {
		t.Fatal("unknown replayed or released", sends.Load(), capacityRecordFor(t, h, r.ID))
	}
	counts, err := h.delegations.CountsForParent(ctx, r.ParentSessionID)
	if err != nil || counts.Active != 1 {
		t.Fatal(counts, err)
	}
	// Canonical native receipt may never adopt a different thread/turn.
	err = h.messageQueue.RecordNativeAcceptance(r.ChildSessionID, r.ChildRequestID, "foreign-child", "foreign-turn")
	if !errors.Is(err, messagequeue.ErrNativeConflict) {
		t.Fatal("native tuple rebound", err)
	}
	if _, found, err = h.messageQueue.ExactNativeAcceptance(r.ChildSessionID, r.ChildRequestID); found || !errors.Is(err, messagequeue.ErrNativeConflict) {
		t.Fatal(found, err)
	}
}

func TestCorrectionWorkspaceDirectoryReplacementFailsBeforeSend(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		t.Run(map[bool]string{false: "held", true: "reopen"}[reopen], func(t *testing.T) {
			h, caller := capacityHub(t, 1)
			ctx := context.Background()
			var sends atomic.Int32
			h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, _ string) { sends.Add(1); h.Emit(protocol.NewDone(s.ID, req)) }})
			cwd := filepath.Join(caller.Parent.Cwd(), "worker-root")
			if err := os.Mkdir(cwd, 0700); err != nil {
				t.Fatal(err)
			}
			receipt, err := h.DelegateVoiceSession(caller, backend.DelegationSpec{Name: "workspace-identity", Cwd: cwd, Instruction: "synthetic"})
			if err != nil {
				t.Fatal(err)
			}
			r := capacityRecordFor(t, h, receipt.ID)
			child := h.registry.Create(r.ChildSessionID, r.ChildName, r.Cwd, backend.Codex, r.Model, r.Sandbox, "")
			child.SetEffort(r.Effort)
			release := child.HoldQueue()
			defer release()
			if err = h.provisionDelegation(ctx, r); err != nil {
				t.Fatal(err)
			}
			if err = os.Rename(cwd, cwd+"-original"); err != nil {
				t.Fatal(err)
			}
			if err = os.Mkdir(cwd, 0700); err != nil {
				t.Fatal(err)
			}
			if reopen {
				correctionReopen(t, h)
				h.resumeQueuedMessages()
			} else {
				release()
			}
			correctionWaitState(t, h, r.ChildSessionID, r.ChildRequestID, messagequeue.Failed)
			if sends.Load() != 0 {
				t.Fatal("changed directory reached Send")
			}
		})
	}
}

func TestCorrectionLegacyQueuedReturnWithoutExpectationFailsAtSend(t *testing.T) {
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	var sends atomic.Int32
	h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, _ string) { sends.Add(1); h.Emit(protocol.NewDone(s.ID, req)) }})
	receipt := capacityDelegate(t, h, caller, "legacy-queue")
	r := capacityRecordFor(t, h, receipt.ID)
	if _, err := h.delegations.Complete(ctx, r.ChildSessionID, r.ChildRequestID, "completed", "preserved legacy result", "", nil); err != nil {
		t.Fatal(err)
	}
	// Simulate an already durable pre-correction payload via the store's normal
	// enqueue API, with no DB edits or fabricated new authority/admission.
	payload, _ := json.Marshal(queuedPayload{Content: "legacy return", MessagePurpose: "result_return"})
	if _, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: r.ParentSessionID, RequestID: r.ParentRequestID, Payload: payload}); err != nil {
		t.Fatal(err)
	}
	if err := h.delegations.MarkDeliveryQueued(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	correctionReopen(t, h)
	h.resumeQueuedMessages()
	correctionWaitState(t, h, r.ParentSessionID, r.ParentRequestID, messagequeue.Failed)
	if err := h.reconcileDelegation(ctx, capacityRecordFor(t, h, r.ID)); err != nil {
		t.Fatal(err)
	}
	final := capacityRecordFor(t, h, r.ID)
	if sends.Load() != 0 || final.DeliveryState != "failed" || final.Result != "preserved legacy result" {
		t.Fatal(sends.Load(), final)
	}
}

func TestCorrectionSendGuardChecksActiveProfileWhenDefaultsStillMatch(t *testing.T) {
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	receipt := capacityDelegate(t, h, caller, "active-profile")
	r := capacityRecordFor(t, h, receipt.ID)
	child := h.registry.Create(r.ChildSessionID, r.ChildName, r.Cwd, backend.Codex, r.Model, r.Sandbox, "")
	child.SetEffort(r.Effort)
	active := make(chan struct{})
	release := make(chan struct{})
	if !child.SubmitNamed(r.ChildRequestID, func() { close(active); <-release }) {
		t.Fatal("submit failed")
	}
	<-active
	defer func() { close(release); child.CloseAndWait() }()
	if err := child.ActivateQueuedConfiguration(session.ConfigurationFrom(child.SettingsSnapshot())); err != nil {
		t.Fatal(err)
	}
	child.ApplyConfig("", "", "danger-full-access")
	if child.SettingsSnapshot().Sandbox != r.Sandbox || child.Snapshot().Sandbox != "danger-full-access" {
		t.Fatal("fixture did not separate actual profile from defaults")
	}
	if err := h.delegationExecutionGuard(ctx, child, r.ChildRequestID, true); err == nil {
		t.Fatal("actual execution sandbox escaped immutable profile guard")
	}
}
