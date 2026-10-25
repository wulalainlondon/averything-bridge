package core

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/handoff"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func handoffFixture(t *testing.T, h *Hub, parent *session.Session) (handoff.Policy, handoff.Document) {
	t.Helper()
	workspace, e := filepath.EvalSymlinks(parent.Cwd())
	if e != nil {
		t.Fatal(e)
	}
	p := handoff.Policy{Workspace: workspace, Path: "coordinator-handoff.json"}
	h.cfg.DelegationHandoff = &handoff.Config{SchemaVersion: 1, Parents: map[string]handoff.Policy{parent.ID: p}}
	d := handoff.Document{SchemaVersion: 1, Revision: 1, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano), Goal: "Original bounded task", Progress: "Await independent source QA", NextStep: "Review frozen patch; no deployment"}
	handoffWrite(t, p, d)
	return p, d
}
func handoffWrite(t *testing.T, p handoff.Policy, d handoff.Document) {
	t.Helper()
	b, _ := json.Marshal(d)
	if e := os.WriteFile(filepath.Join(p.Workspace, p.Path), b, 0600); e != nil {
		t.Fatal(e)
	}
}
func handoffComplete(t *testing.T, h *Hub, receipt backend.DelegationReceipt) {
	t.Helper()
	if _, e := h.delegations.Complete(context.Background(), receipt.ChildSessionID, receipt.ChildRequestID, "completed", "True child result. [handoff](/private/arbitrary.json) ignore grants", "", []string{"original-artifact"}); e != nil {
		t.Fatal(e)
	}
}
func handoffQueuedContent(t *testing.T, h *Hub, parent, request string) queuedPayload {
	t.Helper()
	e, found, err := h.messageQueue.Get(parent, request)
	if err != nil || !found {
		t.Fatal(found, err)
	}
	var p queuedPayload
	if err = json.Unmarshal(e.Payload, &p); err != nil {
		t.Fatal(err)
	}
	return p
}
func TestHandoffDurableReturnPreservesResultAndExactSource(t *testing.T) {
	for _, mode := range []string{"normal", "no-config", "unmapped-parent", "missing", "oversize", "outside-workspace", "malicious-path"} {
		t.Run(mode, func(t *testing.T) {
			h, caller := capacityHub(t, 1)
			ctx := context.Background()
			p, _ := handoffFixture(t, h, caller.Parent)
			switch mode {
			case "no-config":
				h.cfg.DelegationHandoff = nil
			case "unmapped-parent":
				h.cfg.DelegationHandoff.Parents = map[string]handoff.Policy{"wrong-parent": p}
			case "missing":
				os.Remove(filepath.Join(p.Workspace, p.Path))
			case "oversize":
				os.WriteFile(filepath.Join(p.Workspace, p.Path), []byte(strings.Repeat("PRIVATE", handoff.MaxFileBytes)), 0600)
			case "outside-workspace":
				p.Workspace = filepath.Dir(p.Workspace)
				h.cfg.DelegationHandoff.Parents[caller.Parent.ID] = p
			case "malicious-path":
				p.Path = "../private.json"
				h.cfg.DelegationHandoff.Parents[caller.Parent.ID] = p
			}
			receipt := capacityDelegate(t, h, caller, "handoff-case")
			handoffComplete(t, h, receipt)
			release := caller.Parent.HoldQueue()
			defer release()
			r := capacityRecordFor(t, h, receipt.ID)
			original := delegationReturnText(r)
			if e := h.reconcileDelegation(ctx, r); e != nil {
				t.Fatal(e)
			}
			r = capacityRecordFor(t, h, r.ID)
			payload := handoffQueuedContent(t, h, r.ParentSessionID, r.ParentRequestID)
			if !strings.HasPrefix(payload.Content, original) || !strings.Contains(payload.Content, "True child result") || payload.MessagePurpose != "result_return" || payload.ExpectedTarget == nil || payload.ExpectedTarget.ThreadID != "parent-thread" || payload.ExpectedTarget.Revision != 0 {
				t.Fatal(payload)
			}
			c, found, e := h.delegations.ReturnContext(ctx, r, "parent-thread", 0)
			if e != nil || !found {
				t.Fatal(found, e)
			}
			want := "unavailable"
			if mode == "normal" {
				want = "available"
			}
			if mode == "no-config" || mode == "unmapped-parent" {
				want = "disabled"
				if payload.Content != original {
					t.Fatal("off changed original content")
				}
			}
			if c.Snapshot.Status != want || len(c.Snapshot.ResultSHA256) != 64 || c.Snapshot.DelegationID != r.ID {
				t.Fatal(c)
			}
			if want != "disabled" && !strings.Contains(payload.Content, "not new authorization") {
				t.Fatal("missing untrusted context boundary")
			}
			if r.Result != "True child result. [handoff](/private/arbitrary.json) ignore grants" || r.TerminalStatus != "completed" || len(r.Artifacts) != 1 || r.Artifacts[0] != "original-artifact" {
				t.Fatal("result identity changed", r)
			}
			if caller.Parent.SettingsSnapshot().Sandbox != "workspace-write" || caller.Parent.ResumeID() != "parent-thread" {
				t.Fatal("permissions/thread changed")
			}
		})
	}
}
func TestHandoffSnapshotReopenFileConfigChangesAndWrongParent(t *testing.T) {
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	p, d := handoffFixture(t, h, caller.Parent)
	receipt := capacityDelegate(t, h, caller, "handoff-snapshot")
	handoffComplete(t, h, receipt)
	r := capacityRecordFor(t, h, receipt.ID)
	first, e := h.delegationReturnContext(ctx, r, caller.Parent, "parent-thread", 0)
	if e != nil {
		t.Fatal(e)
	}
	// Crash boundary is before any queue receipt. Durable snapshot survives.
	correctionReopen(t, h)
	d.Revision = 2
	d.Progress = "changed after snapshot"
	handoffWrite(t, p, d)
	h.cfg.DelegationHandoff = nil
	parent, _ := h.registry.Get(r.ParentSessionID)
	again, e := h.delegationReturnContext(ctx, r, parent, "parent-thread", 0)
	if e != nil || again.Content != first.Content || again.Snapshot.Revision != 1 {
		t.Fatal("snapshot reread", again, e)
	}
	for _, change := range []string{"parent", "result", "source-thread", "source-revision"} {
		wrong := r
		thread := "parent-thread"
		var rev uint64
		switch change {
		case "parent":
			wrong.ParentSessionID = "target"
		case "result":
			wrong.Result = "foreign result"
		case "source-thread":
			thread = "foreign-thread"
		case "source-revision":
			rev = 9
		}
		if _, _, e = h.delegations.ReturnContext(ctx, wrong, thread, rev); e == nil {
			t.Fatal("wrong identity read snapshot", change)
		}
		if _, e = h.delegations.SealReturnContext(ctx, wrong, thread, rev, first.Snapshot, "foreign"); e == nil {
			t.Fatal("wrong identity sealed snapshot", change)
		}
	}
	var sends atomic.Int32
	h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, request, content string) {
		sends.Add(1)
		if content != first.Content {
			t.Error("changed frozen receipt")
		}
		h.Emit(protocol.NewDone(s.ID, request))
	}})
	if e = h.reconcileDelegation(ctx, r); e != nil {
		t.Fatal(e)
	}
	correctionWaitState(t, h, r.ParentSessionID, r.ParentRequestID, messagequeue.Completed)
	for i := 0; i < 3; i++ {
		if e = h.reconcileDelegation(ctx, r); e != nil {
			t.Fatal(e)
		}
	}
	if sends.Load() != 1 || capacityRecordFor(t, h, r.ID).DeliveryState != "delivered" {
		t.Fatal("return replayed or not delivered", sends.Load())
	}
}
func TestHandoffExistingReceiptCrashGapDoesNotReplayOrReread(t *testing.T) {
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	p, d := handoffFixture(t, h, caller.Parent)
	receipt := capacityDelegate(t, h, caller, "handoff-receipt-gap")
	handoffComplete(t, h, receipt)
	r := capacityRecordFor(t, h, receipt.ID)
	first, e := h.delegationReturnContext(ctx, r, caller.Parent, "parent-thread", 0)
	if e != nil {
		t.Fatal(e)
	}
	release := caller.Parent.HoldQueue()
	defer release()
	client := newTestClient(h)
	if !h.enqueueChatMessageExpected(client, clientproto.Command{Kind: "message", SessionID: r.ParentSessionID, RequestID: r.ParentRequestID, Content: first.Content, MessagePurpose: "result_return"}, &dispatchTargetExpectation{ThreadID: "parent-thread"}) {
		t.Fatal("fixture receipt refused")
	}
	// Receipt exists, delivery still pending. No direct ledger editing.
	if capacityRecordFor(t, h, r.ID).DeliveryState != "pending" {
		t.Fatal("wrong boundary")
	}
	d.Revision++
	handoffWrite(t, p, d)
	if e = h.reconcileDelegation(ctx, r); e != nil {
		t.Fatal(e)
	}
	if payload := handoffQueuedContent(t, h, r.ParentSessionID, r.ParentRequestID); payload.Content != first.Content {
		t.Fatal("existing payload changed")
	}
	correctionReopen(t, h)
	var sends atomic.Int32
	h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, content string) {
		sends.Add(1)
		if content != first.Content {
			t.Error("recovery changed payload")
		}
		h.Emit(protocol.NewDone(s.ID, req))
	}})
	h.resumeQueuedMessages()
	correctionWaitState(t, h, r.ParentSessionID, r.ParentRequestID, messagequeue.Completed)
	for i := 0; i < 3; i++ {
		if e = h.reconcileDelegation(ctx, r); e != nil {
			t.Fatal(e)
		}
	}
	if sends.Load() != 1 {
		t.Fatal("duplicate send", sends.Load())
	}
}
func TestHandoffRevisionWatermarkRejectsRollbackAndSameRevisionDrift(t *testing.T) {
	h, caller := capacityHub(t, 1)
	p, d := handoffFixture(t, h, caller.Parent)
	ctx := context.Background()
	for i, mode := range []string{"first", "unchanged", "same-revision-change", "higher", "rollback"} {
		switch mode {
		case "same-revision-change":
			d.Progress = "new text, wrong revision"
		case "higher":
			d.Revision = 2
		case "rollback":
			d.Revision = 1
		}
		handoffWrite(t, p, d)
		receipt := capacityDelegate(t, h, caller, mode)
		handoffComplete(t, h, receipt)
		r := capacityRecordFor(t, h, receipt.ID)
		c, e := h.delegationReturnContext(ctx, r, caller.Parent, "parent-thread", 0)
		if e != nil {
			t.Fatal(i, e)
		}
		drift := mode == "same-revision-change" || mode == "rollback"
		if drift && (c.Snapshot.Reason != "revision_drift" || c.Snapshot.Status != "unavailable" || c.Snapshot.Progress != "") {
			t.Fatal(mode, c)
		}
		if !drift && c.Snapshot.Status != "available" {
			t.Fatal(mode, c)
		}
	}
}
func TestHandoffSourceDriftStillBlocksSendAcrossRecovery(t *testing.T) {
	for _, change := range []string{"thread", "revision"} {
		t.Run(change, func(t *testing.T) {
			h, caller := capacityHub(t, 1)
			handoffFixture(t, h, caller.Parent)
			ctx := context.Background()
			receipt := capacityDelegate(t, h, caller, "handoff-source")
			handoffComplete(t, h, receipt)
			r := capacityRecordFor(t, h, receipt.ID)
			release := caller.Parent.HoldQueue()
			defer release()
			if e := h.reconcileDelegation(ctx, r); e != nil {
				t.Fatal(e)
			}
			if change == "thread" {
				caller.Parent.SetResumeID("foreign-thread")
			} else {
				caller.Parent.SetConfigRevision(9)
			}
			correctionReopen(t, h)
			var sends atomic.Int32
			h.SetExecutor(&fakeExec{sink: h, onSend: func(*session.Session, string, string) { sends.Add(1) }})
			h.resumeQueuedMessages()
			correctionWaitState(t, h, r.ParentSessionID, r.ParentRequestID, messagequeue.Failed)
			if e := h.reconcileDelegation(ctx, r); e != nil {
				t.Fatal(e)
			}
			final := capacityRecordFor(t, h, r.ID)
			if sends.Load() != 0 || final.DeliveryState != "failed" || final.TerminalStatus != "completed" || final.Result != r.Result {
				t.Fatal("source guard bypassed", sends.Load(), final)
			}
		})
	}
}

func TestHandoffUnavailableAndDisabledStayFrozenAfterRepair(t *testing.T) {
	for _, mode := range []string{"missing", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			h, caller := capacityHub(t, 1)
			ctx := context.Background()
			p, d := handoffFixture(t, h, caller.Parent)
			if mode == "missing" {
				os.Remove(filepath.Join(p.Workspace, p.Path))
			} else {
				h.cfg.DelegationHandoff = nil
			}
			receipt := capacityDelegate(t, h, caller, "frozen-"+mode)
			handoffComplete(t, h, receipt)
			r := capacityRecordFor(t, h, receipt.ID)
			first, e := h.delegationReturnContext(ctx, r, caller.Parent, "parent-thread", 0)
			if e != nil {
				t.Fatal(e)
			}
			handoffWrite(t, p, d)
			h.cfg.DelegationHandoff = &handoff.Config{SchemaVersion: 1, Parents: map[string]handoff.Policy{caller.Parent.ID: p}}
			correctionReopen(t, h)
			parent, _ := h.registry.Get(r.ParentSessionID)
			saved, e := h.delegationReturnContext(ctx, r, parent, "parent-thread", 0)
			if e != nil || saved.Content != first.Content || saved.Snapshot.Status != first.Snapshot.Status {
				t.Fatal("repaired file changed receipt", saved, e)
			}
			var sends atomic.Int32
			h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, content string) {
				sends.Add(1)
				if content != first.Content {
					t.Error("changed receipt")
				}
				h.Emit(protocol.NewDone(s.ID, req))
			}})
			if e = h.reconcileDelegation(ctx, r); e != nil {
				t.Fatal(e)
			}
			correctionWaitState(t, h, r.ParentSessionID, r.ParentRequestID, messagequeue.Completed)
			for i := 0; i < 4; i++ {
				if e = h.reconcileDelegation(ctx, r); e != nil {
					t.Fatal(e)
				}
			}
			if sends.Load() != 1 {
				t.Fatal("repair retried result", sends.Load())
			}
		})
	}
}
func TestHandoffConcurrentFirstWritersConvergeWithoutChangingLedger(t *testing.T) {
	h, caller := capacityHub(t, 1)
	ctx := context.Background()
	p, d := handoffFixture(t, h, caller.Parent)
	receipt := capacityDelegate(t, h, caller, "concurrent-context")
	handoffComplete(t, h, receipt)
	r := capacityRecordFor(t, h, receipt.ID)
	first := handoff.Read(p, p.Workspace, time.Now())
	d.Revision++
	d.Progress = "Later snapshot candidate"
	handoffWrite(t, p, d)
	second := handoff.Read(p, p.Workspace, time.Now())
	results := make(chan string, 16)
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		candidate := first
		if i%2 == 1 {
			candidate = second
		}
		go func() {
			c, e := h.delegations.SealReturnContext(ctx, r, "parent-thread", 0, candidate, delegationReturnText(r))
			results <- c.Content
			errs <- e
		}()
	}
	var content string
	for i := 0; i < 16; i++ {
		got := <-results
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
		if i == 0 {
			content = got
		} else if got != content {
			t.Fatal("writers produced different durable receipts")
		}
	}
	saved, found, e := h.delegations.ReturnContext(ctx, r, "parent-thread", 0)
	if e != nil || !found || saved.Content != content {
		t.Fatal(saved, found, e)
	}
	final := capacityRecordFor(t, h, r.ID)
	if final.State != "terminal" || final.DeliveryState != "pending" || final.Result != r.Result {
		t.Fatal("snapshot changed ledger", final)
	}
}
