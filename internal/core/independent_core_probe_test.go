package core

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "os"
    "path/filepath"
    "strings"
    "sync/atomic"
    "testing"

    "everything-go/internal/messagequeue"
    "everything-go/internal/protocol"
    "everything-go/internal/session"
)

func TestIndependentHandoffClearRejectsOldReturn(t *testing.T) {
    for _, boundary := range []string{"before-snapshot", "queued", "queued-reopen"} {
        t.Run(boundary, func(t *testing.T) {
            h, caller := capacityHub(t, 1)
            handoffFixture(t, h, caller.Parent)
            ctx := context.Background()
            receipt := capacityDelegate(t, h, caller, "independent-clear")
            handoffComplete(t, h, receipt)
            r := capacityRecordFor(t, h, receipt.ID)
            release := caller.Parent.HoldQueue()
            defer release()
            if boundary != "before-snapshot" {
                if err := h.reconcileDelegation(ctx, r); err != nil { t.Fatal(err) }
            }
            // Exercise the actual Session clear identity boundary without native RPC.
            caller.Parent.ClearResumeIDs()
            var sends atomic.Int32
            h.SetExecutor(&fakeExec{sink: h, onSend: func(*session.Session, string, string) { sends.Add(1) }})
            if boundary == "queued-reopen" {
                correctionReopen(t, h)
                h.resumeQueuedMessages()
            } else { release() }
            if boundary != "before-snapshot" {
                correctionWaitState(t, h, r.ParentSessionID, r.ParentRequestID, messagequeue.Failed)
            }
            if err := h.reconcileDelegation(ctx, r); err != nil { t.Fatal(err) }
            final := capacityRecordFor(t, h, r.ID)
            if sends.Load() != 0 || final.DeliveryState != "failed" || final.TerminalStatus != "completed" || final.Result != r.Result {
                t.Fatal("clear crossed source boundary or changed result", sends.Load(), final)
            }
            if boundary == "before-snapshot" {
                if _, found, err := h.delegations.ReturnContext(ctx, r, "parent-thread", 0); found || err != nil { t.Fatal(found, err) }
            } else {
                if _, found, err := h.delegations.ReturnContext(ctx, r, "parent-thread", 0); !found || err != nil { t.Fatal("clear erased original immutable context", found, err) }
            }
        })
    }
}

func TestIndependentHandoffBinaryKeepsLongResultAndReceipt(t *testing.T) {
    h, caller := capacityHub(t, 1)
    ctx := context.Background()
    p, d := handoffFixture(t, h, caller.Parent)
    if err := os.WriteFile(filepath.Join(p.Workspace, p.Path), []byte{0, 1, 255}, 0600); err != nil { t.Fatal(err) }
    receipt := capacityDelegate(t, h, caller, "independent-binary")
    full := strings.Repeat("真", 17000) + "tail-preserved"
    if _, err := h.delegations.Complete(ctx, receipt.ChildSessionID, receipt.ChildRequestID, "completed", full, "original-error", []string{"original-artifact"}); err != nil { t.Fatal(err) }
    r := capacityRecordFor(t, h, receipt.ID)
    first, err := h.delegationReturnContext(ctx, r, caller.Parent, "parent-thread", 0)
    if err != nil { t.Fatal(err) }
    sum := sha256.Sum256([]byte(full))
    if first.Snapshot.Status != "unavailable" || first.Snapshot.Reason != "file_binary_or_control" || first.Snapshot.ResultSHA256 != hex.EncodeToString(sum[:]) || !strings.HasPrefix(first.Content, delegationReturnText(r)) { t.Fatal(first) }
    correctionReopen(t, h)
    handoffWrite(t, p, d)
    var sends atomic.Int32
    h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, content string) {
        sends.Add(1)
        if content != first.Content { t.Error("binary repair rewrote receipt") }
        h.Emit(protocol.NewDone(s.ID, req))
    }})
    if err := h.reconcileDelegation(ctx, r); err != nil { t.Fatal(err) }
    correctionWaitState(t, h, r.ParentSessionID, r.ParentRequestID, messagequeue.Completed)
    for i:=0; i<3; i++ { if err := h.reconcileDelegation(ctx, r); err != nil { t.Fatal(err) } }
    final := capacityRecordFor(t, h, r.ID)
    if sends.Load()!=1 || final.Result!=full || final.Error!="original-error" || len(final.Artifacts)!=1 || final.Artifacts[0]!="original-artifact" { t.Fatal(sends.Load(), final) }
    saved, found, err := h.delegations.ReturnContext(ctx, r, "parent-thread", 0)
    if err != nil || !found || saved.Content != first.Content || saved.Snapshot.CapturedAt != first.Snapshot.CapturedAt { t.Fatal(saved, found, err) }
}
