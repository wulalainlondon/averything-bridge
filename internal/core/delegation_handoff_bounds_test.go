package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/handoff"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func TestHandoffBoundsReturnFreezeReopenAndReplay(t *testing.T) {
	for _, mode := range []string{"depth", "timestamp", "escaped-summary"} {
		for _, boundary := range []string{"before-queue", "queued"} {
			t.Run(mode+"-"+boundary, func(t *testing.T) {
				h, caller := capacityHub(t, 1)
				ctx := context.Background()
				p, d := handoffFixture(t, h, caller.Parent)
				switch mode {
				case "depth":
					raw := `{"schema_version":1,"revision":1,"goal":` + strings.Repeat("[", 8000) + "0" + strings.Repeat("]", 8000) + `}`
					if err := os.WriteFile(filepath.Join(p.Workspace, p.Path), []byte(raw), 0600); err != nil {
						t.Fatal(err)
					}
				case "timestamp":
					d.UpdatedAt = time.Now().Add(-time.Minute).UTC().Format("2006-01-02T15:04:05") + "." + strings.Repeat("0", 7000) + "Z"
					handoffWrite(t, p, d)
				case "escaped-summary":
					d.Goal = strings.Repeat("\t", 1023) + "g"
					d.Progress = d.Goal
					d.NextStep = d.Goal
					handoffWrite(t, p, d)
				}
				receipt := capacityDelegate(t, h, caller, "bounds-"+mode)
				full := strings.Repeat("真", 17000) + "tail-preserved"
				if _, err := h.delegations.Complete(ctx, receipt.ChildSessionID, receipt.ChildRequestID, "completed", full, "original-error", []string{"original-artifact"}); err != nil {
					t.Fatal(err)
				}
				r := capacityRecordFor(t, h, receipt.ID)
				first, err := h.delegationReturnContext(ctx, r, caller.Parent, "parent-thread", 0)
				if err != nil {
					t.Fatal(err)
				}
				original := delegationReturnText(r)
				sum := sha256.Sum256([]byte(full))
				reminder := strings.TrimPrefix(first.Content, original)
				if first.Snapshot.Status != "unavailable" || first.Snapshot.DelegationID != r.ID || first.Snapshot.ResultSHA256 != hex.EncodeToString(sum[:]) || first.Snapshot.Path != filepath.Join(p.Workspace, p.Path) || !strings.HasPrefix(first.Content, original) || len(reminder) > handoff.MaxReminderBytes {
					t.Fatal("bounds lost result association", first.Snapshot)
				}
				if mode == "timestamp" || mode == "escaped-summary" {
					raw, err := os.ReadFile(filepath.Join(p.Workspace, p.Path))
					if err != nil {
						t.Fatal(err)
					}
					fileSum := sha256.Sum256(raw)
					if first.Snapshot.SHA256 != hex.EncodeToString(fileSum[:]) || first.Snapshot.Revision != d.Revision {
						t.Fatal("file association changed")
					}
				}
				// Reminder JSON and persisted snapshot must describe the same association.
				at := strings.Index(reminder, "\n{")
				if at < 0 {
					t.Fatal("no quoted reminder metadata")
				}
				var projected handoff.Snapshot
				if err := json.Unmarshal([]byte(reminder[at+1:]), &projected); err != nil || !reflect.DeepEqual(projected, first.Snapshot) {
					t.Fatal("snapshot/reminder differ", err)
				}
				if boundary == "queued" {
					release := caller.Parent.HoldQueue()
					defer release()
					if err := h.reconcileDelegation(ctx, r); err != nil {
						t.Fatal(err)
					}
					if q := handoffQueuedContent(t, h, r.ParentSessionID, r.ParentRequestID); q.Content != first.Content || q.ExpectedTarget == nil || q.ExpectedTarget.ThreadID != "parent-thread" || q.ExpectedTarget.Revision != 0 {
						t.Fatal("queue source/content changed")
					}
				}
				// Repair and configuration changes cannot replace the first snapshot.
				d.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
				d.Goal = "repaired"
				d.Progress = "repaired"
				d.NextStep = "review"
				d.Revision++
				handoffWrite(t, p, d)
				h.cfg.DelegationHandoff = nil
				correctionReopen(t, h)
				parent, _ := h.registry.Get(r.ParentSessionID)
				saved, err := h.delegationReturnContext(ctx, r, parent, "parent-thread", 0)
				if err != nil || !reflect.DeepEqual(saved, first) {
					t.Fatal("restart changed immutable context", err)
				}
				// Exercise the transaction's existing-snapshot path as well as read/replay.
				resealed, err := h.delegations.SealReturnContext(ctx, r, "parent-thread", 0, handoff.Snapshot{Status: "disabled"}, "replacement")
				if err != nil || !reflect.DeepEqual(resealed, first) {
					t.Fatal("second sealing changed context", err)
				}
				for _, change := range []string{"parent", "task", "request", "result", "thread", "revision"} {
					wrong := r
					thread := "parent-thread"
					var rev uint64
					switch change {
					case "parent":
						wrong.ParentSessionID = "other"
					case "task":
						wrong.ID = "other"
					case "request":
						wrong.ParentRequestID = "other"
					case "result":
						wrong.Result = "other"
					case "thread":
						thread = "other"
					case "revision":
						rev = 1
					}
					if _, err := h.delegations.SealReturnContext(ctx, wrong, thread, rev, first.Snapshot, original); err == nil {
						t.Fatal("foreign association accepted", change)
					}
				}
				var sends atomic.Int32
				h.SetExecutor(&fakeExec{sink: h, onSend: func(s *session.Session, req, content string) {
					sends.Add(1)
					if content != first.Content {
						t.Error("replay changed payload")
					}
					h.Emit(protocol.NewDone(s.ID, req))
				}})
				if boundary == "queued" {
					h.resumeQueuedMessages()
				} else {
					if err := h.reconcileDelegation(ctx, r); err != nil {
						t.Fatal(err)
					}
				}
				correctionWaitState(t, h, r.ParentSessionID, r.ParentRequestID, messagequeue.Completed)
				for i := 0; i < 3; i++ {
					if err := h.reconcileDelegation(ctx, r); err != nil {
						t.Fatal(err)
					}
				}
				final := capacityRecordFor(t, h, r.ID)
				if sends.Load() != 1 || final.Result != full || final.Error != "original-error" || final.TerminalStatus != "completed" || len(final.Artifacts) != 1 || final.Artifacts[0] != "original-artifact" || final.ParentRequestID != r.ParentRequestID || parent.ResumeID() != "parent-thread" || parent.SettingsSnapshot().Sandbox != "workspace-write" {
					t.Fatal("bounded metadata changed child/source/permissions", sends.Load(), final)
				}
			})
		}
	}
}

func TestHandoffBoundsRootCannotWidenParentScope(t *testing.T) {
	for _, mode := range []string{"sibling-root", "workspace-mismatch"} {
		t.Run(mode, func(t *testing.T) {
			h, caller := capacityHub(t, 1)
			ctx := context.Background()
			p, _ := handoffFixture(t, h, caller.Parent)
			receipt := capacityDelegate(t, h, caller, "root-bounds")
			handoffComplete(t, h, receipt)
			r := capacityRecordFor(t, h, receipt.ID)
			if mode == "sibling-root" {
				h.cfg.RootDir = t.TempDir()
			} else {
				p.Workspace = filepath.Dir(p.Workspace)
				h.cfg.DelegationHandoff.Parents[caller.Parent.ID] = p
			}
			c, err := h.delegationReturnContext(ctx, r, caller.Parent, "parent-thread", 0)
			if err != nil || c.Snapshot.Reason != "workspace_scope_mismatch" || c.Snapshot.Status != "unavailable" || c.Snapshot.DelegationID != r.ID || !strings.HasPrefix(c.Content, delegationReturnText(r)) {
				t.Fatal("scope widened/result lost", c.Snapshot, err)
			}
			final := capacityRecordFor(t, h, r.ID)
			if final.Result != r.Result || final.DeliveryState != "pending" || final.TerminalStatus != "completed" || caller.Parent.SettingsSnapshot().Sandbox != "workspace-write" {
				t.Fatal("scope failure changed task", final)
			}
		})
	}
}
