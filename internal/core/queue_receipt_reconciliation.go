package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"time"
)

// A queue cancellation only removes an owned waiting input; it never interrupts
// native execution, repairs uncertain receipts, or consumes a system return.
func (h *Hub) queueInputCancellable(c *Client, e messagequeue.Entry) bool {
	owner := h.pairedTaskDevice(c)
	s, ok := h.registry.Get(e.SessionID)
	if owner == "" || !ok || !h.controllerInScope(s) || s.Snapshot().Hidden || s.State() == session.Closed || !h.controls.MobileMayWrite(s.ID) {
		return false
	}
	if h.work != nil {
		state, err := h.work.Collaboration(context.Background())
		if err != nil {
			return false
		}
		_, task, managed := state.ProjectForSession(s.ID)
		if managed && (task == nil || task.Owner != "human") {
			return false
		}
	}
	if e.State != messagequeue.Queued && e.State != messagequeue.Cancelled {
		return false
	}
	data, found, err := h.messageQueue.TaskAdmission(e.SessionID, e.RequestID)
	if err != nil || !found {
		return false
	}
	var p queuedPayload
	if json.Unmarshal(data, &p) != nil || p.OwnerDevice != owner || p.MessagePurpose != "instruction" {
		return false
	}
	_, accepted, err := h.messageQueue.ExactNativeAcceptance(e.SessionID, e.RequestID)
	return err == nil && !accepted
}

// Pure read. Original states, payloads, revisions and native maps are immutable
// here. The read ID + expected queue revision correlate evidence separately from
// queue state revision, so newly observed acceptance needs no fake transition.
func (h *Hub) reconcileQueueReceipts(c *Client, cmd clientproto.Command) {
	h.reconcileQueueReceiptsWithIdentity(c, cmd, c.readIdentity.Load())
}
func (h *Hub) reconcileQueueReceiptsWithIdentity(c *Client, cmd clientproto.Command, proof *pairedReadIdentity) {
	out := protocol.QueueReceiptsReconciled{Type: "queue_receipts_reconciled", SessionID: cmd.SessionID, RequestID: cmd.RequestID, InstanceID: h.cfg.InstanceID, Status: "forbidden", Receipts: []protocol.QueueReceiptEvidence{}}
	settings := uint64(0)
	originalThread := ""
	bound := false
	var s *session.Session
	send := func() {
		current, ok := h.registry.Get(cmd.SessionID)
		if c.readIdentity.Load() != proof || proof == nil || h.pairedTaskDevice(c) == "" || !ok || (bound && current != s) || !h.controllerInScope(current) || current.Snapshot().Hidden || current.State() == session.Closed || (bound && (current.SettingsSnapshot().ConfigRevision != settings || current.ResumeID() != originalThread)) {
			out.Status = "forbidden"
			out.Receipts = []protocol.QueueReceiptEvidence{}
		}
		c.enqueueEvent(out)
	}
	var ok bool
	s, ok = h.registry.Get(cmd.SessionID)
	if h.pairedTaskDevice(c) == "" || !ok || !h.controllerInScope(s) || s.Snapshot().Hidden || s.State() == session.Closed {
		send()
		return
	}
	settings = s.SettingsSnapshot().ConfigRevision
	bound = true
	originalThread = s.ResumeID()
	if h.messageQueue == nil {
		out.Status = "unsupported"
		send()
		return
	}
	if cmd.RequestID == "" || cmd.ExpectedRevision == nil || len(cmd.TaskRequestIDs) == 0 || len(cmd.TaskRequestIDs) > 100 {
		out.Status = "invalid_request"
		send()
		return
	}
	// Store snapshots and final CAS make this pure read independent of the
	// admission mutex; provider file I/O must not block enqueue/cancel.
	readContext, cancelRead := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelRead()
	snap, err := h.messageQueue.Snapshot(s.ID)
	if err != nil {
		out.Status = "unavailable"
		send()
		return
	}
	out.Revision = snap.Revision
	if out.Revision != *cmd.ExpectedRevision {
		out.Status = "stale_revision"
		send()
		return
	}
	seen := map[string]bool{}
	for _, id := range cmd.TaskRequestIDs {
		if id == "" || seen[id] {
			out.Status = "invalid_request"
			out.Receipts = []protocol.QueueReceiptEvidence{}
			send()
			return
		}
		seen[id] = true
		e, found, err := h.messageQueue.Get(s.ID, id)
		fact := protocol.QueueReceiptEvidence{RequestID: id, Admission: "unknown", Delivery: "unverified", Execution: "unverified", Final: "unverified"}
		if err != nil {
			fact.Reason = "unavailable"
		} else if !found {
			fact.Reason = "not_found"
		} else {
			fact.Admission = string(e.State)
			fact.PayloadHash = e.PayloadHash
			native, accepted, readErr := h.messageQueue.ExactNativeAcceptance(s.ID, id)
			switch {
			case errors.Is(readErr, messagequeue.ErrNativeConflict):
				fact.Reason = "native_conflict"
			case readErr != nil:
				fact.Reason = "native_unavailable"
			case !accepted:
				fact.Reason = "native_map_missing"
			case !taskSourceThread(s, native.ThreadID):
				fact.Reason = "native_thread_mismatch"
			case !h.nativeCorrelationKnown(s, native.ThreadID, id, native.TurnID):
				fact.Reason = "native_correlation_unverified"
			default:
				fact.Delivery = "native_consumed"
				fact.NativeThreadID = native.ThreadID
				fact.NativeTurnID = native.TurnID
				// No last-final/history inference. An adapter must attest this exact
				// persisted original thread/request/turn before exposing final evidence.
				if hr, ok := h.exec.(historyRouter); ok {
					if provider, ok := hr.ProviderFor(s); ok {
						var observed bool
						var readErr error
						if reader, ok := provider.(interface {
							ExactQueueFinalForReceipt(context.Context, string, string, string, string) (bool, error)
						}); ok {
							observed, readErr = reader.ExactQueueFinalForReceipt(readContext, native.ThreadID, id, native.TurnID, e.PayloadHash)
						} else if reader, ok := provider.(interface {
							ExactQueueFinal(string, string, string) (bool, error)
						}); ok {
							observed, readErr = reader.ExactQueueFinal(native.ThreadID, id, native.TurnID)
						} else {
							fact.Reason = "final_unsupported"
						}
						if readErr != nil {
							fact.Reason = "final_unavailable"
							var classified interface{ QueueFinalReason() string }
							if errors.As(readErr, &classified) {
								fact.Reason = classified.QueueFinalReason()
							}
						} else if observed {
							fact.Final = "observed"
						} else if fact.Reason == "" {
							fact.Reason = "final_not_observed"
						}
					}
				}
			}
			fact.CanCancel = h.queueInputCancellable(c, e)
		}
		out.Receipts = append(out.Receipts, fact)
	}
	// Recheck canonical evidence after provider I/O. Cross-process queue changes
	// or a conflicting late native tuple cannot release stale positive facts.
	currentSnapshot, err := h.messageQueue.Snapshot(s.ID)
	if err != nil {
		out.Status = "unavailable"
		out.Receipts = []protocol.QueueReceiptEvidence{}
		send()
		return
	}
	if currentSnapshot.Revision != out.Revision {
		out.Status = "stale_revision"
		out.Revision = currentSnapshot.Revision
		out.Receipts = []protocol.QueueReceiptEvidence{}
		send()
		return
	}
	for i := range out.Receipts {
		fact := &out.Receipts[i]
		if fact.Delivery != "native_consumed" {
			continue
		}
		native, found, err := h.messageQueue.ExactNativeAcceptance(s.ID, fact.RequestID)
		currentEntry, entryFound, entryErr := h.messageQueue.Get(s.ID, fact.RequestID)
		if err != nil || !found || entryErr != nil || !entryFound || currentEntry.PayloadHash != fact.PayloadHash || native.ThreadID != fact.NativeThreadID || native.TurnID != fact.NativeTurnID || !h.nativeCorrelationKnown(s, native.ThreadID, fact.RequestID, native.TurnID) {
			fact.Delivery = "unverified"
			fact.Final = "unverified"
			fact.NativeThreadID = ""
			fact.NativeTurnID = ""
			fact.CanCancel = false
			fact.Reason = "native_changed"
		}
	}
	out.Status = "supported"
	send()
}

func (h *Hub) sendReadonlyMessageQueue(c *Client, cmd clientproto.Command) {
	s, ok := h.registry.Get(cmd.SessionID)
	if h.pairedTaskDevice(c) == "" || !ok || !h.controllerInScope(s) || s.Snapshot().Hidden || s.State() == session.Closed {
		h.queueError(c, cmd, "permission_denied", "Queue scope is unavailable")
		return
	}
	if h.messageQueue == nil {
		h.queueError(c, cmd, "unsupported", "Message queue is unavailable")
		return
	}
	snapshot, err := h.messageQueueSnapshot(cmd.SessionID)
	if err != nil {
		h.queueError(c, cmd, "queue_unavailable", "Original queue cannot be read")
		return
	}
	// No reconcileMaintenance/resume/actor/provider invocation or write.
	current, ok := h.registry.Get(cmd.SessionID)
	if h.pairedTaskDevice(c) == "" || !ok || current != s || !h.controllerInScope(current) || current.Snapshot().Hidden || current.State() == session.Closed {
		h.queueError(c, cmd, "permission_denied", "Queue scope changed")
		return
	}
	c.enqueueEvent(snapshot)
}
