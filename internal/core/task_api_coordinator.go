package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"time"
)

// One instance coordinator reconciles only original durable outboxes. A native
// uncertain receipt is never replayed. It does not spawn model turns of its own.
func (h *Hub) StartTaskCoordinator(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for path, j := range h.apiJournals() {
					if path == "pm_v1" {
						continue
					}
					records, err := j.Pending(ctx)
					if err != nil {
						continue
					}
					for _, r := range records {
						var metadata apiMetadata
						if json.Unmarshal(r.Metadata, &metadata) != nil {
							continue
						}
						if metadata.Request.Operation == "cancel" {
							entry, found, e := h.messageQueue.Get(r.SessionID, r.RequestID)
							if e != nil {
								continue
							}
							if origin, e := h.messageQueue.CancelOrigin(r.SessionID, r.RequestID); e == nil && found && entry.State == messagequeue.Cancelled && origin == messagequeue.Queued {
								_ = j.SaveOutcome(ctx, r.Key, nil)
							} else {
								_ = j.Outbox(ctx, r.Key, "unknown")
							}
							continue
						}
						entry, found, err := h.messageQueue.Get(r.SessionID, r.RequestID)
						if err != nil {
							continue
						}
						if found {
							state := "queued"
							if entry.State == messagequeue.Uncertain {
								state = "unknown"
							}
							if entry.State == messagequeue.Completed || entry.State == messagequeue.Failed || entry.State == messagequeue.Cancelled {
								if entry.State == messagequeue.Cancelled {
									if origin, e := h.messageQueue.CancelOrigin(r.SessionID, r.RequestID); e != nil || origin != messagequeue.Queued {
										_ = j.Outbox(ctx, r.Key, "unknown")
										continue
									}
								}
								state = "resolved"
								if r.Path == "pm_v1" || r.Path == "pm_v2" {
									state = "queued"
								} // PM native delivery/QA adapter is not yet admitted.
								if r.Path == "controller" || r.Path == "delegation" {
									delivery, _ := h.apiDelivery(ctx, r)
									if delivery != "delivered" && delivery != "failed" && delivery != "not_requested" {
										state = "queued"
									}
								}
							}
							_ = j.Outbox(ctx, r.Key, state)
							continue
						}
						if path == "delegation" {
							if d, ok, e := h.delegations.ByChildRequest(ctx, r.SessionID, r.RequestID); e == nil && ok {
								if d.State == "terminal" && (d.DeliveryState == "not_requested" || d.DeliveryState == "failed" || d.DeliveryState == "delivered") {
									_ = j.Outbox(ctx, r.Key, "resolved")
								} else {
									h.wakeDelegations()
								}
								continue
							}
						}
						if metadata.Request.Operation != "create_dispatch" {
							// Missing append queue identity is ambiguous after a crash.
							// Never provision a child or mint/replay another command.
							_ = j.Outbox(ctx, r.Key, "unknown")
							continue
						}
						if metadata.Caller.SourceSessionID != "" {
							source, ok := h.registry.Get(metadata.Caller.SourceSessionID)
							if !ok || source.State() == session.Closed || source.Snapshot().Hidden || source.ResumeID() != metadata.SourceThread || source.SettingsSnapshot().ConfigRevision != metadata.SourceRevision || !h.controls.MobileMayWrite(source.ID) {
								_ = j.Outbox(ctx, r.Key, "unknown")
								continue
							}
						}
						if path == "controller" {
							if original, found, err := h.dispatches.Get(ctx, r.NativeID); err == nil && found && original.State == "prepared" {
								source, ok := h.registry.Get(original.ParentID)
								if !ok || source.ResumeID() != original.ParentThreadID {
									continue
								}
								grant, err := h.dispatches.Grant(ctx, source.ID)
								if err != nil || !grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, original.SessionID) {
									continue
								}
								h.submitLocalSessionDispatch(ctx, original)
							}
						} else if path == "delegation" {
							// A single cyclic scheduler owns promotion/provisioning for both
							// native and API admissions. The journal never bypasses its capacity.
							h.wakeDelegations()
						} else if path == "ordinary" {
							// Ordinary admission is atomic with its original queue
							// entry. Missing identity cannot be a recoverable prepared
							// enqueue; quarantine it rather than inventing an effect.
							_ = j.Outbox(ctx, r.Key, "unknown")
						}
					}
					_ = j.Prune(ctx, time.Now())
				}
			}
		}
	}()
}
