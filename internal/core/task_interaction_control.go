package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/session"
	ti "everything-go/internal/taskinteraction"
	"strings"
)

func interactionControlID(id string) bool {
	return strings.HasPrefix(id, "tiwake_") || strings.HasPrefix(id, "tianswer_")
}

// This is the bounded question-delivery lane, not a new delegated work round.
// It never admits arbitrary input into a child: only the server-owned exact
// control record and frozen original task binding may use this lane.
func (h *Hub) interactionControlGuard(ctx context.Context, target *session.Session, request string) (bool, error) {
	if !interactionControlID(request) {
		return false, nil
	}
	if h.interactions == nil {
		return true, errors.New("interaction_control_unavailable")
	}
	id := strings.TrimPrefix(strings.TrimPrefix(request, "tiwake_"), "tianswer_")
	q, e := h.interactions.Get(ctx, id)
	if e != nil {
		return true, e
	}
	if e = h.validInteraction(ctx, q); e != nil {
		return true, e
	}
	if strings.HasPrefix(request, "tiwake_") {
		if q.State != "pending_parent" && q.State != "pending_human" {
			return true, errors.New("interaction_question_no_longer_pending")
		}
		if q.Binding.ParentAuthority != h.cfg.InstanceID || q.Binding.Parent != target.ID {
			return true, errors.New("interaction_control_parent_changed")
		}
	} else if q.Binding.Authority != h.cfg.InstanceID || q.Binding.Child != target.ID || q.Answer == "" || (q.State != "answer_reserved" && q.State != "awaiting_consumption") {
		return true, errors.New("interaction_control_child_changed")
	}
	if h.delegations != nil {
		if known, err := h.delegations.HasChild(ctx, target.ID); err != nil {
			return true, err
		} else if known {
			root, found, err := h.delegations.OriginalRootForChild(ctx, target.ID)
			if err != nil || !found {
				return true, errors.New("interaction_original_profile_missing")
			}
			thread, revision, pinned, err := h.delegations.NativeThread(ctx, target.ID)
			if err != nil || !pinned {
				return true, errors.New("interaction_native_acceptance_unverified")
			}
			if err = h.validateDelegationChild(ctx, root, target, thread, revision, false); err != nil {
				return true, err
			}
			_, _, identity, err := h.delegations.ExecutionBinding(ctx, root.ID)
			if err != nil {
				return true, err
			}
			if identity != "" {
				if current, err := taskRootIdentity(root.Cwd); err != nil || current != identity {
					return true, errors.New("interaction_workspace_changed")
				}
			}
			if _, err = h.TaskWorkerScope(target); err != nil {
				return true, err
			}
		}
	}
	if e = h.interactions.CheckControl(ctx, request, q.ID, target.ID, ""); e != nil {
		return true, e
	}
	if entry, found, e := h.messageQueue.Get(target.ID, request); e != nil {
		return true, e
	} else if found {
		var payload queuedPayload
		if json.Unmarshal(entry.Payload, &payload) != nil || payload.ExpectedTarget == nil || !payload.ExpectedTarget.matches(target) || payload.MessagePurpose != "control" {
			return true, errors.New("interaction_control_payload_changed")
		}
		if e = h.interactions.CheckControl(ctx, request, q.ID, target.ID, ti.Hash(payload.Content)); e != nil {
			return true, e
		}
	}
	return true, nil
}
func (h *Hub) reserveInteractionControl(ctx context.Context, q ti.Record, target, request, content string) error {
	if entry, found, e := h.messageQueue.Get(target, request); e != nil {
		return e
	} else if found {
		// A request predating a server reservation is never adopted by its prefix.
		if e = h.interactions.CheckControl(ctx, request, q.ID, target, ti.Hash(content)); e != nil {
			return e
		}
		var payload queuedPayload
		if json.Unmarshal(entry.Payload, &payload) != nil || payload.Content != content {
			return errors.New("interaction_control_collision")
		}
	}
	return h.interactions.ReserveControl(ctx, request, q.ID, target, ti.Hash(content))
}
