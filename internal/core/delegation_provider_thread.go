package core

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
)

// Provider evidence is never converted into a fabricated Codex turn tuple.
// Claude must have its exact original immutable terminal seal and provider
// anchor; Codex retains the original correlated native acceptance.
func (h *Hub) originalDelegationThread(ctx context.Context, root delegation.Record) (string, error) {
	child, found := h.registry.Get(root.ChildSessionID)
	if !found {
		return "", errors.New("delegation_child_unavailable_successor_required")
	}
	if child.Backend() == backend.Claude {
		seal, ok := h.canonicalReviewSeal(ctx, root)
		if !ok {
			return "", errors.New("delegation_provider_binding_missing")
		}
		proof, found, err := h.messageQueue.ProviderEvidence(root.ChildSessionID, root.ChildRequestID)
		if err != nil {
			return "", err
		}
		if !found || proof.Backend != backend.Claude || proof.Status != "succeeded" || proof.Text != seal.Text || proof.ConversationID != child.ResumeID() {
			return "", errors.New("delegation_provider_binding_changed")
		}
		return proof.ConversationID, nil
	}
	proof, accepted, err := h.messageQueue.ExactNativeAcceptance(root.ChildSessionID, root.ChildRequestID)
	if err != nil {
		return "", err
	}
	if !accepted || !h.nativeCorrelationKnown(child, proof.ThreadID, root.ChildRequestID, proof.TurnID) {
		return "", errors.New("delegation_native_binding_missing")
	}
	return proof.ThreadID, nil
}
