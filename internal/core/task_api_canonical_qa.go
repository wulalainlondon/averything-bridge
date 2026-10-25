package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
)

// Read the existing immutable review for the selected round. No verdict is
// supplied by a caller, and no review or publisher ledger is written here.
func (h *Hub) projectCanonicalDelegationQA(ctx context.Context, selected string, task map[string]any, axes map[string]string) error {
	qaID, verdict, err := h.delegations.QAForRound(ctx, selected)
	if err != nil {
		return err
	}
	if qaID == "" {
		round, found, err := h.delegations.Round(ctx, selected)
		if err != nil {
			return err
		}
		if found {
			owner, qa, err := h.delegations.Responsibility(ctx, round.LogicalID)
			if err != nil {
				return err
			}
			if owner == round.LogicalID && qa != "" {
				axes["qa"] = "pending"
				task["qa"] = map[string]any{"status": "pending", "reviewer_ref": apiID(h.cfg.InstanceID, "delegation", qa), "evidence_refs": []any{}}
			}
		}
		return nil
	}
	set := func(status string, refs []any) {
		axes["qa"] = status
		task["qa"] = map[string]any{"status": status, "evidence_refs": refs}
	}
	set("unknown", []any{}) // a recorded review without its proof is not PASS
	owner, found, err := h.delegations.ByID(ctx, selected)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	qa, found, err := h.delegations.ByID(ctx, qaID)
	if err != nil {
		return err
	}
	if !found || owner.ParentSessionID != qa.ParentSessionID || owner.ChildSessionID == qa.ChildSessionID {
		return nil
	}
	om, found, err := h.delegations.Round(ctx, owner.ID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	qm, found, err := h.delegations.Round(ctx, qa.ID)
	if err != nil {
		return err
	}
	if !found || om.LogicalID == qm.LogicalID {
		return nil
	}
	boundOwner, boundQA, err := h.delegations.Responsibility(ctx, om.LogicalID)
	if err != nil {
		return err
	}
	if boundOwner != om.LogicalID || boundQA != qm.LogicalID {
		return nil
	}
	ownerSeal, ok := h.canonicalReviewSeal(ctx, owner)
	if !ok || task["seal_id"] != ownerSeal.ID {
		return nil
	}
	var selectedAnchor map[string]any
	if json.Unmarshal(ownerSeal.Anchor, &selectedAnchor) != nil {
		return nil
	}
	wantedAnchor, err := json.Marshal(selectedAnchor)
	if err != nil {
		return err
	}
	projectedAnchor, err := json.Marshal(task["result_anchor"])
	if err != nil {
		return err
	}
	if string(projectedAnchor) != string(wantedAnchor) {
		return nil
	}
	qaSeal, ok := h.canonicalReviewSeal(ctx, qa)
	if !ok {
		return nil
	}
	var qaAnchor map[string]any
	if json.Unmarshal(qaSeal.Anchor, &qaAnchor) != nil {
		return nil
	}
	ownerExecution, _ := selectedAnchor["execution"].(map[string]any)
	qaExecution, _ := qaAnchor["execution"].(map[string]any)
	if ownerExecution["backend"] == qaExecution["backend"] && ownerExecution["native_thread_id"] == qaExecution["native_thread_id"] && ownerExecution["provider_session_id"] == qaExecution["provider_session_id"] {
		return nil
	}
	var review struct {
		OwnerRound string `json:"owner_round_id"`
		Verdict    string `json:"verdict"`
	}
	if json.Unmarshal([]byte(qaSeal.Text), &review) != nil || review.OwnerRound != selected || review.Verdict != verdict || (verdict != "PASS" && verdict != "FAIL") {
		return nil
	}
	status := "failed"
	if verdict == "PASS" {
		status = "passed"
	}
	refs := []any{map[string]any{"kind": "qa", "authority_instance_id": h.cfg.InstanceID, "record_id": qa.ID, "hash": qaSeal.Hash, "verified": true}}
	set(status, refs)
	projected := task["qa"].(map[string]any)
	projected["reviewer_ref"] = apiID(h.cfg.InstanceID, "delegation", qm.LogicalID)
	projected["task_revision"] = task["revision"]
	// The sealed review proves the round verdict, not individual acceptance
	// criteria. The existing public contract requires this collection for PASS;
	// keep it empty instead of inventing criterion-level evidence.
	projected["criterion_evidence"] = []any{}
	return nil
}

func (h *Hub) canonicalReviewSeal(ctx context.Context, record delegation.Record) (messagequeue.TaskSeal, bool) {
	if record.State != "terminal" || record.TerminalStatus != "completed" {
		return messagequeue.TaskSeal{}, false
	}
	seal, err := h.messageQueue.TaskSeal(ctx, record.ChildSessionID, record.ChildRequestID, "")
	if err != nil || seal.Text != record.Result {
		return seal, false
	}
	hash := sha256.Sum256([]byte(seal.Text))
	if hex.EncodeToString(hash[:]) != seal.Hash || seal.ID != "seal_"+seal.Hash {
		return seal, false
	}
	conflict, err := h.messageQueue.NativeConflict(record.ChildSessionID, record.ChildRequestID)
	if err != nil || conflict {
		return seal, false
	}
	var anchor map[string]any
	if json.Unmarshal(seal.Anchor, &anchor) != nil || anchor["instance_id"] != h.cfg.InstanceID || anchor["session_id"] != record.ChildSessionID || anchor["request_id"] != record.ChildRequestID {
		return seal, false
	}
	child, found := h.registry.Get(record.ChildSessionID)
	if !found {
		return seal, false
	}
	if child.Backend() == backend.Claude {
		proof, found, err := h.messageQueue.ProviderEvidence(record.ChildSessionID, record.ChildRequestID)
		execution, ok := anchor["execution"].(map[string]any)
		if err != nil || !found || !ok || proof.Backend != backend.Claude || proof.Status != "succeeded" || proof.Text != seal.Text || proof.TokenKind != "message_uuid" || proof.Token == "" || proof.ConversationID != child.ResumeID() || execution["backend"] != backend.Claude || execution["provider_session_id"] != proof.ConversationID || execution["provider_token"] != proof.Token || execution["token_kind"] != proof.TokenKind {
			return seal, false
		}
		providerAnchor := h.claudeAnchorID(child, proof)
		if providerAnchor == "" || anchor["source_message_id"] != providerAnchor {
			return seal, false
		}
		return seal, true
	}
	thread, turn, known := h.messageQueue.NativeExecution(record.ChildSessionID, record.ChildRequestID)
	return seal, known && h.nativeSealKnown(child, anchor, record.ChildRequestID, thread, turn)
}
