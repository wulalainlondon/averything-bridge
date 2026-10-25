package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

// This scope is the original parent's own delegation facts, not a controller
// grant. No global catalog/foreign-child permission is added to workers.
func (h *Hub) continuityCaller(parent *session.Session, request, call string) error {
	if !apiOwnedAdmissionEnabled {
		return errors.New("delegation_continuation_admission_held")
	}
	if err := h.delegationQueueParent(parent); err != nil {
		return err
	}
	if request == "" || call == "" || parent.ActiveQueuedID() != request || strings.HasPrefix(parent.ID, "s_dg_") || strings.HasPrefix(parent.ID, "pm_") {
		return errors.New("delegation_parent_not_owned")
	}
	return nil
}
func continuityDigest(v any) string {
	b, _ := json.Marshal(v)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// A delegation capability is immutable in the original stored profile and
// source binding. API workers additionally retain the original formal scope.
func delegationGrantDigest(r delegation.Record, thread string, revision uint64) string {
	return continuityDigest([]any{r.ParentSessionID, thread, revision, r.ChildSessionID, r.Cwd, r.Sandbox, r.Model, r.Effort, r.WorkspaceIdentity})
}
func (h *Hub) continuityBase(parent *session.Session, id string) (delegation.Record, delegation.Round, *session.Session, error) {
	return h.continuityBaseAt(parent, id, nil)
}

// continuitySourceAt resolves immutable root provenance and the explicitly
// approved source epoch. It performs no child lookup, native receipt upgrade or
// execution eligibility check, so sealed terminal failures/results can return.
func (h *Hub) continuitySourceAt(ctx context.Context, parent *session.Session, id string, expectedSource *uint64) (delegation.Record, delegation.Round, error) {
	if e := h.delegationQueueParent(parent); e != nil {
		return delegation.Record{}, delegation.Round{}, e
	}
	base, found, e := h.delegations.ByID(ctx, id)
	if e != nil {
		return base, delegation.Round{}, e
	}
	if !found || base.ParentSessionID != parent.ID {
		return base, delegation.Round{}, errors.New("delegation_continuation_forbidden")
	}
	round, found, e := h.delegations.Round(ctx, id)
	if e != nil {
		return base, round, e
	}
	if !found {
		return base, round, errors.New("delegation_continuity_binding_missing")
	}
	root, ok, e := h.delegations.ByID(ctx, round.LogicalID)
	if e != nil {
		return base, round, e
	}
	if !ok || root.ParentSessionID != parent.ID || root.ChildSessionID != base.ChildSessionID {
		return base, round, errors.New("delegation_continuation_forbidden")
	}
	source, revision, bound, e := h.delegations.SourceBinding(ctx, root.ID)
	if e != nil {
		return base, round, e
	}
	_, _, root.WorkspaceIdentity, e = h.delegations.ExecutionBinding(ctx, root.ID)
	if e != nil {
		return base, round, e
	}
	originalRevision := revision
	epoch, hasEpoch, e := h.delegations.SourceEpoch(ctx, root.ID)
	if e != nil {
		return base, round, e
	}
	if hasEpoch {
		if epoch.Parent != parent.ID || epoch.Authority != h.cfg.InstanceID || epoch.Thread != source || epoch.GrantDigest != delegationGrantDigest(root, source, originalRevision) {
			return base, round, errors.New("delegation_reauthorization_binding_changed")
		}
		revision = epoch.ToRevision
	}
	wantRevision := parent.SettingsSnapshot().ConfigRevision
	if expectedSource != nil {
		wantRevision = *expectedSource
	}
	if !bound || parent.ResumeID() != source || wantRevision != revision {
		return base, round, errors.New("delegation_source_binding_changed")
	}
	if round.Authority != "" && round.Authority != h.cfg.InstanceID {
		return base, round, errors.New("delegation_wrong_authority")
	}
	rootMeta, exists, e := h.delegations.Round(ctx, root.ID)
	if e != nil {
		return base, round, e
	}
	if !exists || (rootMeta.Authority != "" && rootMeta.Authority != h.cfg.InstanceID) {
		return base, round, errors.New("delegation_wrong_authority")
	}
	if rootMeta.GrantDigest != "" && rootMeta.GrantDigest != delegationGrantDigest(root, source, originalRevision) {
		return base, round, errors.New("delegation_grants_changed")
	}
	root.SourceThread, root.SourceRevision = source, revision
	return root, round, nil
}

// expectedSource is used only by explicit reauthorization to validate the old epoch.
func (h *Hub) continuityBaseAt(parent *session.Session, id string, expectedSource *uint64) (delegation.Record, delegation.Round, *session.Session, error) {
	ctx := context.Background()
	root, round, e := h.continuitySourceAt(ctx, parent, id, expectedSource)
	if e != nil {
		return root, round, nil, e
	}
	base := root
	proofThread, e := h.originalDelegationThread(ctx, root)
	if e != nil {
		return base, round, nil, e
	}
	thread, rev, pinned, e := h.delegations.NativeThread(ctx, root.ChildSessionID)
	if e != nil {
		return base, round, nil, e
	}
	// Upgrade legacy records only from the exact ORIGINAL acceptance receipt,
	// with a profile match to the original accepted record (never today's profile).
	if !pinned {
		_, rev, _, e = h.delegations.ExecutionBinding(ctx, root.ID)
		if e != nil {
			return base, round, nil, e
		}
		thread = proofThread
		if e = h.delegations.PinNativeThread(ctx, root.ChildSessionID, thread, rev); e != nil {
			return base, round, nil, e
		}
	}
	if pinned && thread != proofThread {
		return base, round, nil, errors.New("delegation_native_binding_changed")
	}
	child, ok := h.registry.Get(root.ChildSessionID)
	if !ok {
		return base, round, nil, errors.New("delegation_child_unavailable_successor_required")
	}
	if e = h.validateDelegationChild(ctx, root, child, thread, rev, true); e != nil {
		return base, round, nil, e
	}
	root.AcceptedBackend = child.Backend()
	root.ExpectedThread, root.ExpectedRevision = thread, rev
	_, _, root.WorkspaceIdentity, e = h.delegations.ExecutionBinding(ctx, root.ID)
	if e != nil {
		return base, round, nil, e
	}
	return root, round, child, nil
}
func (h *Hub) ContinueDelegation(parent *session.Session, request, call string, in backend.DelegationContinuation) (any, error) {
	if err := h.continuityCaller(parent, request, call); err != nil {
		return nil, err
	}
	if in.InstanceID != h.cfg.InstanceID || in.DelegationID == "" || in.ExpectedPreviousID == "" || strings.TrimSpace(in.Content) == "" || len(in.Content) > maxDelegationInstruction || (in.Mode != "queue" && in.Mode != "steer") || (in.Mode == "queue" && in.ActiveRequestID != "") {
		return nil, errors.New("delegation_continuation_invalid")
	}
	ctx := context.Background()
	hash := continuityDigest(in)
	// Unknown acceptance is resolved by the original tool-call receipt before
	// freshness checks; never mint a new ID or reinterpret the old payload.
	if old, found, e := h.delegations.ByOrigin(ctx, parent.ID, request, call); e != nil {
		return nil, e
	} else if found {
		if old.IntentHash != hash {
			return nil, errors.New("delegation_intent_conflict")
		}
		return h.continuityReceipt(ctx, old)
	}
	// Formal bounded workers retain the existing TaskAPI append enforcer.
	// This legacy native continuation route cannot silently replace it.
	original, originalRound, e := h.continuitySourceAt(ctx, parent, in.DelegationID, nil)
	if e != nil {
		return nil, e
	}
	if _, formal, e := h.delegations.APIBackend(ctx, original.ChildSessionID); e != nil {
		return nil, e
	} else if formal && in.Mode == "queue" {
		latest, e := h.delegations.LatestRound(ctx, originalRound.LogicalID)
		if e != nil {
			return nil, e
		}
		if latest.ID != in.ExpectedPreviousID {
			return nil, errors.New("delegation_stale_predecessor")
		}
		child, found := h.registry.Get(original.ChildSessionID)
		if !found || child.ResumeID() != in.ExpectedThreadID || child.SettingsSnapshot().ConfigRevision != in.ExpectedConfigRevision {
			return nil, errors.New("delegation_child_binding_changed")
		}
		return nil, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	root, round, child, e := h.continuityBase(parent, in.DelegationID)
	if e != nil {
		return nil, e
	}
	if _, api, e := h.delegations.APIBackend(ctx, child.ID); e != nil {
		return nil, e
	} else if api && in.Mode == "queue" {
		return nil, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	// The serialized store checks predecessor freshness AFTER same-key lookup,
	// so concurrent retries receive the original receipt instead of stale errors.
	if child.ResumeID() != in.ExpectedThreadID || child.SettingsSnapshot().ConfigRevision != in.ExpectedConfigRevision {
		return nil, errors.New("delegation_child_binding_changed")
	}
	if e = h.validateDelegationBindingForContinuation(ctx, root, parent, child); e != nil {
		return nil, e
	}
	if in.Mode == "steer" {
		active, found, e := h.delegations.ByChildRequest(ctx, child.ID, in.ActiveRequestID)
		if e != nil {
			return nil, e
		}
		if !found || active.ParentSessionID != parent.ID || active.State != "running" || !child.IsStreaming() || child.ActiveQueuedID() != in.ActiveRequestID {
			return nil, errors.New("delegation_steer_active_round_required")
		}
		proof, found, e := h.messageQueue.ExactNativeAcceptance(child.ID, in.ActiveRequestID)
		if e != nil {
			return nil, e
		}
		if !found || proof.ThreadID != root.ExpectedThread {
			return nil, errors.New("delegation_steer_native_binding_missing")
		}
	}
	id := "dgfollow_" + randomID()
	r := delegation.Record{ID: id, LogicalID: round.LogicalID, PreviousID: in.ExpectedPreviousID, Authority: h.cfg.InstanceID, Mode: in.Mode, ExecutionRequestID: in.ActiveRequestID, ParentSessionID: parent.ID, OriginRequestID: request, ToolCallID: call, IntentHash: hash, ChildSessionID: child.ID, ChildRequestID: "r_" + randomID(), ParentRequestID: "dgreturn_" + id, ChildName: root.ChildName, Cwd: root.Cwd, Instruction: in.Content, Model: root.Model, Effort: root.Effort, Sandbox: root.Sandbox, SourceThread: root.SourceThread, SourceRevision: root.SourceRevision, ExpectedThread: root.ExpectedThread, ExpectedRevision: root.ExpectedRevision, WorkspaceIdentity: root.WorkspaceIdentity}
	r.GrantDigest = delegationGrantDigest(root, root.SourceThread, root.SourceRevision)
	stored, e := h.delegations.Continue(ctx, r, func() error {
		if e := h.continuityCaller(parent, request, call); e != nil {
			return e
		}
		return h.validateDelegationBindingForContinuation(ctx, root, parent, child)
	})
	if e != nil {
		return nil, e
	}
	// Scheduler alone owns effects and recovery. The tool ACK is acceptance only.
	h.wakeDelegations()
	return h.continuityReceipt(ctx, stored)
}
func (h *Hub) validateDelegationBindingForContinuation(ctx context.Context, r delegation.Record, parent, child *session.Session) error {
	actual, ok := h.registry.Get(parent.ID)
	target, targetOK := h.registry.Get(child.ID)
	if !ok || actual != parent || !targetOK || target != child || parent.ResumeID() != r.SourceThread || parent.SettingsSnapshot().ConfigRevision != r.SourceRevision || !h.controls.MobileMayWrite(child.ID) {
		return errors.New("delegation_continuation_binding_changed")
	}
	if e := h.delegationQueueParent(parent); e != nil {
		return e
	}
	// Existing validator checks workspace inode, sandbox, original formal scope,
	// archive/delete, backend/model/effort and exact source identity at Send too.
	if e := validateDelegationChildProfile(r, child, r.ExpectedThread, r.ExpectedRevision, r.AcceptedBackend, true); e != nil {
		return e
	}
	if got, e := taskRootIdentity(r.Cwd); e != nil || got != r.WorkspaceIdentity {
		return errors.New("delegation_workspace_identity_changed")
	}
	return nil
}
func (h *Hub) continuityReceipt(ctx context.Context, r delegation.Record) (any, error) {
	meta, _, e := h.delegations.Round(ctx, r.ID)
	if e != nil {
		return nil, e
	}
	return map[string]any{"delegation_id": r.ID, "logical_task_id": meta.LogicalID, "round_version": meta.Version, "previous_round_id": meta.PreviousID, "child_session_id": r.ChildSessionID, "child_request_id": r.ChildRequestID, "parent_return_request_id": r.ParentRequestID, "execution": r.State, "delivery": r.DeliveryState, "transport": "native_dynamic_tool_delegation_continuation", "acceptance": "known_receipt"}, nil
}
func (h *Hub) DelegationRounds(parent *session.Session, id string, after uint64) (any, error) {
	if e := h.delegationQueueParent(parent); e != nil {
		return nil, e
	}
	ctx := context.Background()
	record, found, e := h.delegations.ByID(ctx, id)
	if e != nil {
		return nil, e
	}
	if !found || record.ParentSessionID != parent.ID {
		return nil, errors.New("delegation_continuation_forbidden")
	}
	round, found, e := h.delegations.Round(ctx, id)
	if e != nil {
		return nil, e
	}
	if !found || (round.Authority != "" && round.Authority != h.cfg.InstanceID) {
		return nil, errors.New("delegation_continuity_binding_missing")
	}
	root, found, e := h.delegations.ByID(ctx, round.LogicalID)
	if e != nil {
		return nil, e
	}
	if !found || root.ParentSessionID != parent.ID {
		return nil, errors.New("delegation_continuation_forbidden")
	}
	root.ExpectedThread, _, _, e = h.delegations.NativeThread(ctx, root.ChildSessionID)
	if e != nil {
		return nil, e
	}
	rounds, e := h.delegations.RoundsAfter(ctx, round.LogicalID, after)
	if e != nil {
		return nil, e
	}
	owner, qa, e := h.delegations.Responsibility(context.Background(), round.LogicalID)
	if e != nil {
		return nil, e
	}
	out := []any{}
	for _, m := range rounds {
		r, found, e := h.delegations.ByID(context.Background(), m.ID)
		if e != nil || !found {
			return nil, errors.New("delegation_round_missing")
		}
		review, verdict, e := h.delegations.QAForRound(context.Background(), m.ID)
		if e != nil {
			return nil, e
		}
		out = append(out, map[string]any{"relation": m, "dispatcher_session_id": r.ParentSessionID, "dispatcher_request_id": r.OriginRequestID, "request_id": r.ChildRequestID, "result_version": m.Version, "execution": r.State, "terminal_status": r.TerminalStatus, "delivery": r.DeliveryState, "parent_return_request_id": r.ParentRequestID, "qa_round_id": review, "qa_verdict": verdict, "successor_reason": ""})
	}
	latest, e := h.delegations.LatestRound(ctx, round.LogicalID)
	if e != nil {
		return nil, e
	}
	next := after
	if len(rounds) > 0 {
		next = rounds[len(rounds)-1].Version
	}
	availability, reason := "available", ""
	if child, found := h.registry.Get(root.ChildSessionID); !found {
		availability, reason = "successor_required", "original_session_deleted"
	} else if child.State() == session.Closed || child.SettingsSnapshot().Hidden {
		availability, reason = "successor_required", "original_session_closed_or_archived"
	}
	if _, formal, e := h.delegations.APIBackend(ctx, root.ChildSessionID); e != nil {
		return nil, e
	} else if formal && availability == "available" {
		retained, _, child, e := h.continuityBase(parent, root.ID)
		if e != nil {
			availability, reason = "unsupported", "original_provider_binding_unverified"
		} else {
			raw, _, e := h.delegations.APIChildMetadata(ctx, child.ID)
			var meta apiMetadata
			var input apiInput
			if e != nil || json.Unmarshal(raw, &meta) != nil || json.Unmarshal(meta.Request.Input, &input) != nil || h.enforceExistingScope(child, input) != nil || retained.ExpectedThread != child.ResumeID() {
				availability, reason = "unsupported", "retained_task_scope_unqualified"
			}
		}
	}
	for _, r := range rounds {
		record, _, _ := h.delegations.ByID(ctx, r.ID)
		if strings.Contains(record.Error, "delegation_original_thread_unavailable_successor_required") {
			availability, reason = "successor_required", "original_native_thread_unavailable"
		}
		if strings.Contains(record.Error, "delegation_context_limit_successor_required") {
			availability, reason = "successor_required", "original_context_limit"
		}
	}
	sourceThread, originalRevision, sourceBound, e := h.delegations.SourceBinding(ctx, root.ID)
	if e != nil {
		return nil, e
	}
	epoch, _, e := h.delegations.SourceEpoch(ctx, root.ID)
	if e != nil {
		return nil, e
	}
	effectiveRevision := originalRevision
	if epoch.Epoch > 0 {
		effectiveRevision = epoch.ToRevision
	}
	sourceAuthorization := map[string]any{"original_source_revision": originalRevision, "effective_source_revision": effectiveRevision, "authorization_epoch": epoch.Epoch, "source_thread_id": sourceThread, "matches_current_parent": sourceBound && sourceThread == parent.ResumeID() && effectiveRevision == parent.SettingsSnapshot().ConfigRevision}
	var currentTarget any
	if child, found := h.registry.Get(root.ChildSessionID); found {
		snapshot := child.SettingsSnapshot()
		currentTarget = map[string]any{"instance_id": h.cfg.InstanceID, "session_id": child.ID, "native_thread_id": child.ResumeID(), "config_revision": snapshot.ConfigRevision}
	}
	questions := h.parentInteractionInbox(parent, root.ChildSessionID)
	return map[string]any{"needs_input": len(questions) > 0, "pending_interactions": questions, "current_target": currentTarget, "source_authorization": sourceAuthorization, "continuation_availability": availability, "successor_reason": reason, "has_more": next < latest.Version, "next_version": next, "logical_task_id": round.LogicalID, "session_id": root.ChildSessionID, "native_thread_id": root.ExpectedThread, "owner_logical_task_id": owner, "qa_logical_task_id": qa, "rounds": out}, nil
}
func (h *Hub) BindDelegationQA(parent *session.Session, request, call, ownerID, qaID string) (any, error) {
	if e := h.continuityCaller(parent, request, call); e != nil {
		return nil, e
	}
	owner, om, os, e := h.continuityBase(parent, ownerID)
	if e != nil {
		return nil, e
	}
	qa, qm, qs, e := h.continuityBase(parent, qaID)
	if e != nil {
		return nil, e
	}
	if os.ID == qs.ID || owner.ExpectedThread == qa.ExpectedThread || om.LogicalID == qm.LogicalID {
		return nil, errors.New("delegation_independent_qa_required")
	}
	e = h.delegations.BindQA(context.Background(), om.LogicalID, qm.LogicalID, func() error {
		if e := h.continuityCaller(parent, request, call); e != nil {
			return e
		}
		if e := h.validateDelegationBindingForContinuation(context.Background(), owner, parent, os); e != nil {
			return e
		}
		return h.validateDelegationBindingForContinuation(context.Background(), qa, parent, qs)
	})
	if e != nil {
		return nil, e
	}
	return map[string]any{"owner_logical_task_id": om.LogicalID, "qa_logical_task_id": qm.LogicalID, "independent": true}, nil
}

// The verdict comes from the exact QA final, not an owner-supplied verdict.
// A later owner round cannot replace this immutable FAIL/PASS version.
func (h *Hub) RecordDelegationReview(parent *session.Session, request, call, ownerID, qaID string) (any, error) {
	if e := h.continuityCaller(parent, request, call); e != nil {
		return nil, e
	}
	_, om, _, e := h.continuityBase(parent, ownerID)
	if e != nil {
		return nil, e
	}
	_, qm, _, e := h.continuityBase(parent, qaID)
	if e != nil {
		return nil, e
	}
	owner, qa, e := h.delegations.Responsibility(context.Background(), om.LogicalID)
	if e != nil {
		return nil, e
	}
	if owner != om.LogicalID || qa != qm.LogicalID {
		return nil, errors.New("delegation_independent_qa_required")
	}
	or, ok, e := h.delegations.ByID(context.Background(), ownerID)
	if e != nil || !ok || or.State != "terminal" || or.TerminalStatus != "completed" {
		return nil, errors.New("delegation_owner_final_required")
	}
	qr, ok, e := h.delegations.ByID(context.Background(), qaID)
	if e != nil || !ok || qr.State != "terminal" || qr.TerminalStatus != "completed" {
		return nil, errors.New("delegation_qa_final_required")
	}
	var result struct {
		OwnerRound string `json:"owner_round_id"`
		Verdict    string `json:"verdict"`
	}
	if json.Unmarshal([]byte(qr.Result), &result) != nil || result.OwnerRound != ownerID || (result.Verdict != "PASS" && result.Verdict != "FAIL") {
		return nil, errors.New("delegation_qa_verdict_invalid")
	}
	if e = h.delegations.RecordQA(context.Background(), ownerID, qaID, result.Verdict); e != nil {
		return nil, e
	}
	return map[string]any{"owner_round_id": ownerID, "qa_round_id": qaID, "qa_version": qm.Version, "verdict": result.Verdict}, nil
}

// Steering is allowed only for the exact original active round. The durable
// claim precedes the existing native RPC; restart/unknown never repeats it.
func (h *Hub) provisionDelegationSteer(ctx context.Context, r delegation.Record, meta delegation.Round) error {
	child, ok := h.registry.Get(r.ChildSessionID)
	if !ok {
		return errors.New("delegation_child_unavailable")
	}
	if e := h.validateDelegationBinding(ctx, r); e != nil {
		return e
	}
	if entry, found, e := h.messageQueue.Get(child.ID, r.ChildRequestID); e != nil {
		return e
	} else if found {
		if entry.State == messagequeue.Steered {
			return h.delegations.MarkRunning(ctx, r.ID)
		}
		// An existing receipt is never re-promoted, including queued after restart.
		return nil
	}
	if !child.IsStreaming() || child.ActiveQueuedID() != meta.ExecutionRequestID {
		return h.delegations.MarkProvisionFailed(ctx, r.ID, "delegation_steer_active_round_changed")
	}
	return h.enqueueDelegationSteer(ctx, r, child)
}

func (h *Hub) enqueueDelegationSteer(ctx context.Context, r delegation.Record, child *session.Session) error {
	client := &Client{hub: h, deviceID: "delegation-system", send: make(chan []byte, 64), quit: make(chan struct{}), ctx: ctx}
	thread, revision, _, e := h.delegations.ExecutionBinding(ctx, r.ID)
	if e != nil {
		return e
	}
	expected := &dispatchTargetExpectation{ThreadID: thread, Revision: revision}
	if !h.enqueueChatMessageExpected(client, clientproto.Command{Kind: "message", SessionID: child.ID, RequestID: r.ChildRequestID, Content: r.Instruction}, expected) {
		return errors.New("delegation_steer_enqueue_refused")
	}
	claimed, e := h.delegations.ClaimSteer(ctx, r.ID)
	if e != nil {
		return e
	}
	if claimed {
		h.promoteQueuedMessage(client, clientproto.Command{Kind: "promote_queued_message", SessionID: child.ID, RequestID: r.ChildRequestID})
	}
	return h.delegations.MarkRunning(ctx, r.ID)
}

func (h *Hub) PreserveDelegationContext(s *session.Session) (bool, error) {
	if h.delegations == nil || s == nil {
		return false, nil
	}
	return h.delegations.HasChild(context.Background(), s.ID)
}
