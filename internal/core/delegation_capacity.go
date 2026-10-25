package core

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

// Accepted authority is durable, not reconstructed from a fabricated caller or
// renewed using the parent's current active turn. The immutable source binding
// and workspace/sandbox are checked again immediately before any child effect.
func (h *Hub) validateDelegationBinding(ctx context.Context, r delegation.Record) error {
	parent, ok := h.registry.Get(r.ParentSessionID)
	if !ok || parent.State() == session.Closed || parent.Snapshot().Hidden || !h.controllerInScope(parent) {
		return errors.New("delegation_source_unavailable")
	}
	if !h.controls.MobileMayWrite(parent.ID) {
		return errors.New("delegation_source_not_writable")
	}
	thread, revision, bound, err := h.delegations.SourceBinding(ctx, r.ID)
	if err != nil {
		return err
	}
	if !bound {
		return errors.New("delegation_source_binding_missing")
	}
	if parent.ResumeID() != thread || parent.SettingsSnapshot().ConfigRevision != revision {
		return errors.New("delegation_source_binding_changed")
	}
	expectedThread, expectedRevision, cwdIdentity, e := h.delegations.ExecutionBinding(ctx, r.ID)
	if e != nil {
		return e
	}
	if cwdIdentity != "" {
		if got, e := taskRootIdentity(r.Cwd); e != nil || got != cwdIdentity {
			return errors.New("delegation_workspace_identity_changed")
		}
	}
	// The accepted profile is already durable in the original record, source
	// binding (including an explicitly empty initial thread/revision) and API
	// backend row. Never substitute the registry child's current permissions.
	if child, exists := h.registry.Get(r.ChildSessionID); exists {
		if err := h.validateDelegationChild(ctx, r, child, expectedThread, expectedRevision, false); err != nil {
			return err
		}
	} else if expectedThread != "" {
		return errors.New("delegation_child_binding_changed")
	}
	root, e := filepath.EvalSymlinks(parent.Cwd())
	if e != nil {
		return errors.New("delegation_workspace_unavailable")
	}
	child, e := filepath.EvalSymlinks(r.Cwd)
	if e != nil {
		return errors.New("delegation_workspace_unavailable")
	}
	if child != root && !strings.HasPrefix(child, root+string(os.PathSeparator)) {
		return errors.New("delegation_workspace_changed")
	}
	if fi, e := os.Stat(child); e != nil || !fi.IsDir() {
		return errors.New("delegation_workspace_unavailable")
	}
	if r.Sandbox != parent.Snapshot().Sandbox && (sandboxRank(r.Sandbox) < 0 || sandboxRank(r.Sandbox) > sandboxRank(parent.Snapshot().Sandbox)) {
		return errors.New("delegation_sandbox_escalation")
	}
	if data, api, e := h.delegations.APIChildMetadata(ctx, r.ChildSessionID); e != nil {
		return e
	} else if api {
		var meta apiMetadata
		if json.Unmarshal(data, &meta) != nil || (expectedThread == "" && (parent.ResumeID() != meta.SourceThread || parent.SettingsSnapshot().ConfigRevision != meta.SourceRevision)) {
			return errors.New("delegation_source_binding_changed")
		}
		// Formal scopes also re-check pinned directory identities at execution via
		// TaskWorkerScope. Validate before registry creation where possible.
		for path, want := range meta.RootIdentities {
			if got, e := taskRootIdentity(path); e != nil || got != want {
				return errors.New("delegation_workspace_identity_changed")
			}
		}
	}
	return nil
}

// Check the actual execution snapshot as well as future settings at Send.
// Recovery/first enqueue checks settings before activation; neither path may
// adopt a nonempty thread for an originally empty-thread handoff.
func (h *Hub) validateDelegationChild(ctx context.Context, r delegation.Record, child *session.Session, thread string, revision uint64, active bool) error {
	provider := backend.Codex
	if selected, api, err := h.delegations.APIBackend(ctx, r.ChildSessionID); err != nil {
		return err
	} else if api {
		provider = selected
	}
	return validateDelegationChildProfile(r, child, thread, revision, provider, active)
}

// Pure profile check is safe inside the owning store transaction.
func validateDelegationChildProfile(r delegation.Record, child *session.Session, thread string, revision uint64, provider string, active bool) error {
	match := func(s session.Snapshot) bool {
		cwd, err := filepath.EvalSymlinks(s.Cwd)
		expected, e := filepath.EvalSymlinks(r.Cwd)
		return err == nil && e == nil && cwd == expected && s.ResumeID == thread && s.ConfigRevision == revision && s.Backend == provider && s.Sandbox == r.Sandbox && s.Model == r.Model && s.Effort == r.Effort && s.State != session.Closed && !s.Hidden
	}
	if !match(child.SettingsSnapshot()) || (active && !match(child.Snapshot())) {
		return errors.New("delegation_child_profile_changed")
	}
	return nil
}

// Legacy returns without an original binding or a pinned queue payload cannot
// be safely retargeted. Refuse them before any effect; preserve child results.
func (h *Hub) delegationReturnGuard(ctx context.Context, target *session.Session, request string, expected *dispatchTargetExpectation) error {
	if h.delegations == nil {
		return nil
	}
	r, found, err := h.delegations.ByParentRequest(ctx, target.ID, request)
	if err != nil || !found {
		return err
	}
	actual, ok := h.registry.Get(target.ID)
	if !ok || actual != target {
		return errors.New("delegation_source_unavailable")
	}
	thread, revision, bound, err := h.delegations.SourceBinding(ctx, r.ID)
	if err != nil {
		return err
	}
	if !bound || expected == nil {
		return errors.New("delegation_return_binding_missing")
	}
	if expected.ThreadID != thread || expected.Revision != revision || !expected.matches(target) || !h.controllerInScope(target) {
		return errors.New("delegation_source_binding_changed")
	}
	return nil
}

// Until a child thread exists, cancellation compares the ORIGINAL SOURCE
// identity supplied as target. It never invents an expected child thread ID.
func (h *Hub) checkQueuedCancellationSource(c taskapi.VerifiedContext, request taskapi.Request, d delegation.Record) error {
	var input struct {
		Target *taskapi.TargetIdentity
		Mode   string
	}
	if json.Unmarshal(request.Input, &input) != nil || input.Mode != "queued" || input.Target == nil {
		return taskapi.Failure("invalid_argument", "known_none", "correct_input")
	}
	parent, ok := h.registry.Get(d.ParentSessionID)
	if !ok || c.SourceSessionID != parent.ID || !h.controllerInScope(parent) || parent.State() == session.Closed || parent.Snapshot().Hidden || !h.controls.MobileMayWrite(parent.ID) {
		return taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	t := input.Target
	if t.InstanceID != h.cfg.InstanceID || t.SessionID != parent.ID || t.ResumeID != parent.ResumeID() || t.ConfigRevision != parent.SettingsSnapshot().ConfigRevision || c.SourceResumeID != parent.ResumeID() || !c.SourceConfigKnown || c.SourceConfigRevision != t.ConfigRevision {
		return taskapi.Failure("stale_revision", "known_none", "refresh_identity")
	}
	return nil
}
func (h *Hub) authorizeQueuedDelegationCancel(ctx context.Context, c taskapi.VerifiedContext, request taskapi.Request, record taskapi.IntentRecord, d delegation.Record, revision uint64) error {
	if err := h.checkQueuedCancellationSource(c, request, d); err != nil {
		return err
	}
	task, err := h.apiTask(ctx, record)
	if err != nil {
		return err
	}
	if task["revision"].(uint64) != revision {
		return taskapi.Failure("stale_revision", "known_none", "refresh_identity")
	}
	return nil
}

func (h *Hub) delegationQueueParent(parent *session.Session) error {
	if parent == nil || h.delegations == nil {
		return errors.New("delegation_unavailable")
	}
	actual, ok := h.registry.Get(parent.ID)
	if !ok || actual != parent || parent.State() == session.Closed || parent.Snapshot().Hidden || !h.controllerInScope(parent) || !h.controls.MobileMayWrite(parent.ID) {
		return errors.New("delegation_parent_invalid")
	}
	return nil
}
func (h *Hub) CancelWaitingDelegation(parent *session.Session, id string) (map[string]any, error) {
	if err := h.delegationQueueParent(parent); err != nil {
		return nil, err
	}
	cancelled, err := h.delegations.CancelWaiting(context.Background(), parent.ID, id)
	if err != nil {
		return nil, err
	}
	if !cancelled {
		return nil, errors.New("delegation_already_started")
	}
	h.wakeDelegations()
	return map[string]any{"delegation_id": id, "cancellation": "confirmed", "execution": "not_started"}, nil
}
func (h *Hub) DelegationCapacity(parent *session.Session) (map[string]int, error) {
	if err := h.delegationQueueParent(parent); err != nil {
		return nil, err
	}
	l, err := h.delegations.LimitsForParent(context.Background(), parent.ID)
	if err != nil {
		return nil, err
	}
	n, err := h.delegations.CountsForParent(context.Background(), parent.ID)
	if err != nil {
		return nil, err
	}
	return map[string]int{"max_active": l.Active, "max_queued": l.Queued, "max_delivery_reserved": l.Delivery, "active": n.Active, "queued": n.Queued, "delivery": n.Delivery, "delivery_reserved": n.Reserved}, nil
}

// An API append to a delegated child is another turn with its own reservation,
// not an unaccounted message-queue escape hatch. It retains the original task,
// scope and native child thread, and does not request an extra parent return.
func (h *Hub) appendCapacityDelegation(ctx context.Context, c taskapi.AuthorizedCommand, intent, original taskapi.IntentRecord, in apiInput, target *session.Session) (any, error) {
	base, found, err := h.delegations.ByID(ctx, original.NativeID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, taskapi.Failure("not_found", "known_none", "lookup_original")
	}
	parent, ok := h.registry.Get(base.ParentSessionID)
	if !ok {
		return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	if c.Caller.SourceSessionID != base.ParentSessionID {
		return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	// Use the same validated effective source epoch as the native continuation.
	// The original binding remains immutable; this new round binds to the explicitly
	// approved current revision, with authority/profile/workspace checks unchanged.
	effective, _, registeredChild, e := h.continuityBase(parent, base.ID)
	if e != nil || registeredChild != target {
		return nil, taskapi.Failure("permission", "known_none", "refresh_identity")
	}
	sourceThread, sourceRevision := effective.SourceThread, effective.SourceRevision
	rootRound, has, e := h.delegations.Round(ctx, base.ID)
	if e != nil {
		return nil, e
	}
	if !has {
		return nil, errors.New("delegation_continuity_binding_missing")
	}
	latestRound, e := h.delegations.LatestRound(ctx, rootRound.LogicalID)
	if e != nil {
		return nil, e
	}
	snap := target.SettingsSnapshot()
	// Retain the accepted profile, never copy an elevated registry profile.
	_, revision, _, e := h.delegations.ExecutionBinding(ctx, base.ID)
	if e != nil {
		return nil, e
	}
	proofThread, e := h.originalDelegationThread(ctx, base)
	if e != nil {
		return nil, e
	}
	if proofThread != target.ResumeID() {
		return nil, taskapi.Failure("stale_revision", "known_none", "refresh_identity")
	}
	if e = h.validateDelegationChild(ctx, base, target, proofThread, revision, true); e != nil {
		return nil, taskapi.Failure("permission", "known_none", "request_scope_change")
	}
	if thread, rev, pinned, e := h.delegations.NativeThread(ctx, target.ID); e != nil {
		return nil, e
	} else if pinned && (thread != target.ResumeID() || rev != snap.ConfigRevision) {
		return nil, taskapi.Failure("stale_revision", "known_none", "refresh_identity")
	}
	id := "dgfollow_" + intent.RequestID
	r := delegation.Record{ID: id, LogicalID: rootRound.LogicalID, PreviousID: latestRound.ID, Authority: h.cfg.InstanceID, ParentSessionID: parent.ID, ChildSessionID: target.ID, ChildRequestID: intent.RequestID, ParentRequestID: "dgreturn_" + id, OriginRequestID: c.Caller.SourceRequestID, ToolCallID: c.Caller.ToolCallID, IntentHash: c.IntentHash, ChildName: snap.Name, Cwd: snap.Cwd, Instruction: in.Content, Model: snap.Model, Effort: snap.Effort, Sandbox: base.Sandbox, SourceThread: sourceThread, SourceRevision: sourceRevision, ExpectedThread: target.ResumeID(), ExpectedRevision: snap.ConfigRevision, DeliveryState: "not_requested"}
	if in.ExpectedPreviousRoundID != "" {
		r.PreviousID = in.ExpectedPreviousRoundID
	}
	if in.ReturnToParent {
		r.DeliveryState = "pending"
	}
	r.GrantDigest = delegationGrantDigest(r, r.SourceThread, r.SourceRevision)
	r.WorkspaceIdentity, err = taskRootIdentity(r.Cwd)
	if err != nil {
		return nil, err
	}
	admission := c
	admission.Revalidate = func(check context.Context) error {
		if err := c.Revalidate(check); err != nil {
			return err
		}
		current, ok := h.registry.Get(target.ID)
		source, sourceOK := h.registry.Get(parent.ID)
		if !ok || current != target || !sourceOK || source != parent || target.ResumeID() != r.ExpectedThread || target.SettingsSnapshot().ConfigRevision != r.ExpectedRevision || parent.ResumeID() != r.SourceThread || parent.SettingsSnapshot().ConfigRevision != r.SourceRevision || target.State() == session.Closed || !h.controls.MobileMayWrite(target.ID) || !h.controls.MobileMayWrite(parent.ID) {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		return validateDelegationChildProfile(base, target, r.ExpectedThread, r.ExpectedRevision, snap.Backend, true)
	}
	stored, created, err := h.delegations.CreateAPIContinuation(ctx, admission, intent, r)
	if err != nil {
		return nil, err
	}
	if created {
		_ = h.provisionDelegation(ctx, r)
		h.wakeDelegations()
	}
	return h.apiReceipt(ctx, c, stored)
}

func (h *Hub) delegationMessageRevision(ctx context.Context, task string) (uint64, error) {
	commands, err := h.delegations.TaskJournal().TaskCommands(ctx, task)
	if err != nil {
		return 0, err
	}
	var revision uint64
	for _, c := range commands {
		n, e := h.messageQueue.TaskRevision(ctx, c.SessionID, c.RequestID)
		if e != nil {
			return 0, e
		}
		if n > revision {
			revision = n
		}
	}
	return revision, nil
}

// Bare messages must not turn a one-turn child into an unbudgeted worker.
// Authenticated API append owns the durable continuation reservation.
func (h *Hub) delegationExecutionGuard(ctx context.Context, target *session.Session, request string, active bool) error {
	if handled, e := h.interactionControlGuard(ctx, target, request); handled {
		return e
	}
	child := target.ID
	if h.delegations == nil {
		return nil
	}
	r, found, err := h.delegations.ByChildRequest(ctx, child, request)
	if err != nil {
		return err
	}
	if !found {
		known, e := h.delegations.HasChild(ctx, child)
		if e != nil {
			return e
		}
		if known {
			return errors.New("delegation_capacity_reservation_required")
		}
		return nil
	}
	if r.State != "provisioning" && r.State != "running" {
		return errors.New("delegation_execution_not_admitted")
	}
	if err := h.validateDelegationBinding(ctx, r); err != nil {
		return err
	}
	actual, ok := h.registry.Get(child)
	if !ok || actual != target {
		return errors.New("delegation_child_missing")
	}
	thread, revision, _, err := h.delegations.ExecutionBinding(ctx, r.ID)
	if err != nil {
		return err
	}
	if meta, found, e := h.delegations.Round(ctx, r.ID); e != nil {
		return e
	} else if found && meta.Mode == "steer" {
		if active {
			return errors.New("delegation_steer_cannot_start_new_turn")
		}
		if !actual.IsStreaming() || actual.ActiveQueuedID() != meta.ExecutionRequestID {
			return errors.New("delegation_steer_active_round_changed")
		}
	}
	return h.validateDelegationChild(ctx, r, actual, thread, revision, active)
}
