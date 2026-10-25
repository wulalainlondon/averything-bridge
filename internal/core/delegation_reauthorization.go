package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/delegation"
	"everything-go/internal/session"
	"path/filepath"
	"strings"
)

type delegationReauthorization struct {
	Owner         string `json:"owner_delegation_id"`
	QA            string `json:"qa_delegation_id"`
	Instance      string `json:"instance_id"`
	SourceThread  string `json:"expected_source_thread_id"`
	From          uint64 `json:"expected_source_revision"`
	To            uint64 `json:"new_source_revision"`
	OwnerThread   string `json:"expected_owner_thread_id"`
	QAThread      string `json:"expected_qa_thread_id"`
	OwnerRevision uint64 `json:"expected_owner_config_revision"`
	QARevision    uint64 `json:"expected_qa_config_revision"`
}

func (h *Hub) reauthorizeDelegationPair(parent *session.Session, request, call string, in delegationReauthorization) (any, error) {
	if e := h.continuityCaller(parent, request, call); e != nil {
		return nil, e
	}
	if in.Instance != h.cfg.InstanceID || in.SourceThread != parent.ResumeID() || in.To != parent.SettingsSnapshot().ConfigRevision || in.To <= in.From {
		return nil, errors.New("delegation_reauthorization_stale")
	}
	owner, om, os, e := h.continuityBaseAt(parent, in.Owner, &in.From)
	if e != nil {
		return nil, e
	}
	qa, qm, qs, e := h.continuityBaseAt(parent, in.QA, &in.From)
	if e != nil {
		return nil, e
	}
	if in.Owner != om.LogicalID || in.QA != qm.LogicalID || os.ID == qs.ID || owner.ExpectedThread == qa.ExpectedThread {
		return nil, errors.New("delegation_independent_qa_required")
	}
	if owner.ExpectedThread != in.OwnerThread || qa.ExpectedThread != in.QAThread || owner.ExpectedRevision != in.OwnerRevision || qa.ExpectedRevision != in.QARevision {
		return nil, errors.New("delegation_child_binding_changed")
	}
	ctx := context.Background()
	var pair [2]delegation.SourceEpoch
	for i, r := range []delegation.Record{owner, qa} {
		thread, revision, bound, e := h.delegations.SourceBinding(ctx, r.ID)
		if e != nil {
			return nil, e
		}
		if !bound {
			return nil, errors.New("delegation_source_binding_missing")
		}
		old, _, e := h.delegations.SourceEpoch(ctx, r.ID)
		if e != nil {
			return nil, e
		}
		peer := qa.ID
		if i == 1 {
			peer = owner.ID
		}
		pair[i] = delegation.SourceEpoch{LogicalID: r.ID, Parent: parent.ID, Authority: h.cfg.InstanceID, Thread: thread, FromRevision: in.From, ToRevision: in.To, Epoch: old.Epoch + 1, GrantDigest: delegationGrantDigest(r, thread, revision), Peer: peer, Request: request, Call: call}
	}
	owner.SourceRevision = in.To
	qa.SourceRevision = in.To
	validate := func() error {
		if e := h.continuityCaller(parent, request, call); e != nil {
			return e
		}
		if e := h.validateDelegationBindingForContinuation(ctx, owner, parent, os); e != nil {
			return e
		}
		if e := h.validateDelegationBindingForContinuation(ctx, qa, parent, qs); e != nil {
			return e
		}
		// Reauthorization cannot widen either worker's original sandbox/workspace.
		for _, r := range []delegation.Record{owner, qa} {
			if sandboxRank(r.Sandbox) > sandboxRank(parent.SettingsSnapshot().Sandbox) {
				return errors.New("delegation_sandbox_escalation")
			}
			if !workspaceWithin(parent.Cwd(), r.Cwd) {
				return errors.New("delegation_workspace_changed")
			}
		}
		return nil
	}
	if e = h.delegations.ReauthorizePair(ctx, pair, validate); e != nil {
		return nil, e
	}
	return map[string]any{"owner_logical_task_id": owner.ID, "qa_logical_task_id": qa.ID, "source_thread_id": in.SourceThread, "previous_source_revision": in.From, "source_revision": in.To, "owner_authorization_epoch": pair[0].Epoch, "qa_authorization_epoch": pair[1].Epoch, "original_bindings_preserved": true}, nil
}

func workspaceWithin(parent, child string) bool {
	root, e := filepath.EvalSymlinks(parent)
	if e != nil {
		return false
	}
	target, e := filepath.EvalSymlinks(child)
	if e != nil {
		return false
	}
	rel, e := filepath.Rel(root, target)
	return e == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// formalDelegationReturnSource relates immutable formal metadata to the original
// logical root and source authority epoch, without requiring a finished child
// to remain executable. Caller cannot supply a replacement or renew execution.
func (h *Hub) formalDelegationReturnSource(ctx context.Context, r delegation.Record, parent *session.Session, metadata apiMetadata) (string, uint64, error) {
	round, found, e := h.delegations.Round(ctx, r.ID)
	if e != nil {
		return "", 0, e
	}
	if !found {
		return "", 0, errors.New("delegation_continuity_binding_missing")
	}
	original, found, e := h.delegations.ByID(ctx, round.LogicalID)
	if e != nil {
		return "", 0, e
	}
	if !found || original.ParentSessionID != parent.ID || original.ChildSessionID != r.ChildSessionID {
		return "", 0, errors.New("delegation_continuation_forbidden")
	}
	thread, revision, bound, e := h.delegations.SourceBinding(ctx, original.ID)
	if e != nil {
		return "", 0, e
	}
	if !bound || metadata.SourceThread != thread || metadata.SourceRevision != revision {
		return "", 0, errors.New("delegation_source_binding_changed")
	}
	effective, _, e := h.continuitySourceAt(ctx, parent, r.ID, nil)
	if e != nil {
		return "", 0, e
	}
	// Validate the stored formal provenance, never today's child settings or
	// workspace inode. Returning old data does not authorize another execution.
	var input apiInput
	if json.Unmarshal(metadata.Request.Input, &input) != nil || input.New == nil || metadata.Request.Operation != "create_dispatch" ||
		metadata.Caller.Authority != h.cfg.InstanceID || metadata.Caller.SourceSessionID != parent.ID ||
		metadata.Caller.SourceRequestID != original.OriginRequestID || metadata.Caller.ToolCallID != original.ToolCallID || metadata.Caller.Transport != metadata.Transport ||
		metadata.Caller.SourceResumeID != thread || !metadata.Caller.SourceConfigKnown || metadata.Caller.SourceConfigRevision != revision ||
		metadata.CanonicalCwd != original.Cwd || metadata.RootIdentities[original.Cwd] != effective.WorkspaceIdentity || effective.WorkspaceIdentity == "" ||
		input.New.Profile.Model != original.Model || input.New.Profile.Effort != original.Effort || input.Scope.Sandbox != original.Sandbox ||
		r.ParentSessionID != parent.ID || r.Cwd != original.Cwd || r.Model != original.Model || r.Effort != original.Effort || r.Sandbox != original.Sandbox {
		return "", 0, errors.New("delegation_grants_changed")
	}
	provider, api, e := h.delegations.APIBackend(ctx, original.ChildSessionID)
	if e != nil {
		return "", 0, e
	}
	if !api || provider != input.New.Profile.Backend || len(metadata.CanonicalRoots) == 0 {
		return "", 0, errors.New("delegation_grants_changed")
	}
	for _, path := range metadata.CanonicalRoots {
		if metadata.RootIdentities[path] == "" {
			return "", 0, errors.New("delegation_grants_changed")
		}
	}
	return effective.SourceThread, effective.SourceRevision, nil
}
