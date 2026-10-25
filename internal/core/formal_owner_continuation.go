package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

func (h *Hub) formalContinuationTask(ctx context.Context, parent *session.Session, args json.RawMessage) (string, error) {
	var in backend.DelegationContinuation
	if json.Unmarshal(args, &in) != nil || in.Mode != "queue" {
		return "", nil
	}
	root, round, err := h.continuitySourceAt(ctx, parent, in.DelegationID, nil)
	if err != nil {
		return "", err
	}
	_, formal, err := h.delegations.APIBackend(ctx, root.ChildSessionID)
	if err != nil || !formal {
		return "", err
	}
	return apiID(h.cfg.InstanceID, "delegation", round.LogicalID), nil
}

// This immutable binding is inserted with the outer reservation BEFORE the
// original formal mutation. Migration rows lacking it never recover effects.
type formalOwnerBinding struct {
	Request            taskapi.Request   `json:"request"`
	Hash               string            `json:"intent_hash"`
	Namespace          taskapi.Namespace `json:"namespace"`
	DefinitionRevision uint64            `json:"definition_revision"`
	SourceThread       string            `json:"source_thread"`
	SourceRevision     uint64            `json:"source_revision"`
}

func (h *Hub) prepareFormalOwner(ctx context.Context, bound taskapi.VerifiedContext, command *delegation.ContinuityCommand, request taskapi.Request, args json.RawMessage) error {
	var in backend.DelegationContinuation
	if json.Unmarshal(args, &in) != nil || in.InstanceID != h.cfg.InstanceID || in.Mode != "queue" || in.ActiveRequestID != "" {
		return taskapi.Failure("invalid_argument", "known_none", "correct_input")
	}
	record, err := h.apiRecord(ctx, bound, command.EffectTaskID)
	if err != nil {
		return err
	}
	task, err := h.apiTask(ctx, record)
	if err != nil {
		return err
	}
	input, err := json.Marshal(map[string]any{"task_id": record.TaskID, "expected_revision": task["revision"], "target": map[string]any{"instance_id": in.InstanceID, "session_id": record.SessionID, "expected_native_thread_id": in.ExpectedThreadID, "expected_config_revision": in.ExpectedConfigRevision}, "content": in.Content, "mode": "queue", "return_to_parent": true, "expected_previous_round_id": in.ExpectedPreviousID})
	if err != nil {
		return err
	}
	inner := taskapi.Request{Type: request.Type, Version: request.Version, CorrelationID: request.CorrelationID, Operation: "append", IdempotencyKey: request.IdempotencyKey, Input: input}
	digest, err := taskapi.IntentHash(inner)
	if err != nil {
		return err
	}
	// Persist the first authoritative definition revision alongside every
	// mutation-affecting request field; recovery never reads a newer revision.
	revisionRaw, err := json.Marshal(task["definition_revision"])
	if err != nil {
		return err
	}
	var definitionRevision uint64
	if json.Unmarshal(revisionRaw, &definitionRevision) != nil || definitionRevision == 0 {
		return taskapi.Failure("unsupported", "known_none", "lookup_original")
	}
	binding := formalOwnerBinding{Request: inner, Hash: digest, Namespace: taskapi.Namespace{Authority: h.cfg.InstanceID, StableScopeID: bound.StableScopeID, Generation: bound.NamespaceGeneration, Path: "delegation", Operation: "append", TaskID: command.EffectTaskID}, DefinitionRevision: definitionRevision, SourceThread: bound.SourceResumeID, SourceRevision: bound.SourceConfigRevision}
	command.FormalBinding, err = json.Marshal(binding)
	return err
}

func (h *Hub) checkedFormalOwner(ctx context.Context, bound taskapi.VerifiedContext, parent *session.Session, command delegation.ContinuityCommand) (formalOwnerBinding, error) {
	var binding formalOwnerBinding
	if len(command.FormalBinding) == 0 || json.Unmarshal(command.FormalBinding, &binding) != nil || binding.DefinitionRevision == 0 || binding.Hash == "" {
		return binding, taskapi.Failure("unknown_acceptance", "unknown", "lookup_original")
	}
	expected := taskapi.Namespace{Authority: h.cfg.InstanceID, StableScopeID: bound.StableScopeID, Generation: bound.NamespaceGeneration, Path: "delegation", Operation: "append", TaskID: command.EffectTaskID}
	if parent == nil || command.Parent != parent.ID || bound.SourceSessionID != parent.ID || binding.Namespace != expected || !bound.SourceConfigKnown || binding.SourceThread == "" || binding.SourceThread != bound.SourceResumeID || binding.SourceRevision != bound.SourceConfigRevision {
		return binding, taskapi.Failure("permission", "known_none", "lookup_original")
	}
	var input struct {
		TaskID   string `json:"task_id"`
		Previous string `json:"expected_previous_round_id"`
	}
	digest, err := taskapi.IntentHash(binding.Request)
	if err != nil || digest != binding.Hash || binding.Request.Operation != "append" || binding.Request.IdempotencyKey != command.Key || json.Unmarshal(binding.Request.Input, &input) != nil || input.TaskID != command.EffectTaskID {
		return binding, taskapi.Failure("unknown_acceptance", "unknown", "lookup_original")
	}
	// Revalidate the original owner/root grant/source binding. This selected
	// original round is not a freshness CAS or a recomputed inner revision.
	if _, _, err := h.continuitySourceAt(ctx, parent, input.Previous, nil); err != nil {
		return binding, taskapi.Failure("permission", "known_none", "lookup_original")
	}
	return binding, nil
}

// Original native owner command -> original formal TaskAPI append enforcer.
// Execute only the reservation's original request, never regenerated input.
func (h *Hub) continueFormalOwner(ctx context.Context, caller taskapi.BoundCaller, parent *session.Session, command delegation.ContinuityCommand) (any, error) {
	bound, err := caller.Check(ctx)
	if err != nil {
		return nil, err
	}
	binding, err := h.checkedFormalOwner(ctx, bound, parent, command)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(binding.Request)
	if err != nil {
		return nil, err
	}
	response := h.taskService.Execute(ctx, caller, raw)
	if !response.OK {
		return nil, response.Error
	}
	return h.recoverFormalOwner(ctx, bound, parent, command)
}

// A reserved command only queries its original canonical append receipt. It
// never regenerates a revision/input or invokes a second mutation.
func (h *Hub) recoverFormalOwner(ctx context.Context, bound taskapi.VerifiedContext, parent *session.Session, command delegation.ContinuityCommand) (any, error) {
	binding, err := h.checkedFormalOwner(ctx, bound, parent, command)
	if err != nil {
		return nil, err
	}
	receipt, err := h.delegations.TaskJournal().Lookup(ctx, binding.Namespace, command.Key)
	if err != nil {
		return nil, taskapi.Failure("unknown_acceptance", "unknown", "lookup_original")
	}
	if receipt.Hash != binding.Hash {
		return nil, taskapi.Failure("idempotency_conflict", "known_receipt", "lookup_original")
	}
	if receipt.Key != binding.Namespace.Key(command.Key) || receipt.Path != "delegation" || receipt.TaskID != command.EffectTaskID || receipt.ScopeID != bound.StableScopeID || receipt.Generation != bound.NamespaceGeneration {
		return nil, taskapi.Failure("permission", "known_none", "lookup_original")
	}
	r, found, err := h.delegations.ByChildRequest(ctx, receipt.SessionID, receipt.RequestID)
	if err != nil || !found || r.ParentSessionID != parent.ID {
		return nil, taskapi.Failure("unknown_acceptance", "known_receipt", "lookup_original")
	}
	return h.continuityReceipt(ctx, r)
}
