package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

// Codex 0.160.0 accepts dynamicTools on thread/start, not thread/resume.
// Existing parent threads therefore use the already installed typed get/append
// envelopes. This route does not create a caller, controller grant or new thread.
// Paired-client HTTP/WS requests do not call this native-only adapter.
func (h *Hub) executeNativeContinuity(ctx context.Context, caller taskapi.BoundCaller, parent *session.Session, raw []byte) (taskapi.Response, bool) {
	if len(raw) > contract.MaxBytes {
		return taskapi.Response{}, false
	}
	var header struct {
		Operation   string                     `json:"operation"`
		Correlation string                     `json:"correlation_id"`
		Input       map[string]json.RawMessage `json:"input"`
	}
	if json.Unmarshal(raw, &header) != nil || header.Input["continuity"] == nil {
		return taskapi.Response{}, false
	}
	op := header.Operation
	response := taskapi.Response{Type: "task_api_response", Version: contract.Version, CorrelationID: header.Correlation, Operation: &op}
	fail := func(code, acceptance string) (taskapi.Response, bool) {
		response.OK = false
		response.Result = nil
		response.Error = taskapi.Failure(code, acceptance, "lookup_original")
		return response, true
	}
	if op != "get" && op != "append" {
		return fail("invalid_argument", "known_none")
	}
	validator, err := contract.New()
	if err != nil {
		return fail("unsupported", "known_none")
	}
	if err = validator.Validate(raw, "Request"); err != nil {
		return fail("invalid_argument", "known_none")
	}
	var request taskapi.Request
	if json.Unmarshal(raw, &request) != nil {
		return fail("invalid_argument", "known_none")
	}
	var in struct {
		Continuity struct {
			Action    string          `json:"action"`
			Arguments json.RawMessage `json:"arguments"`
		} `json:"continuity"`
	}
	if json.Unmarshal(request.Input, &in) != nil {
		return fail("invalid_argument", "known_none")
	}
	bound, err := caller.Check(ctx)
	if err != nil || parent == nil || bound.Authority != h.cfg.InstanceID || bound.SourceSessionID != parent.ID || bound.SourceResumeID != parent.ResumeID() || (bound.BindingKind != "native_tool" && bound.BindingKind != "mcp_process_binding") {
		return fail("caller_unbound", "known_none")
	}
	if err = h.continuityCaller(parent, bound.SourceRequestID, bound.ToolCallID); err != nil {
		return fail("permission", "known_none")
	}
	// Serializes this compatibility admission; underlying continuation remains
	// transactional and preserves its independent predecessor CAS and scope checks.
	h.continuityCompatMu.Lock()
	defer h.continuityCompatMu.Unlock()
	if _, err = caller.Check(ctx); err != nil {
		return fail("caller_unbound", "known_none")
	}
	action := in.Continuity.Action
	var data any
	state := "read"
	var command delegation.ContinuityCommand
	if op == "append" {
		digest, e := taskapi.IntentHash(request)
		if e != nil {
			return fail("invalid_argument", "known_none")
		}
		command = delegation.ContinuityCommand{Parent: parent.ID, Key: request.IdempotencyKey, Hash: digest, Action: action, Request: bound.SourceRequestID, Call: "continuity-api:" + request.IdempotencyKey}
		_, exists, e := h.delegations.ContinuityCommand(ctx, parent.ID, request.IdempotencyKey)
		if e != nil {
			return fail("unknown_acceptance", "unknown")
		}
		if !exists && action == "continue" {
			taskID, e := h.formalContinuationTask(ctx, parent, in.Continuity.Arguments)
			if e != nil {
				return fail("permission", "known_none")
			}
			command.EffectTaskID = taskID
			if taskID != "" {
				if e := h.prepareFormalOwner(ctx, bound, &command, request, in.Continuity.Arguments); e != nil {
					return fail("permission", "known_none")
				}
			}
		}
		fresh := false
		if !exists {
			fresh, e = h.delegations.ReserveContinuityCommand(ctx, command)
		}
		if e != nil {
			return fail("unknown_acceptance", "unknown")
		}
		if !fresh {
			stored, found, e := h.delegations.ContinuityCommand(ctx, parent.ID, request.IdempotencyKey)
			if e != nil || !found {
				return fail("unknown_acceptance", "unknown")
			}
			if stored.Hash != digest || stored.Action != action {
				return fail("idempotency_conflict", "known_receipt")
			}
			command = stored
			if stored.EffectTaskID != "" {
				if _, e := h.checkedFormalOwner(ctx, bound, parent, stored); e != nil {
					return fail("unknown_acceptance", "unknown")
				}
			}
			if stored.State == "refused" {
				return fail(stored.Error, "known_none")
			}
			if stored.State == "completed" {
				if json.Unmarshal(stored.Result, &data) != nil {
					return fail("unknown_acceptance", "known_receipt")
				}
			} else if action == "continue" && stored.EffectTaskID != "" {
				data, err = h.recoverFormalOwner(ctx, bound, parent, stored)
				if err != nil {
					return fail("unknown_acceptance", "unknown")
				}
			} else if action == "continue" {
				// Recover the original durable core receipt, never re-execute a
				// reserved call after a crash or ambiguous acceptance.
				r, found, e := h.delegations.ByOrigin(ctx, parent.ID, stored.Request, stored.Call)
				if e != nil || !found {
					return fail("unknown_acceptance", "unknown")
				}
				data, err = h.continuityReceipt(ctx, r)
				if err != nil {
					return fail("unknown_acceptance", "known_receipt")
				}
			} else {
				return fail("unknown_acceptance", "unknown")
			}
		}
		state = "completed"
	}
	if data == nil {
		switch action {
		case "continue":
			var a backend.DelegationContinuation
			err = json.Unmarshal(in.Continuity.Arguments, &a)
			if err == nil {
				if command.EffectTaskID != "" {
					data, err = h.continueFormalOwner(ctx, caller, parent, command)
				} else {
					data, err = h.ContinueDelegation(parent, bound.SourceRequestID, command.Call, a)
				}
			}
		case "reauthorize_pair":
			var a delegationReauthorization
			err = json.Unmarshal(in.Continuity.Arguments, &a)
			if err == nil {
				data, err = h.reauthorizeDelegationPair(parent, bound.SourceRequestID, command.Call, a)
			}
		case "bind_qa":
			var a struct {
				Owner string `json:"owner_delegation_id"`
				QA    string `json:"qa_delegation_id"`
			}
			err = json.Unmarshal(in.Continuity.Arguments, &a)
			if err == nil {
				data, err = h.BindDelegationQA(parent, bound.SourceRequestID, command.Call, a.Owner, a.QA)
			}
		case "record_review":
			var a struct {
				Owner string `json:"owner_round_id"`
				QA    string `json:"qa_round_id"`
			}
			err = json.Unmarshal(in.Continuity.Arguments, &a)
			if err == nil {
				data, err = h.RecordDelegationReview(parent, bound.SourceRequestID, command.Call, a.Owner, a.QA)
			}
		case "rounds":
			var a struct {
				ID    string `json:"delegation_id"`
				After uint64 `json:"after_version"`
			}
			err = json.Unmarshal(in.Continuity.Arguments, &a)
			if err == nil {
				data, err = h.DelegationRounds(parent, a.ID, a.After)
			}
		case "command":
			var a struct {
				Key string `json:"idempotency_key"`
			}
			err = json.Unmarshal(in.Continuity.Arguments, &a)
			if err == nil {
				var found bool
				command, found, err = h.delegations.ContinuityCommand(ctx, parent.ID, a.Key)
				if err == nil && !found {
					err = errors.New("not_found")
				}
				if err == nil {
					state = command.State
					data = map[string]any{"idempotency_key": command.Key, "state": command.State, "error_code": command.Error}
					if command.State == "reserved" && command.EffectTaskID != "" {
						recovered, recoveryErr := h.recoverFormalOwner(ctx, bound, parent, command)
						data.(map[string]any)["effect_task_id"] = command.EffectTaskID
						if recoveryErr == nil {
							data.(map[string]any)["recovered_original_effect"] = recovered
						} else {
							data.(map[string]any)["acceptance"] = "unknown"
						}
					}
					if len(command.Result) > 0 {
						var result any
						if json.Unmarshal(command.Result, &result) == nil {
							data.(map[string]any)["result"] = result
						}
					}
				}
			}
		default:
			err = errors.New("unsupported")
		}
	}
	if err != nil {
		// A precise unsupported refusal is not permission failure. Persist the
		// original command outcome without retrying or relaxing the enforcer.
		if failure, ok := err.(*taskapi.APIError); ok && failure.Acceptance == "known_none" && op == "append" {
			_, accepted, lookupErr := h.delegations.ByOrigin(ctx, parent.ID, command.Request, command.Call)
			if lookupErr == nil && !accepted {
				if finishErr := h.delegations.FinishContinuityCommand(ctx, command, "refused", nil, failure.Code); finishErr == nil {
					return fail(failure.Code, "known_none")
				}
			}
		}
		// Core validation failures precede its transactional effect reservation.
		// Database/transport failures remain uncertain; never guess that an
		// absent reply means the original operation did not happen.
		if op == "append" && strings.HasPrefix(err.Error(), "delegation_") {
			_, accepted, lookupErr := h.delegations.ByOrigin(ctx, parent.ID, command.Request, command.Call)
			if lookupErr == nil && !accepted {
				if finishErr := h.delegations.FinishContinuityCommand(ctx, command, "refused", nil, "permission"); finishErr == nil {
					return fail("permission", "known_none")
				}
			}
		}
		// A native effect can have been accepted before an error reached this
		// caller. Keep reservation uncertain rather than stamping false refusal.
		return fail("unknown_acceptance", "unknown")
	}
	if op == "append" && command.State != "completed" {
		encoded, e := json.Marshal(data)
		if e != nil {
			return fail("unknown_acceptance", "known_receipt")
		}
		if e = h.delegations.FinishContinuityCommand(ctx, command, "completed", encoded, ""); e != nil {
			return fail("unknown_acceptance", "known_receipt")
		}
	}
	if _, err = caller.Check(ctx); err != nil {
		return fail("caller_unbound", "known_receipt")
	}
	result := map[string]any{"action": action, "data": data, "state": state}
	if op == "append" {
		result["idempotency_key"] = request.IdempotencyKey
	}
	response.OK = true
	response.Result = map[string]any{"continuity": result}
	encoded, e := json.Marshal(response)
	if e != nil || validator.Validate(encoded, "Response") != nil {
		return fail("unknown_acceptance", "known_receipt")
	}
	return response, true
}
