package core

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	artifact "everything-go/contracts/task-api/artifacts-v3"
	artifact4 "everything-go/contracts/task-api/artifacts-v4"
	"everything-go/internal/delegation"
	"everything-go/internal/messagequeue"
	"everything-go/internal/taskapi"
)

// The immutable seal is the original result owner. No filesystem path, URL,
// inbox broadcast, model artifact assertion or second artifact ledger exists.
func (h *Hub) executeArtifactBytes(ctx context.Context, caller taskapi.BoundCaller, raw []byte) (taskapi.Response, bool) {
	var request taskapi.Request
	if json.Unmarshal(raw, &request) != nil || request.Operation != "read_artifact_bytes" && request.Operation != "artifact_capabilities" {
		return taskapi.Response{}, false
	}
	op := request.Operation
	response := taskapi.Response{Type: "task_api_response", Version: artifact.Version, CorrelationID: request.CorrelationID, Operation: &op}
	fail := func(code string) (taskapi.Response, bool) {
		response.OK = false
		response.Result = nil
		response.Error = taskapi.Failure(code, "known_none", "read_capabilities")
		return response, true
	}
	version := artifact.Version
	validate := artifact.Validate
	hash := artifact.Hash
	if request.Version == artifact4.Version {
		version = artifact4.Version
		validate = artifact4.Validate
		hash = artifact4.Hash
		response.Version = version
	}
	if request.Version != version {
		response.RequestVersion = &request.Version
		return fail("unsupported")
	}
	if validate(raw, "Request") != nil {
		return fail("invalid_argument")
	}
	bound, err := caller.Check(ctx)
	if err != nil {
		return fail("caller_unbound")
	}
	if bound.Authority != h.cfg.InstanceID {
		return fail("wrong_authority")
	}
	if bound.SourceSessionID != "" {
		child, err := h.delegations.HasChild(ctx, bound.SourceSessionID)
		if err != nil || child {
			return fail("permission")
		}
	}
	if request.Operation == "artifact_capabilities" {
		response.OK = true
		response.Result = map[string]any{"api_versions": []string{version}, "schema_hash": hash(), "allowed_operations": []string{"artifact_capabilities", "read_artifact_bytes"}, "mutation_authority_instance_id": h.cfg.InstanceID, "max_range_bytes": 32768, "artifact_owner": "immutable_sealed_final", "reported_paths": "unsupported"}
		return response, true
	}
	var in struct {
		OwnerRequest string `json:"owner_request_id"`
		TaskID       string `json:"task_id"`
		SealID       string `json:"seal_id"`
		ArtifactID   string `json:"artifact_id"`
		Hash         string `json:"expected_hash"`
		Offset       int64  `json:"offset"`
		Limit        int64  `json:"limit"`
	}
	if json.Unmarshal(request.Input, &in) != nil {
		return fail("invalid_argument")
	}
	// The original scoped authorization runs BEFORE any sealed body access.
	getInput, _ := json.Marshal(map[string]any{"task_id": in.TaskID})
	if _, err := h.Authorize(ctx, bound, taskapi.Request{Operation: "get", Input: getInput}); err != nil {
		return fail("permission")
	}
	record, err := h.apiRecord(ctx, bound, in.TaskID)
	if err != nil {
		return fail("permission")
	}
	if record.Path != "delegation" || in.ArtifactID != "result.txt" {
		return fail("unsupported")
	}
	if _, err := caller.Check(ctx); err != nil {
		return fail("caller_unbound")
	}
	owner, seal, code := h.resolveArtifactRound(ctx, record, in.OwnerRequest, in.SealID, in.Hash)
	if code != "" {
		return fail(code)
	}
	// Owner-generated result.txt is precisely the immutable sealed UTF-8 body.
	data := []byte(seal.Text)
	if in.Offset > int64(len(data)) {
		return fail("invalid_argument")
	}
	end := in.Offset + in.Limit
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	chunk := data[in.Offset:end]
	chunkDigest := sha256.Sum256(chunk)
	if _, err := caller.Check(ctx); err != nil {
		return fail("caller_unbound")
	}
	// Revalidate current scope and exact immutable membership after copying the
	// bounded range. A newly sealed equal-content round must not turn a v3
	// unique selection into a silently ambiguous receipt.
	current, e := caller.Check(ctx)
	if e != nil {
		return fail("caller_unbound")
	}
	if _, e := h.Authorize(ctx, current, taskapi.Request{Operation: "get", Input: getInput}); e != nil {
		return fail("permission")
	}
	again, checked, code := h.resolveArtifactRound(ctx, record, in.OwnerRequest, in.SealID, in.Hash)
	if code != "" {
		return fail(code)
	}
	if again.ID != owner.ID || again.ChildRequestID != owner.ChildRequestID || checked.ID != seal.ID || checked.Hash != seal.Hash {
		return fail("stale_revision")
	}
	response.OK = true
	response.Result = map[string]any{"task_id": record.TaskID, "seal_id": seal.ID, "artifact_id": "result.txt", "owner_request_id": owner.ChildRequestID, "total_bytes": len(data), "offset": in.Offset, "next_offset": end, "has_more": end < int64(len(data)), "full_hash": seal.Hash, "chunk_hash": hex.EncodeToString(chunkDigest[:]), "data_base64": base64.StdEncoding.EncodeToString(chunk)}
	if version == artifact4.Version {
		response.Result.(map[string]any)["owner_round_id"] = owner.ID
	}
	return response, true
}

func (h *Hub) resolveArtifactRound(ctx context.Context, record taskapi.IntentRecord, request, sealID, hash string) (delegation.Record, messagequeue.TaskSeal, string) {
	reject := func(code string) (delegation.Record, messagequeue.TaskSeal, string) {
		return delegation.Record{}, messagequeue.TaskSeal{}, code
	}
	relation, found, e := h.delegations.Round(ctx, record.NativeID)
	if e != nil || !found || relation.Authority != h.cfg.InstanceID || apiID(h.cfg.InstanceID, "delegation", relation.LogicalID) != record.TaskID {
		return reject("permission")
	}
	root, found, e := h.delegations.ByID(ctx, relation.LogicalID)
	if e != nil || !found || root.ChildSessionID != record.SessionID {
		return reject("permission")
	}
	child, found := h.registry.Get(root.ChildSessionID)
	if !found {
		return reject("result_not_ready")
	}
	if scope, e := h.TaskWorkerScope(child); e != nil || scope == nil {
		return reject("permission")
	}
	rounds, e := h.delegations.ArtifactRounds(ctx, relation.LogicalID, root.ParentSessionID, root.ChildSessionID, request)
	if e != nil {
		return reject("owner_selector_required")
	}
	// Read-only membership resolution. Count matching immutable owner tuples
	// BEFORE selecting/verifying an anchor; never silently choose first/latest.
	var selected delegation.Record
	var seal messagequeue.TaskSeal
	matches := 0
	for _, round := range rounds {
		if round.Authority != h.cfg.InstanceID || round.LogicalID != relation.LogicalID {
			return reject("permission")
		}
		r, found, e := h.delegations.ByID(ctx, round.ID)
		if e != nil || !found {
			return reject("result_not_ready")
		}
		candidate, e := h.messageQueue.TaskSeal(ctx, r.ChildSessionID, r.ChildRequestID, sealID)
		if e != nil {
			continue
		}
		if candidate.ID != sealID || candidate.Hash != hash {
			continue
		}
		matches++
		selected = r
		seal = candidate
	}
	if matches == 0 {
		return reject("stale_revision")
	}
	if matches != 1 {
		return reject("ambiguous_owner")
	}
	verified, ok := h.canonicalReviewSeal(ctx, selected)
	if !ok || verified.ID != seal.ID || verified.Hash != seal.Hash || verified.RequestID != selected.ChildRequestID {
		return reject("stale_revision")
	}
	return selected, verified, ""
}
