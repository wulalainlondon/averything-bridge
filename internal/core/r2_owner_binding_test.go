package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/taskapi"
	"testing"
)

func TestR2ConflictingAppendReceiptCannotBecomeSuccess(t *testing.T) {
	h, p, c, intent, r := extensionFormal(t)
	ctx := context.Background()
	bc := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + p.ID, SourceSessionID: p.ID, SourceResumeID: p.ResumeID(), SourceConfigKnown: true, SourceConfigRevision: p.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool", SourceRequestID: c.RequestID, ToolCallID: c.CallID, Transport: "native_dynamic_tool"}
	record, e := h.apiRecord(ctx, bc, intent.TaskID)
	if e != nil {
		t.Fatal(e)
	}
	task, e := h.apiTask(ctx, record)
	if e != nil {
		t.Fatal(e)
	}
	a := continuityInput(r, p, h, "original append A")
	input := map[string]any{"task_id": intent.TaskID, "expected_revision": task["revision"], "target": map[string]any{"instance_id": a.InstanceID, "session_id": record.SessionID, "expected_native_thread_id": a.ExpectedThreadID, "expected_config_revision": a.ExpectedConfigRevision}, "content": a.Content, "mode": "queue", "return_to_parent": true, "expected_previous_round_id": a.ExpectedPreviousID}
	key := "qa-shared-key"
	raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "qa-A", "operation": "append", "idempotency_key": key, "input": input})
	c.CallID = "qa-A"
	prior := h.ExecuteTask(ctx, c, raw)
	if !prior.OK {
		t.Fatal("setup append", prior.Error)
	}
	// Different outer continuation B uses same inner append namespace/key.
	a.Content = "different continuation B"
	invoke := func() bool {
		c.CallID = "qa-B"
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "qa-B", "operation": "append", "idempotency_key": key, "input": map[string]any{"continuity": map[string]any{"action": "continue", "arguments": a}}})
		x := h.ExecuteTask(ctx, c, raw)
		t.Logf("B response: %+v", x)
		return x.OK
	}
	if invoke() {
		t.Fatal("initial conflicting intent accepted")
	}
	if invoke() {
		t.Fatal("foreign intent receipt promoted to B success on recovery")
	}
	latest, e := h.delegations.LatestRound(ctx, r.ID)
	if e != nil || latest.Version != 2 {
		t.Fatal("collision caused second effect", latest, e)
	}
	saved, found, e := h.delegations.ContinuityCommand(ctx, p.ID, key)
	if e != nil || !found {
		t.Fatal(e)
	}
	var binding formalOwnerBinding
	if json.Unmarshal(saved.FormalBinding, &binding) != nil || binding.Hash == "" {
		t.Fatal("B binding not persisted before effect")
	}
	priorReceipt, e := h.delegations.TaskJournal().Lookup(ctx, binding.Namespace, key)
	if e != nil || priorReceipt.Hash == binding.Hash {
		t.Fatal("fixture did not create distinct A/B intent", e)
	}
}

func r2OwnerRequest(t *testing.T, key string, in backend.DelegationContinuation) taskapi.Request {
	t.Helper()
	input, _ := json.Marshal(map[string]any{"continuity": map[string]any{"action": "continue", "arguments": in}})
	return taskapi.Request{Type: "task_api_request", Version: contract.Version, CorrelationID: key, Operation: "append", IdempotencyKey: key, Input: input}
}
func TestR2LostAckExactIntentAndChangedRevision(t *testing.T) {
	h, p, c, intent, r := extensionFormal(t)
	ctx := context.Background()
	c.CallID = "r2-lost-ack"
	// Reserve exactly as the production public owner route does, then execute its
	// original formal request and lose the owner ACK before command completion.
	bc := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + p.ID, SourceSessionID: p.ID, SourceResumeID: p.ResumeID(), SourceConfigKnown: true, SourceConfigRevision: p.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool", SourceRequestID: c.RequestID, ToolCallID: c.CallID, Transport: "native_dynamic_tool"}
	in := continuityInput(r, p, h, "original immutable B")
	request := r2OwnerRequest(t, "r2-lost-ack", in)
	digest, _ := taskapi.IntentHash(request)
	command := delegation.ContinuityCommand{Parent: p.ID, Key: request.IdempotencyKey, Hash: digest, Action: "continue", Request: c.RequestID, Call: "continuity-api:" + request.IdempotencyKey, EffectTaskID: intent.TaskID}
	args, _ := json.Marshal(in)
	if e := h.prepareFormalOwner(ctx, bc, &command, request, args); e != nil {
		t.Fatal(e)
	}
	if fresh, e := h.delegations.ReserveContinuityCommand(ctx, command); e != nil || !fresh {
		t.Fatal(fresh, e)
	}
	var binding formalOwnerBinding
	json.Unmarshal(command.FormalBinding, &binding)
	raw, _ := json.Marshal(binding.Request)
	effect := h.ExecuteTask(ctx, c, raw)
	if !effect.OK {
		t.Fatal(effect.Error)
	}
	latest, e := h.delegations.LatestRound(ctx, r.ID)
	if e != nil || latest.Version != 2 {
		t.Fatal(latest, e)
	}
	continuityWait(t, h, latest.ID, "terminal")
	rec, e := h.apiRecord(ctx, bc, intent.TaskID)
	if e != nil {
		t.Fatal(e)
	}
	current, e := h.apiTask(ctx, rec)
	if e != nil {
		t.Fatal(e)
	}
	var original map[string]any
	json.Unmarshal(binding.Request.Input, &original)
	aRevision, _ := json.Marshal(original["expected_revision"])
	bRevision, _ := json.Marshal(current["revision"])
	if string(aRevision) == string(bRevision) {
		t.Fatal("fixture did not advance original revision")
	}
	raw, _ = json.Marshal(request)
	result := h.ExecuteTask(ctx, c, raw)
	if !result.OK {
		t.Fatal("lost ACK recovery", result.Error)
	}
	saved, found, e := h.delegations.ContinuityCommand(ctx, p.ID, command.Key)
	if e != nil || !found || string(saved.FormalBinding) != string(command.FormalBinding) {
		t.Fatal("binding changed", e)
	}
	latest, e = h.delegations.LatestRound(ctx, r.ID)
	if e != nil || latest.Version != 2 {
		t.Fatal("recovery mutated again", latest, e)
	}
	// Changed content under the same original owner key cannot overwrite binding.
	in.Content = "conflicting C"
	other := r2OwnerRequest(t, command.Key, in)
	raw, _ = json.Marshal(other)
	if x := h.ExecuteTask(ctx, c, raw); x.OK || x.Error.Code != "idempotency_conflict" {
		t.Fatal("content conflict", x)
	}
	foreign := bc
	foreign.SourceSessionID = "foreign"
	if _, e := h.recoverFormalOwner(ctx, foreign, p, saved); e == nil {
		t.Fatal("foreign source recovered")
	}
	stale := bc
	stale.SourceConfigRevision++
	if _, e := h.recoverFormalOwner(ctx, stale, p, saved); e == nil {
		t.Fatal("changed source cfg recovered")
	}
	stale = bc
	stale.NamespaceGeneration++
	if _, e := h.recoverFormalOwner(ctx, stale, p, saved); e == nil {
		t.Fatal("foreign namespace recovered")
	}
	missing := saved
	missing.FormalBinding = nil
	if _, e := h.recoverFormalOwner(ctx, bc, p, missing); e == nil {
		t.Fatal("missing migration binding recovered")
	}
	corrupt := saved
	binding.Hash = "wrong"
	corrupt.FormalBinding, _ = json.Marshal(binding)
	if _, e := h.recoverFormalOwner(ctx, bc, p, corrupt); e == nil {
		t.Fatal("corrupt hash recovered")
	}
	c.Validate = func() bool { return false }
	raw, _ = json.Marshal(request)
	if x := h.ExecuteTask(ctx, c, raw); x.OK || x.Error.Code != "caller_unbound" {
		t.Fatal("revoked source recovered", x)
	}
}
func TestR2MigrationReservationFailsClosedPublicPath(t *testing.T) {
	for _, state := range []string{"reserved", "completed"} {
		t.Run(state, func(t *testing.T) {
			h, p, c, intent, r := extensionFormal(t)
			in := continuityInput(r, p, h, "migration no binding")
			request := r2OwnerRequest(t, "r2-migration", in)
			digest, _ := taskapi.IntentHash(request)
			command := delegation.ContinuityCommand{Parent: p.ID, Key: request.IdempotencyKey, Hash: digest, Action: "continue", Request: c.RequestID, Call: "old", EffectTaskID: intent.TaskID}
			if fresh, e := h.delegations.ReserveContinuityCommand(context.Background(), command); e != nil || !fresh {
				t.Fatal(e)
			}
			if state == "completed" {
				if e := h.delegations.FinishContinuityCommand(context.Background(), command, "completed", []byte(`{"old":"unbound"}`), ""); e != nil {
					t.Fatal(e)
				}
			}
			raw, _ := json.Marshal(request)
			c.CallID = "migration"
			for i := 0; i < 2; i++ {
				if x := h.ExecuteTask(context.Background(), c, raw); x.OK {
					t.Fatal("unbound migration completed")
				}
			}
			latest, e := h.delegations.LatestRound(context.Background(), r.ID)
			if e != nil || latest.Version != 1 {
				t.Fatal("migration emitted effect", latest, e)
			}
		})
	}
}

func TestR2AbsentOriginalReceiptNeverReexecutes(t *testing.T) {
	h, p, c, intent, r := extensionFormal(t)
	ctx := context.Background()
	in := continuityInput(r, p, h, "reserved before crash")
	request := r2OwnerRequest(t, "r2-absent", in)
	digest, _ := taskapi.IntentHash(request)
	bc := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "session:" + p.ID, SourceSessionID: p.ID, SourceResumeID: p.ResumeID(), SourceConfigKnown: true, SourceConfigRevision: p.SettingsSnapshot().ConfigRevision, BindingKind: "native_tool"}
	command := delegation.ContinuityCommand{Parent: p.ID, Key: request.IdempotencyKey, Hash: digest, Action: "continue", Request: c.RequestID, Call: "continuity-api:" + request.IdempotencyKey, EffectTaskID: intent.TaskID}
	args, _ := json.Marshal(in)
	if e := h.prepareFormalOwner(ctx, bc, &command, request, args); e != nil {
		t.Fatal(e)
	}
	if fresh, e := h.delegations.ReserveContinuityCommand(ctx, command); e != nil || !fresh {
		t.Fatal(e)
	}
	raw, _ := json.Marshal(request)
	c.CallID = "r2-absent-readback"
	for i := 0; i < 2; i++ {
		x := h.ExecuteTask(ctx, c, raw)
		if x.OK || x.Error.Code != "unknown_acceptance" {
			t.Fatal("absent receipt synthesized success", x)
		}
	}
	latest, e := h.delegations.LatestRound(ctx, r.ID)
	if e != nil || latest.Version != 1 {
		t.Fatal("unknown acceptance reexecuted", latest, e)
	}
	saved, _, e := h.delegations.ContinuityCommand(ctx, p.ID, command.Key)
	if e != nil || saved.State != "reserved" || string(saved.FormalBinding) != string(command.FormalBinding) {
		t.Fatal("unknown intent rewritten", saved, e)
	}
}
