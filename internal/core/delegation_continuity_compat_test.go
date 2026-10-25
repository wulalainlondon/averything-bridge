package core

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

func continuityCompatCall(t *testing.T, h *Hub, p *session.Session, op, key, action string, args any) taskapi.Response {
	t.Helper()
	frame := map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "compat-" + key, "operation": op, "input": map[string]any{"continuity": map[string]any{"action": action, "arguments": args}}}
	if op == "append" {
		frame["idempotency_key"] = key
	}
	raw, _ := json.Marshal(frame)
	return h.ExecuteTask(context.Background(), backend.TaskCaller{Session: p, RequestID: p.ActiveQueuedID(), ThreadID: p.ResumeID(), TurnID: "synthetic-parent-turn", CallID: "call-" + key, ProviderVersion: "0.160.0", Validate: func() bool { return true }}, raw)
}
func continuityCompatData(t *testing.T, r taskapi.Response) map[string]any {
	t.Helper()
	if !r.OK {
		t.Fatalf("compat refused: %+v", r.Error)
	}
	return r.Result.(map[string]any)["continuity"].(map[string]any)["data"].(map[string]any)
}

// Calls the unchanged, already installed task_get/task_append tool envelope.
// No new dynamic tool registration, parent thread or child session is created.
func TestContinuityCompatExistingToolsOwnerAndQASequence(t *testing.T) {
	h, p, f := continuityFixture(t)
	owner, original := continuityDelegate(t, h, p, "compat-owner", "owner first")
	qa, failed := continuityDelegate(t, h, p, "compat-qa", fmt.Sprintf(`{"owner_round_id":%q,"verdict":"FAIL"}`, owner.ID))
	continuityCompatData(t, continuityCompatCall(t, h, p, "append", "bind-compat-0001", "bind_qa", map[string]any{"owner_delegation_id": owner.ID, "qa_delegation_id": qa.ID}))
	continuityCompatData(t, continuityCompatCall(t, h, p, "append", "record-fail-0001", "record_review", map[string]any{"owner_round_id": owner.ID, "qa_round_id": qa.ID}))
	ownerChild, _ := h.registry.Get(owner.ChildSessionID)
	qaChild, _ := h.registry.Get(qa.ChildSessionID)
	ownerThread, qaThread := ownerChild.ResumeID(), qaChild.ResumeID()
	continuityDeliver(t, h, p, original, failed)
	continuityParentTurn(t, p, "r_compat_fix")
	in := continuityInput(original, p, h, "owner corrected")
	response := continuityCompatCall(t, h, p, "append", "fix-compat-000001", "continue", in)
	got := continuityCompatData(t, response)
	id := got["delegation_id"].(string)
	replayed := continuityCompatData(t, continuityCompatCall(t, h, p, "append", "fix-compat-000001", "continue", in))
	if replayed["delegation_id"] != id {
		t.Fatal("duplicate continuation")
	}
	fixed := continuityWait(t, h, id, "terminal")
	if fixed.ChildSessionID != owner.ChildSessionID || ownerChild.ResumeID() != ownerThread {
		t.Fatal("owner context replaced")
	}
	passText := fmt.Sprintf(`{"owner_round_id":%q,"verdict":"PASS"}`, id)
	qaGot := continuityCompatData(t, continuityCompatCall(t, h, p, "append", "qa-recheck-000001", "continue", continuityInput(failed, p, h, passText)))
	checked := continuityWait(t, h, qaGot["delegation_id"].(string), "terminal")
	if checked.ChildSessionID != qa.ChildSessionID || qaChild.ResumeID() != qaThread {
		t.Fatal("QA context replaced")
	}
	continuityCompatData(t, continuityCompatCall(t, h, p, "append", "record-pass-0001", "record_review", map[string]any{"owner_round_id": fixed.ID, "qa_round_id": checked.ID}))
	continuityDeliver(t, h, p, fixed, checked)
	continuityParentTurn(t, p, "r_compat_later_parent_turn")
	later := continuityCompatData(t, continuityCompatCall(t, h, p, "append", "fix-compat-000001", "continue", in))
	if later["delegation_id"] != id {
		t.Fatal("cross-turn key replayed")
	}
	f.mu.Lock()
	sends := f.sends[fixed.ChildRequestID]
	f.mu.Unlock()
	if sends != 1 {
		t.Fatal("repeated native send", sends)
	}
	if counts, e := h.delegations.CountsForParent(context.Background(), p.ID); e != nil || counts != (delegation.Counts{}) {
		t.Fatal("reservation leak", counts, e)
	}
	for _, v := range []struct{ id, verdict string }{{original.ID, "FAIL"}, {fixed.ID, "PASS"}} {
		_, verdict, e := h.delegations.QAForRound(context.Background(), v.id)
		if e != nil || verdict != v.verdict {
			t.Fatal("verdict history", verdict, e)
		}
	}
	continuityCompatData(t, continuityCompatCall(t, h, p, "get", "read-rounds-0001", "rounds", map[string]any{"delegation_id": owner.ID}))
	continuityCompatData(t, continuityCompatCall(t, h, p, "get", "read-command-001", "command", map[string]any{"idempotency_key": "fix-compat-000001"}))
	in.Content = "different intent"
	conflict := continuityCompatCall(t, h, p, "append", "fix-compat-000001", "continue", in)
	if conflict.OK || conflict.Error.Code != "idempotency_conflict" {
		t.Fatal("key collision accepted", conflict)
	}
}

func TestContinuityCompatRejectsMalformedForeignAndUnbound(t *testing.T) {
	h, p, _ := continuityFixture(t)
	owner, original := continuityDelegate(t, h, p, "compat-owner", "result")
	in := continuityInput(original, p, h, "fix")
	bad := map[string]any{"delegation_id": owner.ID, "sandbox": "danger-full-access"}
	if r := continuityCompatCall(t, h, p, "append", "bad-fields-00001", "continue", bad); r.OK || r.Error.Code != "invalid_argument" {
		t.Fatal("unknown fields accepted", r)
	}
	if r := continuityCompatCall(t, h, p, "get", "bad-get-000001", "continue", in); r.OK {
		t.Fatal("get mutation accepted")
	}
	if r := continuityCompatCall(t, h, p, "append", "bad-action-0001", "rounds", map[string]any{"delegation_id": owner.ID}); r.OK {
		t.Fatal("append read accepted")
	}
	in.DelegationID = "foreign-task"
	if r := continuityCompatCall(t, h, p, "append", "foreign-0000001", "continue", in); r.OK {
		t.Fatal("foreign accepted")
	}
	raw := []byte(`{"type":"task_api_request","api_version":"1.0.0-rc.1","correlation_id":"unbound","operation":"get","input":{"continuity":{"action":"rounds","arguments":{"delegation_id":"` + owner.ID + `"}}}}`)
	if r := h.ExecuteTask(context.Background(), backend.TaskCaller{}, raw); r.OK {
		t.Fatal("unbound accepted")
	}
	child, _ := h.registry.Get(owner.ChildSessionID)
	continuityParentTurn(t, child, "r_child_attempt")
	if r := continuityCompatCall(t, h, child, "get", "child-deny-0001", "rounds", map[string]any{"delegation_id": owner.ID}); r.OK {
		t.Fatal("worker management accepted")
	}
}

func TestContinuityCompatReservedCommandNeverReplays(t *testing.T) {
	h, p, _ := continuityFixture(t)
	_, original := continuityDelegate(t, h, p, "compat-owner", "first")
	in := continuityInput(original, p, h, "fix")
	frame := map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "unknown", "operation": "append", "idempotency_key": "unknown-command-0001", "input": map[string]any{"continuity": map[string]any{"action": "continue", "arguments": in}}}
	raw, _ := json.Marshal(frame)
	var req taskapi.Request
	json.Unmarshal(raw, &req)
	hash, _ := taskapi.IntentHash(req)
	fresh, e := h.delegations.ReserveContinuityCommand(context.Background(), delegation.ContinuityCommand{Parent: p.ID, Key: req.IdempotencyKey, Hash: hash, Action: "continue", Request: "r_original_unknown", Call: "unknown-call"})
	if e != nil || !fresh {
		t.Fatal(e)
	}
	r := continuityCompatCall(t, h, p, "append", req.IdempotencyKey, "continue", in)
	if r.OK || r.Error.Code != "unknown_acceptance" {
		t.Fatal("unknown operation replayed", r)
	}
	rows, e := h.delegations.Rounds(context.Background(), original.ID)
	if e != nil || len(rows) != 1 {
		t.Fatal("unknown created successor", rows, e)
	}
}

func TestContinuityCompatConcurrentKeyAndDurableCommand(t *testing.T) {
	h, p, f := continuityFixture(t)
	owner, original := continuityDelegate(t, h, p, "compat-concurrent-owner", "first")
	continuityDeliver(t, h, p, original)
	continuityParentTurn(t, p, "r_compat_concurrent")
	in := continuityInput(original, p, h, "one correction")
	var wg sync.WaitGroup
	answers := make(chan taskapi.Response, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			answers <- continuityCompatCall(t, h, p, "append", "concurrent-key-0001", "continue", in)
		}()
	}
	wg.Wait()
	close(answers)
	id := ""
	for response := range answers {
		data := continuityCompatData(t, response)
		got := data["delegation_id"].(string)
		if id != "" && id != got {
			t.Fatal("concurrent duplicate", id, got)
		}
		id = got
	}
	final := continuityWait(t, h, id, "terminal")
	f.mu.Lock()
	sends := f.sends[final.ChildRequestID]
	f.mu.Unlock()
	if sends != 1 {
		t.Fatal("duplicate sends", sends)
	}
	reopened, err := delegation.Open(h.cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	command, found, err := reopened.ContinuityCommand(context.Background(), p.ID, "concurrent-key-0001")
	if err != nil || !found || command.State != "completed" {
		t.Fatal("missing durable command", command, err)
	}
	var data map[string]any
	if json.Unmarshal(command.Result, &data) != nil || data["delegation_id"] != id {
		t.Fatal("wrong durable result", data)
	}
	rows, err := reopened.Rounds(context.Background(), owner.ID)
	if err != nil || len(rows) != 2 {
		t.Fatal("wrong round count", rows, err)
	}
}
