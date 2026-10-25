//go:build taskapihold

package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/messagequeue"
	"everything-go/internal/taskapi"
	"testing"
)

func TestTaskAPICompatibleRollbackHoldsOnlyOwnedNewAdmission(t *testing.T) {
	h, _ := newTestHub(t)
	reader := sharedReadClient(t, h, "rollback-fixture")
	bound, err := h.taskHuman(reader)
	if err != nil {
		t.Fatal(err)
	}
	caller, err := bound.Check(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	target := h.registry.Create("hold-target", "Target", t.TempDir(), backend.Codex, "gpt-6.1-sol", "read-only", "hold-thread")
	definition := map[string]any{"goal": "isolated saved fixture", "instruction": "fixture", "scope": map[string]any{"workspace_roots": []string{target.Snapshot().Cwd}, "allowed_operations": []string{"read"}, "sandbox": "read-only", "network": "deny", "max_children": 0}, "acceptance": []any{}, "dependencies": []any{}, "workspace": map[string]any{"cwd": target.Snapshot().Cwd, "input_artifact_ids": []any{}}, "existing_target": map[string]any{"instance_id": h.cfg.InstanceID, "session_id": target.ID, "expected_native_thread_id": target.ResumeID(), "expected_config_revision": target.SettingsSnapshot().ConfigRevision}}
	taskID := apiID(h.cfg.InstanceID, "ordinary", target.ID+"/r_saved_create")
	save := func(op, key, requestID string, input any) {
		raw, _ := json.Marshal(input)
		request := taskapi.Request{Version: contract.Version, Operation: op, IdempotencyKey: key, Input: raw}
		locator := taskapi.Locator{Authority: h.cfg.InstanceID, Path: "ordinary", Operation: op, Key: key}
		if op == "append" {
			locator.TaskID = taskID
		}
		ns, err := taskapi.MakeNamespace(caller, locator, request)
		if err != nil {
			t.Fatal(err)
		}
		hash, _ := taskapi.IntentHash(request)
		meta, _ := json.Marshal(apiMetadata{Caller: caller, Request: request, Transport: "authenticated_ws"})
		command := taskapi.AuthorizedCommand{Caller: caller, Request: request, Locator: locator, Namespace: ns, IntentHash: hash}
		record := taskapi.IntentRecord{ReceiptID: "tr_saved_" + op, TaskID: taskID, NativeID: target.ID + "/r_saved_create", SessionID: target.ID, RequestID: requestID, Metadata: meta}
		_, _, err = h.messageQueue.Enqueue(messagequeue.Entry{SessionID: target.ID, RequestID: requestID, Content: "Saved original fixture", Payload: []byte(`{"content":"fixture"}`), API: &messagequeue.APIAdmission{Command: command, Record: record}})
		if err != nil {
			t.Fatal(err)
		}
	}
	execute := func(op, key string, input any) taskapi.Response {
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "hold-fixture", "operation": op, "idempotency_key": key, "input": input})
		return h.taskService.Execute(context.Background(), bound, raw)
	}
	save("create_dispatch", "persisted-saved-create", "r_saved_create", definition)
	appendInput := map[string]any{"task_id": taskID, "expected_revision": 0, "target": definition["existing_target"], "mode": "queue", "content": "Saved supplement"}
	save("append", "persisted-saved-append", "r_saved_append", appendInput)
	for _, row := range []struct {
		op, key string
		input   map[string]any
	}{{"create_dispatch", "persisted-saved-create", definition}, {"append", "persisted-saved-append", appendInput}} {
		response := execute(row.op, row.key, row.input)
		if !response.OK {
			t.Fatal("hold refused already accepted original key", row.op, response.Error)
		}
		receipt := response.Result.(map[string]any)
		if receipt["receipt_id"] != "tr_saved_"+row.op {
			t.Fatal("hold minted new receipt", receipt)
		}
		changed := map[string]any{}
		for k, v := range row.input {
			changed[k] = v
		}
		if row.op == "append" {
			changed["content"] = "different"
		} else {
			changed["instruction"] = "different"
		}
		response = execute(row.op, row.key, changed)
		if response.OK || response.Error.Code != "idempotency_conflict" {
			t.Fatal("hold masked key conflict", response)
		}
		response = execute(row.op, row.key+"-new", row.input)
		if response.OK || response.Error.Code != "busy" || response.Error.Acceptance != "known_none" {
			t.Fatal("new admission not held before effects", response)
		}
	}
	snap, _ := h.messageQueue.Snapshot(target.ID)
	if len(snap.Items) != 2 || target.QueueLen() != 0 {
		t.Fatal("hold produced a new effect", snap)
	}
	caps, err := h.Read(context.Background(), taskapi.AuthorizedCommand{Caller: caller, Request: taskapi.Request{Operation: "capabilities"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range caps.(map[string]any)["allowed_operations"].([]string) {
		if op == "create_dispatch" || op == "append" {
			t.Fatal("hold advertises new admission")
		}
	}
	if len(caps.(map[string]any)["create_routes"].(map[string]string)) != 0 {
		t.Fatal("hold exposes admission route")
	}
}
