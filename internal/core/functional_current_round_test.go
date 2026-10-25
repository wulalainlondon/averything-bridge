package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	a4 "everything-go/contracts/task-api/artifacts-v4"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// This exercises the original public native entry and canonical stores. The
// executor/native anchors are explicit synthetic fixtures, not model evidence.
func TestFunctionalPublicTaskReadsCurrentCanonicalRound(t *testing.T) {
	for _, provider := range []string{backend.Codex, backend.Claude} {
		t.Run(provider, func(t *testing.T) {
			h, parent, caller, intent, first := extensionFormalBackend(t, provider)
			exec := h.exec.(*extensionExec)
			exec.final = "current round: 中文 🧪 e\u0301\n"
			in := continuityInput(first, parent, h, "continue the same original owner")
			invoke := func(operation string, input any, key string) map[string]any {
				t.Helper()
				request := map[string]any{"type": "task_api_request", "api_version": contract.Version, "operation": operation, "correlation_id": "synthetic-" + operation, "input": input}
				if key != "" {
					request["idempotency_key"] = key
				}
				raw, _ := json.Marshal(request)
				caller.CallID = "synthetic-current-" + operation
				response := h.ExecuteTask(context.Background(), caller, raw)
				if !response.OK {
					t.Fatalf("%s refused: %+v", operation, response.Error)
				}
				encoded, err := json.Marshal(response.Result)
				var public map[string]any
				if err != nil || json.Unmarshal(encoded, &public) != nil {
					t.Fatal("invalid public response", err)
				}
				return public
			}
			accepted := invoke("append", map[string]any{"continuity": map[string]any{"action": "continue", "arguments": in}}, "synthetic-current-round-once")
			data := accepted["continuity"].(map[string]any)["data"].(map[string]any)
			current := continuityWait(t, h, data["delegation_id"].(string), "terminal")
			task := invoke("get", map[string]any{"task_id": intent.TaskID}, "")
			if task["target"].(map[string]any)["request_id"] != current.ChildRequestID {
				t.Fatalf("current round %s is hidden by the first request: %+v", current.ChildRequestID, task["target"])
			}
			if task["task_id"] != intent.TaskID || task["task_ref"].(map[string]any)["native_record_id"] != first.ID {
				t.Fatal("logical task identity changed")
			}
			hash := sha256.Sum256([]byte(exec.final))
			wantedHash := hex.EncodeToString(hash[:])
			if task["final_hash"] != wantedHash || task["result_anchor"].(map[string]any)["request_id"] != current.ChildRequestID {
				t.Fatal("current result borrowed the historical seal/anchor", task)
			}
			result := invoke("read_result", map[string]any{"task_id": intent.TaskID, "seal_id": "seal_" + wantedHash, "offset": 0, "limit": 1024}, "")
			if result["text"] != exec.final || result["anchor"].(map[string]any)["request_id"] != current.ChildRequestID {
				t.Fatal("read_result did not retain the selected current owner", result)
			}
		})
	}
}

func TestFunctionalPendingRoundDoesNotExposeHistoricalCompletion(t *testing.T) {
	for _, provider := range []string{backend.Codex, backend.Claude} {
		t.Run(provider, func(t *testing.T) {
			h, parent, caller, intent, first := extensionFormalBackend(t, provider)
			old, err := h.messageQueue.TaskSeal(context.Background(), first.ChildSessionID, first.ChildRequestID, "")
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan string, 1)
			h.exec.(*extensionExec).onSend = func(_ *session.Session, request, _ string) { started <- request }
			in := continuityInput(first, parent, h, "hold the next original round")
			raw, _ := json.Marshal(r2OwnerRequest(t, "synthetic-pending-current", in))
			caller.CallID = "synthetic-pending-append"
			if response := h.ExecuteTask(context.Background(), caller, raw); !response.OK {
				t.Fatal(response.Error)
			}
			var request string
			select {
			case request = <-started:
			case <-time.After(time.Second):
				t.Fatal("current round did not start")
			}
			read := func(operation string, input any) map[string]any {
				t.Helper()
				raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-pending-" + operation, "operation": operation, "input": input})
				caller.CallID = "synthetic-pending-" + operation
				response := h.ExecuteTask(context.Background(), caller, raw)
				if !response.OK {
					t.Fatal(response.Error)
				}
				encoded, _ := json.Marshal(response.Result)
				var result map[string]any
				json.Unmarshal(encoded, &result)
				return result
			}
			check := func(task map[string]any) {
				t.Helper()
				if task["target"].(map[string]any)["request_id"] != request || task["seal_id"] != nil || task["result_anchor"] != nil || task["axes"].(map[string]any)["final"] == "sealed" {
					t.Fatal("pending round revived an old result", task)
				}
			}
			check(read("get", map[string]any{"task_id": intent.TaskID}))
			page := read("list", map[string]any{"session_id": parent.ID, "path": "delegation", "limit": 100})
			found := 0
			for _, value := range page["items"].([]any) {
				task := value.(map[string]any)
				if task["task_id"] == intent.TaskID {
					check(task)
					found++
				}
			}
			if found != 1 {
				t.Fatal("logical task missing or duplicated", found)
			}
			// Exact historical owner reads remain available through unchanged rc4.
			assertArtifactOwner(t, artifactRoundRead(t, h, caller, a4.Version, intent.TaskID, first.ChildRequestID, old.ID, old.Hash), first, first.Result, a4.Version)
		})
	}
}
