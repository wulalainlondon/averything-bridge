package core

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	a4 "everything-go/contracts/task-api/artifacts-v4"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/delegation"
)

func TestFunctionalOneCaptureConfigVerifiesCurrentAndHistoricalOwners(t *testing.T) {
	h, p, caller, intent, root := extensionFormal(t)
	first := correctionSeal(t, h, root, false)
	next := artifactFollow(t, h, p, caller, root, root.ID, "synthetic-history-next", "current 中文 e\u0301 🧪")
	second := correctionSeal(t, h, next, false)
	path, cfg := runtimeCaptureConfig(t, h, p, intent.TaskID, second.Hash, true)
	cfg.Version = 2
	cfg.Verifier.SDK4Hash = captureHash(filepath.Join(filepath.Dir(cfg.Verifier.Script), "artifact-v4.mjs"))
	encoded, _ := json.Marshal(cfg)
	var selected map[string]any
	json.Unmarshal(encoded, &selected)
	selected["additional_task_hashes"] = map[string]any{intent.TaskID: []string{first.Hash}}
	encoded, _ = json.Marshal(selected)
	if err := os.WriteFile(path, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.ConfigureNativeResponseCapture(path); err != nil {
		t.Fatal("bounded current/history capture config refused", err)
	}
	beforeHash := captureHash(path)
	get, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "operation": "get", "correlation_id": "synthetic-current-public", "input": map[string]any{"task_id": intent.TaskID}})
	caller.CallID = "synthetic-current-public"
	current := h.ExecuteTask(context.Background(), caller, get)
	if !current.OK {
		t.Fatal(current.Error)
	}
	if err := h.CaptureTaskResponse(context.Background(), caller, get, current); err != nil {
		t.Fatal(err)
	}
	for i, entry := range []struct {
		r          delegation.Record
		seal, hash string
	}{{root, first.ID, first.Hash}, {next, second.ID, second.Hash}} {
		input := map[string]any{"task_id": intent.TaskID, "owner_request_id": entry.r.ChildRequestID, "seal_id": entry.seal, "artifact_id": "result.txt", "expected_hash": entry.hash, "offset": 0, "limit": 3}
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": a4.Version, "operation": "read_artifact_bytes", "correlation_id": fmt.Sprint("synthetic-history-", i), "input": input})
		caller.CallID = fmt.Sprint("synthetic-history-", i)
		response := h.ExecuteTask(context.Background(), caller, raw)
		if !response.OK {
			t.Fatal(response.Error)
		}
		if err := h.CaptureTaskResponse(context.Background(), caller, raw, response); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]string{}
	currentMetadata := false
	files, _ := os.ReadDir(h.nativeCapture.Load().Directory)
	for _, file := range files {
		raw, _ := os.ReadFile(filepath.Join(h.nativeCapture.Load().Directory, file.Name()))
		var record map[string]any
		json.Unmarshal(raw, &record)
		if record["evidence_kind"] == "original_response_owner_artifact_sdk_verifier" {
			seen[record["owner_request_id"].(string)] = record["full_hash"].(string)
		}
		if record["operation"] == "get" {
			public := record["public_result"].(map[string]any)
			currentMetadata = public["target"].(map[string]any)["request_id"] == next.ChildRequestID && public["continuity"].(map[string]any)["current_round_id"] == next.ID && public["result_anchor"].(map[string]any)["request_id"] == next.ChildRequestID
		}
	}
	if !currentMetadata || seen[root.ChildRequestID] != first.Hash || seen[next.ChildRequestID] != second.Hash || captureHash(path) != beforeHash {
		t.Fatal("current/history verification requires config replacement or borrowed owner", seen)
	}
}

func TestFunctionalCaptureHashSetCannotExpandTaskAuthority(t *testing.T) {
	for _, kind := range []string{"v1-extra", "foreign-task", "empty", "duplicate", "primary-duplicate", "too-many", "invalid-hash"} {
		t.Run(kind, func(t *testing.T) {
			h, p, _, intent, root := extensionFormal(t)
			seal := correctionSeal(t, h, root, false)
			path, cfg := runtimeCaptureConfig(t, h, p, intent.TaskID, seal.Hash, false)
			cfg.Version = 2
			encoded, _ := json.Marshal(cfg)
			var selected map[string]any
			json.Unmarshal(encoded, &selected)
			hash := fmt.Sprintf("%064x", 1)
			extra := map[string]any{intent.TaskID: []string{hash}}
			switch kind {
			case "v1-extra":
				selected["version"] = 1
			case "foreign-task":
				extra = map[string]any{"foreign": []string{hash}}
			case "empty":
				extra[intent.TaskID] = []string{}
			case "duplicate":
				extra[intent.TaskID] = []string{hash, hash}
			case "primary-duplicate":
				extra[intent.TaskID] = []string{seal.Hash}
			case "too-many":
				extra[intent.TaskID] = []string{hash, fmt.Sprintf("%064x", 2), fmt.Sprintf("%064x", 3), fmt.Sprintf("%064x", 4), fmt.Sprintf("%064x", 5)}
			case "invalid-hash":
				extra[intent.TaskID] = []string{"unknown"}
			}
			selected["additional_task_hashes"] = extra
			encoded, _ = json.Marshal(selected)
			os.WriteFile(path, encoded, 0600)
			if err := h.ConfigureNativeResponseCapture(path); err == nil || h.nativeCapture.Load() != nil {
				t.Fatal("invalid hash set enabled capture", kind)
			}
		})
	}
}
