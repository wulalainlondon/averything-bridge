package artifactcontract

import (
	"encoding/json"
	old "everything-go/contracts/task-api/artifacts-v3"
	base "everything-go/contracts/task-api/v1"
	"testing"
)

func TestOwnerSelectorClosedVersionedNativeTools(t *testing.T) {
	tools, e := CompatibleToolInputs()
	if e != nil {
		t.Fatal(e)
	}
	for _, version := range []string{old.Version, Version} {
		input := map[string]any{"task_id": "owned", "seal_id": "seal", "artifact_id": "result.txt", "expected_hash": "0000000000000000000000000000000000000000000000000000000000000000", "offset": 0, "limit": 3}
		if version == Version {
			input["owner_request_id"] = "owner"
		}
		req := map[string]any{"type": "task_api_request", "api_version": version, "operation": "read_artifact_bytes", "correlation_id": "native", "input": input}
		raw, _ := json.Marshal(req)
		schema, _ := json.Marshal(tools["task_read_artifact_bytes"])
		if e := base.Validate2020(schema, raw); e != nil {
			t.Fatal(version, e)
		}
		if version == Version {
			delete(input, "owner_request_id")
		} else {
			input["owner_request_id"] = "must-not-alias"
		}
		raw, _ = json.Marshal(req)
		if e := base.Validate2020(schema, raw); e == nil {
			t.Fatal("closed version selector bypass", version)
		}
	}
}
