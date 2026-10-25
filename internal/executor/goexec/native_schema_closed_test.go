package goexec

import (
	"encoding/json"
	a3 "everything-go/contracts/task-api/artifacts-v3"
	a4 "everything-go/contracts/task-api/artifacts-v4"
	base "everything-go/contracts/task-api/v1"
	"testing"
)

func TestNativeAdaptedArtifactBranchesRemainClosed(t *testing.T) {
	defs, e := a4.CompatibleToolInputs()
	if e != nil {
		t.Fatal(e)
	}
	for _, op := range []string{"artifact_capabilities", "read_artifact_bytes"} {
		schema, e := mcpTaskInputSchema(defs["task_"+op])
		if e != nil {
			t.Fatal(e)
		}
		encoded, _ := json.Marshal(schema)
		for _, version := range []string{a3.Version, a4.Version} {
			input := map[string]any{}
			if op == "read_artifact_bytes" {
				input = map[string]any{"task_id": "owned", "seal_id": "seal", "artifact_id": "result.txt", "expected_hash": "0000000000000000000000000000000000000000000000000000000000000000", "offset": 0, "limit": 3}
				if version == a4.Version {
					input["owner_request_id"] = "owner"
				}
			}
			request := map[string]any{"type": "task_api_request", "api_version": version, "operation": op, "correlation_id": "actual-adapter", "input": input}
			check := func(want bool) {
				raw, _ := json.Marshal(request)
				e := base.Validate2020(encoded, raw)
				if (e == nil) != want {
					t.Fatal(op, version, want, e)
				}
			}
			check(true)
			input["foreign_field"] = true
			check(false)
			delete(input, "foreign_field")
			if op == "read_artifact_bytes" {
				if version == a4.Version {
					delete(input, "owner_request_id")
				} else {
					input["owner_request_id"] = "unsupported-extra"
				}
				check(false)
			}
			request["api_version"] = "1.0.0-rc.2"
			check(false)
		}
	}
}
