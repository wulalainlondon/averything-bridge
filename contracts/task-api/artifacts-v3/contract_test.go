package artifactcontract

import (
	"encoding/json"
	"testing"
)

func TestArtifactClosedVersionAndNoReportedPaths(t *testing.T) {
	request := map[string]any{"type": "task_api_request", "api_version": Version, "correlation_id": "synthetic", "operation": "read_artifact_bytes", "input": map[string]any{"task_id": "owned", "seal_id": "seal", "artifact_id": "result.txt", "expected_hash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "offset": 0, "limit": 3}}
	raw, _ := json.Marshal(request)
	if err := Validate(raw, "Request"); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"1.0.0-rc.1", "1.0.0-rc.2"} {
		copy := map[string]any{}
		for key, value := range request {
			copy[key] = value
		}
		copy["api_version"] = version
		raw, _ = json.Marshal(copy)
		if Validate(raw, "Request") == nil {
			t.Fatal("silent version alias")
		}
	}
	input := request["input"].(map[string]any)
	input["path"] = "/arbitrary"
	raw, _ = json.Marshal(request)
	if Validate(raw, "Request") == nil {
		t.Fatal("path accepted")
	}
	delete(input, "path")
	input["limit"] = 32769
	raw, _ = json.Marshal(request)
	if Validate(raw, "Request") == nil {
		t.Fatal("unbounded range")
	}
	if Validate([]byte(`{"type":"task_api_request","api_version":"1.0.0-rc.3","correlation_id":"a","operation":"artifact_capabilities","input":{},"input":{}}`), "Request") == nil {
		t.Fatal("duplicate body accepted")
	}
	tools, err := ToolInputs()
	if err != nil || len(tools) != 2 {
		t.Fatal("unexpected artifact tools", err)
	}
}
