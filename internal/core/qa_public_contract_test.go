package core

import (
	"context"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"fmt"
	"testing"
)

// Isolated native-entry/store fixtures, never a claim about live model calls.
func TestPublishedCurrentQAReadableThroughPublicContract(t *testing.T) {
	for _, verdict := range []string{"PASS", "FAIL"} {
		t.Run(verdict, func(t *testing.T) {
			h, p, c, intent, root := extensionFormal(t)
			ctx := context.Background()
			owner := artifactFollow(t, h, p, c, root, root.ID, "synthetic-reviewed-owner-round2", "same canonical e\u0301 🧪")
			invoke := func(op string, input any, key string) taskapi.Response {
				t.Helper()
				request := map[string]any{"type": "task_api_request", "api_version": contract.Version, "correlation_id": "synthetic-qa-contract-" + op, "operation": op, "input": input}
				if key != "" {
					request["idempotency_key"] = key
				}
				raw, _ := json.Marshal(request)
				c.CallID = "synthetic-qa-contract-" + op
				response := h.ExecuteTask(ctx, c, raw)
				if !response.OK {
					t.Fatalf("public %s after %s review: %+v", op, verdict, response.Error)
				}
				return response
			}
			f := h.exec.(*extensionExec)
			f.final = fmt.Sprintf(`{"owner_round_id":%q,"verdict":%q}`, owner.ID, verdict)
			originalSend := f.onSend
			f.onSend = func(s *session.Session, req, content string) {
				if s.ResumeID() == "" {
					s.SetResumeID("synthetic-independent-qa-" + s.ID)
				}
				originalSend(s, req, content)
			}
			var metadata apiMetadata
			json.Unmarshal(intent.Metadata, &metadata)
			var input map[string]any
			json.Unmarshal(metadata.Request.Input, &input)
			input["instruction"] = "independently compare synthetic owner"
			created := invoke("create_dispatch", input, "synthetic-distinct-reviewer")
			qaIntent, err := h.delegations.TaskJournal().Task(ctx, "session:"+p.ID, 0, created.Result.(map[string]any)["task_id"].(string))
			if err != nil {
				t.Fatal(err)
			}
			qa := continuityWait(t, h, qaIntent.NativeID, "terminal")
			invoke("append", map[string]any{"continuity": map[string]any{"action": "bind_qa", "arguments": map[string]any{"owner_delegation_id": root.ID, "qa_delegation_id": qa.ID}}}, "synthetic-bind-reviewer")
			invoke("append", map[string]any{"continuity": map[string]any{"action": "record_review", "arguments": map[string]any{"owner_round_id": owner.ID, "qa_round_id": qa.ID}}}, "synthetic-publish-review")
			check := func(task map[string]any) {
				t.Helper()
				expected := "passed"
				if verdict == "FAIL" {
					expected = "failed"
				}
				encoded, _ := json.Marshal(task)
				var public map[string]any
				json.Unmarshal(encoded, &public)
				if public["target"].(map[string]any)["request_id"] != owner.ChildRequestID || public["qa"].(map[string]any)["status"] != expected {
					t.Fatal("wrong owner/current verdict", public)
				}
				if verdict == "PASS" {
					criteria, ok := public["qa"].(map[string]any)["criterion_evidence"].([]any)
					if !ok || len(criteria) != 0 {
						t.Fatal("must not invent criterion-level assertions", public["qa"])
					}
				}
			}
			check(invoke("get", map[string]any{"task_id": intent.TaskID}, "").Result.(map[string]any))
			for _, op := range []string{"list", "snapshot"} {
				in := map[string]any{"session_id": p.ID, "limit": 100}
				if op == "list" {
					in["path"] = "delegation"
				} else {
					in["views"] = []string{"source_children", "results"}
				}
					page := invoke(op, in, "").Result
				encoded, _ := json.Marshal(page)
				var parsed map[string]any
				json.Unmarshal(encoded, &parsed)
				found := false
				for _, item := range parsed["items"].([]any) {
					task := item.(map[string]any)
					if task["task_id"] == intent.TaskID {
						check(task)
						found = true
					}
				}
				if !found {
					t.Fatal(op, "lost reviewed task")
				}
			}
		})
	}
}
