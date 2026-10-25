package core

import (
	"context"
	"database/sql"
	"encoding/json"
	contract "everything-go/contracts/task-api/v1"
	"everything-go/internal/taskapi"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestFormalContinuationEventsKeepCurrentRequestIdentity(t *testing.T) {
	h, p, c, intent, root := extensionFormal(t)
	ctx := context.Background()
	current := artifactFollow(t, h, p, c, root, root.ID, "synthetic-event-round2", "round2 final")
	seal := correctionSeal(t, h, current, false)
	cfgPath, cfg := runtimeCaptureConfig(t, h, p, intent.TaskID, seal.Hash, false)
	cfg.Version = 3
	cfg.Operations = append(cfg.Operations, "events")
	cfgBytes, _ := json.Marshal(cfg)
	if err := os.WriteFile(cfgPath, cfgBytes, 0600); err != nil {
		t.Fatal(err)
	}
	if err := h.ConfigureNativeResponseCapture(cfgPath); err != nil {
		t.Fatal(err)
	}
	// Add uniquely identifiable invalidations in the original durable journal.
	// They are synthetic receipts, not model call evidence.
	journal := h.messageQueue.TaskJournal()
	if err := journal.Transaction(ctx, func(tx *sql.Tx) error {
		return taskapi.ChangeTx(tx, current.ChildSessionID, current.ChildRequestID, "native_tool_invoked")
	}); err != nil {
		t.Fatal(err)
	}
	changes, _, err := journal.Changes(ctx, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	want := map[uint64]bool{}
	for _, change := range changes {
		if change.SessionID == current.ChildSessionID && change.RequestID == current.ChildRequestID {
			want[change.Sequence] = true
		}
	}
	if len(want) == 0 {
		t.Fatal("fixture missing current-round changes")
	}
	cursor := ""
	seen := map[uint64]bool{}
	closed := false
	for page := 0; page < 100; page++ {
		input := map[string]any{"session_id": current.ChildSessionID, "path": "delegation", "limit": 2, "subscribe": false}
		if cursor != "" {
			input["cursor"] = cursor
		}
		callID := fmt.Sprint("synthetic-current-events-", page)
		raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": contract.Version, "operation": "events", "correlation_id": callID, "input": input})
		c.CallID = callID
		response := h.ExecuteTask(ctx, c, raw)
		if !response.OK {
			t.Fatal(response.Error)
		}
		if err := h.CaptureTaskResponse(ctx, c, raw, response); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(response.Result)
		var result map[string]any
		json.Unmarshal(b, &result)
		for _, value := range result["events"].([]any) {
			event := value.(map[string]any)
			ref := event["task_ref"].(map[string]any)
			if ref["native_record_id"] != root.ID {
				t.Fatal("foreign task mapping", event, intent.TaskID)
			}
			mark := event["store_sequence"].(map[string]any)
			if mark["store"] == "ordinary" {
				seen[uint64(mark["sequence"].(float64))] = true
			}
		}
		next := result["next_cursor"].(string)
		if result["has_more"] == false {
			closed = true
			break
		}
		if next == cursor {
			t.Fatal("cursor did not progress")
		}
		cursor = next
	}
	if !closed {
		t.Fatal("events did not reach end")
	}
	for sequence := range want {
		if !seen[sequence] {
			t.Fatalf("current round request %s event %d missing from logical task stream", current.ChildRequestID, sequence)
		}
	}
	files, err := os.ReadDir(h.nativeCapture.Load().Directory)
	if err != nil || len(files) == 0 {
		t.Fatal("missing original public event pages", err)
	}
	for _, file := range files {
		data, err := os.ReadFile(filepath.Join(h.nativeCapture.Load().Directory, file.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var record map[string]any
		json.Unmarshal(data, &record)
		body := record["public_result"].(map[string]any)
		if record["operation"] != "events" || body["requested_session_id"] != current.ChildSessionID || body["request_cursor_sha256"] == nil || body["next_cursor"] == nil {
			t.Fatal("event page lost original request/cursor binding", record)
		}
	}
}
