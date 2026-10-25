package goexec

import (
	"os"
	"path/filepath"
	"testing"

	"everything-go/internal/history"
)

func TestClaudeReadResultMarkerUsesNativeEndTurnNotFreshProgress(t *testing.T) {
	path := filepath.Join(t.TempDir(), "native.jsonl")
	rows := `{"type":"assistant","timestamp":"2026-10-01T10:00:01Z","message":{"stop_reason":"tool_use","content":[{"type":"text","text":"checking files"}]}}
{"type":"assistant","timestamp":"2026-10-01T10:00:02Z","message":{"stop_reason":"tool_use","content":[{"type":"thinking","thinking":"still working"}]}}
{"type":"assistant","timestamp":"2026-10-01T10:00:03Z","message":{"stop_reason":"end_turn","content":[{"type":"text","text":"completed answer"}]}}
`
	if err := os.WriteFile(path, []byte(rows), 0600); err != nil {
		t.Fatal(err)
	}
	messages, _ := loadClaudeHistoryMessages(path, "native", 0)
	if len(messages) != 3 {
		t.Fatalf("canonical messages changed: %d", len(messages))
	}
	for index, want := range []bool{false, false, true} {
		if messages[index]["history_read_result_verified"] != want {
			t.Fatalf("row %d marker=%v", index, messages[index]["history_read_result_verified"])
		}
	}
}

func TestCodexReadResultMarkerRequiresNativeFinalAnswer(t *testing.T) {
	lines := []history.TailLine{
		{LineNo: 1, Data: []byte(`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":"checking"}}`)},
		{LineNo: 2, Data: []byte(`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":"completed answer"}}`)},
		{LineNo: 3, Data: []byte(`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":"next result still flushing"}}`)},
	}
	messages, _, err := parseCodexHistoryLines(lines, "native", false)
	if err != nil || len(messages) != 2 {
		t.Fatalf("history %v %+v", err, messages)
	}
	if messages[0]["history_read_result_verified"] != true || messages[1]["history_read_result_verified"] != false {
		t.Fatalf("completion marker %+v", messages)
	}
	if messages[0]["source_message_id"] != "codex:native:line:2" {
		t.Fatal("native source identity changed")
	}
}
