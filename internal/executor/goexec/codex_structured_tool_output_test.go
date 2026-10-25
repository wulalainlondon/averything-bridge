package goexec

import (
	"encoding/json"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexStructuredToolOutputTextParts(t *testing.T) {
	cases := []struct{ name, output, want string }{
		{"legacy-string", `"unchanged\n負值:-3 null"`, "unchanged\n負值:-3 null"},
		{"typed-text-parts", `[{"type":"input_text","text":"first\n"},{"type":"input_text","text":"第二份 -3 null"}]`, "first\n\n第二份 -3 null"},
		{"output-text", `[{"type":"output_text","text":"complete"}]`, "complete"},
		{"empty-parts", `[]`, ""},
		{"null", `null`, ""},
		{"unknown-structured-content", `[{"type":"input_text","text":"kept"},{"type":"future_part","value":-3}]`, `[{"type":"input_text","text":"kept"},{"type":"future_part","value":-3}]`},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			p := json.RawMessage(`{"type":"custom_tool_call_output","call_id":"original","output":` + tt.output + `}`)
			id, got, ok := codexResponseToolOutput(p)
			if !ok || id != "original" || got != tt.want {
				t.Fatalf("original output: id=%q ok=%v got=%q want=%q", id, ok, got, tt.want)
			}
		})
	}
}

func TestCodexCompletedCommandExecutionAggregatedOutput(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	s := session.NewRegistry().Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "root"
	st.reqID = "original-r"
	c.threadToSession["root"] = s
	params := map[string]any{"threadId": "root", "item": map[string]any{"id": "native-command", "type": "commandExecution", "command": "cat synthetic.txt", "aggregatedOutput": "all original rows\n負值:-3 null\n", "status": "completed"}}
	for _, method := range []string{"item/started", "item/completed"} {
		raw, _ := json.Marshal(map[string]any{"method": method, "params": params})
		c.dispatch(raw)
	}
	results := 0
	ended := false
	for _, e := range sink.events {
		switch v := e.(type) {
		case protocol.ToolResult:
			results++
			if v.ToolUseID != "native-command" || v.RequestID != "original-r" || v.Output != "all original rows\n負值:-3 null\n" {
				t.Fatalf("wrong original tool result: %+v", v)
			}
		case protocol.ToolEnd:
			ended = true
		}
	}
	if results != 1 || !ended {
		t.Fatalf("want one full result before end, got results=%d ended=%v", results, ended)
	}
}

func TestCodexHistoryStructuredToolOutputRetained(t *testing.T) {
	root := t.TempDir()
	p := filepath.Join(root, "synthetic.jsonl")
	records := []map[string]any{
		{"type": "response_item", "payload": map[string]any{"type": "custom_tool_call", "name": "exec", "call_id": "outer-original", "input": "read only synthetic"}},
		{"type": "response_item", "payload": map[string]any{"type": "custom_tool_call_output", "call_id": "outer-original", "output": []map[string]any{{"type": "input_text", "text": "first original row"}, {"type": "input_text", "text": "第二列 -3 null"}}}},
	}
	writeJSONL(t, p, records)
	c := NewCodex(&capSink{}, "codex")
	messages, _, err := c.readCodexHistory(p, "original-thread")
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages=%d", len(messages))
	}
	blocks := messages[0]["blocks"].([]map[string]any)
	if got := blocks[0]["output"]; got != "first original row\n第二列 -3 null" {
		t.Fatalf("history output lost: %v", got)
	}
}

func TestS10OriginalB13HistoryOutputPreview(t *testing.T) {
	p := os.Getenv("S10_B13_NATIVE_HISTORY")
	dest := os.Getenv("S10_B13_HISTORY_PREVIEW")
	if p == "" || dest == "" {
		t.Skip("scoped original S10 synthetic native input not supplied")
	}
	if !strings.Contains(filepath.Base(p), "01a14d7b-bc34-7252-bfed-052fa6c45515") {
		t.Fatal("only original ownB native file allowed")
	}
	c := NewCodex(&capSink{}, "codex")
	messages, _, err := c.readCodexHistory(p, "01a14d7b-bc34-7252-bfed-052fa6c45515")
	if err != nil {
		t.Fatal(err)
	}
	type result struct{ ID, Output string }
	found := []result{}
	for _, m := range messages {
		blocks, _ := m["blocks"].([]map[string]any)
		for _, b := range blocks {
			cmd, _ := b["command"].(string)
			if !strings.Contains(cmd, "own-B13-tool-inputs334/") {
				continue
			}
			output, _ := b["output"].(string)
			if strings.Count(output, " row=") != 50 {
				t.Fatalf("actual B13 missing rows: id=%v output bytes=%d", b["tool_use_id"], len(output))
			}
			id, _ := b["tool_use_id"].(string)
			found = append(found, result{id, output})
		}
	}
	if len(found) != 3 {
		t.Fatalf("want actual3outer wrappers, got%d", len(found))
	}
	raw, _ := json.MarshalIndent(found, "", "  ")
	if err := os.WriteFile(dest, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCodexStructuredOutputDoesNotReuseOldEmptyCache(t *testing.T) {
	root := t.TempDir()
	thread := "00000000-0000-0000-0000-000000000134"
	p := filepath.Join(root, "rollout-2026-10-19T00-00-00-"+thread+".jsonl")
	records := []map[string]any{
		{"type": "response_item", "payload": map[string]any{"type": "custom_tool_call", "name": "exec", "call_id": "cached-original", "input": "synthetic only"}},
		{"type": "response_item", "payload": map[string]any{"type": "custom_tool_call_output", "call_id": "cached-original", "output": []map[string]any{{"type": "input_text", "text": "retained original output"}}}},
	}
	writeJSONL(t, p, records)
	fi, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	key := history.FileKey{Path: p, MtimeNS: fi.ModTime().UnixNano(), Size: fi.Size()}
	history.DefaultCache().Save("codex:turns-v3:"+thread, key, []map[string]any{{"role": "assistant", "content": "stale empty tool", "blocks": []map[string]any{{"type": "tool_call", "tool_use_id": "cached-original", "name": "exec", "command": "synthetic only", "output": ""}}}})
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	got, err := c.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 20})
	if err != nil || len(got.Messages) != 1 {
		t.Fatalf("new history: result=%+v err=%v", got, err)
	}
	blocks := got.Messages[0]["blocks"].([]map[string]any)
	if blocks[0]["output"] != "retained original output" {
		t.Fatalf("reused pre-fix empty cache: %+v", blocks)
	}
}
