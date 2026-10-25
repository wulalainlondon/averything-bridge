package goexec

import (
	"encoding/json"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"testing"
)

func TestCodexNativeMessageIdentitySurvivesLiveAndHistory(t *testing.T) {
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "session", t.TempDir(), "codex", "", "", "root")
	st := c.state(s.ID)
	st.threadID = "root"
	st.reqID = "r1"
	c.threadToSession["root"] = s
	c.dispatch(json.RawMessage(`{"method":"item/agentMessage/delta","params":{"threadId":"root","itemId":"msg-progress","delta":"QA progress","phase":"commentary"}}`))
	if sink.count(func(e any) bool {
		x, ok := e.(protocol.TextChunk)
		return ok && x.SourceItemID == "msg-progress" && x.Content == "QA progress"
	}) != 1 {
		t.Fatal("live native identity lost")
	}
	raw := []string{
		`{"type":"turn_context","payload":{"turn_id":"turn"}}`,
		`{"type":"response_item","payload":{"type":"message","id":"msg-progress","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"QA progress"}]}}`,
		`{"type":"response_item","payload":{"type":"message","id":"msg-repeat","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"QA progress"}]}}`,
		`{"type":"response_item","payload":{"type":"custom_tool_call","call_id":"outer","name":"exec","input":"nested commands"}}`,
	}
	lines := make([]history.TailLine, len(raw))
	for i, line := range raw {
		lines[i] = history.TailLine{LineNo: i + 1, Data: []byte(line)}
	}
	messages, _, err := parseCodexHistoryWindow(lines, "root", false, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages=%d", len(messages))
	}
	blocks := messages[0]["blocks"].([]map[string]any)
	if len(blocks) != 3 || blocks[0]["source_item_id"] != "msg-progress" || blocks[1]["source_item_id"] != "msg-repeat" {
		t.Fatalf("distinct native paragraphs lost: %+v", blocks)
	}
}
