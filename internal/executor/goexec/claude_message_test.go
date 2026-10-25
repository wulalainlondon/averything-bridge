package goexec

import (
	"encoding/json"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// TestUserMessageJSONAttachments pins the stream-json content-block order/shape
// (images → files → text) matching claude_cli.py.
func TestUserMessageJSONAttachments(t *testing.T) {
	raw := userMessageJSON("hello",
		[]backend.ImageAttachment{{Data: "AAAA", MediaType: "image/png"}},
		[]backend.FileAttachment{
			{Name: "a.go", Content: "package x", MediaType: "text/plain"},
			{Name: "doc.pdf", Content: "JVBER", MediaType: "application/pdf"},
		},
	)
	var f struct {
		Type    string `json:"type"`
		Message struct {
			Role    string           `json:"role"`
			Content []map[string]any `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if f.Type != "user" || f.Message.Role != "user" {
		t.Fatalf("frame wrong: %+v", f)
	}
	c := f.Message.Content
	if len(c) != 4 {
		t.Fatalf("want 4 blocks (image, txt, pdf, text), got %d: %v", len(c), c)
	}
	// image first
	if c[0]["type"] != "image" {
		t.Errorf("block0 = %v, want image", c[0]["type"])
	}
	if src, _ := c[0]["source"].(map[string]any); src["media_type"] != "image/png" || src["data"] != "AAAA" {
		t.Errorf("image source wrong: %v", c[0]["source"])
	}
	// text file fenced
	if c[1]["type"] != "text" || c[1]["text"] != "[File: a.go]\n```go\npackage x\n```" {
		t.Errorf("txt file block wrong: %q", c[1]["text"])
	}
	// pdf as document
	if c[2]["type"] != "document" {
		t.Errorf("pdf block = %v, want document", c[2]["type"])
	}
	// content text last
	if c[3]["type"] != "text" || c[3]["text"] != "hello" {
		t.Errorf("text block wrong: %v", c[3])
	}
}

// TestUserMessageJSONTextOnly: no attachments → single text block.
func TestUserMessageJSONTextOnly(t *testing.T) {
	raw := userMessageJSON("hi", nil, nil)
	var f struct {
		Message struct {
			Content []map[string]any `json:"content"`
		} `json:"message"`
	}
	_ = json.Unmarshal(raw, &f)
	if len(f.Message.Content) != 1 || f.Message.Content[0]["text"] != "hi" {
		t.Fatalf("text-only wrong: %v", f.Message.Content)
	}
}

func TestClaudeSpawnArgsSandboxAndPlanParity(t *testing.T) {
	base := session.Snapshot{Model: "claude-sonnet-4", Sandbox: "read-only", ResumeID: "uuid", Effort: "high"}
	args := claudeSpawnArgs(base, "")
	joined := strings.Join(args, " ")
	for _, want := range []string{
		"--allowedTools", "Read,Glob,Grep,WebSearch,WebFetch",
		"--tools Read,Glob,Grep,WebSearch,WebFetch", "--permission-mode dontAsk", "--strict-mcp-config",
		"--model claude-sonnet-4", "--resume uuid", "--effort high",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("read-only args missing %q: %v", want, args)
		}
	}
	if strings.Contains(joined, "--dangerously-skip-permissions") {
		t.Fatalf("read-only must not bypass tool permissions: %v", args)
	}

	ws := session.Snapshot{Model: "claude-sonnet-4", Sandbox: "workspace-write"}
	args = claudeSpawnArgs(ws, "")
	joined = strings.Join(args, " ")
	if !strings.Contains(joined, "--disallowedTools Bash") {
		t.Fatalf("workspace-write should disallow Bash: %v", args)
	}
	if strings.Contains(joined, "--dangerously-skip-permissions") || !strings.Contains(joined, "--permission-mode acceptEdits") {
		t.Fatalf("workspace-write must use explicit edit mode, not bypass: %v", args)
	}

	plan := session.Snapshot{Model: "opusplan", Sandbox: "danger-full-access"}
	args = claudeSpawnArgs(plan, "")
	joined = strings.Join(args, " ")
	if !strings.Contains(joined, "--model opus --permission-mode plan") || strings.Contains(joined, "--dangerously-skip-permissions") {
		t.Fatalf("opusplan args wrong: %v", args)
	}
}

func TestClaudeReadStdoutCapturesSystemInitSessionID(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	reg := session.NewRegistry()
	s := reg.Create("s1", "claude", "/tmp", "claude", "", "", "")
	p := &proc{reqID: "r1", tools: newToolNormalizer(sink, c)}

	c.readStdout(s, p, strings.NewReader(`{"type":"system","subtype":"init","session_id":"uuid-1"}`+"\n"))

	if got := s.ResumeID(); got != "uuid-1" {
		t.Fatalf("resume id not captured from system/init: %q", got)
	}
	if sink.count(func(e any) bool {
		ev, ok := e.(backend.SessionUUID)
		return ok && ev.ClaudeUUID == "uuid-1"
	}) != 1 {
		t.Fatalf("session_uuid event not emitted: %+v", sink.events)
	}
}

func TestClaudeReadStdoutResultErrorDoesNotEmitDone(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	reg := session.NewRegistry()
	s := reg.Create("s1", "claude", "/tmp", "claude", "", "", "")
	p := &proc{reqID: "r1", tools: newToolNormalizer(sink, c)}

	c.readStdout(s, p, strings.NewReader(`{"type":"result","subtype":"error","result":"boom"}`+"\n"))

	if sink.count(func(e any) bool {
		_, ok := e.(backend.Done)
		return ok
	}) != 0 {
		t.Fatalf("error result must not emit done: %+v", sink.events)
	}
	if sink.count(func(e any) bool {
		ev, ok := e.(backend.Error)
		return ok && ev.Code == backend.ErrTurn && strings.Contains(ev.Message, "boom")
	}) != 1 {
		t.Fatalf("turn error not emitted: %+v", sink.events)
	}
}

func TestClaudeCompactResultEmitsSessionCommandDone(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	reg := session.NewRegistry()
	s := reg.Create("s1", "claude", "/tmp", "claude", "", "", "")
	p := &proc{model: "claude-sonnet-4", tools: newToolNormalizer(sink, c)}
	p.beginTurn("compact_s1", true)

	c.readStdout(s, p, strings.NewReader(`{"type":"result","subtype":"success"}`+"\n"))

	if sink.count(func(e any) bool {
		ev, ok := e.(backend.SessionCommandDone)
		return ok && ev.RequestID == "compact_s1"
	}) != 1 {
		t.Fatalf("compact done command event missing: %+v", sink.events)
	}
}

func TestClaudeCompactResultErrorEmitsSessionCommandFailed(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	reg := session.NewRegistry()
	s := reg.Create("s1", "claude", "/tmp", "claude", "", "", "")
	p := &proc{model: "claude-sonnet-4", tools: newToolNormalizer(sink, c)}
	p.beginTurn("compact_s1", true)

	c.readStdout(s, p, strings.NewReader(`{"type":"result","subtype":"error","result":"compact boom"}`+"\n"))

	if sink.count(func(e any) bool {
		ev, ok := e.(backend.SessionCommandFailed)
		return ok && ev.RequestID == "compact_s1" && strings.Contains(ev.Message, "compact boom")
	}) != 1 {
		t.Fatalf("compact failed command event missing: %+v", sink.events)
	}
}

func TestClaudeReadStdoutRecordsContextUsage(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	reg := session.NewRegistry()
	s := reg.Create("s1", "claude", "/tmp", "claude", "", "", "")
	p := &proc{reqID: "r1", model: "claude-sonnet-4", tools: newToolNormalizer(sink, c)}

	c.readStdout(s, p, strings.NewReader(`{"type":"result","subtype":"success","usage":{"input_tokens":1200,"cache_creation_input_tokens":300}}`+"\n"))

	snap := s.Snapshot()
	if snap.ContextUsed != 1500 || snap.ContextMax != 200000 {
		t.Fatalf("context usage not recorded: %+v", snap)
	}
}

func TestClaudeAutoCompactThreshold(t *testing.T) {
	c := NewClaude(&capSink{}, "claude")
	if !c.shouldAutoCompact(160000, 200000) {
		t.Fatal("expected auto compact at 80 percent")
	}
	if c.shouldAutoCompact(159999, 200000) {
		t.Fatal("must not compact below threshold")
	}
	if got := claudeContextLimit("claude-sonnet-4-1000000"); got != 1_000_000 {
		t.Fatalf("1m context limit = %d", got)
	}
}
