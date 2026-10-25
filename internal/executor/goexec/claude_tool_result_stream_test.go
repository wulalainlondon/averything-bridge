package goexec

import (
	"os"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func TestClaude291RealReadToolStreamCompletesCardAndReportsContext(t *testing.T) {
	raw, err := os.ReadFile("testdata/claude-2.1.291-read-tool.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	s := session.NewRegistry().Create("qa", "QA", t.TempDir(), "claude", "sonnet", "read-only", "")
	p := &proc{reqID: "r_exact", tools: newToolNormalizer(sink, c)}
	c.readStdout(s, p, strings.NewReader(string(raw)))
	var started, ended int
	for _, event := range sink.events {
		switch e := event.(type) {
		case backend.ToolStart:
			started++
		case backend.ToolResult:
			if e.RequestID != "r_exact" || !strings.Contains(e.Output, "return a + b") {
				t.Fatalf("Read output missing/correlation lost")
			}
		case backend.ToolEnd:
			ended++
		}
	}
	if started != 1 || ended != 1 || s.Snapshot().ContextMax != 1000000 {
		t.Fatalf("real CLI lifecycle/context mismatch: start=%d end=%d context=%d", started, ended, s.Snapshot().ContextMax)
	}
}

func TestClaudeUserToolResultEndsVisibleToolBeforeDone(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	s := session.NewRegistry().Create("qa", "QA", t.TempDir(), "claude", "sonnet", "read-only", "")
	p := &proc{reqID: "r_exact", tools: newToolNormalizer(sink, c)}
	c.readStdout(s, p, strings.NewReader(`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"read-1","name":"Read","input":{"file_path":"add.py"}}]}}
{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"read-1","content":[{"type":"text","text":"def add(a,b): return a+b"}]}]}}
{"type":"result","subtype":"success","session_id":"native","result":"done"}
`))
	var order []string
	for _, event := range sink.events {
		switch e := event.(type) {
		case backend.ToolStart:
			order = append(order, "start")
		case backend.ToolResult:
			if e.RequestID != "r_exact" || e.ToolUseID != "read-1" || !strings.Contains(e.Output, "return a+b") {
				t.Fatalf("uncorrelated tool result: %+v", e)
			}
			order = append(order, "result")
		case backend.ToolEnd:
			order = append(order, "end")
		case backend.Done:
			order = append(order, "done")
		}
	}
	if strings.Join(order, ",") != "start,result,end,done" {
		t.Fatalf("visible tool lifecycle incomplete: %v", order)
	}
}

func TestClaudeUserToolResultSidechainAndReplayedUserTextAreNotVisibleResults(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	s := session.NewRegistry().Create("qa", "QA", t.TempDir(), "claude", "sonnet", "read-only", "")
	p := &proc{reqID: "r_exact", tools: newToolNormalizer(sink, c)}
	c.readStdout(s, p, strings.NewReader(`{"type":"user","message":{"content":[{"type":"text","text":"user replay"}]}}
{"type":"user","parent_tool_use_id":"agent-1","message":{"content":[{"type":"tool_result","tool_use_id":"sidechain","content":"private agent output"}]}}
`))
	if len(sink.events) != 0 {
		t.Fatalf("non-root/replayed input emitted as output: %+v", sink.events)
	}
}

func TestClaudeResultContextCountsCachedInputAndUsesReportedWindow(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	s := session.NewRegistry().Create("qa", "QA", t.TempDir(), "claude", "sonnet", "read-only", "")
	p := &proc{reqID: "r_exact", model: "claude-sonnet-5-5", tools: newToolNormalizer(sink, c)}
	c.readStdout(s, p, strings.NewReader(`{"type":"result","subtype":"success","result":"done","usage":{"input_tokens":3,"cache_creation_input_tokens":20,"cache_read_input_tokens":40000},"modelUsage":{"claude-sonnet-5-5":{"contextWindow":1000000}}}`+"\n"))
	snap := s.Snapshot()
	if snap.ContextUsed != 40023 || snap.ContextMax != 1000000 {
		t.Fatalf("cached context/window lost: used=%d max=%d", snap.ContextUsed, snap.ContextMax)
	}
}

func TestClaudeContextUsesLatestMainStepRatherThanAccumulatedToolSteps(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	s := session.NewRegistry().Create("qa", "QA", t.TempDir(), "claude", "sonnet", "read-only", "")
	p := &proc{reqID: "r_exact", model: "claude-sonnet-5-5", tools: newToolNormalizer(sink, c)}
	c.readStdout(s, p, strings.NewReader(`{"type":"assistant","message":{"content":[],"usage":{"input_tokens":2,"cache_creation_input_tokens":10,"cache_read_input_tokens":1000}}}
{"type":"assistant","parent_tool_use_id":"agent","message":{"content":[],"usage":{"input_tokens":99999}}}
{"type":"assistant","message":{"content":[],"usage":{"input_tokens":3,"cache_creation_input_tokens":20,"cache_read_input_tokens":40000}}}
{"type":"result","subtype":"success","result":"done","usage":{"input_tokens":5,"cache_creation_input_tokens":30,"cache_read_input_tokens":41000},"modelUsage":{"claude-sonnet-5-5":{"contextWindow":1000000}}}
`))
	if got := s.Snapshot().ContextUsed; got != 40023 {
		t.Fatalf("context counted sidechain or cumulative steps: %d", got)
	}
}

func TestClaudeSpawnRequiresExplicitFullAccessAndCarriesNativeName(t *testing.T) {
	for _, sandbox := range []string{"", "read-only", "workspace-write"} {
		args := strings.Join(claudeSpawnArgs(session.Snapshot{Sandbox: sandbox, Name: "Remote QA"}, ""), " ")
		if strings.Contains(args, "--dangerously-skip-permissions") || !strings.Contains(args, "--name Remote QA") {
			t.Fatalf("unsafe default or missing name: sandbox=%q args=%s", sandbox, args)
		}
	}
	args := strings.Join(claudeSpawnArgs(session.Snapshot{Sandbox: "danger-full-access"}, ""), " ")
	if !strings.Contains(args, "--dangerously-skip-permissions") {
		t.Fatal("explicit full access policy lost")
	}
}

func TestClaudeReadOnlyFableKeepsPolicyAndAllowsOnlyBridgeQuestionBroker(t *testing.T) {
	args := claudeSpawnArgs(session.Snapshot{Sandbox: "read-only", Model: "fable"}, "http://127.0.0.1/mcp/qa")
	joined := strings.Join(args, " ")
	for _, want := range []string{"--model claude-fable-5", "--permission-mode dontAsk", "--strict-mcp-config", "--tools Read,Glob,Grep,WebSearch,WebFetch", "--allowedTools Read,Glob,Grep,WebSearch,WebFetch,mcp__ask_user__ask_question"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("model alias dropped read-only/question policy: missing %s", want)
		}
	}
	if strings.Contains(joined, "--dangerously-skip-permissions") {
		t.Fatal("Fable alias bypassed the sandbox policy")
	}
}

func TestClaudeOversizedStreamFrameFailsExactRequestWithoutDoneOrReplay(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	s := session.NewRegistry().Create("qa", "QA", t.TempDir(), "claude", "sonnet", "read-only", "")
	cancelled := false
	p := &proc{reqID: "r_oversize", tools: newToolNormalizer(sink, c), cancel: func() { cancelled = true }}
	c.readStdout(s, p, strings.NewReader(strings.Repeat("x", maxLine+1)+"\n"))
	if !cancelled || p.currentReqID() != "" {
		t.Fatal("unreadable stream left request/process active")
	}
	var failures int
	for _, event := range sink.events {
		switch e := event.(type) {
		case backend.Error:
			if e.RequestID == "r_oversize" && e.Code == "claude_stream_read_failed" {
				failures++
			}
		case backend.Done, backend.CompletedAnswer:
			t.Fatal("unreadable stream was reported as complete")
		}
	}
	if failures != 1 {
		t.Fatal("missing exact correlated stream failure")
	}
}

func TestClaudeMissingUsageDoesNotPublishGuessedZero(t *testing.T) {
	sink := &capSink{}
	c := NewClaude(sink, "claude")
	s := session.NewRegistry().Create("qa", "QA", t.TempDir(), "claude", "sonnet", "read-only", "")
	p := &proc{reqID: "r_exact", model: "sonnet", tools: newToolNormalizer(sink, c)}
	c.readStdout(s, p, strings.NewReader(`{"type":"result","subtype":"success","result":"done"}`+"\n"))
	if s.Snapshot().ContextUsedKnown {
		t.Fatal("missing provider usage became known zero")
	}
}
