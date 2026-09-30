package goexec

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func TestPMArgsRemoveExecutionAndInheritedCapabilities(t *testing.T) {
	args := claudePMSpawnArgs(session.Snapshot{Model: "sonnet", ResumeID: "existing"}, backend.PMConfiguration{Instructions: "PM only", MCPURL: "http://127.0.0.1:123/mcp/pm1", Token: "secret"})
	get := func(key string) string {
		idx := slices.Index(args, key)
		if idx < 0 || idx+1 >= len(args) {
			t.Fatal("missing", key)
		}
		return args[idx+1]
	}
	if get("--tools") != "" || get("--permission-mode") != "dontAsk" || get("--system-prompt") != "PM only" || get("--resume") != "existing" {
		t.Fatal(args)
	}
	for _, required := range []string{"--strict-mcp-config", "--disable-slash-commands", "--setting-sources", "--max-turns"} {
		if !slices.Contains(args, required) {
			t.Fatal(required)
		}
	}
	if slices.Contains(args, "--dangerously-skip-permissions") {
		t.Fatal("PM bypasses permissions")
	}
	if strings.Contains(strings.Join(args, " "), "secret") {
		t.Fatal("PM bearer credential exposed in argv")
	}
	var cfg map[string]map[string]any
	if err := json.Unmarshal([]byte(get("--mcp-config")), &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg["mcpServers"]) != 1 || cfg["mcpServers"]["bridge_pm"] == nil {
		t.Fatal(cfg)
	}
	if validPMTools([]string{"Bash"}) || validPMTools([]string{"Edit"}) || validPMTools([]string{"Agent"}) || validPMTools([]string{"mcp__other__run"}) {
		t.Fatal("unexpected tools allowed")
	}
	if !validPMTools([]string{"mcp__bridge_pm__project_get_context"}) {
		t.Fatal("PM tool rejected")
	}
}

type pmTestSink struct{ events []any }

func (s *pmTestSink) Emit(event any) { s.events = append(s.events, event) }
func TestClaudeAPIErrorIsNotSuccessfulCompletion(t *testing.T) {
	sink := &pmTestSink{}
	c := &Claude{sink: sink}
	p := &proc{pmSession: true, reqID: "request", cancel: func() {}}
	reg := session.NewRegistry()
	s := reg.Create("pm_test", "PM", "/tmp", "claude", "", "read-only", "")
	c.readStdout(s, p, strings.NewReader("{\"type\":\"assistant\",\"isApiErrorMessage\":true,\"error\":\"authentication_failed\",\"message\":{\"content\":[]}}\n{\"type\":\"result\",\"subtype\":\"success\"}\n"))
	if len(sink.events) != 1 {
		t.Fatalf("events=%+v", sink.events)
	}
	e, ok := sink.events[0].(protocol.Error)
	if !ok || e.RequestID != "request" || e.Code != "pm_provider_authentication_failed" {
		t.Fatal(sink.events)
	}
}

func TestReadOnlyCollaborationWorkerRemovesBuiltins(t *testing.T) {
	args := claudeReadOnlyWorkerArgs(session.Snapshot{Sandbox: "read-only"}, "http://127.0.0.1/mcp/worker")
	i := slices.Index(args, "--tools")
	if i < 0 || args[i+1] != "Read,Glob,Grep" {
		t.Fatal("write tools not removed", args)
	}
	if !slices.Contains(args, "--strict-mcp-config") || !slices.Contains(args, "--disable-slash-commands") {
		t.Fatal("inherited tools remain")
	}
}
