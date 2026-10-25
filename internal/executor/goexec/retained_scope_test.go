package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type retainedScopeFixture struct {
	scope    taskapi.ChildScope
	badTools bool
}

func TestExtensionCodexRetainedResumeProbeFailClosed(t *testing.T) {
	for _, wrong := range []bool{false, true} {
		t.Run(fmt.Sprint("wrong-thread-", wrong), func(t *testing.T) {
			root := t.TempDir()
			p := &retainedScopeFixture{scope: taskapi.ChildScope{Roots: []string{root}, Operations: []string{"read"}, Sandbox: "read-only", Network: "deny", MaxChildren: 0}}
			c := NewCodex(&capSink{}, "unused")
			c.taskProvider = p
			c.appServerMode = "stdio"
			c.runtimeDiagnostics = map[string]any{"codex": map[string]any{"running_version": "0.162.1"}}
			s := session.NewRegistry().Create("owned-codex", "fixture", root, "codex", "gpt-6.1-sol", "read-only", "synthetic-original-thread")
			st := c.state(s.ID)
			st.threadID = s.ResumeID()
			writer := &rpcCaptureWriter{writes: make(chan []byte, 4)}
			c.rpc.setWriter(writer)
			done := make(chan error, 1)
			go func() { done <- c.ensureThread(s, st) }()
			for i := 0; i < 2; i++ {
				var frame struct {
					ID     int
					Method string
					Params map[string]any
				}
				select {
				case raw := <-writer.writes:
					if json.Unmarshal(raw, &frame) != nil {
						t.Fatal("fixture RPC malformed")
					}
				case <-time.After(time.Second):
					t.Fatal("retained policy probe missing")
				}
				result := `{"config":{"mcp_servers":{"ambient":{"enabled":true}}}}`
				if i == 0 && frame.Method != "config/read" {
					t.Fatal("no policy inspection before resume")
				}
				if i == 1 {
					if frame.Method != "thread/resume" || frame.Params["threadId"] != s.ResumeID() || frame.Params["sandbox"] != "read-only" || frame.Params["approvalPolicy"] != "never" {
						t.Fatal("resume policy not retained")
					}
					if _, present := frame.Params["dynamicTools"]; present {
						t.Fatal("resume advertised an unsupported dynamic catalog override")
					}
					config := frame.Params["config"].(map[string]any)
					if config["web_search"] != "disabled" || frame.Params["baseInstructions"] == "" {
						t.Fatal("retained gateway-only policy missing")
					}
					id := s.ResumeID()
					if wrong {
						id = "foreign-native-thread"
					}
					result = fmt.Sprintf(`{"thread":{"id":%q}}`, id)
				}
				c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":%s}`, frame.ID, result)))
			}
			select {
			case err := <-done:
				if wrong && err == nil || !wrong && err != nil {
					t.Fatal("resume proof outcome", err)
				}
			case <-time.After(time.Second):
				t.Fatal("probe did not settle")
			}
			select {
			case <-writer.writes:
				t.Fatal("unexpected model input/start/fork after probe")
			default:
			}
		})
	}
}

func (p *retainedScopeFixture) TaskWorkerScope(*session.Session) (*taskapi.ChildScope, error) {
	return &p.scope, nil
}
func (p *retainedScopeFixture) TaskTools(*session.Session) ([]map[string]any, error) {
	tools := []map[string]any{{"name": "task_capabilities", "inputSchema": map[string]any{"type": "object"}}, {"name": "task_read_input", "inputSchema": map[string]any{"type": "object"}}}
	if p.badTools {
		tools = append(tools, map[string]any{"name": "Bash"})
	}
	return tools, nil
}
func (p *retainedScopeFixture) ExecuteTask(context.Context, backend.TaskCaller, []byte) taskapi.Response {
	return taskapi.Response{}
}

func TestExtensionClaudeRetainedPresetBeforeInput(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "owned-cli-version-probe")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nif [ \"$1\" = --version ]; then echo '2.1.296 (fixture)'; exit 0; fi\nexit 88\n"), 0700); err != nil {
		t.Fatal(err)
	}
	p := &retainedScopeFixture{scope: taskapi.ChildScope{Roots: []string{root}, Operations: []string{"read"}, Sandbox: "read-only", Network: "deny", MaxChildren: 0}}
	c := NewClaude(&capSink{}, binary)
	c.taskProvider = p
	c.taskMCP = &claudeTaskMCP{c: c, leases: map[string]*claudeTaskLease{}, url: "http://127.0.0.1/owned-fixture"}
	s := session.NewRegistry().Create("owned-retained", "fixture", root, "claude", "sonnet", "read-only", "11111111-1111-4111-8111-111111111111")
	if err := c.ValidateExistingTaskScope(s, p.scope, root); err != nil {
		t.Fatal(err)
	}
	args := claudeSpawnArgs(s.Snapshot(), "")
	args, lease, err := c.taskSpawn(s, args, nil)
	if err != nil || lease == nil {
		t.Fatal("qualified preset unavailable", err)
	}
	defer lease.revoke()
	flag := func(name string) string {
		for i := range args {
			if args[i] == name && i+1 < len(args) {
				return args[i+1]
			}
		}
		return "<missing>"
	}
	if flag("--resume") != s.ResumeID() || flag("--tools") != "" || flag("--setting-sources") != "" {
		t.Fatal("resume or builtin/source restriction missing")
	}
	var settings map[string]any
	if json.Unmarshal([]byte(flag("--settings")), &settings) != nil || settings["disableAllHooks"] != true || settings["autoMemoryEnabled"] != false {
		t.Fatal("ambient policy weaker")
	}
	p.badTools = true
	if err := c.Send(context.Background(), s, "owned-rejected-input", "must-not-reach-fixture", nil, nil); err == nil {
		t.Fatal("bad toolset reached model input")
	}
	p.badTools = false
	weaker := p.scope
	weaker.Network = "inherit_authorized"
	if c.ValidateExistingTaskScope(s, weaker, root) == nil {
		t.Fatal("weaker network scope accepted")
	}
	if err := os.WriteFile(binary, []byte("#!/bin/sh\necho unknown\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if c.ValidateExistingTaskScope(s, p.scope, root) == nil {
		t.Fatal("unknown CLI version qualified")
	}
}
