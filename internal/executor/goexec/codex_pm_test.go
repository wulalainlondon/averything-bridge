package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

type codexPMFixtureProvider struct {
	p   *backend.PMConfiguration
	err error
}

func (p codexPMFixtureProvider) PMConfiguration(id string) (*backend.PMConfiguration, error) {
	if id == "pm_test" {
		return p.p, p.err
	}
	return nil, nil
}

func TestCodexPMTurnCannotEscalateOrSwitchRole(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.SetPMProvider(codexPMFixtureProvider{p: &backend.PMConfiguration{RuntimeDir: t.TempDir()}})
	params := map[string]any{"sandboxPolicy": map[string]any{"type": "dangerFullAccess"}, "approvalPolicy": "on-request", "model": "other", "collaborationMode": map[string]any{"mode": "plan"}}
	if err := c.applyPMTurnPolicy("pm_test", params); err != nil {
		t.Fatal(err)
	}
	if params["sandboxPolicy"].(map[string]any)["type"] != "readOnly" || params["approvalPolicy"] != "never" || params["model"] != "gpt-5.6-sol" || params["collaborationMode"] != nil {
		t.Fatal(params)
	}
	s := session.NewRegistry().Create("pm_test", "PM", t.TempDir(), backend.Codex, "", "read-only", "")
	if err := c.UpdateSessionSettings(context.Background(), s); err == nil {
		t.Fatal("PM settings mutable")
	}
	ordinary := map[string]any{"model": "user-model"}
	if err := c.applyPMTurnPolicy("worker", ordinary); err != nil || ordinary["model"] != "user-model" || len(ordinary) != 1 {
		t.Fatal("ordinary worker changed")
	}
	c.SetPMProvider(codexPMFixtureProvider{err: errors.New("database unavailable")})
	if c.applyPMTurnPolicy("pm_test", params) == nil {
		t.Fatal("missing durable role must fail closed")
	}
}

func TestCodexPMStartAndResumeHaveIdenticalHardPolicy(t *testing.T) {
	for _, resume := range []string{"", "native-thread"} {
		t.Run("resume_"+resume, func(t *testing.T) {
			c := NewCodex(&capSink{}, "codex")
			c.appServerMode = "daemon"
			c.SetPMProvider(codexPMFixtureProvider{p: &backend.PMConfiguration{RuntimeDir: t.TempDir(), Instructions: "FIXED PM ROLE", Tools: []map[string]any{{"name": "project_get_context", "description": "Read context", "inputSchema": map[string]any{"type": "object"}}}}})
			writer := &rpcCaptureWriter{writes: make(chan []byte, 8)}
			c.rpc.setWriter(writer)
			s := session.NewRegistry().Create("pm_test", "PM", t.TempDir(), backend.Codex, "", "read-only", resume)
			done := make(chan error, 1)
			go func() { done <- c.ensureThread(s, c.state(s.ID)) }()
			for i := 0; i < 2; i++ {
				var frame struct {
					ID     int            `json:"id"`
					Method string         `json:"method"`
					Params map[string]any `json:"params"`
				}
				select {
				case raw := <-writer.writes:
					if err := json.Unmarshal(raw, &frame); err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("missing policy RPC")
				}
				response := `{"config":{"mcp_servers":{"dangerous":{"command":"ignored"}}}}`
				if i == 0 {
					if frame.Method != "config/read" {
						t.Fatal(frame.Method)
					}
				} else {
					expected := "thread/start"
					if resume != "" {
						expected = "thread/resume"
					}
					if frame.Method != expected || frame.Params["sandbox"] != "read-only" || frame.Params["baseInstructions"] != "FIXED PM ROLE" {
						t.Fatal(frame)
					}
					cfg := frame.Params["config"].(map[string]any)
					if cfg["mcp_servers"].(map[string]any)["dangerous"].(map[string]any)["enabled"] != false || cfg["agents"].(map[string]any)["enabled"] != false {
						t.Fatal(cfg)
					}
					encoded, _ := json.Marshal(cfg)
					if !strings.Contains(string(encoded), `"excluded_tool_namespaces":["functions"]`) {
						t.Fatal(string(encoded))
					}
					response = `{"thread":{"id":"native-thread"}}`
				}
				c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":%s}`, frame.ID, response)))
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCodexPMDeclinesApproval(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.SetPMProvider(codexPMFixtureProvider{p: &backend.PMConfiguration{}})
	s := session.NewRegistry().Create("pm_test", "PM", t.TempDir(), backend.Codex, "", "read-only", "")
	c.threadToSession["native-thread"] = s
	writer := &rpcCaptureWriter{writes: make(chan []byte, 8)}
	c.rpc.setWriter(writer)
	for _, method := range []string{"item/commandExecution/requestApproval", "item/fileChange/requestApproval", "item/permissions/requestApproval"} {
		c.handleServerRequest(1, method, json.RawMessage(`{"threadId":"native-thread","permissions":{"network":{"enabled":true}}}`))
		select {
		case raw := <-writer.writes:
			if strings.Contains(string(raw), "accept") || strings.Contains(string(raw), `"enabled":true`) {
				t.Fatal(string(raw))
			}
		case <-time.After(time.Second):
			t.Fatal("approval not denied")
		}
	}
}
