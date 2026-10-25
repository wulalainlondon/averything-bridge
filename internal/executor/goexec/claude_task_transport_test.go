package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type transportFixtureProvider struct{}

func (transportFixtureProvider) TaskTools(*session.Session) ([]map[string]any, error) {
	return []map[string]any{{"type": "function", "name": "task_capabilities", "description": "fixture", "inputSchema": map[string]any{"type": "object"}}}, nil
}
func (transportFixtureProvider) TaskWorkerScope(*session.Session) (*taskapi.ChildScope, error) {
	return nil, nil
}
func (transportFixtureProvider) ExecuteTask(context.Context, backend.TaskCaller, []byte) taskapi.Response {
	return taskapi.Response{}
}
func TestClaudeTaskMCPTransportKeepsAuthenticationAndNegotiatesLifecycle(t *testing.T) {
	c := &Claude{taskProvider: transportFixtureProvider{}}
	m := &claudeTaskMCP{c: c, leases: map[string]*claudeTaskLease{}}
	lease := m.newLease(session.NewRegistry().Create("fixture", "Fixture", t.TempDir(), "claude", "", "read-only", ""), false)
	if strings.HasPrefix(lease.secret, lease.generation) {
		t.Fatal("public generation shared credential bytes")
	}
	call := func(method, body, auth, sessionID, version string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(method, "http://127.0.0.1/task/"+lease.generation, strings.NewReader(body))
		request.RemoteAddr = "127.0.0.1:1234"
		if auth != "" {
			request.Header.Set("Authorization", auth)
		}
		if sessionID != "" {
			request.Header.Set("Mcp-Session-Id", sessionID)
		}
		if version != "" {
			request.Header.Set("MCP-Protocol-Version", version)
		}
		out := httptest.NewRecorder()
		m.ServeHTTP(out, request)
		return out
	}
	if call("GET", "", "", "", "").Code != http.StatusUnauthorized {
		t.Fatal("GET bypassed credential boundary")
	}
	auth := "Bearer " + lease.secret
	if call("GET", "", auth, "", "").Code != http.StatusMethodNotAllowed {
		t.Fatal("authenticated no-SSE GET was mistaken for auth refusal")
	}
	initialized := call("POST", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}`, auth, "", "")
	var response map[string]any
	json.Unmarshal(initialized.Body.Bytes(), &response)
	if initialized.Code != 200 || response["result"].(map[string]any)["protocolVersion"] != "2025-06-18" || initialized.Header().Get("Mcp-Session-Id") != lease.generation {
		t.Fatal("negotiation failed")
	}
	query := `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`
	if call("POST", query, auth, "forged-session", "2025-06-18").Code != 400 || call("POST", query, auth, lease.generation, "unknown-version").Code != 400 {
		t.Fatal("session/version mismatch accepted")
	}
	listed := call("POST", query, auth, lease.generation, "2025-06-18")
	if listed.Code != 200 || !lease.loaded {
		t.Fatal("authenticated catalog not loaded")
	}
	json.Unmarshal(listed.Body.Bytes(), &response)
	tool := response["result"].(map[string]any)["tools"].([]any)[0].(map[string]any)
	if _, exists := tool["type"]; exists {
		t.Fatal("Codex registration shape leaked into MCP")
	}
	if call("DELETE", "", auth, lease.generation, "2025-06-18").Code != 200 {
		t.Fatal("own session teardown failed")
	}
	if call("POST", query, auth, lease.generation, "2025-06-18").Code != 401 {
		t.Fatal("revoked lease remained callable")
	}
}
