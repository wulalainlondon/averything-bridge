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

// Actual original MCP handler, isolated response/sink fixtures, no model E2E.
type captureHandlerFixture struct {
	raw      string
	calls    int
	identity backend.TaskCaller
}

func (p *captureHandlerFixture) TaskWorkerScope(*session.Session) (*taskapi.ChildScope, error) {
	return nil, nil
}
func (p *captureHandlerFixture) TaskTools(*session.Session) ([]map[string]any, error) {
	return []map[string]any{{"name": "task_read_result", "inputSchema": map[string]any{"type": "object"}}}, nil
}
func (p *captureHandlerFixture) ExecuteTask(_ context.Context, _ backend.TaskCaller, raw []byte) taskapi.Response {
	var r taskapi.Request
	json.Unmarshal(raw, &r)
	op := r.Operation
	return taskapi.Response{Type: "task_api_response", Version: "1.0.0-rc.1", CorrelationID: r.CorrelationID, Operation: &op, OK: true, Result: map[string]any{"task_id": "synthetic-owned-task", "text": "e\u0301 🧪", "final_hash": "synthetic-upstream"}}
}
func (p *captureHandlerFixture) CaptureTaskResponse(_ context.Context, c backend.TaskCaller, _ json.RawMessage, response taskapi.Response) error {
	if c.Validate == nil || !c.Validate() {
		return nil
	}
	bytes, _ := json.Marshal(response)
	p.raw = string(bytes)
	p.identity = c
	p.calls++
	return nil
}

func TestExtensionOriginalMCPHandlerCapturesBeforeModel(t *testing.T) {
	provider := &captureHandlerFixture{}
	c := NewClaude(&capSink{}, "unused")
	c.taskProvider = provider
	m := &claudeTaskMCP{c: c, leases: map[string]*claudeTaskLease{}}
	s := session.NewRegistry().Create("synthetic-source", "source", t.TempDir(), "claude", "sonnet", "read-only", "11111111-1111-4111-8111-111111111111")
	lease := m.newLease(s, false)
	process := &proc{reqID: "r_synthetic_actual_handler", exited: make(chan struct{})}
	process.taskLease = lease
	lease.p = process
	lease.requestID = process.reqID
	lease.threadID = s.ResumeID()
	lease.loaded = true
	lease.initialized = true
	lease.protocolVersion = "2025-06-18"
	call := func(auth bool) {
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/task/"+lease.generation, strings.NewReader(`{"jsonrpc":"2.0","id":77,"method":"tools/call","params":{"name":"task_read_result","arguments":{"type":"task_api_request","api_version":"1.0.0-rc.1","correlation_id":"synthetic-original-correlation","operation":"read_result","input":{}}}}`))
		request.RemoteAddr = "127.0.0.1:1"
		if auth {
			request.Header.Set("Authorization", "Bearer "+lease.secret)
		}
		request.Header.Set("Mcp-Session-Id", lease.generation)
		request.Header.Set("MCP-Protocol-Version", "2025-06-18")
		m.ServeHTTP(httptest.NewRecorder(), request)
	}
	call(false)
	if provider.calls != 0 {
		t.Fatal("unauthenticated capture")
	}
	call(true)
	if provider.calls != 1 || provider.identity.Session != s || provider.identity.RequestID != lease.requestID || provider.identity.CallID != "77" || provider.identity.ThreadID != s.ResumeID() {
		t.Fatal("original handler identity lost")
	}
	var observed map[string]any
	if json.Unmarshal([]byte(provider.raw), &observed) != nil || observed["result"].(map[string]any)["text"] != "e\u0301 🧪" {
		t.Fatal("model normalization occurred before sink")
	}
	lease.revoke()
	call(true)
	if provider.calls != 1 {
		t.Fatal("revoked original lease captured")
	}
}
