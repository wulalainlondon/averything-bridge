package goexec

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/core"
	"everything-go/internal/delegation"
	"everything-go/internal/governance"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type runtimeConfiguredOwner struct {
	*captureHandlerFixture
	hub  *core.Hub
	task string
	hash string
}

func (p *runtimeConfiguredOwner) ExecuteTask(_ context.Context, _ backend.TaskCaller, raw []byte) taskapi.Response {
	var r taskapi.Request
	json.Unmarshal(raw, &r)
	op := r.Operation
	return taskapi.Response{Type: "task_api_response", Version: r.Version, CorrelationID: r.CorrelationID, Operation: &op, OK: true, Result: map[string]any{"task_id": p.task, "text": "e\u0301 🧪", "final_hash": p.hash}}
}
func (p *runtimeConfiguredOwner) CaptureTaskResponse(ctx context.Context, c backend.TaskCaller, raw json.RawMessage, response taskapi.Response) error {
	return p.hub.CaptureTaskResponse(ctx, c, raw, response)
}
func TestRuntimeProductionBootstrapThroughActualMCPHandler(t *testing.T) {
	dir, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	reg := session.NewRegistry()
	s := reg.Create("synthetic-source", "source", dir, "claude", "sonnet", "read-only", "11111111-1111-4111-8111-111111111111")
	started := make(chan struct{})
	s.SubmitNamed("r_source", func() { close(started) })
	<-started
	hub := core.NewHub(reg, core.Config{DataDir: dir, InstanceID: "synthetic-owned-authority"}, governance.NewPairing(filepath.Join(dir, "pairing.json")), 0)
	defer hub.CloseDelegations()
	defer hub.CloseSessionDispatches()
	store, e := delegation.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	record := delegation.Record{ID: "dg_owned", ParentSessionID: s.ID, ChildSessionID: "s_child", ChildRequestID: "req_child", ParentRequestID: "return", State: "terminal", TerminalStatus: "completed"}
	if e := store.Create(context.Background(), record); e != nil {
		t.Fatal(e)
	}
	ref, _ := json.Marshal([]string{"synthetic-owned-authority", "delegation", record.ID})
	task := "ta1:" + base64.RawURLEncoding.EncodeToString(ref)
	command := taskapi.AuthorizedCommand{Caller: taskapi.VerifiedContext{StableScopeID: "session:" + s.ID}, Namespace: taskapi.Namespace{Authority: "synthetic-owned-authority", StableScopeID: "session:" + s.ID, Path: "delegation", Operation: "create_dispatch"}, Locator: taskapi.Locator{Path: "delegation"}, Request: taskapi.Request{IdempotencyKey: "seed"}, IntentHash: "synthetic"}
	if e := store.TaskJournal().Transaction(context.Background(), func(tx *sql.Tx) error {
		_, _, e := store.TaskJournal().ClaimTx(tx, command, taskapi.IntentRecord{TaskID: task, NativeID: record.ID, SessionID: record.ChildSessionID, RequestID: record.ChildRequestID, Metadata: []byte(`{}`)})
		return e
	}); e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256([]byte("e\u0301 🧪"))
	digest := hex.EncodeToString(hash[:])
	cfg := core.NativeCaptureConfig{Version: 1, Authority: "synthetic-owned-authority", Source: s.ID, Thread: s.ResumeID(), Revision: 0, Sink: "owned-handler", Operations: []string{"read_result"}, Tasks: map[string]string{task: digest}}
	b, _ := json.Marshal(cfg)
	path := filepath.Join(dir, "operator.json")
	os.WriteFile(path, b, 0600)
	if e := hub.ConfigureNativeResponseCapture(path); e != nil {
		t.Fatal(e)
	}
	provider := &runtimeConfiguredOwner{captureHandlerFixture: &captureHandlerFixture{}, hub: hub, task: task, hash: digest}
	claude := NewClaude(&capSink{}, "unused")
	claude.taskProvider = provider
	m := &claudeTaskMCP{c: claude, leases: map[string]*claudeTaskLease{}}
	lease := m.newLease(s, false)
	lease.p = &proc{reqID: "r_source", exited: make(chan struct{})}
	lease.p.taskLease = lease
	lease.requestID = "r_source"
	lease.threadID = s.ResumeID()
	lease.loaded = true
	lease.initialized = true
	lease.protocolVersion = "2025-06-18"
	call := func(auth bool) {
		body := `{"jsonrpc":"2.0","id":77,"method":"tools/call","params":{"name":"task_read_result","arguments":{"type":"task_api_request","api_version":"1.0.0-rc.1","correlation_id":"actual-mcp","operation":"read_result","input":{}}}}`
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/task/"+lease.generation, strings.NewReader(body))
		request.RemoteAddr = "127.0.0.1:1"
		if auth {
			request.Header.Set("Authorization", "Bearer "+lease.secret)
		}
		request.Header.Set("Mcp-Session-Id", lease.generation)
		request.Header.Set("MCP-Protocol-Version", "2025-06-18")
		m.ServeHTTP(httptest.NewRecorder(), request)
	}
	sink := filepath.Join(dir, "native-task-evidence", "owned-handler")
	call(false)
	files, _ := os.ReadDir(sink)
	if len(files) != 0 {
		t.Fatal("unauthenticated handler captured")
	}
	call(true)
	files, _ = os.ReadDir(sink)
	if len(files) != 1 {
		t.Fatal("production configured handler did not capture", len(files))
	}
	raw, _ := os.ReadFile(filepath.Join(sink, files[0].Name()))
	var value map[string]any
	json.Unmarshal(raw, &value)
	if value["native_call_id"] != "77" || value["public_result"].(map[string]any)["text"] != "e\u0301 🧪" {
		t.Fatal("original frame corrupted")
	}
	lease.revoke()
	call(true)
	files, _ = os.ReadDir(sink)
	if len(files) != 1 {
		t.Fatal("revoked handler wrote")
	}
}
