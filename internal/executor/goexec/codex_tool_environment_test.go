package goexec

import (
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"everything-go/internal/session"
	"everything-go/internal/toolenv"
)

type toolTestWriter struct {
	c       *Codex
	mu      sync.Mutex
	methods []string
	reply   func(string, json.RawMessage) (any, error)
}

func (w *toolTestWriter) Write(b []byte) (int, error) {
	var r struct {
		ID     int             `json:"id"`
		Method string          `json:"method"`
		Params json.RawMessage `json:"params"`
	}
	if e := json.Unmarshal(b, &r); e != nil {
		return 0, e
	}
	w.mu.Lock()
	w.methods = append(w.methods, r.Method)
	w.mu.Unlock()
	result, e := w.reply(r.Method, r.Params)
	response := map[string]any{"id": r.ID, "result": result}
	if e != nil {
		delete(response, "result")
		response["error"] = map[string]any{"code": -32601, "message": e.Error()}
	}
	raw, _ := json.Marshal(response)
	w.c.rpc.dispatchResponse(raw)
	return len(b), nil
}
func toolFixture(t *testing.T) (*Codex, *session.Session, *toolTestWriter) {
	t.Helper()
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.appServerSocket = filepath.Join(t.TempDir(), "daemon.sock")
	c.runtimeDiagnostics = map[string]any{"codex": map[string]any{"running_version": "0.153.2", "managed_version": "0.153.4"}}
	s := session.NewRegistry().Create("s1", "test", "/work", "codex", "gpt-6-astra", "read-only", "t1")
	w := &toolTestWriter{c: c}
	w.reply = func(m string, p json.RawMessage) (any, error) {
		switch m {
		case "thread/read":
			var args struct {
				ID string `json:"threadId"`
			}
			json.Unmarshal(p, &args)
			return map[string]any{"thread": map[string]any{"id": args.ID, "status": map[string]string{"type": "idle"}}}, nil
		case "mcpServerStatus/list":
			return map[string]any{"data": []any{map[string]any{"name": "node_repl", "runtimeStatus": "connected", "authStatus": "unsupported", "tools": map[string]any{"js": map[string]any{"description": "SECRET must not leave inventory"}}}}, "nextCursor": nil}, nil
		case "thread/loaded/list":
			return map[string]any{"data": []string{"t1"}}, nil
		case "config/mcpServer/reload":
			return map[string]any{}, nil
		case "thread/fork":
			return map[string]any{"thread": map[string]any{"id": "t2", "forkedFromId": "t1", "cwd": "/work"}}, nil
		default:
			return nil, io.EOF
		}
	}
	c.rpc.setWriter(w)
	return c, s, w
}
func TestToolInventoryIsReadOnlySanitizedAndNotBrowserSuccess(t *testing.T) {
	c, s, w := toolFixture(t)
	v, e := c.InspectToolEnvironment(context.Background(), s, true)
	if e != nil {
		t.Fatal(e)
	}
	if v.State != "listed_unverified" || v.Evidence != "inventory" || v.ReloadAllowed {
		t.Fatalf("false readiness=%+v", v)
	}
	raw, _ := json.Marshal(v)
	if strings.Contains(string(raw), "SECRET") {
		t.Fatal("leaked tool definition")
	}
	if _, e = c.InspectToolEnvironment(context.Background(), s, false); e != nil {
		t.Fatal(e)
	}
	if strings.Join(w.methods, ",") != "thread/read,mcpServerStatus/list" {
		t.Fatal(w.methods)
	}
}
func TestToolInventoryIncludesPaginationAndRejectsCursorLoop(t *testing.T) {
	c, s, w := toolFixture(t)
	base := w.reply
	pages := 0
	w.reply = func(m string, p json.RawMessage) (any, error) {
		if m != "mcpServerStatus/list" {
			return base(m, p)
		}
		pages++
		return map[string]any{"data": []any{}, "nextCursor": "repeat"}, nil
	}
	if _, e := c.InspectToolEnvironment(context.Background(), s, true); toolenv.Code(e) != "invalid_inventory_cursor" {
		t.Fatal(e)
	}
	if pages != 2 {
		t.Fatal(pages)
	}
}
func TestEmptyOrIntegratedInventoryIsNotMissingInstallation(t *testing.T) {
	c, s, w := toolFixture(t)
	base := w.reply
	w.reply = func(m string, p json.RawMessage) (any, error) {
		if m != "mcpServerStatus/list" {
			return base(m, p)
		}
		return map[string]any{"data": []any{map[string]any{"name": "cua_repl", "runtimeStatus": "connected", "authStatus": "unsupported", "tools": map[string]any{}}}}, nil
	}
	v, e := c.InspectToolEnvironment(context.Background(), s, true)
	if e != nil || v.State != "unknown" || v.Coverage != "configured_mcp_only" {
		t.Fatalf("%+v %v", v, e)
	}
}
func TestReloadRequiresHostWindowAndIdleOtherThreads(t *testing.T) {
	c, s, w := toolFixture(t)
	if _, e := c.RepairToolEnvironment(context.Background(), s, "reload", c.toolGeneration()); toolenv.Code(e) != "maintenance_window_required" {
		t.Fatal(e)
	}
	t.Setenv("EVERYTHING_GO_CODEX_TOOL_MAINTENANCE", "true")
	base := w.reply
	w.reply = func(m string, p json.RawMessage) (any, error) {
		if m == "thread/loaded/list" {
			return map[string]any{"data": []string{"t1", "other"}}, nil
		}
		if m == "thread/read" && strings.Contains(string(p), "other") {
			return map[string]any{"thread": map[string]any{"id": "other", "status": map[string]string{"type": "active"}}}, nil
		}
		return base(m, p)
	}
	if _, e := c.RepairToolEnvironment(context.Background(), s, "reload", c.toolGeneration()); toolenv.Code(e) != "waiting_idle" {
		t.Fatal(e)
	}
	for _, m := range w.methods {
		if m == "config/mcpServer/reload" {
			t.Fatal("reloaded while busy")
		}
	}
}
func TestReloadAckIsOnlyCompletedInventoryAndChangesGeneration(t *testing.T) {
	t.Setenv("EVERYTHING_GO_CODEX_TOOL_MAINTENANCE", "true")
	c, s, w := toolFixture(t)
	gen := c.toolGeneration()
	r, e := c.RepairToolEnvironment(context.Background(), s, "reload", gen)
	if e != nil {
		t.Fatal(e)
	}
	if r.Snapshot.Evidence != "inventory" || r.Snapshot.State == "ready" || gen == c.toolGeneration() {
		t.Fatal(r)
	}
	count := 0
	for _, m := range w.methods {
		if m == "config/mcpServer/reload" {
			count++
		}
	}
	if count != 1 {
		t.Fatal(count)
	}
	if _, e = c.RepairToolEnvironment(context.Background(), s, "reload", gen); toolenv.Code(e) != "environment_changed" {
		t.Fatal(e)
	}
}
func TestNativeForkPreservesParentAndExcludesLargeHistory(t *testing.T) {
	c, s, w := toolFixture(t)
	base := w.reply
	w.reply = func(m string, p json.RawMessage) (any, error) {
		if m == "thread/fork" && !strings.Contains(string(p), `"excludeTurns":true`) {
			t.Fatal(string(p))
		}
		return base(m, p)
	}
	r, e := c.RepairToolEnvironment(context.Background(), s, "fork", c.toolGeneration())
	if e != nil {
		t.Fatal(e)
	}
	if r.NewThread != "t2" || s.ResumeID() != "t1" {
		t.Fatal(r, s.ResumeID())
	}
}
func TestDiagnosticNoConnectionNeverStartsDaemon(t *testing.T) {
	c, s, _ := toolFixture(t)
	c.rpc.setWriter(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := c.InspectToolEnvironment(ctx, s, true); toolenv.Code(e) != "daemon_not_connected" {
		t.Fatal(e)
	}
}
