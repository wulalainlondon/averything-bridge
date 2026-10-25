package goexec

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"everything-go/internal/session"
)

// Real MCP handler/lifecycle with synthetic task execution. Transport health
// after a completed turn is distinct from permission to invoke another task.
func TestFunctionalCompletedClaudeTurnKeepsTransportButRevokesCaller(t *testing.T) {
	provider := &captureHandlerFixture{}
	c := NewClaude(&capSink{}, "unused")
	c.taskProvider = provider
	m := &claudeTaskMCP{c: c, leases: map[string]*claudeTaskLease{}}
	s := session.NewRegistry().Create("synthetic-source", "source", t.TempDir(), "claude", "sonnet", "read-only", "11111111-1111-4111-8111-111111111111")
	lease := m.newLease(s, false)
	defer lease.revoke()
	p := &proc{reqID: "r_original_turn", exited: make(chan struct{}), taskLease: lease}
	lease.p, lease.requestID, lease.threadID = p, p.reqID, s.ResumeID()
	call := func(method string, params any) *httptest.ResponseRecorder {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 77, "method": method, "params": params})
		r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/task/"+lease.generation, bytes.NewReader(body))
		r.RemoteAddr = "127.0.0.1:1"
		r.Header.Set("Authorization", "Bearer "+lease.secret)
		r.Header.Set("Mcp-Session-Id", lease.generation)
		r.Header.Set("MCP-Protocol-Version", "2025-06-18")
		out := httptest.NewRecorder()
		m.ServeHTTP(out, r)
		return out
	}
	if call("initialize", map[string]any{"protocolVersion": "2025-06-18"}).Code != 200 || call("tools/list", nil).Code != 200 {
		t.Fatal("original process transport did not initialize")
	}
	args := map[string]any{"name": "task_read_result", "arguments": map[string]any{"type": "task_api_request", "api_version": "1.0.0-rc.1", "correlation_id": "synthetic-original", "operation": "read_result", "input": map[string]any{}}}
	if call("tools/call", args).Code != 200 || provider.calls != 1 || !provider.identity.Validate() {
		t.Fatal("original active request was not bound")
	}
	original := provider.identity
	if request, _ := p.finishTurn(); request != "r_original_turn" || original.Validate() {
		t.Fatal("completed request retained business authority")
	}
	if response := call("ping", nil); response.Code != 200 {
		t.Fatalf("normal turn completion disconnected the process MCP transport: HTTP %d", response.Code)
	}
	if call("tools/list", nil).Code != 200 {
		t.Fatal("idle process lost its declared catalog")
	}
	if response := call("tools/call", args); response.Code != http.StatusForbidden || provider.calls != 1 {
		t.Fatal("idle transport revived an old task request")
	}
	lease.revoke()
	if call("ping", nil).Code != http.StatusUnauthorized || provider.calls != 1 {
		t.Fatal("explicit process revocation did not close transport")
	}
}

func TestFunctionalNextClaudeTurnGetsNewProcessLease(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "owned-cli-fixture")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nif [ \"$1\" = --version ]; then echo '2.1.296 (fixture)'; exit 0; fi\nexec /bin/cat >/dev/null\n"), 0700); err != nil {
		t.Fatal(err)
	}
	c := NewClaude(&capSink{}, binary)
	c.SetTaskAPIProvider(&captureHandlerFixture{})
	s := session.NewRegistry().Create("synthetic-rebind", "source", root, "claude", "sonnet", "read-only", "11111111-1111-4111-8111-111111111111")
	t.Cleanup(func() {
		c.mu.Lock()
		p := c.procs[s.ID]
		delete(c.procs, s.ID)
		c.mu.Unlock()
		if p != nil {
			p.taskLease.revoke()
			p.cancel()
			select {
			case <-p.exited:
			case <-time.After(5 * time.Second):
				t.Error("owned fixture did not exit")
			}
		}
	})
	if err := c.Send(context.Background(), s, "r_first_owned", "synthetic first", nil, nil); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	first := c.procs[s.ID]
	c.mu.Unlock()
	first.finishTurn()
	first.markCompact("compact_synthetic")
	if err := c.Send(context.Background(), s, "r_next_owned", "synthetic second", nil, nil); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	next := c.procs[s.ID]
	c.mu.Unlock()
	if next == first || next.taskLease == first.taskLease || next.taskLease.generation == first.taskLease.generation || next.taskLease.requestID != "r_next_owned" || first.taskLease.requestID != "" || s.ResumeID() != "11111111-1111-4111-8111-111111111111" {
		t.Fatal("new turn reused completed process authority or changed native thread")
	}
	select {
	case <-first.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("old owned fixture did not exit")
	}
}

func TestFunctionalRevokedButUnfinishedClaudeTurnIsNotReplaced(t *testing.T) {
	for _, revoked := range []bool{false, true} {
		provider := &captureHandlerFixture{}
		c := NewClaude(&capSink{}, "unused")
		c.taskProvider = provider
		m := &claudeTaskMCP{c: c, leases: map[string]*claudeTaskLease{}}
		s := session.NewRegistry().Create("synthetic-active", "source", t.TempDir(), "claude", "sonnet", "read-only", "11111111-1111-4111-8111-111111111111")
		lease := m.newLease(s, false)
		cancelled := false
		p := &proc{reqID: "r_still_owned", exited: make(chan struct{}), taskLease: lease, cancel: func() { cancelled = true }}
		lease.p, lease.requestID, lease.threadID = p, p.reqID, s.ResumeID()
		c.procs[s.ID] = p
		if revoked {
			lease.revoke()
		}
		if err := c.Send(context.Background(), s, "r_other_request", "must not be sent", nil, nil); err == nil || cancelled || p.currentReqID() != "r_still_owned" || c.procs[s.ID] != p {
			t.Fatal("unfinished native ownership replaced", revoked, err)
		}
		lease.revoke()
	}
}
