package core

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

type accessProvider struct{ denied atomic.Bool }

func (p *accessProvider) LoadHistory(string, history.Opts) (*history.Result, error) {
	if p.denied.Load() {
		return nil, history.SourceError("codex", "/test/source", os.ErrPermission)
	}
	return history.Slice([]map[string]any{{"role": "user", "content": "test", "source_message_id": "one"}}, history.Opts{}), nil
}
func (p *accessProvider) ResumableSessions(int) ([]history.ResumableSession, error) {
	if p.denied.Load() {
		return nil, history.SourceError("codex", "/test/source", os.ErrPermission)
	}
	return []history.ResumableSession{{ID: "test", Cwd: "/tmp"}}, nil
}

type accessHistExec struct {
	fakeExec
	providers []*accessProvider
}

func (e *accessHistExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.providers[0], true
}
func (e *accessHistExec) AllProviders() []backend.HistoryProvider {
	out := []backend.HistoryProvider{}
	for _, p := range e.providers {
		out = append(out, p)
	}
	return out
}
func accessWire(t *testing.T, c *Client) map[string]any {
	t.Helper()
	select {
	case b := <-c.send:
		var x map[string]any
		if err := json.Unmarshal(b, &x); err != nil {
			t.Fatal(err)
		}
		return x
	case <-time.After(time.Second):
		t.Fatal("missing client result")
		return nil
	}
}
func TestHistoryPermissionErrorReachesClientAndRecoversWithoutCacheTTL(t *testing.T) {
	h, _ := newTestHub(t)
	p := &accessProvider{}
	p.denied.Store(true)
	h.SetExecutor(&accessHistExec{fakeExec: fakeExec{sink: h}, providers: []*accessProvider{p}})
	s := h.registry.Create("test", "test", "/tmp", "codex", "", "", "")
	s.SetResumeID("fixture")
	c := newDeviceClient(h, "test-access", 8)
	h.registerLatest(c)
	cmd := clientproto.NewAppV1().ParseCommand(protocol.Inbound{Type: "request_history", SessionID: s.ID, Mode: "snapshot"})
	h.sendHistory(c, s, cmd)
	errFrame := accessWire(t, c)
	if errFrame["type"] != "error" || errFrame["code"] != "history_permission_denied" {
		t.Fatal(errFrame)
	}
	p.denied.Store(false)
	h.sendHistory(c, s, cmd)
	frame := accessWire(t, c)
	if frame["type"] == "error" {
		t.Fatal("cached prior permission error", frame)
	}
}
func TestResumablePermissionErrorDoesNotEraseHealthyProviders(t *testing.T) {
	h, _ := newTestHub(t)
	bad, good := &accessProvider{}, &accessProvider{}
	bad.denied.Store(true)
	h.SetExecutor(&accessHistExec{fakeExec: fakeExec{sink: h}, providers: []*accessProvider{bad, good}})
	c := newDeviceClient(h, "test-partial", 8)
	h.registerLatest(c)
	h.sendResumable(c, 10)
	errFrame, partial := accessWire(t, c), accessWire(t, c)
	if errFrame["code"] != "history_permission_denied" || partial["type"] != "resumable_sessions" {
		t.Fatal(errFrame, partial)
	}
	bad.denied.Store(false)
	h.sendResumable(c, 10)
	frame := accessWire(t, c)
	if frame["type"] != "resumable_sessions" {
		t.Fatal(frame)
	}
}
