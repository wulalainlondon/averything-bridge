package core

import (
	"context"
	"everything-go/internal/recap"
	"everything-go/internal/session"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type recapExec struct {
	*fakeExec
	calls    atomic.Int32
	generate atomic.Bool
	force    atomic.Bool
}

func (f *recapExec) SessionRecap(_ context.Context, s *session.Session, generate, force bool) (recap.Result, error) {
	f.calls.Add(1)
	f.generate.Store(generate)
	f.force.Store(force)
	return recap.Result{Snapshot: &recap.Snapshot{ThreadID: s.ResumeID(), SourceHash: strings.Repeat("a", 64), Summary: "Built, not deployed", GeneratedAt: time.Now().UnixMilli()}}, nil
}

func TestRecapIsRequesterOnlyAndCannotCompleteChat(t *testing.T) {
	h, f := newControlTestHub(t, t.TempDir())
	exec := &recapExec{fakeExec: f}
	h.SetExecutor(exec)
	c := newTestClient(h)
	c.deviceID = "phone"
	c.ctx = context.Background()
	h.registerLatest(c)
	other := newTestClient(h)
	other.deviceID = "other"
	s := h.registry.Create("s", "test", "/work", "codex", "", "", "native")
	route(h, c, `{"type":"generate_session_recap","session_id":"s","request_id":"r","force":true}`)
	e := waitForType(t, c, "session_recap")
	if e["request_id"] != "r" || e["authority_instance_id"] != "i1" || !exec.generate.Load() || !exec.force.Load() || s.IsStreaming() || s.ResumeID() != "native" {
		t.Fatal(e)
	}
	select {
	case raw := <-other.send:
		t.Fatalf("recap broadcast to other device: %s", raw)
	default:
	}
	select {
	case raw := <-c.send:
		t.Fatalf("extra task event: %s", raw)
	default:
	}
}

func TestRecapReadDoesNotGenerateAndWritesRefuseBusySession(t *testing.T) {
	h, f := newControlTestHub(t, t.TempDir())
	exec := &recapExec{fakeExec: f}
	h.SetExecutor(exec)
	c := newTestClient(h)
	c.deviceID = "phone"
	c.ctx = context.Background()
	h.registerLatest(c)
	s := h.registry.Create("s", "test", "/work", "codex", "", "", "native")
	s.Submit(func() {})
	waitState(t, s, session.Streaming)
	t.Cleanup(func() { s.EndTurn(); h.registry.Delete(s.ID) })
	route(h, c, `{"type":"request_session_recap","session_id":"s","request_id":"read"}`)
	e := waitForType(t, c, "session_recap")
	if exec.generate.Load() || e["stale"] != true {
		t.Fatal(e)
	}
	route(h, c, `{"type":"generate_session_recap","session_id":"s","request_id":"write"}`)
	e = waitForType(t, c, "session_recap")
	if e["error_code"] != "session_busy" || exec.calls.Load() != 1 || !s.IsStreaming() {
		t.Fatal(e, exec.calls.Load())
	}
}

func TestRecapRequiresPairingSessionAndRequestCorrelation(t *testing.T) {
	h, f := newControlTestHub(t, t.TempDir())
	exec := &recapExec{fakeExec: f}
	h.SetExecutor(exec)
	c := newTestClient(h)
	h.registry.Create("s", "test", "/work", "codex", "", "", "native")
	route(h, c, `{"type":"generate_session_recap","session_id":"s","request_id":"r"}`)
	if e := waitForType(t, c, "session_recap"); e["error_code"] != "pairing_required" {
		t.Fatal(e)
	}
	c.deviceID = "phone"
	for _, v := range []struct{ raw, code string }{
		{`{"type":"generate_session_recap","session_id":"s"}`, "invalid_request"},
		{`{"type":"generate_session_recap","session_id":"missing","request_id":"r"}`, "no_session"},
	} {
		route(h, c, v.raw)
		if e := waitForType(t, c, "session_recap"); e["error_code"] != v.code {
			t.Fatal(e)
		}
	}
	if exec.calls.Load() != 0 {
		t.Fatal("unauthorized generation")
	}
}

func TestRecapBurstIsBoundedWithoutUsingChatHistorySlots(t *testing.T) {
	h, f := newControlTestHub(t, t.TempDir())
	exec := &recapExec{fakeExec: f}
	h.SetExecutor(exec)
	c := newTestClient(h)
	c.deviceID = "phone"
	c.ctx = context.Background()
	h.registerLatest(c)
	h.registry.Create("s", "test", "/work", "codex", "", "", "native")
	for range cap(h.recapJobs) {
		h.recapJobs <- struct{}{}
	}
	route(h, c, `{"type":"generate_session_recap","session_id":"s","request_id":"r"}`)
	e := waitForType(t, c, "session_recap")
	if e["error_code"] != "recap_busy" || exec.calls.Load() != 0 || len(h.storm.heavySem) != 0 {
		t.Fatal(e, exec.calls.Load())
	}
}
