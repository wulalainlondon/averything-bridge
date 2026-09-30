package executor

import (
	"context"
	"errors"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type capSink struct{ events []any }

func (s *capSink) Emit(e any) { s.events = append(s.events, e) }

type reliableFake struct {
	sink   Sink
	err    error
	panic  bool
	emit   any
	called bool
}

func (f *reliableFake) Send(ctx context.Context, s *session.Session, reqID, content string, images []backend.ImageAttachment, files []backend.FileAttachment) error {
	f.called = true
	if f.panic {
		panic("boom")
	}
	if f.emit != nil {
		f.sink.Emit(f.emit)
	}
	return f.err
}
func (f *reliableFake) Stop(context.Context, *session.Session) error  { return nil }
func (f *reliableFake) Clear(context.Context, *session.Session) error { return nil }
func (f *reliableFake) Close(context.Context, *session.Session) error { return nil }

func TestReliableMuxEmitsErrorOnSendError(t *testing.T) {
	sink := &capSink{}
	terminal := NewTerminalSinkWithTimeout(sink, 0)
	inner := &reliableFake{sink: terminal, err: errors.New("nope")}
	mux := NewReliableMux(map[string]Executor{"claude": inner}, inner, terminal)
	s := session.NewRegistry().Create("s1", "n", "/tmp", "claude", "", "", "")

	if err := mux.Send(context.Background(), s, "r1", "hi", nil, nil); err == nil {
		t.Fatal("Send should return backend error")
	}
	if len(sink.events) != 1 {
		t.Fatalf("want one emitted error, got %d: %+v", len(sink.events), sink.events)
	}
	ev, ok := sink.events[0].(protocol.Error)
	if !ok || ev.Code != "send_error" || ev.SessionID != "s1" {
		t.Fatalf("wrong error event: %#v", sink.events[0])
	}
}

func TestReliableMuxDoesNotEmitErrorAfterTerminal(t *testing.T) {
	sink := &capSink{}
	terminal := NewTerminalSinkWithTimeout(sink, 0)
	inner := &reliableFake{sink: terminal, err: errors.New("late"), emit: protocol.NewDone("s1", "r1")}
	mux := NewReliableMux(map[string]Executor{"claude": inner}, inner, terminal)
	s := session.NewRegistry().Create("s1", "n", "/tmp", "claude", "", "", "")

	_ = mux.Send(context.Background(), s, "r1", "hi", nil, nil)
	if len(sink.events) != 1 {
		t.Fatalf("terminal event should suppress fallback error, got %+v", sink.events)
	}
	if _, ok := sink.events[0].(protocol.Done); !ok {
		t.Fatalf("want done event, got %T", sink.events[0])
	}
}

func TestReliableMuxRecoversPanic(t *testing.T) {
	sink := &capSink{}
	terminal := NewTerminalSinkWithTimeout(sink, 0)
	inner := &reliableFake{sink: terminal, panic: true}
	mux := NewReliableMux(map[string]Executor{"claude": inner}, inner, terminal)
	s := session.NewRegistry().Create("s1", "n", "/tmp", "claude", "", "", "")

	if err := mux.Send(context.Background(), s, "r1", "hi", nil, nil); err == nil {
		t.Fatal("panic should be returned as error")
	}
	if len(sink.events) != 1 {
		t.Fatalf("want one panic error event, got %+v", sink.events)
	}
	ev, ok := sink.events[0].(protocol.Error)
	if !ok || ev.Code != "executor_panic" {
		t.Fatalf("wrong panic error: %#v", sink.events[0])
	}
}

type historyFake struct{ reliableFake }

func (h *historyFake) LoadHistory(string, history.Opts) (*history.Result, error) {
	return &history.Result{}, nil
}
func (h *historyFake) ResumableSessions(int) ([]history.ResumableSession, error) {
	return []history.ResumableSession{{ID: "r1"}}, nil
}

func TestReliableMuxDoesNotHideHistoryCapability(t *testing.T) {
	sink := &capSink{}
	terminal := NewTerminalSinkWithTimeout(sink, 0)
	inner := &historyFake{}
	mux := NewReliableMux(map[string]Executor{"claude": inner}, inner, terminal)
	s := session.NewRegistry().Create("s1", "n", "/tmp", "claude", "", "", "")

	if _, ok := mux.ProviderFor(s); !ok {
		t.Fatal("history capability should still be visible through mux")
	}
}

type diagnosticFake struct{ reliableFake }

func (d *diagnosticFake) RuntimeDiagnostics() map[string]any {
	return map[string]any{"codex": map[string]any{"status": "ready", "running_version": "0.153.0"}}
}

func TestMuxAggregatesRuntimeDiagnostics(t *testing.T) {
	inner := &diagnosticFake{}
	mux := NewMux(map[string]Executor{"codex": inner, "alias": inner}, inner)
	got := mux.RuntimeDiagnostics()
	codex, ok := got["codex"].(map[string]any)
	if !ok || codex["status"] != "ready" || codex["running_version"] != "0.153.0" {
		t.Fatalf("unexpected diagnostics: %+v", got)
	}
}
