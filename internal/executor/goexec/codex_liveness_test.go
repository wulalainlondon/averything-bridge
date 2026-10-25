package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func livenessFixture(t *testing.T) (*Codex, *session.Session, *codexState, *toolTestWriter, *capSink) {
	t.Helper()
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	c.appServerMode = "stdio"
	c.dataDir = t.TempDir()
	s := session.NewRegistry().Create("s1", "test", "/work", "codex", "", "", "root")
	st := c.state(s.ID)
	st.threadID = "root"
	st.currentTurnID = "turn-1"
	st.reqID = "request-1"
	st.turnActive = true
	st.turnDone = make(chan struct{})
	st.lastEventAt = time.Now()
	c.threadToSession["root"] = s
	w := &toolTestWriter{c: c, reply: func(string, json.RawMessage) (any, error) { return map[string]any{}, nil }}
	c.rpc.setWriter(w)
	return c, s, st, w, sink
}

func completeLivenessTurn(c *Codex, status string) {
	raw, _ := json.Marshal(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "root", "turn": map[string]any{"id": "turn-1", "status": status}}})
	c.dispatch(raw)
}

func TestLongCodexTurnWithProgressDoesNotHaveWallClockCutoff(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	started := time.Unix(1_000_000, 0)
	// Cross the former 100-minute cutoff and then a full day, without changing
	// production thresholds or waiting in real time.
	for _, elapsed := range []time.Duration{99 * time.Minute, 100 * time.Minute, 101 * time.Minute, 6 * time.Hour, 24 * time.Hour} {
		now := started.Add(elapsed)
		st.touch(now.Add(-5 * time.Second))
		c.checkCodexTurnLiveness(s, st, now)
		if !st.turnActive || st.inactivityStopRequested {
			t.Fatalf("cut off live turn at %s", elapsed)
		}
	}
	if len(w.methods) != 0 {
		t.Fatal("sent interrupt for a live turn", w.methods)
	}
	completeLivenessTurn(c, "completed")
	if st.turnActive || st.turnErr != "" {
		t.Fatal("normal completion changed", st.turnErr)
	}
}
func TestDefaultInactivityOnlyWarnsAndNeverInterrupts(t *testing.T) {
	c, s, state, writer, sink := livenessFixture(t)
	for _, elapsed := range []time.Duration{6 * time.Minute, time.Hour, 24 * time.Hour} {
		c.checkCodexTurnLiveness(s, state, state.lastEventAt.Add(elapsed))
	}
	if len(writer.methods) != 0 || !state.turnActive || state.inactivityStopRequested {
		t.Fatal("default inactivity automatically stopped live work")
	}
	if sink.count(func(event any) bool {
		value, ok := event.(protocol.SessionWarning)
		return ok && strings.Contains(value.Message, "可手動停止")
	}) != 1 {
		t.Fatal("warning-only message missing or repeated")
	}
	completeLivenessTurn(c, "completed")
}
func TestManualStopFailureAndAckDoNotManufactureTerminal(t *testing.T) {
	c, s, state, writer, _ := livenessFixture(t)
	writer.reply = func(string, json.RawMessage) (any, error) { return nil, errors.New("interrupt unavailable") }
	if err := c.Stop(context.Background(), s); err == nil {
		t.Fatal("interrupt failure ignored")
	}
	if !state.turnActive || state.turnErr != "" || state.stopping {
		t.Fatal("failed stop finished turn or prevented retry")
	}
	writer.reply = func(string, json.RawMessage) (any, error) { return map[string]any{}, nil }
	if err := c.Stop(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if !state.turnActive || state.turnErr != "" {
		t.Fatal("RPC ACK was treated as native terminal")
	}
	completeLivenessTurn(c, "interrupted")
	if state.turnActive || state.turnErr != "stopped" {
		t.Fatal("confirmed native stop did not finish")
	}
}
func TestNaturalCompletionWinsRaceWithManualStopIntent(t *testing.T) {
	c, s, state, writer, sink := livenessFixture(t)
	writer.reply = func(method string, _ json.RawMessage) (any, error) {
		if method == "turn/start" {
			state.mu.Lock()
			state.stopping = true
			state.mu.Unlock()
			completeLivenessTurn(c, "completed")
			return map[string]any{"turn": map[string]string{"id": "turn-1", "status": "completed"}}, nil
		}
		return map[string]any{}, nil
	}
	c.runTurn(s, state, "root", nil, state.turnDone, "")
	if sink.count(func(event any) bool { _, ok := event.(protocol.Done); return ok }) != 1 {
		t.Fatal("confirmed natural completion lost")
	}
	if sink.count(func(event any) bool { _, ok := event.(protocol.Stopped); return ok }) != 0 {
		t.Fatal("stop intent overwrote natural completion")
	}
}

func TestCodexWaiterUsesProgressTicksNotElapsedDeadline(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	ticks := make(chan time.Time)
	returned := make(chan struct{})
	go func() { c.waitForCodexTurn(s, st, st.turnDone, ticks); close(returned) }()
	future := time.Now().Add(4 * time.Hour)
	st.touch(future.Add(-time.Second))
	ticks <- future
	close(ticks)
	completeLivenessTurn(c, "completed")
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("waiter did not settle")
	}
	if len(w.methods) != 0 {
		t.Fatal(w.methods)
	}
}

func TestInactivityWarnsOnceThenInterruptsOnlyTheCurrentTurn(t *testing.T) {
	c, s, st, w, sink := livenessFixture(t)
	c.stallAbortAfter = 30 * time.Minute // Explicit opt-in; production defaults to warning-only.
	start := st.lastEventAt
	c.checkCodexTurnLiveness(s, st, start.Add(6*time.Minute))
	c.checkCodexTurnLiveness(s, st, start.Add(7*time.Minute))
	if sink.count(func(e any) bool { _, ok := e.(protocol.SessionWarning); return ok }) != 1 {
		t.Fatal("warning not deduplicated")
	}
	w.reply = func(method string, p json.RawMessage) (any, error) {
		if method != "turn/interrupt" || !strings.Contains(string(p), `"threadId":"root"`) || !strings.Contains(string(p), `"turnId":"turn-1"`) {
			t.Fatalf("wrong interrupt %s %s", method, p)
		}
		return map[string]any{}, nil
	}
	c.checkCodexTurnLiveness(s, st, start.Add(30*time.Minute))
	if !st.turnActive {
		t.Fatal("RPC ACK prematurely released the queue")
	}
	c.checkCodexTurnLiveness(s, st, start.Add(31*time.Minute))
	if len(w.methods) != 1 {
		t.Fatal("repeated interrupt", w.methods)
	}
	completeLivenessTurn(c, "interrupted")
	if st.turnActive || st.turnErrorCode != codexInactivityTimeoutCode || !strings.Contains(st.turnErr, "無回應逾時") {
		t.Fatalf("lost inactivity reason code=%s error=%s", st.turnErrorCode, st.turnErr)
	}
}

func TestPendingHumanInputDoesNotConsumeInactivityWindow(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	c.interactions["question"] = codexInteraction{payload: backend.UserInputPayload{SessionID: s.ID, RequestID: "question", Status: "pending"}}
	future := time.Now().Add(48 * time.Hour)
	c.checkCodexTurnLiveness(s, st, future)
	if len(w.methods) != 0 || !st.turnActive {
		t.Fatal("interrupted pending human input")
	}
	delete(c.interactions, "question")
	c.checkCodexTurnLiveness(s, st, future.Add(time.Second))
	if len(w.methods) != 0 {
		t.Fatal("human wait counted as model inactivity")
	}
}

func TestAnswerResetsInactivityEvenBeforeNextModelEvent(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	st.lastEventAt = time.Now().Add(-24 * time.Hour)
	c.interactions["question"] = codexInteraction{payload: backend.UserInputPayload{SessionID: s.ID, RequestID: "question", Status: "pending"}}
	before := time.Now()
	if !c.RespondUserInput("question", map[string]any{"answer": "yes"}, false) {
		t.Fatal("answer rejected")
	}
	if st.lastEventAt.Before(before) {
		t.Fatal("answer did not reset inactivity")
	}
	c.checkCodexTurnLiveness(s, st, time.Now())
	if len(w.methods) != 0 {
		t.Fatal("interrupted immediately after answer")
	}
}

func TestUnconfirmedInactivityStopDoesNotFinishOrRetryTurn(t *testing.T) {
	c, s, st, w, sink := livenessFixture(t)
	c.stallAbortAfter = 30 * time.Minute
	w.reply = func(string, json.RawMessage) (any, error) { return nil, errors.New("connection unavailable") }
	c.checkCodexTurnLiveness(s, st, st.lastEventAt.Add(time.Hour))
	c.checkCodexTurnLiveness(s, st, st.lastEventAt.Add(2*time.Hour))
	if !st.turnActive || len(w.methods) != 1 {
		t.Fatal("unconfirmed stop finished/retried", w.methods)
	}
	if sink.count(func(e any) bool {
		v, ok := e.(protocol.SessionWarning)
		return ok && strings.Contains(v.Message, "尚未確認")
	}) != 1 {
		t.Fatal("missing unconfirmed-stop warning")
	}
}

func TestNaturalCompletionWinsRaceWithInactivityInterrupt(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	c.stallAbortAfter = 30 * time.Minute
	w.reply = func(string, json.RawMessage) (any, error) {
		completeLivenessTurn(c, "completed")
		return map[string]any{}, nil
	}
	c.checkCodexTurnLiveness(s, st, st.lastEventAt.Add(time.Hour))
	if st.turnActive || st.turnErr != "" {
		t.Fatal("natural completion became a timeout", st.turnErr)
	}
}

func TestManualAndExternalInterruptNeverBecomeSuccessfulCompletion(t *testing.T) {
	for _, manual := range []bool{false, true} {
		c, s, st, _, _ := livenessFixture(t)
		if manual {
			if err := c.Stop(context.Background(), s); err != nil {
				t.Fatal(err)
			}
			if !st.turnActive {
				t.Fatal("manual stop ACK prematurely finished the turn")
			}
			completeLivenessTurn(c, "interrupted")
		} else {
			completeLivenessTurn(c, "interrupted")
		}
		if st.turnActive || st.turnErr != "stopped" {
			t.Fatal("interrupted turn was treated as completed", st.turnErr)
		}
	}
}

func TestInactivityTerminalCarriesReasonAndNeverEmitsDone(t *testing.T) {
	c, s, st, w, sink := livenessFixture(t)
	c.stallAbortAfter = 30 * time.Minute
	c.stallCheckEvery = time.Millisecond
	st.lastEventAt = time.Now().Add(-31 * time.Minute)
	w.reply = func(method string, p json.RawMessage) (any, error) {
		switch method {
		case "turn/start":
			return map[string]any{"turn": map[string]string{"id": "turn-1", "status": "inProgress"}}, nil
		case "turn/interrupt":
			completeLivenessTurn(c, "interrupted")
			return map[string]any{}, nil
		default:
			t.Errorf("unexpected RPC %s", method)
			return map[string]any{}, nil
		}
	}
	returned := make(chan struct{})
	go func() { c.runTurn(s, st, "root", nil, st.turnDone, ""); close(returned) }()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not settle on correlated interruption")
	}
	if sink.count(func(e any) bool {
		v, ok := e.(protocol.Error)
		return ok && v.SessionID == "s1" && v.RequestID == "request-1" && v.Code == codexInactivityTimeoutCode && strings.Contains(v.Message, "無回應逾時")
	}) != 1 {
		t.Fatal("missing typed inactivity error")
	}
	if sink.count(func(e any) bool { _, ok := e.(protocol.Done); return ok }) != 0 {
		t.Fatal("inactivity emitted successful completion")
	}
}

func TestLateLivenessTickCannotInterruptAnAlreadyFinishedTurn(t *testing.T) {
	c, s, st, w, _ := livenessFixture(t)
	start := st.lastEventAt
	completeLivenessTurn(c, "completed")
	c.checkCodexTurnLiveness(s, st, start.Add(48*time.Hour))
	if len(w.methods) != 0 {
		t.Fatal("interrupted an already completed turn")
	}
}
