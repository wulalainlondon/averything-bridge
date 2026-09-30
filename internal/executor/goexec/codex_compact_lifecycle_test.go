package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestLiveCompactLifecycle(t *testing.T) {
	if os.Getenv("BRIDGE_TEST_COMPACT_LIVE") != "1" {
		t.Skip("explicit live compact probe")
	}
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	dataDir := t.TempDir()
	c.SetDataDir(dataDir)
	c.appServerMode = "daemon"
	defer func() { c.remoteReconnect = false; c.startMu.Lock(); _ = c.stopServerLocked(); c.startMu.Unlock() }()
	reg := session.NewRegistry()
	s := reg.Create("compact-lifecycle-qa", "compact lifecycle QA", t.TempDir(), "codex", "gpt-6-astra", "read-only", "")
	if err := c.Send(context.Background(), s, "qa-seed", "Connectivity test only. Do not use tools or read files. Reply exactly COMPACT_QA_OK.", nil, nil); err != nil {
		t.Fatal(err)
	}
	waitForCodexDone(t, sink, 60*time.Second)
	st := c.state(s.ID)
	started := time.Now()
	result := make(chan error, 1)
	var recovered *Codex
	go func() {
		result <- c.runCompact(st, time.Millisecond, func() {
			recovered = NewCodex(&capSink{}, "codex")
			recovered.SetDataDir(dataDir)
		})
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("no correlated compact completion within 60s")
	}
	t.Logf("real daemon compact completed after local wait deadline: %s thread=%s", time.Since(started), s.ResumeID())
	if recovered == nil {
		t.Fatal("missing persisted slow operation")
	}
	r, err := recovered.ReconcileMaintenance(context.Background(), s.ID, false)
	if err != nil || r.State != "completed" {
		t.Fatalf("restart reconciliation: %+v %v", r, err)
	}
	t.Log("fresh backend recovered the completed operation using its saved turn ID")
	if err := c.Send(context.Background(), s, "qa-after-compact", "Connectivity test only. Do not use tools. Reply exactly AFTER_COMPACT_OK.", nil, nil); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(40 * time.Second)
	for sink.count(func(e any) bool { v, ok := e.(protocol.Done); return ok && v.RequestID == "qa-after-compact" }) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("next turn did not finish after compact")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if sink.count(func(e any) bool { v, ok := e.(protocol.TextChunk); return ok && v.RequestID == "qa-after-compact" }) == 0 {
		t.Fatal("next turn falsely completed without response")
	}
}

func TestCompactWaitDeadlineDoesNotFinishOperation(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	st := newCodexState()
	st.threadID = "thread-1"
	writer := &rpcCaptureWriter{writes: make(chan []byte, 1)}
	c.rpc.setWriter(writer)
	slow := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- c.runCompact(st, 10*time.Millisecond, func() { close(slow) }) }()
	request := <-writer.writes
	var frame struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(request, &frame); err != nil {
		t.Fatal(err)
	}
	c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{}}`, frame.ID)))
	select {
	case <-slow:
	case <-time.After(time.Second):
		t.Fatal("missing slow notification")
	}
	select {
	case err := <-result:
		t.Fatalf("premature completion: %v", err)
	default:
	}
	st.mu.Lock()
	active := st.compactActive
	st.mu.Unlock()
	if !active {
		t.Fatal("wait deadline cleared actual operation")
	}
	st.finishCompact("")
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("late completion not observed")
	}
}

func TestCompactRejectsUnrelatedTerminal(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	reg := session.NewRegistry()
	s := reg.Create("s1", "codex", "/tmp", "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID = "thread-1"
	st.compactActive = true
	st.compactTurnID = "compact-2"
	st.compactDone = make(chan struct{})
	c.threadToSession[st.threadID] = s
	c.dispatch(json.RawMessage(`{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"old-turn","status":"completed"}}}`))
	c.dispatch(json.RawMessage(`{"method":"thread/compacted","params":{"threadId":"thread-1"}}`))
	select {
	case <-st.compactDone:
		t.Fatal("uncorrelated event released compact")
	default:
	}
	c.dispatch(json.RawMessage(`{"method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"compact-2","status":"completed"}}}`))
	select {
	case <-st.compactDone:
	default:
		t.Fatal("correlated completion ignored")
	}
}

func TestCodexSafetyCode(t *testing.T) {
	for _, info := range []string{`"misalignment_policy_violation"`, `"misalignmentPolicyViolation"`} {
		if got := codexErrorCode(json.RawMessage(info), "provider denied"); got != "misalignment_policy_violation" {
			t.Fatal(got)
		}
	}
	if got := codexErrorCode(nil, "This request was blocked by our safety systems."); got != "misalignment_policy_violation" {
		t.Fatal(got)
	}
	if got := codexErrorCode(nil, "connection timeout"); got != "turn_error" {
		t.Fatal(got)
	}
}
