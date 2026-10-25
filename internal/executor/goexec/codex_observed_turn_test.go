package goexec

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func observedCodexFixture(t *testing.T) (*Codex, *codexState, *capSink) {
	t.Helper()
	sink := &capSink{}
	c := NewCodex(sink, "codex")
	c.dataDir = t.TempDir()
	s := session.NewRegistry().Create("s1", "Ghostty", t.TempDir(), "codex", "", "", "")
	st := c.state(s.ID)
	st.threadID, st.reqID = "root", "previous-bridge-request"
	c.threadToSession["root"] = s
	return c, st, sink
}

func TestCodexObservedTurnLifecycle(t *testing.T) {
	for _, status := range []string{"completed", "interrupted", "failed"} {
		t.Run(status, func(t *testing.T) {
			c, st, sink := observedCodexFixture(t)
			c.dispatch(json.RawMessage(`{"method":"turn/started","params":{"threadId":"root","turn":{"id":"native-1"}}}`))
			c.dispatch(json.RawMessage(`{"method":"turn/started","params":{"threadId":"root","turn":{"id":"native-1"}}}`))
			c.dispatch(json.RawMessage(`{"method":"item/agentMessage/delta","params":{"threadId":"root","turnId":"native-1","delta":"progress","phase":"commentary"}}`))
			c.dispatch(json.RawMessage(fmt.Sprintf(`{"method":"turn/completed","params":{"threadId":"root","turn":{"id":"native-1","status":%q}}}`, status)))
			before := len(sink.events)
			c.dispatch(json.RawMessage(`{"method":"item/agentMessage/delta","params":{"threadId":"root","turnId":"native-1","delta":"late"}}`))
			c.dispatch(json.RawMessage(`{"method":"turn/started","params":{"threadId":"root","turn":{"id":"native-1"}}}`))
			if len(sink.events) != before {
				t.Fatal("late activity resurrected a terminal turn")
			}
			var lifecycle []backend.ObservedTurn
			for _, event := range sink.events {
				switch e := event.(type) {
				case backend.ObservedTurn:
					lifecycle = append(lifecycle, e)
				case protocol.TextChunk:
					if e.RequestID != "codex_external_native-1" {
						t.Fatalf("stale request identity: %+v", e)
					}
				}
			}
			if len(lifecycle) != 2 || lifecycle[0].Phase != "running" || lifecycle[1].Phase != status {
				t.Fatalf("lifecycle=%+v", lifecycle)
			}
			if st.turnActive || st.turnDone != nil || st.reqID != "previous-bridge-request" || st.observedTurnID != "" {
				t.Fatal("observation mutated Bridge execution ownership")
			}
			journal, err := c.loadTurnRequests("root")
			if err != nil || journal.Requests["native-1"] != "codex_external_native-1" {
				t.Fatalf("history identity: %+v %v", journal, err)
			}
		})
	}
}

func TestCodexObservedTurnRepairsMissedStartAndRejectsOldTerminal(t *testing.T) {
	c, st, sink := observedCodexFixture(t)
	c.dispatch(json.RawMessage(`{"method":"item/started","params":{"threadId":"root","turnId":"native-current","item":{"id":"tool-1","type":"commandExecution","command":"pwd"}}}`))
	if st.observedTurnID != "native-current" || len(sink.events) == 0 {
		t.Fatal("live event did not recover missed start")
	}
	start, ok := sink.events[0].(backend.ObservedTurn)
	if !ok || start.Phase != "running" {
		t.Fatalf("lifecycle must precede content: %+v", sink.events)
	}
	c.dispatch(json.RawMessage(`{"method":"turn/completed","params":{"threadId":"root","turn":{"id":"previous-native","status":"completed"}}}`))
	if st.observedTurnID != "native-current" {
		t.Fatal("old terminal ended the current observation")
	}
	st.turnActive, st.currentTurnID, st.reqID, st.turnDone = true, "owned", "owned-request", make(chan struct{})
	c.dispatch(json.RawMessage(`{"method":"turn/completed","params":{"threadId":"root","turn":{"id":"native-current","status":"completed"}}}`))
	if !st.turnActive {
		t.Fatal("external terminal released owned turn")
	}
	select {
	case <-st.turnDone:
		t.Fatal("owned waiter released")
	default:
	}
}

func TestCodexObservedTurnDoesNotInferActivityFromMetadata(t *testing.T) {
	c, st, _ := observedCodexFixture(t)
	c.dispatch(json.RawMessage(`{"method":"thread/tokenUsage/updated","params":{"threadId":"root","turnId":"native-1"}}`))
	c.dispatch(json.RawMessage(`{"method":"item/agentMessage/delta","params":{"threadId":"root","delta":"uncorrelated"}}`))
	if st.observedTurnID != "" {
		t.Fatal("uncorrelated metadata fabricated a running turn")
	}
}

func TestCodexNativeFileObservationNeedsNoDaemonOrWriterClaim(t *testing.T) {
	c, st, sink := observedCodexFixture(t)
	s := c.threadToSession["root"]
	s.SetResumeID("root")
	st.threadID = ""
	c.ObserveNativeLifecycle(s, "native-cold", "running")
	if st.observedTurnID != "native-cold" || st.threadID != "" || st.turnActive || c.remoteConn != nil {
		t.Fatal("native file observation claimed execution ownership")
	}
	journal, err := c.loadTurnRequests("root")
	if err != nil || journal.Requests["native-cold"] != "codex_external_native-cold" {
		t.Fatal("native file identity was not persisted")
	}
	c.ObserveNativeLifecycle(s, "native-cold", "completed")
	if st.observedTurnID != "" || sink.events[len(sink.events)-1].(backend.ObservedTurn).Phase != "completed" {
		t.Fatal("native file terminal lost")
	}
}

func TestCodexObservedTurnRetainsDurableIdentityAndRecoversTransportLoss(t *testing.T) {
	c, st, sink := observedCodexFixture(t)
	if err := c.rememberTurnRequest("root", "native-1", "existing-request"); err != nil {
		t.Fatal(err)
	}
	activity := json.RawMessage(`{"method":"item/agentMessage/delta","params":{"threadId":"root","turnId":"native-1","delta":"progress"}}`)
	c.dispatch(activity)
	if st.observedRequestID != "existing-request" {
		t.Fatal("reattachment split the request identity")
	}
	c.failLiveOperations("test transport loss")
	last := sink.events[len(sink.events)-1].(backend.ObservedTurn)
	if last.Phase != "interrupted" || last.RequestID != "existing-request" || st.observedTurnID != "" {
		t.Fatalf("disconnect did not settle observation: %+v", last)
	}
	c.dispatch(activity)
	if st.observedRequestID != "existing-request" {
		t.Fatal("live activity could not recover the same native turn")
	}
}

func TestCodexStopObservedTurnWaitsForNativeConfirmation(t *testing.T) {
	c, st, sink := observedCodexFixture(t)
	c.dispatch(json.RawMessage(`{"method":"turn/started","params":{"threadId":"root","turn":{"id":"native-1"}}}`))
	writer := &rpcCaptureWriter{writes: make(chan []byte, 1)}
	c.rpc.setWriter(writer)
	go func() {
		var frame struct {
			ID     int            `json:"id"`
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.Unmarshal(<-writer.writes, &frame)
		if frame.Method != "turn/interrupt" || frame.Params["turnId"] != "native-1" {
			t.Errorf("wrong interrupt target: %+v", frame)
		}
		c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"result":{}}`, frame.ID)))
	}()
	if err := c.Stop(context.Background(), c.threadToSession["root"]); err != nil {
		t.Fatal(err)
	}
	if st.observedTurnID != "native-1" {
		t.Fatal("RPC ACK prematurely completed native turn")
	}
	if got := sink.count(func(e any) bool { o, ok := e.(backend.ObservedTurn); return ok && o.Phase != "running" }); got != 0 {
		t.Fatal("unconfirmed terminal was emitted")
	}
	c.dispatch(json.RawMessage(`{"method":"turn/completed","params":{"threadId":"root","turn":{"id":"native-1","status":"interrupted"}}}`))
	if st.observedTurnID != "" {
		t.Fatal("native interrupt confirmation did not end observation")
	}
}
