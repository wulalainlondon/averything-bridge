package goexec

import (
	"encoding/json"
	"everything-go/internal/protocol"
	"testing"
)

func TestNativeAutomaticCompactionProgress(t *testing.T) {
	c, st, sink := observedCodexFixture(t)
	c.dispatch(json.RawMessage(`{"method":"turn/started","params":{"threadId":"root","turn":{"id":"native-1"}}}`))
	c.dispatch(json.RawMessage(`{"method":"item/started","params":{"threadId":"root","turnId":"native-1","item":{"id":"compact-item","type":"contextCompaction"}}}`))
	var last protocol.TurnProgress
	for _, event := range sink.events {
		if p, ok := event.(protocol.TurnProgress); ok {
			last = p
		}
	}
	if last.Stage != "composing" || last.Message != "正在整理上下文；後續訊息會依序處理" || last.RequestID != "codex_external_native-1" {
		t.Fatalf("missing automatic compact progress: %+v", last)
	}
	c.dispatch(json.RawMessage(`{"method":"item/completed","params":{"threadId":"root","turnId":"native-1","item":{"id":"compact-item","type":"contextCompaction"}}}`))
	last = sink.events[len(sink.events)-1].(protocol.TurnProgress)
	if last.Stage != "thinking" || last.Message != "" || st.observedTurnID != "native-1" {
		t.Fatalf("compaction completion must continue the same turn: %+v", last)
	}
	c.dispatch(json.RawMessage(`{"method":"turn/completed","params":{"threadId":"root","turn":{"id":"native-1","status":"completed"}}}`))
	before := len(sink.events)
	c.dispatch(json.RawMessage(`{"method":"item/started","params":{"threadId":"root","turnId":"native-1","item":{"id":"late","type":"contextCompaction"}}}`))
	if len(sink.events) != before {
		t.Fatal("late compaction resurrected a completed turn")
	}
}

func TestChildCompactionDoesNotChangeRootProgress(t *testing.T) {
	c, _, sink := observedCodexFixture(t)
	c.threadToSession["child"] = c.threadToSession["root"]
	c.dispatch(json.RawMessage(`{"method":"item/started","params":{"threadId":"child","turnId":"child-1","item":{"id":"compact-item","type":"contextCompaction"}}}`))
	for _, event := range sink.events {
		if _, ok := event.(protocol.TurnProgress); ok {
			t.Fatal("child compaction changed root progress")
		}
	}
}
