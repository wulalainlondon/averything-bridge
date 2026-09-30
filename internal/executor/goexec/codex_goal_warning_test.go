package goexec

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"github.com/coder/websocket"
)

func TestCodexGoalFailureDoesNotEmitTurnError(t *testing.T) {
	for _, operation := range []string{"get", "set", "clear"} {
		t.Run(operation, func(t *testing.T) {
			sink := &capSink{}
			c := NewCodex(sink, "codex")
			// Use an in-memory RPC transport without starting a real daemon.
			c.appServerMode = "daemon"
			c.remoteConn = &websocket.Conn{}
			writer := &rpcCaptureWriter{writes: make(chan []byte, 1)}
			c.rpc.setWriter(writer)
			s := session.NewRegistry().Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "thread-1")
			st := c.state(s.ID)
			st.threadID, st.reqID, st.currentTurnID, st.turnActive = "thread-1", "r1", "turn-1", true
			go func() {
				var req struct {
					ID int `json:"id"`
				}
				if err := json.Unmarshal(<-writer.writes, &req); err != nil {
					t.Error(err)
					return
				}
				c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,"error":{"code":-32000,"message":"goal operation failed"}}`, req.ID)))
			}()
			var err error
			switch operation {
			case "get":
				err = c.GetGoal(context.Background(), s)
			case "set":
				err = c.SetGoal(context.Background(), s, "ship", "active", nil)
			case "clear":
				err = c.ClearGoal(context.Background(), s)
			}
			if err == nil {
				t.Fatal("expected Goal RPC failure")
			}
			if len(sink.events) != 1 {
				t.Fatalf("events = %+v", sink.events)
			}
			if warning, ok := sink.events[0].(protocol.SessionWarning); !ok || warning.SessionID != s.ID {
				t.Fatalf("want nonterminal session warning, got %+v", sink.events)
			}
			if !st.turnActive || st.reqID != "r1" || st.currentTurnID != "turn-1" {
				t.Fatal("Goal failure changed active turn")
			}
		})
	}
}
