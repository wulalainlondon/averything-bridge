package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"testing"
)

type questionObserverFixture struct {
	transportFixtureProvider
	evidence []backend.TaskQuestionEvidence
}

func (p *questionObserverFixture) ObserveTaskQuestion(_ context.Context, e backend.TaskQuestionEvidence) {
	p.evidence = append(p.evidence, e)
}
func TestOwnedNativeQuestionsObserveOnlyExactActiveTurn(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	observer := &questionObserverFixture{}
	c.SetTaskAPIProvider(observer)
	s := session.NewRegistry().Create("child", "child", t.TempDir(), "codex", "", "read-only", "")
	c.threadToSession["original-thread"] = s
	st := c.state(s.ID)
	st.threadID = "original-thread"
	st.currentTurnID = "original-turn"
	st.reqID = "original-request"
	st.turnActive = true
	cases := []struct {
		kind    string
		run     func(any, json.RawMessage) bool
		payload string
	}{
		{"user", c.createUserInputRequest, `{"threadId":"original-thread","turnId":"original-turn","itemId":"user-call","questions":[{"question":"Choose?"}]}`},
		{"mcp", c.createMcpElicitationRequest, `{"threadId":"original-thread","turnId":"original-turn","elicitationId":"mcp-call","mode":"url","url":"https://example.invalid","message":"Authorize"}`},
		{"dynamic", c.createDynamicToolRequest, `{"threadId":"original-thread","turnId":"original-turn","callId":"dynamic-call","tool":"custom","arguments":{}}`},
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			before := len(observer.evidence)
			if !tc.run(1, json.RawMessage(tc.payload)) || len(observer.evidence) != before+1 {
				t.Fatal("owned question not forwarded")
			}
			got := observer.evidence[before]
			if got.RequestID != "original-request" || got.TurnID != "original-turn" || got.CallID == "" || got.Session != s {
				t.Fatal("ownership changed")
			}
			st.turnStartPending = true
			tc.run(2, json.RawMessage(tc.payload))
			st.turnStartPending = false
			st.currentTurnID = "replacement-turn"
			tc.run(3, json.RawMessage(tc.payload))
			st.currentTurnID = "original-turn"
			if len(observer.evidence) != before+1 {
				t.Fatal("foreign/pending turn forwarded")
			}
		})
	}
}
