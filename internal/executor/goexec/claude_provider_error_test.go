package goexec

import (
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"strings"
	"testing"
)

func TestClaudeOrdinaryAuthenticationErrorWithoutHistoryFlagIsNotCompletion(t *testing.T) {
	sink := &pmTestSink{}
	c := &Claude{sink: sink}
	p := &proc{reqID: "request", cancel: func() {}}
	s := session.NewRegistry().Create("ordinary", "QA", "/tmp", "claude", "", "read-only", "")
	c.readStdout(s, p, strings.NewReader(`{"type":"assistant","error":"authentication_failed","message":{"content":[{"type":"text","text":"Failed to authenticate: OAuth session expired and could not be refreshed"}]}}
{"type":"result","subtype":"success"}
`))
	if len(sink.events) != 1 {
		t.Fatalf("events=%+v", sink.events)
	}
	e, ok := sink.events[0].(protocol.Error)
	if !ok || e.RequestID != "request" || e.Code != "claude_provider_authentication_failed" {
		t.Fatal(sink.events)
	}
}

func TestClaudeErrorResultCannotBeSealedAsSuccessfulAnswer(t *testing.T) {
	sink := &pmTestSink{}
	c := &Claude{sink: sink}
	p := &proc{reqID: "request", cancel: func() {}}
	s := session.NewRegistry().Create("ordinary", "QA", "/tmp", "claude", "", "read-only", "")
	c.readStdout(s, p, strings.NewReader(`{"type":"result","subtype":"success","is_error":true,"result":"Authentication failed"}
`))
	if len(sink.events) != 1 {
		t.Fatalf("events=%+v", sink.events)
	}
	if _, ok := sink.events[0].(protocol.Error); !ok {
		t.Fatal(sink.events)
	}
}
