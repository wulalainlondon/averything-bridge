package session

import (
	"testing"
	"time"
)

func TestCloseAndWaitJoinsActiveCallback(t *testing.T) {
	s := NewRegistry().Create("synthetic-join", "synthetic", "", "codex", "", "read-only", "")
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	if !s.SubmitNamed("synthetic", func() { close(started); <-release }) {
		t.Fatal("submit failed")
	}
	<-started
	go func() { s.CloseAndWait(); close(returned) }()
	select {
	case <-returned:
		t.Fatal("join returned with actor still active")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("join did not finish")
	}
	s.CloseAndWait()
}
