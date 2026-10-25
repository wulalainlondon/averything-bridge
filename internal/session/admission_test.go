package session

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestQueuedAdmissionKeepsHeadCancellableAndDoesNotOvertake(t *testing.T) {
	s := newIdle("waiting")
	defer s.CloseAndWait()
	checked, release := make(chan struct{}), make(chan struct{})
	started := make(chan string, 3)
	s.SubmitNamedWhenReady("a", func() bool { close(checked); <-release; return true }, func() { started <- "a"; s.EndTurn() })
	<-checked
	if s.State() != Idle || s.QueueLen() != 1 || s.ActiveQueuedID() != "" {
		t.Fatal("waiting head claimed a turn")
	}
	s.SubmitNamed("b", func() { started <- "b"; s.EndTurn() })
	finish, err := s.ReserveWaiting("a")
	if err != nil {
		t.Fatal(err)
	}
	finish(true)
	close(release)
	select {
	case got := <-started:
		if got != "b" {
			t.Fatal("cancelled head executed", got)
		}
	case <-time.After(time.Second):
		t.Fatal("queue did not continue")
	}
}

func TestDeferredUnsubmittedHeadRetainsOriginalFIFO(t *testing.T) {
	s := newIdle("race")
	defer s.CloseAndWait()
	var ready atomic.Bool
	starts := make(chan string, 4)
	deferred := make(chan struct{})
	var attempts atomic.Int32
	var run func()
	run = func() {
		if attempts.Add(1) == 1 {
			if !s.DeferActiveNamed("a", ready.Load, run) {
				t.Error("cannot defer active head")
			}
			close(deferred)
			return
		}
		starts <- "a"
		s.EndTurn()
	}
	s.SubmitNamed("a", run)
	<-deferred
	s.SubmitNamed("b", func() { starts <- "b"; s.EndTurn() })
	select {
	case got := <-starts:
		t.Fatal("overtook busy head", got)
	case <-time.After(30 * time.Millisecond):
	}
	ready.Store(true)
	// A queue change wakes the metadata gate without waiting for its timer.
	s.SubmitNamed("c", func() { starts <- "c"; s.EndTurn() })
	for _, want := range []string{"a", "b", "c"} {
		select {
		case got := <-starts:
			if got != want {
				t.Fatal(got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("queue stalled")
		}
	}
	if attempts.Load() != 2 {
		t.Fatal("duplicate original execution")
	}
}
