package runtimejournal

import "testing"

func TestObservedTurnResumesSameIdentityAfterTransportInterruption(t *testing.T) {
	s := New(t.TempDir())
	s.Update("s1", "running", "native-request", 0, "", "")
	s.Update("s1", "interrupted", "native-request", 0, "interrupted", "connection lost")
	view, changed := s.Update("s1", "running", "native-request", 0, "", "")
	if !changed || view.Phase != "running" || view.ActiveRequestID != "native-request" || view.LastTerminal != "" || view.CompletedAt != 0 || view.LastError != "" {
		t.Fatalf("live native activity retained the interruption: %+v", view)
	}
}
