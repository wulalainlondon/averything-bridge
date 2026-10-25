package runtimejournal

import "testing"

func TestCallbackSourceEpochPersistsWithoutReading(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("s", "running", "r", 0, "", "")
	s.Update("s", "idle", "r", 0, "completed", "")
	before := s.Snapshot("phone", []string{"s"})[0]
	epoch, e := s.SourceEpoch("s")
	if e != nil || epoch == "" {
		t.Fatal(epoch, e)
	}
	after := s.Snapshot("phone", []string{"s"})[0]
	if before.Unread != after.Unread || after.Unread != 1 {
		t.Fatal(before, after)
	}
	reopened := New(dir)
	again, e := reopened.SourceEpoch("s")
	if e != nil || again != epoch {
		t.Fatal(again, epoch, e)
	}
}
