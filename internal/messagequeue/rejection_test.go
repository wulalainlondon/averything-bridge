package messagequeue

import (
	"errors"
	"testing"
)

func TestNegativeReceiptSurvivesRestartAndNeverBecomesQueued(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Reject("s", "old", "pm_takeover_required"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, payload := range []string{"original forbidden work", "changed payload"} {
		_, inserted, err := s.Enqueue(Entry{SessionID: "s", RequestID: "old", Content: payload, Payload: []byte(payload)})
		if inserted || !errors.Is(err, ErrRejected) {
			t.Fatalf("rejection replayed: %v %v", inserted, err)
		}
	}
	if _, found, err := s.Get("s", "old"); err != nil || found {
		t.Fatal("negative receipt entered execution queue")
	}
	if _, inserted, err := s.Enqueue(Entry{SessionID: "s", RequestID: "new", Content: "explicit new message", Payload: []byte("new")}); err != nil || !inserted {
		t.Fatal(err)
	}
	if err := s.Reject("s", "new", "late stale rejection"); err != nil {
		t.Fatal(err)
	}
	e, inserted, err := s.Enqueue(Entry{SessionID: "s", RequestID: "new", Content: "explicit new message", Payload: []byte("new")})
	if err != nil || inserted || e.State != Queued {
		t.Fatal("accepted receipt corrupted", e, err)
	}
}
