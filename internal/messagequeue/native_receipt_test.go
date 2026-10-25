package messagequeue

import (
	"errors"
	"testing"
)

func TestExactNativeReceiptConflictSurvivesReopenAndNoAbsenceOnError(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.RecordNativeAcceptance("s", "r", "thread", "turn"); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordNativeAcceptance("s", "r", "thread", "turn"); err != nil {
		t.Fatal(err)
	}
	if err = s.RecordNativeAcceptance("s", "r", "wrong", "turn2"); !errors.Is(err, ErrNativeConflict) {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := s.ExactNativeAcceptance("s", "r"); found || !errors.Is(err, ErrNativeConflict) {
		t.Fatal(found, err)
	}
	if _, found, err := s.ExactNativeAcceptance("s", "missing"); found || err != nil {
		t.Fatal(found, err)
	}
	s.Close()
	if _, found, err := s.ExactNativeAcceptance("s", "missing"); found || err == nil {
		t.Fatal("database failure became absence")
	}
}

func TestCancelWaitingNativeRaceAndPayloadCAS(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, id := range []string{"before", "after", "hash"} {
		if _, _, err = s.Enqueue(Entry{SessionID: "s", RequestID: id, Payload: []byte(`{"content":"waiting"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	e, _, _ := s.Get("s", "before")
	s.RecordNativeAcceptance("s", "before", "original-thread", "original-turn")
	if _, changed, err := s.CancelWaiting("s", "before", e.PayloadHash, nil); changed || err != nil {
		t.Fatal("native acceptance lost cancellation race", changed, err)
	}
	e, _, _ = s.Get("s", "hash")
	if _, changed, err := s.CancelWaiting("s", "hash", "wrong", nil); changed || err != nil {
		t.Fatal("payload CAS bypassed")
	}
	e, _, _ = s.Get("s", "after")
	if _, changed, err := s.CancelWaiting("s", "after", e.PayloadHash, nil); !changed || err != nil {
		t.Fatal(changed, err)
	}
	if err = s.RecordNativeAcceptance("s", "after", "original-thread", "late-native"); err != nil {
		t.Fatal(err)
	}
	after, _, _ := s.Get("s", "after")
	proof, found, err := s.ExactNativeAcceptance("s", "after")
	if after.State != Cancelled || !found || err != nil || proof.TurnID != "late-native" {
		t.Fatal("late consumption erased or mislabelled cancellation", after, proof, err)
	}
}

func TestCancelWaitingRevisionCASAcrossStores(t *testing.T) {
	dir := t.TempDir()
	first, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	entry, _, err := first.Enqueue(Entry{SessionID: "s", RequestID: "original", Payload: []byte(`{"content":"waiting"}`)})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := first.Snapshot("s")
	expected := snapshot.Revision
	second.Enqueue(Entry{SessionID: "s", RequestID: "independent", Payload: []byte(`{"content":"other waiting input"}`)})
	if _, changed, err := first.CancelWaiting("s", "original", entry.PayloadHash, &expected); changed || err != nil {
		t.Fatal("cross-process stale revision cancelled", changed, err)
	}
	after, _, _ := first.Get("s", "original")
	if after.State != Queued {
		t.Fatal("stale cancellation mutated receipt")
	}
}
