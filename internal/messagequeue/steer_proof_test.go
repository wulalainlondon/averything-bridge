package messagequeue

import "testing"

func TestStrictSteerProofForwardOnlyReopenAndAtomicRollback(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"legacy", "verified", "invalid"} {
		if _, _, err = s.Enqueue(Entry{SessionID: "s", RequestID: id, Payload: []byte(id)}); err != nil {
			t.Fatal(err)
		}
	}
	legacy, _, err := s.Transition("s", "legacy", []State{Queued}, Steered, "", "active", "turn")
	if err != nil || s.StrictSteerProven(legacy) {
		t.Fatal("legacy upgraded", err)
	}
	verified, _, err := s.Transition("s", "verified", []State{Queued}, Steered, "", "active", "turn", true)
	if err != nil || !s.StrictSteerProven(verified) {
		t.Fatal("valid proof missing", err)
	}
	snap, _ := s.Snapshot("s")
	original, _, _ := s.Get("s", "invalid")
	if _, _, err = s.Transition("s", "invalid", []State{Queued}, Steered, "", "invalid", "turn", true); err == nil {
		t.Fatal("self steer accepted")
	}
	now, _, _ := s.Get("s", "invalid")
	after, _ := s.Snapshot("s")
	if after.Revision != snap.Revision || now.State != Queued || now.PayloadHash != original.PayloadHash {
		t.Fatal("failed tx changed receipt")
	}
	forged := verified
	forged.TurnID = "other"
	if s.StrictSteerProven(forged) {
		t.Fatal("wrong tuple proof")
	}
	forged = verified
	forged.PayloadHash = "other"
	if s.StrictSteerProven(forged) {
		t.Fatal("wrong payload proof")
	}
	s.Close()
	s, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, id := range []string{"legacy", "verified"} {
		e, _, _ := s.Get("s", id)
		if s.StrictSteerProven(e) != (id == "verified") {
			t.Fatal("reopen promoted/lost proof", id)
		}
	}
}
