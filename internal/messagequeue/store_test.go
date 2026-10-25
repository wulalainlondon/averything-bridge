package messagequeue

import (
	"errors"
	"testing"
)

func fixture(id string) Entry {
	return Entry{SessionID: "s1", RequestID: id, Content: id, Payload: []byte(`{"content":"` + id + `"}`)}
}
func TestDurableQueueAndUnknownExecutionRecovery(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.Enqueue(fixture("a")); err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.Enqueue(fixture("b")); err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.Transition("s1", "a", []State{Queued}, Steering, "", "active", "turn"); err != nil {
		t.Fatal(err)
	}
	store.Close()
	restored, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if err = restored.Recover(); err != nil {
		t.Fatal(err)
	}
	a, ok, err := restored.Get("s1", "a")
	if err != nil || !ok || a.State != Uncertain {
		t.Fatalf("unsafe recovery: %+v %v", a, err)
	}
	queued, err := restored.Queued()
	if err != nil || len(queued) != 1 || queued[0].RequestID != "b" {
		t.Fatalf("lost pending message: %+v %v", queued, err)
	}
	snapshot, err := restored.Snapshot("s1")
	if err != nil || snapshot.Revision != 4 {
		t.Fatalf("revision: %+v %v", snapshot, err)
	}
}
func TestDurableReceiptDeduplicatesAfterCompletion(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e := fixture("a")
	if _, created, err := store.Enqueue(e); err != nil || !created {
		t.Fatal(created, err)
	}
	if _, _, err = store.Transition("s1", "a", []State{Queued}, Completed, "", "", ""); err != nil {
		t.Fatal(err)
	}
	if existing, created, err := store.Enqueue(e); err != nil || created || existing.State != Completed {
		t.Fatalf("duplicate replay: %+v %v %v", existing, created, err)
	}
	e.Payload = []byte(`{"content":"different"}`)
	if _, _, err := store.Enqueue(e); !errors.Is(err, ErrConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
}
