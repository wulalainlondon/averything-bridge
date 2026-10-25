package messagequeue

import (
	"encoding/json"
	"testing"
)

func TestTaskAdmissionPersistsOriginAcrossTerminalAndReopen(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []State{Completed, Failed, Cancelled, Steered} {
		id := string(state)
		payload := []byte(`{"owner_device":"owner","task_origin":{"instance_id":"local","session_id":"parent","thread_id":"source-thread","request_id":"r_parent123","config_revision":1},"expected_target":{"thread_id":"child-thread","revision":2},"content":"original"}`)
		_, _, err = store.Enqueue(Entry{SessionID: "child", RequestID: id, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		_, _, err = store.Transition("child", id, []State{Queued}, state, "", "", "")
		if err != nil {
			t.Fatal(err)
		}
	}
	// The ordinary legacy empty terminal payload must never break the typed query.
	_, _, err = store.Enqueue(Entry{SessionID: "legacy", RequestID: "old", Payload: []byte(`{"content":"legacy"}`)})
	if err != nil {
		t.Fatal(err)
	}
	store.Transition("legacy", "old", []State{Queued}, Completed, "", "", "")
	store.Close()
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rows, err := store.OriginEntries("local", "parent")
	if err != nil || len(rows) != 4 {
		t.Fatal(rows, err)
	}
	for _, e := range rows {
		data, found, err := store.TaskAdmission(e.SessionID, e.RequestID)
		if err != nil || !found || !json.Valid(data) {
			t.Fatal(string(data), found, err)
		}
	}
	if rows, err := store.OriginEntries("other", "parent"); err != nil || len(rows) != 0 {
		t.Fatal("foreign authority matched", rows, err)
	}
}
