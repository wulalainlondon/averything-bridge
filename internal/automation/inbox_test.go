package automation

import (
	"context"
	"encoding/json"
	"testing"
)

func TestEventAssignmentPersistsOriginalIntent(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, "i1")
	if err != nil {
		t.Fatal(err)
	}
	a := EventAssignment{ID: "r_original", EventIDs: []string{"e1"}, SessionID: "s1", OwnerInstanceID: "i1", ThreadID: "thread", ConfigRevision: 2, OwnerDevice: "phone", Mode: "analyze", Content: "trusted policy + data"}
	first, err := s.SaveEventAssignment(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.SaveEventAssignment(context.Background(), a)
	if err != nil || second.CreatedAt != first.CreatedAt {
		t.Fatalf("retry changed receipt: %+v %v", second, err)
	}
	changed := a
	changed.SessionID = "s2"
	if _, err = s.SaveEventAssignment(context.Background(), changed); err != ErrConflict {
		t.Fatalf("changed original target accepted: %v", err)
	}
	changed = a
	changed.ID = "r_duplicate"
	if _, err = s.SaveEventAssignment(context.Background(), changed); err != ErrConflict {
		t.Fatalf("event duplicated: %v", err)
	}
	s.Close()
	s, err = Open(dir, "i1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	items, err := s.EventAssignments(context.Background())
	if err != nil || len(items) != 1 || items[0].Content != a.Content {
		t.Fatalf("restart lost original: %+v %v", items, err)
	}
}

func TestEmptyAutomationSnapshotUsesArrays(t *testing.T) {
	s, err := Open(t.TempDir(), "i1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	snapshot, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	_ = json.Unmarshal(raw, &payload)
	for _, key := range []string{"accounts", "poll_state", "routes", "bindings", "batches", "proposals", "assignments"} {
		if payload[key] == nil {
			t.Fatalf("%s serialized as null", key)
		}
	}
}

func TestExplicitReprocessingComparesOwnersAndKeepsOriginalReceipts(t *testing.T) {
	s, err := Open(t.TempDir(), "i1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a := EventAssignment{ID: "r_one", SourceInstanceID: "i1", EventIDs: []string{"e1", "e2"}, SessionID: "s1", OwnerDevice: "phone", Mode: "analyze", Content: "original"}
	first, err := s.SaveEventAssignment(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	b := a
	b.ID = "r_two"
	b.SessionID = "s2"
	b.EventIDs = []string{"e1"}
	b.ReprocessOf = []string{a.ID}
	b.Content = "new explicit analysis"
	second, err := s.SaveEventAssignment(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := s.SaveEventAssignment(t.Context(), b)
	if err != nil || retry.CreatedAt != second.CreatedAt {
		t.Fatalf("retry: %+v %v", retry, err)
	}
	stale := b
	stale.ID = "r_stale"
	stale.SessionID = "s3"
	if _, err = s.SaveEventAssignment(t.Context(), stale); err != ErrConflict {
		t.Fatalf("stale owner accepted: %v", err)
	}
	c := b
	c.ID = "r_three"
	c.EventIDs = []string{"e1", "e2"}
	c.ReprocessOf = []string{a.ID, b.ID}
	if _, err = s.SaveEventAssignment(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	rows, err := s.EventAssignments(t.Context())
	if err != nil || len(rows) != 3 {
		t.Fatalf("receipts lost: %+v %v", rows, err)
	}
	for _, row := range rows {
		if row.ID == a.ID && (row.SessionID != first.SessionID || row.Content != first.Content || row.CreatedAt != first.CreatedAt) {
			t.Fatal("original changed")
		}
	}
	foreign := c
	foreign.ID = "r_foreign"
	foreign.SourceInstanceID = "other"
	foreign.ReprocessOf = []string{c.ID}
	if _, err = s.SaveEventAssignment(t.Context(), foreign); err != ErrConflict {
		t.Fatalf("foreign parent accepted: %v", err)
	}
}
