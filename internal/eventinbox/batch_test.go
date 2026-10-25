package eventinbox

import (
	"context"
	"testing"
)

func TestSourceBatchCursorCASDedupAndRollback(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	input := []Input{{Source: "line_personal.a", EventKey: "one", Kind: "message", Title: "LINE"}}
	events, err := s.CommitSourceBatch(ctx, "line_personal.a", "talk", nil, []byte("cursor1"), input)
	if err != nil || len(events) != 1 {
		t.Fatalf("first: %v %v", events, err)
	}
	if _, err = s.CommitSourceBatch(ctx, "line_personal.a", "talk", nil, []byte("wrong"), input); err == nil {
		t.Fatal("stale writer accepted")
	}
	bad := append(input, Input{Source: "line_personal.a", EventKey: "two", Kind: "message", Title: ""})
	if _, err = s.CommitSourceBatch(ctx, "line_personal.a", "talk", []byte("cursor1"), []byte("bad"), bad); err == nil {
		t.Fatal("invalid batch accepted")
	}
	cursor, _ := s.SourceCursor(ctx, "line_personal.a", "talk")
	if string(cursor) != "cursor1" {
		t.Fatal("checkpoint advanced without events")
	}
	events, err = s.CommitSourceBatch(ctx, "line_personal.a", "talk", cursor, []byte("cursor2"), input)
	if err != nil || len(events) != 0 {
		t.Fatal("replay duplicated event", err)
	}
	other, _ := s.SourceCursor(ctx, "line_personal.b", "talk")
	if len(other) != 0 {
		t.Fatal("cursor crossed account")
	}
	if _, err = s.CommitSourceBatch(ctx, "line_personal.b", "talk", nil, []byte("cursor"), input); err == nil {
		t.Fatal("source substitution accepted")
	}
	snapshot, _ := s.Snapshot(ctx, "phone", 0)
	if len(snapshot.Items) != 1 {
		t.Fatal("rollback/replay changed event count")
	}
}
