package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

// Opt-in measured engineering baseline, never a physical-device SLO or model QA.
func TestWorkspaceLargeDatasetBaseline(t *testing.T) {
	path := os.Getenv("WORKSPACE_PERFORMANCE_REPORT")
	if path == "" {
		t.Skip("large fixture not requested")
	}
	f := setup(t)
	ctx := context.Background()
	start := time.Now()
	tx, err := f.s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < 10000; i++ {
		id := fmt.Sprintf("fixture_message_%05d", i)
		if _, err = f.s.put(ctx, tx, f.owner, f.room, Entity{ID: id, Kind: "message"}, Message{Body: fmt.Sprintf("Engineering fixture message %d", i), ThreadID: id}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		if _, err = f.s.put(ctx, tx, f.owner, f.room, Entity{ID: fmt.Sprintf("fixture_document_%04d", i), Kind: "document"}, map[string]any{"title": fmt.Sprintf("Fixture document %d", i), "body": "Synthetic content"}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 999; i++ {
		id := fmt.Sprintf("fixture_room_%04d", i)
		if _, err = tx.ExecContext(ctx, "INSERT INTO rooms VALUES(?,?,?,?,?,1,1,0,0,?,'')", id, f.workspace.ID, "Fixture room", "Synthetic", f.owner.ID, f.s.now().UnixMilli()); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO members VALUES(?,?,?,'owner',1,1)", id, f.owner.ID, f.owner.Name); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	seed := time.Since(start)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start = time.Now()
	head := f.call(f.owner, Command{Action: "snapshot", RoomID: f.room.ID})
	snapshotTime := time.Since(start)
	if len(head.Snapshot.Data["message"].Items) != 30 || len(head.Snapshot.Data["document"].Items) != 30 {
		t.Fatal("unbounded snapshot")
	}
	start = time.Now()
	ids := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		p := f.call(f.owner, Command{Action: "page", RoomID: f.room.ID, Kind: "message", Limit: 100, Cursor: cursor}).Page
		pages++
		if len(p.Items) > 100 {
			t.Fatal("unbounded page")
		}
		for _, e := range p.Items {
			if ids[e.ID] {
				t.Fatal("duplicate", e.ID)
			}
			ids[e.ID] = true
		}
		if !p.HasMore {
			break
		}
		if p.Cursor == "" || p.Cursor == cursor {
			t.Fatal("cursor stalled")
		}
		cursor = p.Cursor
	}
	pagingTime := time.Since(start)
	if len(ids) != 10000 {
		t.Fatal("missing messages", len(ids))
	}
	roomIDs := map[string]bool{}
	cursor = ""
	roomPages := 0
	for {
		r := f.call(f.owner, Command{Action: "list", Cursor: cursor})
		roomPages++
		for _, room := range r.Rooms {
			if roomIDs[room.ID] {
				t.Fatal("duplicate room")
			}
			roomIDs[room.ID] = true
		}
		if r.Page == nil || r.Page.Cursor == "" {
			break
		}
		cursor = r.Page.Cursor
	}
	if len(roomIDs) != 1000 {
		t.Fatal("missing rooms", len(roomIDs))
	}
	runtime.ReadMemStats(&after)
	report := map[string]any{"schema": "workspace-synthetic-performance.v1", "dataset": map[string]int{"messages": 10000, "documents": 100, "rooms": 1000}, "seed_ms": seed.Milliseconds(), "snapshot_ms": snapshotTime.Milliseconds(), "message_paging_ms": pagingTime.Milliseconds(), "message_pages": pages, "room_pages": roomPages, "heap_alloc_before": before.HeapAlloc, "heap_alloc_after": after.HeapAlloc, "physical_device": false, "real_model": false, "slo_defined": false, "go_version": runtime.Version(), "state": "ENGINEERING_PASS_NO_DEVICE_PERFORMANCE_CLAIM"}
	b, _ := json.MarshalIndent(report, "", "  ")
	if err = os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}
