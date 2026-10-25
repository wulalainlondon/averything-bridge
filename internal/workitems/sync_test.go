package workitems

import (
	"context"
	"testing"
	"time"
)

func TestPerDeviceReadStateIsIndependent(t *testing.T) {
	s := openTestStore(t)
	item := seedItem(t, s, "p1", "i1")
	ctx := context.Background()
	view, err := s.MarkRead(ctx, "device-a", item.ID, item.ActivityRevision)
	if err != nil || view.Unread != 0 {
		t.Fatalf("mark read view=%+v err=%v", view, err)
	}
	a, err := s.SnapshotForDevice(ctx, "device-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.SnapshotForDevice(ctx, "device-b")
	if err != nil {
		t.Fatal(err)
	}
	if a.Items[0].Unread != 0 || b.Items[0].Unread != 1 {
		t.Fatalf("per-device unread a=%d b=%d", a.Items[0].Unread, b.Items[0].Unread)
	}
}

func TestChangesSinceAndCompactedCursor(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	seedItem(t, s, "p1", "i1")
	changes, latest, compacted, err := s.ChangesSince(ctx, 0, 100)
	if err != nil || compacted || len(changes) != 2 || latest != 2 {
		t.Fatalf("changes=%+v latest=%d compacted=%v err=%v", changes, latest, compacted, err)
	}
	if changes[0].Entity != "project" || changes[1].Entity != "work_item" {
		t.Fatalf("unexpected ordering: %+v", changes)
	}
}

func TestClientCursorAheadOfAuthorityRequiresSnapshot(t *testing.T) {
	s := openTestStore(t)
	seedItem(t, s, "p1", "i1")
	changes, latest, compacted, err := s.ChangesSince(context.Background(), 99, 100)
	if err != nil || !compacted || len(changes) != 0 || latest != 2 {
		t.Fatalf("changes=%+v latest=%d compacted=%v err=%v", changes, latest, compacted, err)
	}
}

func TestMutationResponsePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	want := []byte(`{"type":"work_mutation_ack","mutation_id":"m1"}`)
	if err := s.RememberMutation(ctx, "phone", "m1", want); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, ok, err := s.MutationResponse(ctx, "phone", "m1")
	if err != nil || !ok || string(got) != string(want) {
		t.Fatalf("response=%s ok=%v err=%v", got, ok, err)
	}
	if _, ok, _ := s.MutationResponse(ctx, "other", "m1"); ok {
		t.Fatal("mutation response leaked to another device")
	}
}

func TestCompactionRespectsActiveDeviceAndExpiresStaleCursor(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	project, err := s.CreateProject(ctx, CreateProjectInput{ID: "p1", Name: "P"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < minimumChangeTail+30; i++ {
		if _, err := s.CreateItem(ctx, CreateItemInput{ProjectID: project.ID, Title: "item"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AckSync(ctx, "active", 10, 10); err != nil {
		t.Fatal(err)
	}
	removed, err := s.CompactChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 10 {
		t.Fatalf("active cursor removed=%d want 10", removed)
	}

	s.now = func() time.Time { return time.UnixMilli(1_800_000_000_000).Add(deviceCursorTTL + time.Hour) }
	removed, err = s.CompactChanges(ctx)
	if err != nil || removed == 0 {
		t.Fatalf("stale cursor compaction removed=%d err=%v", removed, err)
	}
	_, _, compacted, err := s.ChangesSince(ctx, 0, 10)
	if err != nil || !compacted {
		t.Fatalf("expired device should require snapshot: compacted=%v err=%v", compacted, err)
	}
}

func TestDiagnosticsReportsBoundedSyncState(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	item := seedItem(t, s, "p1", "i1")
	if err := s.AckSync(ctx, "note20", item.ActivityRevision, item.ActivityRevision); err != nil {
		t.Fatal(err)
	}
	diagnostics, err := s.Diagnostics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if diagnostics.Projects != 1 || diagnostics.Items != 1 || diagnostics.ActiveDevices != 1 || diagnostics.Revision == 0 {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
}
