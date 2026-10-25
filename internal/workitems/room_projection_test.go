package workitems

import (
	"context"
	"errors"
	"testing"
)

func TestRoomProjectionUsesOriginalRowsWithoutSchedulingOrGlobalLeak(t *testing.T) {
	ctx := context.Background()
	s, err := OpenService(t.TempDir(), "authority")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	p, err := s.CreateProject(ctx, CreateProjectInput{Name: "Original project", WorkspacePath: "/private/project", Context: "private original context"})
	if err != nil {
		t.Fatal(err)
	}
	refs, err := s.ProjectRefs(ctx)
	if err != nil || len(refs) != 1 || refs[0].ID != p.ID {
		t.Fatal(refs, err)
	}
	in := RoomTaskProjection{ProjectID: p.ID, RoomID: "room", TaskID: "task", SessionID: "s_cw_actual", RequestID: "r_original", ConfigRevision: 7, Execution: "waiting"}
	link, err := s.ProjectRoomTask(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	again, err := s.ProjectRoomTask(ctx, in)
	if err != nil || again != link {
		t.Fatal("unstable projection", again, err)
	}
	var n int
	if err = s.store.db.QueryRow("SELECT COUNT(*) FROM work_items WHERE id=?", link.WorkItemID).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	var request, session, status string
	if err = s.store.db.QueryRow("SELECT request_id,session_id,status FROM work_item_runs WHERE id=?", link.RunID).Scan(&request, &session, &status); err != nil || request != in.RequestID || session != in.SessionID || status != "room_waiting" {
		t.Fatal(request, session, status, err)
	}
	if _, err = s.GetItem(ctx, link.WorkItemID); !errors.Is(err, ErrNotFound) {
		t.Fatal("legacy read bypass", err)
	}
	snap, err := s.Snapshot(ctx)
	if err != nil || len(snap.Items) != 0 || len(snap.Runs) != 0 || len(snap.SessionLinks) != 0 {
		t.Fatal("legacy snapshot contains room rows", snap, err)
	}
	if owns, err := s.OwnsRequest(ctx, in.SessionID, in.RequestID); err != nil || owns {
		t.Fatal("legacy notification ownership", owns, err)
	}
	if _, err = s.AdvanceRun(ctx, in.SessionID, in.RequestID, "succeeded", ""); err != nil {
		t.Fatal(err)
	}
	if err = s.store.db.QueryRow("SELECT status FROM work_item_runs WHERE id=?", link.RunID).Scan(&status); err != nil || status != "room_waiting" {
		t.Fatal("legacy transition touched room", status, err)
	}
	in.Execution = "terminal"
	in.Final = "sealed"
	in.Human = "accepted"
	if _, err = s.ProjectRoomTask(ctx, in); err != nil {
		t.Fatal(err)
	}
	in.Execution = "running"
	in.Final = "pending"
	in.Human = "unreviewed"
	if _, err = s.ProjectRoomTask(ctx, in); err != nil {
		t.Fatal(err)
	}
	var lifecycle string
	if err = s.store.db.QueryRow("SELECT status FROM work_item_runs WHERE id=?", link.RunID).Scan(&status); err != nil || status != "room_sealed" {
		t.Fatal("sealed projection regressed", status, err)
	}
	if err = s.store.db.QueryRow("SELECT lifecycle FROM work_items WHERE id=?", link.WorkItemID).Scan(&lifecycle); err != nil || lifecycle != "done" {
		t.Fatal("human acceptance regressed", lifecycle, err)
	}
	// Even a corrupt/recovered status must not enter the ordinary scheduler.
	if _, err = s.store.db.Exec("UPDATE work_item_runs SET status='queued',finished_at=NULL WHERE id=?", link.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RecoverQueue(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, claimed, err := s.ClaimNextRun(ctx, 1<<62); err != nil || claimed {
		t.Fatal("duplicate scheduler admission", claimed, err)
	}
	in.RequestID = "r_replacement"
	if _, err = s.ProjectRoomTask(ctx, in); err == nil {
		t.Fatal("replacement original identity accepted")
	}
}
