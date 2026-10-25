package workitems

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

// RoomTaskProjection is a host-authenticated observation of the original
// message queue request. It never creates a schedulable queue entry. Private
// instructions, source content and result bodies remain in the room store.
type RoomTaskProjection struct {
	RoomID, TaskID, ProjectID, SessionID, RequestID, NativeThreadID string
	ConfigRevision                                                  uint64
	Execution, Final, Admission, Human                              string
}
type RoomTaskWorkLink struct{ WorkItemID, RunID string }

func roomProjectionItem(id string) bool { return strings.HasPrefix(id, "wi_cw_") }
func roomProjectionIDs(room, task string) RoomTaskWorkLink {
	sum := sha256.Sum256([]byte(room + "\x00" + task))
	key := hex.EncodeToString(sum[:16])
	return RoomTaskWorkLink{"wi_cw_" + key, "wr_cw_" + key}
}

// ProjectRoomTask stores actual WorkItem/Run rows linked to the same r_ request.
// Those rows are excluded from legacy global work views and scheduler recovery;
// their only execution authority remains the original room queue adapter.
func (s *Service) ProjectRoomTask(ctx context.Context, in RoomTaskProjection) (RoomTaskWorkLink, error) {
	link := roomProjectionIDs(in.RoomID, in.TaskID)
	if in.RoomID == "" || in.TaskID == "" || in.ProjectID == "" || in.SessionID == "" || !strings.HasPrefix(in.RequestID, "r_") {
		return link, errors.New("room_projection_identity_invalid")
	}
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return link, err
	}
	defer tx.Rollback()
	var authority string
	var archived *int64
	if err = tx.QueryRowContext(ctx, "SELECT authority_instance_id,archived_at FROM work_projects WHERE id=?", in.ProjectID).Scan(&authority, &archived); err != nil || authority != s.store.instanceID {
		return link, errors.New("room_project_unavailable")
	}
	// Archived projects can still receive terminal reconciliation, while queue
	// admission independently rejects a newly archived project.
	_, err = tx.ExecContext(ctx, `INSERT INTO work_room_task_links(room_id,task_id,project_id,work_item_id,run_id,session_id,request_id,config_revision) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(room_id,task_id) DO NOTHING`, in.RoomID, in.TaskID, in.ProjectID, link.WorkItemID, link.RunID, in.SessionID, in.RequestID, in.ConfigRevision)
	if err != nil {
		return link, err
	}
	var project, session, request, item, run string
	var config uint64
	err = tx.QueryRowContext(ctx, `SELECT project_id,work_item_id,run_id,session_id,request_id,config_revision FROM work_room_task_links WHERE room_id=? AND task_id=?`, in.RoomID, in.TaskID).Scan(&project, &item, &run, &session, &request, &config)
	if err != nil {
		return link, err
	}
	if project != in.ProjectID || item != link.WorkItemID || run != link.RunID || session != in.SessionID || request != in.RequestID || config != in.ConfigRevision {
		return link, errors.New("room_projection_identity_conflict")
	}
	now := s.store.now().UnixMilli()
	_, err = tx.ExecContext(ctx, `INSERT INTO work_items(id,project_id,title,lifecycle,priority,sort_key,automation_mode,created_at,updated_at) VALUES(?,?,'協作頻道工作','active','none',0,'manual',?,?) ON CONFLICT(id) DO NOTHING`, link.WorkItemID, in.ProjectID, now, now)
	if err != nil {
		return link, err
	}
	// A historical link avoids claiming the ordinary interactive session's
	// active WorkItem association when several room turns share that session.
	sessionLink := "wl_cw_" + strings.TrimPrefix(link.WorkItemID, "wi_cw_")
	_, err = tx.ExecContext(ctx, `INSERT INTO work_item_session_links(id,work_item_id,session_id,thread_id_snapshot,role,linked_at,unlinked_at) VALUES(?,?,?,?,'primary',?,?) ON CONFLICT(id) DO UPDATE SET thread_id_snapshot=CASE WHEN thread_id_snapshot='' THEN excluded.thread_id_snapshot ELSE thread_id_snapshot END`, sessionLink, link.WorkItemID, in.SessionID, in.NativeThreadID, now, now)
	if err != nil {
		return link, err
	}
	status := "room_waiting"
	if in.Execution == "running" {
		status = "room_running"
	}
	if in.Execution == "unknown" || in.Admission == "unknown" {
		status = "room_unknown"
	}
	if in.Execution == "terminal" {
		status = "room_terminal"
	}
	if in.Final == "sealed" {
		status = "room_sealed"
	}
	var finished *int64
	if in.Execution == "terminal" {
		finished = &now
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO work_item_runs(id,work_item_id,session_link_id,request_id,kind,status,started_at,finished_at,session_id,max_attempts,queue_reason) VALUES(?,?,?,?,'implementation',?,?,?,?,0,'room_owned_observation_only') ON CONFLICT(id) DO UPDATE SET status=CASE WHEN work_item_runs.status='room_sealed' THEN 'room_sealed' WHEN work_item_runs.finished_at IS NOT NULL AND excluded.finished_at IS NULL THEN work_item_runs.status ELSE excluded.status END,finished_at=COALESCE(work_item_runs.finished_at,excluded.finished_at)`, link.RunID, link.WorkItemID, sessionLink, in.RequestID, status, now, finished, in.SessionID)
	if err != nil {
		return link, err
	}
	lifecycle := "active"
	if in.Final == "sealed" {
		lifecycle = "review"
	}
	if in.Human == "accepted" {
		lifecycle = "done"
	}
	_, err = tx.ExecContext(ctx, `UPDATE work_items SET lifecycle=?,updated_at=? WHERE id=? AND lifecycle<>'done' AND lifecycle<>?`, lifecycle, now, link.WorkItemID, lifecycle)
	if err != nil {
		return link, err
	}
	return link, tx.Commit()
}
