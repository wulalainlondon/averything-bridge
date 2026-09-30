package workitems

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type StartRunInput struct {
	ID              string
	WorkItemID      string
	SessionID       string
	RequestID       string
	Kind            string
	Instruction     string
	MaxAttempts     int
	ExpectedVersion uint64
	Actor           Actor
}

type RunUpdate struct {
	Run       Run
	Item      WorkItem
	Changed   bool
	Attention string
}

func (s *Store) StartRun(ctx context.Context, in StartRunInput) (Run, WorkItem, error) {
	if in.ID == "" {
		in.ID = newID("wr")
	}
	if in.RequestID == "" || in.SessionID == "" {
		return Run{}, WorkItem{}, errors.New("run request and session are required")
	}
	if in.Kind == "" {
		in.Kind = "implementation"
	}
	if in.Kind != "implementation" && in.Kind != "research" && in.Kind != "verification" {
		return Run{}, WorkItem{}, fmt.Errorf("invalid run kind %q", in.Kind)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Run{}, WorkItem{}, err
	}
	defer tx.Rollback()
	item, err := getItemTx(ctx, tx, in.WorkItemID)
	if err != nil {
		return Run{}, WorkItem{}, err
	}
	if item.Version != in.ExpectedVersion {
		return Run{}, WorkItem{}, &ConflictError{Expected: in.ExpectedVersion, Current: item}
	}
	if item.Lifecycle != LifecycleReady && item.Lifecycle != LifecycleActive {
		return Run{}, WorkItem{}, fmt.Errorf("%w: start run from %s", ErrInvalidTransition, item.Lifecycle)
	}
	var linkID string
	err = tx.QueryRowContext(ctx, `SELECT id FROM work_item_session_links
		WHERE work_item_id=? AND session_id=? AND unlinked_at IS NULL LIMIT 1`, in.WorkItemID, in.SessionID).Scan(&linkID)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, WorkItem{}, errors.New("run session is not linked to this work item")
	}
	if err != nil {
		return Run{}, WorkItem{}, err
	}
	now := s.now().UnixMilli()
	if in.MaxAttempts <= 0 {
		in.MaxAttempts = 3
	}
	run := Run{ID: in.ID, WorkItemID: in.WorkItemID, SessionLinkID: linkID,
		RequestID: in.RequestID, Kind: in.Kind, Status: "queued", StartedAt: now,
		SessionID: in.SessionID, Instruction: in.Instruction, AvailableAt: now, MaxAttempts: in.MaxAttempts}
	if _, err := tx.ExecContext(ctx, `INSERT INTO work_item_runs
		(id,work_item_id,session_link_id,request_id,kind,status,started_at,session_id,instruction,
		 available_at,attempt,max_attempts,queue_reason)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, run.ID, run.WorkItemID, run.SessionLinkID, run.RequestID,
		run.Kind, run.Status, run.StartedAt, run.SessionID, run.Instruction, run.AvailableAt,
		run.Attempt, run.MaxAttempts, run.QueueReason); err != nil {
		return Run{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = now
	if item.Lifecycle == LifecycleReady {
		item.Lifecycle = LifecycleActive
	}
	updated, err := s.writeItemMutation(ctx, tx, item, "run_started", in.Actor, ChangePayload{Run: &run})
	return run, updated, err
}

// OwnsRequest reports whether the exact session request was explicitly started
// as a WorkItem run. Terminal runs remain owned: completion notification
// routing happens independently from lifecycle projection, and may run before
// or after AdvanceRun records finished_at (including after a Bridge restart).
func (s *Store) OwnsRequest(ctx context.Context, sessionID, requestID string) (bool, error) {
	if sessionID == "" || requestID == "" {
		return false, nil
	}
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM work_item_runs r
		JOIN work_item_session_links l ON l.id=r.session_link_id
		WHERE l.session_id=? AND r.request_id=?
	)`, sessionID, requestID).Scan(&exists)
	return exists == 1, err
}

func (s *Store) HasActiveRun(ctx context.Context, workItemID string) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM work_item_runs
		WHERE work_item_id=? AND finished_at IS NULL)`, workItemID).Scan(&exists)
	return exists == 1, err
}

// AdvanceRun projects only explicitly-started WorkItem runs. Ordinary messages
// in a linked session never move the human-owned WorkItem lifecycle.
func (s *Store) AdvanceRun(ctx context.Context, sessionID, requestID, status, reason string) (RunUpdate, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RunUpdate{}, err
	}
	defer tx.Rollback()
	var run Run
	err = tx.QueryRowContext(ctx, `SELECT r.id,r.work_item_id,r.session_link_id,r.request_id,r.kind,
		r.status,r.started_at,r.finished_at,r.terminal_reason,r.session_id,r.instruction,r.available_at,
		r.attempt,r.max_attempts,r.claimed_at,r.queue_reason
		FROM work_item_runs r JOIN work_item_session_links l ON l.id=r.session_link_id
		WHERE l.session_id=? AND r.finished_at IS NULL AND (?='' OR r.request_id=?)
		ORDER BY r.started_at DESC LIMIT 1`, sessionID, requestID, requestID).Scan(
		&run.ID, &run.WorkItemID, &run.SessionLinkID, &run.RequestID, &run.Kind,
		&run.Status, &run.StartedAt, &run.FinishedAt, &run.TerminalReason, &run.SessionID,
		&run.Instruction, &run.AvailableAt, &run.Attempt, &run.MaxAttempts, &run.ClaimedAt, &run.QueueReason)
	if errors.Is(err, sql.ErrNoRows) {
		return RunUpdate{}, nil
	}
	if err != nil {
		return RunUpdate{}, err
	}
	if run.Status == status {
		return RunUpdate{Run: run}, nil
	}
	now := s.now().UnixMilli()
	run.Status = status
	run.TerminalReason = reason
	terminal := status == "succeeded" || status == "failed" || status == "interrupted"
	if terminal {
		run.FinishedAt = &now
	}
	if _, err := tx.ExecContext(ctx, `UPDATE work_item_runs SET status=?,finished_at=?,terminal_reason=? WHERE id=?`,
		run.Status, run.FinishedAt, run.TerminalReason, run.ID); err != nil {
		return RunUpdate{}, err
	}
	item, err := getItemTx(ctx, tx, run.WorkItemID)
	if err != nil {
		return RunUpdate{}, err
	}
	item.Version++
	item.UpdatedAt = now
	attention := ""
	managed, err := collaborationItemTx(ctx, tx, item.ID)
	if err != nil {
		return RunUpdate{}, err
	}
	if status == "succeeded" && item.Lifecycle == LifecycleActive && !managed {
		item.Lifecycle = LifecycleReview
		attention = "review_ready"
	} else if status == "failed" {
		item.BlockedReasonCode = "execution_failed"
		item.BlockedNote = reason
		attention = "execution_failed"
	} else if status == "waiting_user" {
		attention = "needs_input"
	}
	updated, err := s.writeItemMutation(ctx, tx, item, "run_"+status, Actor{Type: ActorSystem}, ChangePayload{Run: &run})
	if err != nil {
		return RunUpdate{}, err
	}
	return RunUpdate{Run: run, Item: updated, Changed: true, Attention: attention}, nil
}

// RecoverQueue makes a process crash retryable without creating a second Run.
// Completed/failed/interrupted runs are terminal and are never resurrected.
func (s *Store) RecoverQueue(ctx context.Context) (int64, error) {
	now := s.now().UnixMilli()
	result, err := s.db.ExecContext(ctx, `UPDATE work_item_runs
		SET status='queued',available_at=?,claimed_at=NULL,queue_reason='bridge_restarted'
		WHERE finished_at IS NULL AND status IN ('submitted','dispatching','running')`, now)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Store) MarkRunSubmitted(ctx context.Context, runID string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE work_item_runs SET status='submitted',queue_reason='client_submitted'
		WHERE id=? AND finished_at IS NULL AND status IN ('queued','dispatching')`, runID)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return ErrConflict
	}
	return nil
}

// ClaimNextRun obtains one persistent queue lease. SQLite is configured with a
// single writer, and the conditional UPDATE protects against future workers.
func (s *Store) ClaimNextRun(ctx context.Context, now int64) (Run, WorkItem, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Run{}, WorkItem{}, false, err
	}
	defer tx.Rollback()
	var run Run
	err = tx.QueryRowContext(ctx, `SELECT r.id,r.work_item_id,r.session_link_id,r.request_id,r.kind,r.status,
		started_at,finished_at,terminal_reason,session_id,instruction,available_at,attempt,max_attempts,claimed_at,queue_reason
		FROM work_item_runs r JOIN work_items w ON w.id=r.work_item_id
		WHERE r.finished_at IS NULL AND r.status IN ('queued','deferred')
		AND r.available_at<=? AND r.attempt<r.max_attempts
		ORDER BY CASE w.priority
			WHEN 'urgent' THEN 400 WHEN 'high' THEN 300 WHEN 'medium' THEN 200
			WHEN 'low' THEN 100 ELSE 0 END DESC,
			r.available_at,r.started_at,r.id LIMIT 1`, now).Scan(
		&run.ID, &run.WorkItemID, &run.SessionLinkID, &run.RequestID, &run.Kind, &run.Status,
		&run.StartedAt, &run.FinishedAt, &run.TerminalReason, &run.SessionID, &run.Instruction,
		&run.AvailableAt, &run.Attempt, &run.MaxAttempts, &run.ClaimedAt, &run.QueueReason)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, WorkItem{}, false, nil
	}
	if err != nil {
		return Run{}, WorkItem{}, false, err
	}
	run.Status = "dispatching"
	run.Attempt++
	run.ClaimedAt = &now
	run.QueueReason = ""
	result, err := tx.ExecContext(ctx, `UPDATE work_item_runs SET status=?,attempt=?,claimed_at=?,queue_reason=''
		WHERE id=? AND finished_at IS NULL AND status IN ('queued','deferred')`, run.Status, run.Attempt, now, run.ID)
	if err != nil {
		return Run{}, WorkItem{}, false, err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return Run{}, WorkItem{}, false, nil
	}
	item, err := getItemTx(ctx, tx, run.WorkItemID)
	if err != nil {
		return Run{}, WorkItem{}, false, err
	}
	item.Version++
	item.UpdatedAt = now
	updated, err := s.writeItemMutation(ctx, tx, item, "run_dispatching", Actor{Type: ActorSystem}, ChangePayload{Run: &run})
	return run, updated, err == nil, err
}

func (s *Store) DeferRun(ctx context.Context, runID string, availableAt int64, reason string) (Run, WorkItem, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Run{}, WorkItem{}, err
	}
	defer tx.Rollback()
	var run Run
	err = tx.QueryRowContext(ctx, `SELECT id,work_item_id,session_link_id,request_id,kind,status,
		started_at,finished_at,terminal_reason,session_id,instruction,available_at,attempt,max_attempts,claimed_at,queue_reason
		FROM work_item_runs WHERE id=?`, runID).Scan(&run.ID, &run.WorkItemID, &run.SessionLinkID,
		&run.RequestID, &run.Kind, &run.Status, &run.StartedAt, &run.FinishedAt, &run.TerminalReason,
		&run.SessionID, &run.Instruction, &run.AvailableAt, &run.Attempt, &run.MaxAttempts, &run.ClaimedAt, &run.QueueReason)
	if err != nil {
		return Run{}, WorkItem{}, err
	}
	run.Status, run.AvailableAt, run.QueueReason, run.ClaimedAt = "deferred", availableAt, reason, nil
	if _, err := tx.ExecContext(ctx, `UPDATE work_item_runs SET status='deferred',available_at=?,queue_reason=?,claimed_at=NULL WHERE id=?`,
		availableAt, reason, run.ID); err != nil {
		return Run{}, WorkItem{}, err
	}
	item, err := getItemTx(ctx, tx, run.WorkItemID)
	if err != nil {
		return Run{}, WorkItem{}, err
	}
	item.Version++
	item.UpdatedAt = s.now().UnixMilli()
	updated, err := s.writeItemMutation(ctx, tx, item, "run_deferred", Actor{Type: ActorSystem}, ChangePayload{Run: &run})
	return run, updated, err
}

func (s *Store) EnqueueAutomaticRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, itemSelect+` WHERE lifecycle='ready' AND automation_mode='auto'
		AND archived_at IS NULL AND NOT EXISTS (SELECT 1 FROM work_item_runs r WHERE r.work_item_id=work_items.id AND r.finished_at IS NULL)
		ORDER BY CASE priority
			WHEN 'urgent' THEN 400 WHEN 'high' THEN 300 WHEN 'medium' THEN 200
			WHEN 'low' THEN 100 ELSE 0 END DESC,sort_key,id`)
	if err != nil {
		return nil, err
	}
	var items []WorkItem
	for rows.Next() {
		item, scanErr := scanItem(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		items = append(items, item)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var queued []Run
	for _, item := range items {
		var sessionID string
		err := s.db.QueryRowContext(ctx, `SELECT session_id FROM work_item_session_links
			WHERE work_item_id=? AND unlinked_at IS NULL ORDER BY CASE role WHEN 'primary' THEN 0 ELSE 1 END,linked_at LIMIT 1`, item.ID).Scan(&sessionID)
		if err != nil {
			continue
		}
		run, _, err := s.StartRun(ctx, StartRunInput{WorkItemID: item.ID, SessionID: sessionID,
			RequestID: newID("auto"), Kind: "implementation", Instruction: item.NextStep,
			ExpectedVersion: item.Version, Actor: Actor{Type: ActorSystem}})
		if err == nil {
			queued = append(queued, run)
		}
	}
	return queued, nil
}
