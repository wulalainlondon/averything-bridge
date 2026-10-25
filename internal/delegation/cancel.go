package delegation

import (
	"context"
	"database/sql"
	"everything-go/internal/taskapi"
	"time"
)

func (s *Store) Revision(ctx context.Context, session, request string) (uint64, error) {
	var n uint64
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE((SELECT revision FROM task_api_revisions WHERE session_id=? AND request_id=?),0)", session, request).Scan(&n)
	return n, err
}

// CancelQueuedAPI atomically claims the exact cancellation and resolves its
// outcome alongside the original waiting record. Promotion is serialized by
// SQLite; once provisioning starts cancellation cannot invent a stopped child.
func (s *Store) CancelQueuedAPI(ctx context.Context, c taskapi.AuthorizedCommand, intent, original taskapi.IntentRecord, expected uint64, messageRevision ...uint64) (taskapi.IntentRecord, error) {
	var stored taskapi.IntentRecord
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		if c.Revalidate == nil {
			return taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		}
		if err := c.Revalidate(ctx); err != nil {
			return err
		}
		// Compare the whole task revision BEFORE claiming this cancel. The core
		// passes its message-queue watermark while holding its mutation fence.
		var before uint64
		if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(v.revision),0) FROM task_api_revisions v JOIN task_api_intents i ON i.session_id=v.session_id AND i.request_id=v.request_id WHERE i.task_id=?", original.TaskID).Scan(&before); err != nil {
			return err
		}
		if len(messageRevision) > 0 && messageRevision[0] > before {
			before = messageRevision[0]
		}
		var created bool
		var err error
		stored, created, err = s.TaskJournal().ClaimTx(tx, c, intent)
		if err != nil || !created {
			return err
		}
		if before != expected {
			return taskapi.Failure("stale_revision", "known_none", "refresh_identity")
		}

		res, err := tx.ExecContext(ctx, `UPDATE session_delegations SET state='terminal',terminal_status='interrupted',error='cancelled_before_start',updated_at=? WHERE child_session_id=? AND child_request_id=? AND state='queued'`, time.Now().UnixMilli(), original.SessionID, original.RequestID)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return taskapi.Failure("busy", "known_none", "lookup_original")
		}
		if err = taskapi.ChangeTx(tx, original.SessionID, original.RequestID, "cancelled_before_start"); err != nil {
			return err
		}
		if err = c.Revalidate(ctx); err != nil {
			return err
		}
		return taskapi.SaveOutcomeTx(tx, stored.Key, nil)
	})
	if err != nil {
		return taskapi.IntentRecord{}, err
	}
	return stored, nil
}

func (s *Store) CancelWaiting(ctx context.Context, parent, id string) (bool, error) {
	cancelled := false
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var state, reason, child, request string
		err := tx.QueryRowContext(ctx, "SELECT state,error,child_session_id,child_request_id FROM session_delegations WHERE id=? AND parent_session_id=?", id, parent).Scan(&state, &reason, &child, &request)
		if err != nil {
			return err
		}
		if state == "terminal" && reason == "cancelled_before_start" {
			cancelled = true
			return nil
		}
		if state != "queued" {
			return nil
		}
		_, err = tx.ExecContext(ctx, "UPDATE session_delegations SET state='terminal',terminal_status='interrupted',error='cancelled_before_start',updated_at=? WHERE id=? AND state='queued'", time.Now().UnixMilli(), id)
		if err != nil {
			return err
		}
		cancelled = true
		return taskapi.ChangeTx(tx, child, request, "cancelled_before_start")
	})
	return cancelled, err
}
