package delegation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
)

func (s *Store) TaskJournal() *taskapi.Journal { return taskapi.AttachedJournal(s.db, "delegation") }

func (s *Store) CreateAPI(ctx context.Context, c taskapi.AuthorizedCommand, intent taskapi.IntentRecord, r Record, backend string) (taskapi.IntentRecord, bool, error) {
	return s.createAPI(ctx, c, intent, r, backend, true)
}
func (s *Store) CreateAPIContinuation(ctx context.Context, c taskapi.AuthorizedCommand, intent taskapi.IntentRecord, r Record) (taskapi.IntentRecord, bool, error) {
	return s.createAPI(ctx, c, intent, r, "", false)
}
func (s *Store) createAPI(ctx context.Context, c taskapi.AuthorizedCommand, intent taskapi.IntentRecord, r Record, backend string, newChild bool) (taskapi.IntentRecord, bool, error) {
	var result taskapi.IntentRecord
	created := false
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		// The original caller is checked at the canonical admission boundary,
		// after all capability/workspace I/O, not only at tool receipt.
		if c.Revalidate == nil {
			return taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
		}
		if err := c.Revalidate(ctx); err != nil {
			return err
		}
		var err error
		result, created, err = s.TaskJournal().ClaimTx(tx, c, intent)
		if err != nil || !created {
			return err
		}
		if _, err = insertBounded(ctx, tx, r); err != nil {
			if errors.Is(err, ErrQueueLimit) || errors.Is(err, ErrDeliveryLimit) {
				return taskapi.Failure("busy", "known_none", "wait")
			}
			return err
		}
		if newChild {
			data, _ := json.Marshal(map[string]any{"backend": backend, "metadata": json.RawMessage(intent.Metadata)})
			if _, err = tx.Exec("INSERT INTO task_api_child_profiles VALUES(?,?)", r.ChildSessionID, data); err != nil {
				return err
			}
		}
		if err = taskapi.ChangeTx(tx, r.ChildSessionID, r.ChildRequestID, "admission"); err != nil {
			return err
		}
		// Revocation during the inserts rolls back the same intent, profile,
		// outbox/events and original delegation facts before commit.
		return c.Revalidate(ctx)
	})
	if err != nil {
		return taskapi.IntentRecord{}, false, err
	}
	return result, created, nil
}
func (s *Store) APIBackend(ctx context.Context, child string) (string, bool, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT profile FROM task_api_child_profiles WHERE session_id=?", child).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var data struct{ Backend string }
	err = json.Unmarshal(raw, &data)
	return data.Backend, true, err
}

func (s *Store) APIChildMetadata(ctx context.Context, child string) ([]byte, bool, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT profile FROM task_api_child_profiles WHERE session_id=?", child).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var profile struct{ Metadata json.RawMessage }
	err = json.Unmarshal(raw, &profile)
	return profile.Metadata, true, err
}

// OriginalRootForChild reads the canonical round owner, not a new ledger.
func (s *Store) OriginalRootForChild(ctx context.Context, child string) (Record, bool, error) {
	r, err := scanRecord(s.db.QueryRowContext(ctx, "SELECT "+columns+" FROM session_delegations WHERE child_session_id=? AND id IN (SELECT logical_id FROM delegation_rounds)", child))
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	return r, err == nil, err
}
