package delegation

import (
	"context"
	"database/sql"
	"errors"
)

// Additive relation metadata: original execution/result/return rows are never
// rewritten. An ordinary r_ request is persisted before any native effect.
const continuityDDL = `
CREATE TABLE IF NOT EXISTS delegation_rounds (
 id TEXT PRIMARY KEY, logical_id TEXT NOT NULL, version INTEGER NOT NULL,
 previous_id TEXT NOT NULL, authority TEXT NOT NULL, grant_digest TEXT NOT NULL,
 mode TEXT NOT NULL, execution_request TEXT NOT NULL, steer_claimed INTEGER NOT NULL DEFAULT 0,
 UNIQUE(logical_id,version));
-- Upgrade only relation metadata; original records and receipts stay untouched.
INSERT OR IGNORE INTO delegation_rounds(id,logical_id,version,previous_id,authority,grant_digest,mode,execution_request)
 SELECT id,FIRST_VALUE(id) OVER w,ROW_NUMBER() OVER w,COALESCE(LAG(id) OVER w,''),'','','queue',''
 FROM session_delegations WINDOW w AS (PARTITION BY parent_session_id,child_session_id ORDER BY rowid);
CREATE TABLE IF NOT EXISTS delegation_native_threads (
 child TEXT PRIMARY KEY, thread TEXT NOT NULL, revision INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS delegation_responsibilities (
 owner TEXT PRIMARY KEY, qa TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS delegation_qa_versions (
 owner_round TEXT PRIMARY KEY, qa_round TEXT NOT NULL UNIQUE, verdict TEXT NOT NULL);
`

type Round struct {
	ID                 string `json:"delegation_id"`
	LogicalID          string `json:"logical_task_id"`
	Version            uint64 `json:"round_version"`
	PreviousID         string `json:"previous_round_id"`
	Authority          string `json:"authority_instance_id"`
	GrantDigest        string `json:"-"`
	Mode               string `json:"mode"`
	ExecutionRequestID string `json:"execution_request_id"`
}

func insertRound(ctx context.Context, tx *sql.Tx, r Record) error {
	logical := r.LogicalID
	if logical == "" {
		logical = r.ID
	}
	mode := r.Mode
	if mode == "" {
		mode = "queue"
	}
	var version uint64 = 1
	if logical != r.ID {
		var latest string
		if err := tx.QueryRowContext(ctx, "SELECT id,version FROM delegation_rounds WHERE logical_id=? ORDER BY version DESC LIMIT 1", logical).Scan(&latest, &version); err != nil {
			return err
		}
		if latest != r.PreviousID {
			return errors.New("delegation_round_stale")
		}
		version++
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO delegation_rounds(id,logical_id,version,previous_id,authority,grant_digest,mode,execution_request) VALUES(?,?,?,?,?,?,?,?)", r.ID, logical, version, r.PreviousID, r.Authority, r.GrantDigest, mode, r.ExecutionRequestID)
	return err
}
func scanRound(row interface{ Scan(...any) error }) (Round, error) {
	var r Round
	err := row.Scan(&r.ID, &r.LogicalID, &r.Version, &r.PreviousID, &r.Authority, &r.GrantDigest, &r.Mode, &r.ExecutionRequestID)
	return r, err
}

const roundColumns = "id,logical_id,version,previous_id,authority,grant_digest,mode,execution_request"

func (s *Store) Round(ctx context.Context, id string) (Round, bool, error) {
	r, err := scanRound(s.db.QueryRowContext(ctx, "SELECT "+roundColumns+" FROM delegation_rounds WHERE id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	return r, err == nil, err
}
func (s *Store) Rounds(ctx context.Context, logical string) ([]Round, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+roundColumns+" FROM delegation_rounds WHERE logical_id=? ORDER BY version LIMIT 128", logical)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Round{}
	for rows.Next() {
		r, e := scanRound(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Pin only using exact original native acceptance. Same-thread retries are
// harmless; adopting a later changed thread or revision is never allowed.
func (s *Store) PinNativeThread(ctx context.Context, child, thread string, revision uint64) error {
	return s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO delegation_native_threads VALUES(?,?,?)", child, thread, revision); err != nil {
			return err
		}
		var old string
		var rev uint64
		if err := tx.QueryRowContext(ctx, "SELECT thread,revision FROM delegation_native_threads WHERE child=?", child).Scan(&old, &rev); err != nil {
			return err
		}
		if old != thread || rev != revision {
			return errors.New("delegation_native_binding_changed")
		}
		return nil
	})
}
func (s *Store) NativeThread(ctx context.Context, child string) (string, uint64, bool, error) {
	var thread string
	var revision uint64
	err := s.db.QueryRowContext(ctx, "SELECT thread,revision FROM delegation_native_threads WHERE child=?", child).Scan(&thread, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, false, nil
	}
	return thread, revision, err == nil, err
}

// Revalidation runs inside the same serialized admission/receipt transaction.
func (s *Store) Continue(ctx context.Context, r Record, validate func() error) (Record, error) {
	var out Record
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		old, e := scanRecord(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM session_delegations WHERE parent_session_id=? AND origin_request_id=? AND tool_call_id=?", r.ParentSessionID, r.OriginRequestID, r.ToolCallID))
		if e == nil {
			if old.IntentHash != r.IntentHash {
				return errors.New("delegation_intent_conflict")
			}
			out = old
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if e = validate(); e != nil {
			return e
		}
		if _, e = insertBounded(ctx, tx, r); e != nil {
			return e
		}
		if e = validate(); e != nil {
			return e
		}
		out, e = scanRecord(tx.QueryRowContext(ctx, "SELECT "+columns+" FROM session_delegations WHERE id=?", r.ID))
		return e
	})
	return out, err
}
func (s *Store) ClaimSteer(ctx context.Context, id string) (bool, error) {
	res, e := s.db.ExecContext(ctx, "UPDATE delegation_rounds SET steer_claimed=1 WHERE id=? AND mode='steer' AND steer_claimed=0", id)
	if e != nil {
		return false, e
	}
	n, e := res.RowsAffected()
	return n == 1, e
}
func (s *Store) BindQA(ctx context.Context, owner, qa string, validate func() error) error {
	return s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		if e := validate(); e != nil {
			return e
		}
		var old string
		e := tx.QueryRowContext(ctx, "SELECT qa FROM delegation_responsibilities WHERE owner=?", owner).Scan(&old)
		if e == nil {
			if old != qa {
				return errors.New("delegation_responsibility_immutable")
			}
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO delegation_responsibilities VALUES(?,?)", owner, qa); e != nil {
			return e
		}
		return validate()
	})
}
func (s *Store) Responsibility(ctx context.Context, logical string) (string, string, error) {
	var owner, qa string
	e := s.db.QueryRowContext(ctx, "SELECT owner,qa FROM delegation_responsibilities WHERE owner=? OR qa=?", logical, logical).Scan(&owner, &qa)
	if errors.Is(e, sql.ErrNoRows) {
		return "", "", nil
	}
	return owner, qa, e
}
func (s *Store) RecordQA(ctx context.Context, owner, qa, verdict string) error {
	return s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var oldQA, oldVerdict string
		e := tx.QueryRowContext(ctx, "SELECT qa_round,verdict FROM delegation_qa_versions WHERE owner_round=?", owner).Scan(&oldQA, &oldVerdict)
		if e == nil {
			if oldQA != qa || oldVerdict != verdict {
				return errors.New("delegation_review_conflict")
			}
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		_, e = tx.ExecContext(ctx, "INSERT INTO delegation_qa_versions VALUES(?,?,?)", owner, qa, verdict)
		return e
	})
}
func (s *Store) QAForRound(ctx context.Context, id string) (string, string, error) {
	var qa, verdict string
	e := s.db.QueryRowContext(ctx, "SELECT qa_round,verdict FROM delegation_qa_versions WHERE owner_round=?", id).Scan(&qa, &verdict)
	if errors.Is(e, sql.ErrNoRows) {
		return "", "", nil
	}
	return qa, verdict, e
}

func (s *Store) LatestRound(ctx context.Context, logical string) (Round, error) {
	return scanRound(s.db.QueryRowContext(ctx, "SELECT "+roundColumns+" FROM delegation_rounds WHERE logical_id=? ORDER BY version DESC LIMIT 1", logical))
}
func (s *Store) RoundsAfter(ctx context.Context, logical string, after uint64) ([]Round, error) {
	rows, e := s.db.QueryContext(ctx, "SELECT "+roundColumns+" FROM delegation_rounds WHERE logical_id=? AND version>? ORDER BY version LIMIT 128", logical, after)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Round{}
	for rows.Next() {
		r, e := scanRound(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

const authorityDDL = `CREATE TABLE IF NOT EXISTS delegation_authority(id INTEGER PRIMARY KEY CHECK(id=1),authority TEXT NOT NULL);`

func (s *Store) BindAuthority(ctx context.Context, authority string) error {
	if authority == "" {
		return errors.New("delegation_authority_missing")
	}
	return s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		if _, e := tx.ExecContext(ctx, authorityDDL); e != nil {
			return e
		}
		if _, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO delegation_authority VALUES(1,?)", authority); e != nil {
			return e
		}
		var old string
		if e := tx.QueryRowContext(ctx, "SELECT authority FROM delegation_authority WHERE id=1").Scan(&old); e != nil {
			return e
		}
		if old != authority {
			return errors.New("delegation_wrong_authority")
		}
		return nil
	})
}
