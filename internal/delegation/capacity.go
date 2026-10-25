package delegation

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// Limits are server-owned. Delivery bounds ALL promises awaiting a result or
// delivery, including queued work: each accepted job reserves one future result.
// Thus simultaneous completion/cancellation cannot overflow the result outbox.
type Limits struct {
	Active   int `json:"active"`
	Queued   int `json:"queued"`
	Delivery int `json:"delivery"`
}

var DefaultLimits = Limits{Active: 6, Queued: 64, Delivery: 128}

type CapacityConfig struct {
	Default Limits            `json:"default"`
	Parents map[string]Limits `json:"parents,omitempty"`
}

func (l Limits) Validate() error {
	if l.Active < 1 || l.Active > 256 || l.Queued < 0 || l.Queued > 4096 || l.Delivery < l.Active || l.Delivery > 8192 {
		return errors.New("invalid delegation capacity")
	}
	return nil
}
func (c CapacityConfig) Validate() error {
	if err := c.Default.Validate(); err != nil {
		return err
	}
	for parent, l := range c.Parents {
		if parent == "" {
			return errors.New("empty capacity parent")
		}
		if err := l.Validate(); err != nil {
			return fmt.Errorf("parent %s: %w", parent, err)
		}
	}
	return nil
}
func ReadCapacityConfig(r io.Reader) (CapacityConfig, error) {
	var c CapacityConfig
	raw, err := io.ReadAll(io.LimitReader(r, 65537))
	if err != nil {
		return c, err
	}
	if len(raw) > 65536 {
		return c, errors.New("capacity configuration exceeds 64 KiB")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return c, errors.New("trailing capacity configuration")
	}
	return c, c.Validate()
}

const capacityDDL = `
CREATE TABLE IF NOT EXISTS delegation_capacity(parent TEXT PRIMARY KEY, active INTEGER NOT NULL, queued INTEGER NOT NULL, delivery INTEGER NOT NULL);
INSERT OR IGNORE INTO delegation_capacity VALUES('',6,64,128);
CREATE TABLE IF NOT EXISTS delegation_scan_cursor(id INTEGER PRIMARY KEY CHECK(id=1),position INTEGER NOT NULL,high_water INTEGER NOT NULL);
INSERT OR IGNORE INTO delegation_scan_cursor VALUES(1,0,0);
CREATE TABLE IF NOT EXISTS delegation_source_bindings(id TEXT PRIMARY KEY,thread TEXT NOT NULL,revision INTEGER NOT NULL,expected_thread TEXT NOT NULL,expected_revision INTEGER NOT NULL,cwd_identity TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS delegation_parent_state ON session_delegations(parent_session_id,state);
`

// ConfigureCapacity is a startup-only configuration seam, not a tool permission.
// Lowering limits never kills/revokes existing work; new admissions/promotions
// stop until the relevant counts drain below the configured limits.
func (s *Store) ConfigureCapacity(ctx context.Context, c CapacityConfig) error {
	if err := c.Validate(); err != nil {
		return err
	}
	return s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "DELETE FROM delegation_capacity"); err != nil {
			return err
		}
		for parent, l := range c.Parents {
			if _, err := tx.ExecContext(ctx, "INSERT INTO delegation_capacity VALUES(?,?,?,?)", parent, l.Active, l.Queued, l.Delivery); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO delegation_capacity VALUES('',?,?,?)", c.Default.Active, c.Default.Queued, c.Default.Delivery)
		return err
	})
}

type queryRow interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func limitsFor(ctx context.Context, q queryRow, parent string) (Limits, error) {
	var l Limits
	err := q.QueryRowContext(ctx, "SELECT active,queued,delivery FROM delegation_capacity WHERE parent IN ('',?) ORDER BY length(parent) DESC LIMIT 1", parent).Scan(&l.Active, &l.Queued, &l.Delivery)
	return l, err
}
func (s *Store) LimitsForParent(ctx context.Context, parent string) (Limits, error) {
	return limitsFor(ctx, s.db, parent)
}

type Counts struct{ Active, Queued, Delivery, Reserved int }

func countsFor(ctx context.Context, q queryRow, parent string) (Counts, error) {
	var n Counts
	err := q.QueryRowContext(ctx, `SELECT
 COALESCE(SUM(state IN ('provisioning','running') AND id NOT IN (SELECT id FROM delegation_rounds WHERE mode='steer')),0),COALESCE(SUM(state='queued'),0),
 COALESCE(SUM(state='collecting' OR (state='terminal' AND delivery_state IN ('pending','queued'))),0),
 COALESCE(SUM(state!='terminal' OR delivery_state IN ('pending','queued')),0)
 FROM session_delegations WHERE parent_session_id=?`, parent).Scan(&n.Active, &n.Queued, &n.Delivery, &n.Reserved)
	return n, err
}
func (s *Store) CountsForParent(ctx context.Context, parent string) (Counts, error) {
	return countsFor(ctx, s.db, parent)
}

var ErrQueueLimit = errors.New("delegation_parent_queue_limit")
var ErrDeliveryLimit = errors.New("delegation_parent_delivery_limit")

func admissionState(ctx context.Context, tx *sql.Tx, parent, child string) (string, error) {
	l, err := limitsFor(ctx, tx, parent)
	if err != nil {
		return "", err
	}
	n, err := countsFor(ctx, tx, parent)
	if err != nil {
		return "", err
	}
	if n.Reserved >= l.Delivery {
		return "", ErrDeliveryLimit
	}
	var sameChild bool
	if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM session_delegations WHERE child_session_id=? AND state IN ('provisioning','running','collecting') AND id NOT IN (SELECT id FROM delegation_rounds WHERE mode='steer'))", child).Scan(&sameChild); err != nil {
		return "", err
	}
	if n.Active < l.Active && n.Queued == 0 && !sameChild && n.Active+n.Delivery < l.Delivery {
		return "provisioning", nil
	}
	if n.Queued >= l.Queued {
		return "", ErrQueueLimit
	}
	return "queued", nil
}
func insertBounded(ctx context.Context, tx *sql.Tx, r Record) (string, error) {
	if r.ID == "" || r.ParentSessionID == "" || r.ChildSessionID == "" || r.ChildRequestID == "" || r.ParentRequestID == "" {
		return "", errors.New("incomplete delegation identity")
	}
	state, err := admissionState(ctx, tx, r.ParentSessionID, r.ChildSessionID)
	if r.Mode == "steer" {
		// A supplement reserves a separate result, but shares the original active slot.
		l, e := limitsFor(ctx, tx, r.ParentSessionID)
		if e != nil {
			return "", e
		}
		n, e := countsFor(ctx, tx, r.ParentSessionID)
		if e != nil {
			return "", e
		}
		if n.Reserved >= l.Delivery {
			return "", ErrDeliveryLimit
		}
		state, err = "provisioning", nil
	}
	if err != nil {
		return "", err
	}
	now := time.Now().UnixMilli()
	_, err = tx.ExecContext(ctx, `INSERT INTO session_delegations(id,parent_session_id,origin_request_id,tool_call_id,intent_hash,child_session_id,child_request_id,child_name,cwd,instruction,model,effort,sandbox,parent_request_id,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, r.ID, r.ParentSessionID, r.OriginRequestID, r.ToolCallID, r.IntentHash, r.ChildSessionID, r.ChildRequestID, r.ChildName, r.Cwd, r.Instruction, r.Model, r.Effort, r.Sandbox, r.ParentRequestID, state, now, now)
	if err != nil {
		return "", err
	}
	if r.DeliveryState == "not_requested" {
		if _, err = tx.ExecContext(ctx, "UPDATE session_delegations SET delivery_state='not_requested' WHERE id=?", r.ID); err != nil {
			return "", err
		}
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO delegation_source_bindings VALUES(?,?,?,?,?,?)", r.ID, r.SourceThread, r.SourceRevision, r.ExpectedThread, r.ExpectedRevision, r.WorkspaceIdentity); err != nil {
		return "", err
	}
	// Both native and Task API ACKs now promise durable queued work. This fence
	// is committed with authority validation and all identity/outbox facts.
	_, err = tx.ExecContext(ctx, "INSERT INTO native_delegation_commitments VALUES(?,?)", r.ID, now)
	if err == nil {
		err = insertRound(ctx, tx, r)
	}
	return state, err
}
func (s *Store) SourceBinding(ctx context.Context, id string) (string, uint64, bool, error) {
	var thread string
	var revision uint64
	err := s.db.QueryRowContext(ctx, "SELECT thread,revision FROM delegation_source_bindings WHERE id=?", id).Scan(&thread, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, false, nil
	}
	return thread, revision, err == nil, err
}

// TryPromote reserves a slot under the same IMMEDIATE writer lock as admission
// and cancellation. rowid gives commit-order FIFO even within a millisecond.
func (s *Store) TryPromote(ctx context.Context, id string) (bool, error) {
	promoted := false
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var parent, state, child string
		var seq int64
		err := tx.QueryRowContext(ctx, "SELECT parent_session_id,state,child_session_id,rowid FROM session_delegations WHERE id=?", id).Scan(&parent, &state, &child, &seq)
		if err != nil {
			return err
		}
		if state != "queued" {
			return nil
		}
		l, err := limitsFor(ctx, tx, parent)
		if err != nil {
			return err
		}
		n, err := countsFor(ctx, tx, parent)
		if err != nil {
			return err
		}
		if n.Active >= l.Active || n.Active+n.Delivery >= l.Delivery {
			return nil
		}
		var sameChild bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM session_delegations WHERE child_session_id=? AND state IN ('provisioning','running','collecting') AND id NOT IN (SELECT id FROM delegation_rounds WHERE mode='steer'))", child).Scan(&sameChild); err != nil || sameChild {
			return err
		}
		var older bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM session_delegations q WHERE q.parent_session_id=? AND q.state='queued' AND q.rowid<? AND NOT EXISTS(SELECT 1 FROM session_delegations a WHERE a.child_session_id=q.child_session_id AND a.state IN ('provisioning','running','collecting') AND a.id NOT IN (SELECT id FROM delegation_rounds WHERE mode='steer')))", parent, seq).Scan(&older); err != nil || older {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE session_delegations SET state='provisioning',updated_at=? WHERE id=? AND state='queued'", time.Now().UnixMilli(), id)
		if err == nil {
			promoted = true
		}
		return err
	})
	return promoted, err
}

// Native terminal evidence frees the execution slot before final sealing. No
// wall-clock timeout or bridge_recovery marker is allowed to release a slot.
func (s *Store) MarkCollecting(ctx context.Context, id, status string) error {
	if status != "completed" && status != "failed" && status != "interrupted" {
		return errors.New("invalid terminal status")
	}
	_, err := s.db.ExecContext(ctx, "UPDATE session_delegations SET state='collecting',terminal_status=?,updated_at=? WHERE id=? AND state IN ('provisioning','running')", status, time.Now().UnixMilli(), id)
	return err
}

// PendingBatch persists a cyclic keyset cursor; stuck early rows cannot hide
// later parents behind LIMIT 128. The cursor is scheduling only, never evidence
// of task progress. A fixed high-water mark bounds every cycle even under continuous arrivals.
// A crash skips at most one bounded cycle, never a record.
func (s *Store) PendingBatch(ctx context.Context, limit int) ([]Record, error) {
	if limit < 1 || limit > 128 {
		return nil, errors.New("invalid delegation batch")
	}
	out := []Record{}
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var cursor, high int64
		if err := tx.QueryRowContext(ctx, "SELECT position,high_water FROM delegation_scan_cursor WHERE id=1").Scan(&cursor, &high); err != nil {
			return err
		}
		if high == 0 {
			if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(rowid),0) FROM session_delegations").Scan(&high); err != nil {
				return err
			}
		}

		rows, err := tx.QueryContext(ctx, "SELECT "+columns+" FROM session_delegations WHERE (state!='terminal' OR delivery_state IN ('pending','queued')) AND rowid>? AND rowid<=? ORDER BY rowid LIMIT ?", cursor, high, limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			r, e := scanRecord(rows)
			if e != nil {
				rows.Close()
				return e
			}
			out = append(out, r)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(out) < limit {
			_, err = tx.ExecContext(ctx, "UPDATE delegation_scan_cursor SET position=0,high_water=0 WHERE id=1")
		} else {
			_, err = tx.ExecContext(ctx, "UPDATE delegation_scan_cursor SET position=(SELECT rowid FROM session_delegations WHERE id=?),high_water=? WHERE id=1", out[len(out)-1].ID, high)
		}
		return err
	})
	return out, err
}

func (s *Store) ExecutionBinding(ctx context.Context, id string) (string, uint64, string, error) {
	var thread, identity string
	var revision uint64
	err := s.db.QueryRowContext(ctx, "SELECT expected_thread,expected_revision,cwd_identity FROM delegation_source_bindings WHERE id=?", id).Scan(&thread, &revision, &identity)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, "", nil
	}
	return thread, revision, identity, err
}

func (s *Store) HasChild(ctx context.Context, child string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM session_delegations WHERE child_session_id=?)", child).Scan(&exists)
	return exists, err
}
