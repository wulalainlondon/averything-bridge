// Package delegation persists one-off, independent Session handoffs. A turn
// finishing is not itself a delivery: the result and the parent wake are
// recorded separately so a Bridge restart can reconcile either boundary.
package delegation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Record struct {
	RoundVersion                                                            uint64
	AcceptedBackend                                                         string
	LogicalID, PreviousID, Authority, GrantDigest, Mode, ExecutionRequestID string
	ExpectedThread                                                          string
	ExpectedRevision                                                        uint64
	WorkspaceIdentity                                                       string
	SourceThread                                                            string
	SourceRevision                                                          uint64
	ID, ParentSessionID, ChildSessionID, ChildRequestID                     string
	OriginRequestID, ToolCallID, IntentHash                                 string
	ChildName, Cwd, Instruction, Model, Effort, Sandbox                     string
	ParentRequestID, State, TerminalStatus, Result, Error                   string
	DeliveryState, DeliveryError                                            string
	Artifacts                                                               []string
	CreatedAt, UpdatedAt                                                    int64
}

type Store struct{ db *sql.DB }

const ddl = `
CREATE TABLE IF NOT EXISTS native_delegation_commitments(id TEXT PRIMARY KEY, committed_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS task_api_child_profiles(session_id TEXT PRIMARY KEY,profile BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS session_delegations (
  id TEXT PRIMARY KEY,
  parent_session_id TEXT NOT NULL,
  origin_request_id TEXT NOT NULL DEFAULT '',
  tool_call_id TEXT NOT NULL DEFAULT '',
  intent_hash TEXT NOT NULL DEFAULT '',
  child_session_id TEXT NOT NULL,
  child_request_id TEXT NOT NULL,
  child_name TEXT NOT NULL,
  cwd TEXT NOT NULL,
  instruction TEXT NOT NULL,
  model TEXT NOT NULL DEFAULT '',
  effort TEXT NOT NULL DEFAULT '',
  sandbox TEXT NOT NULL DEFAULT '',
  parent_request_id TEXT NOT NULL,
  state TEXT NOT NULL,
  terminal_status TEXT NOT NULL DEFAULT '',
  result TEXT NOT NULL DEFAULT '',
  artifacts_json TEXT NOT NULL DEFAULT '[]',
  error TEXT NOT NULL DEFAULT '',
  delivery_state TEXT NOT NULL DEFAULT 'pending',
  delivery_error TEXT NOT NULL DEFAULT '',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(child_session_id, child_request_id),
  UNIQUE(parent_session_id, parent_request_id)
);
CREATE INDEX IF NOT EXISTS session_delegations_pending ON session_delegations(state,delivery_state,updated_at);
CREATE UNIQUE INDEX IF NOT EXISTS session_delegations_origin ON session_delegations(parent_session_id,origin_request_id,tool_call_id)
  WHERE origin_request_id!='' AND tool_call_id!='';
`

func Open(dataDir string) (*Store, error) {
	if dataDir == "" {
		return nil, errors.New("delegation data directory is required")
	}
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dataDir, "everything_go_delegations.db")+
		"?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(ddl + capacityDDL + handoffDDL + continuityDDL + continuityCommandDDL + reauthorizationDDL); err != nil {
		db.Close()
		return nil, err
	}
	if err := installContinuityEffectBinding(db); err != nil {
		db.Close()
		return nil, err
	}
	if err := taskapi.InstallJournal(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Create(ctx context.Context, r Record) error {
	if r.ID == "" || r.ParentSessionID == "" || r.ChildSessionID == "" || r.ChildRequestID == "" || r.ParentRequestID == "" {
		return errors.New("incomplete delegation identity")
	}
	now := time.Now().UnixMilli()
	_, err := s.db.ExecContext(ctx, `INSERT INTO session_delegations
	  (id,parent_session_id,origin_request_id,tool_call_id,intent_hash,child_session_id,child_request_id,child_name,cwd,instruction,model,effort,sandbox,parent_request_id,state,created_at,updated_at)
	  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'provisioning',?,?)`, r.ID, r.ParentSessionID, r.OriginRequestID, r.ToolCallID, r.IntentHash,
		r.ChildSessionID, r.ChildRequestID, r.ChildName, r.Cwd, r.Instruction, r.Model, r.Effort, r.Sandbox, r.ParentRequestID, now, now)
	return err
}

// CreateBounded atomically accepts execution or bounded durable waiting work.
func (s *Store) CreateBounded(ctx context.Context, r Record) error {
	return s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error { _, err := insertBounded(ctx, tx, r); return err })
}

func (s *Store) NativeCommitment(ctx context.Context, id string) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, "SELECT 1 FROM native_delegation_commitments WHERE id=?", id).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) MarkRunning(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE session_delegations SET state='running',updated_at=? WHERE id=? AND state='provisioning'`, time.Now().UnixMilli(), id)
	return err
}

func (s *Store) MarkProvisionFailed(ctx context.Context, id, reason string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE session_delegations SET state='terminal',terminal_status='failed',error=?,updated_at=?
	  WHERE id=? AND state IN ('queued','provisioning')`, reason, time.Now().UnixMilli(), id)
	return err
}

func (s *Store) Complete(ctx context.Context, childSessionID, childRequestID, status, result, reason string, artifacts []string) (bool, error) {
	if status != "completed" && status != "failed" && status != "interrupted" {
		return false, errors.New("invalid terminal status")
	}
	encoded, err := json.Marshal(artifacts)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, `UPDATE session_delegations SET state='terminal',terminal_status=?,result=?,artifacts_json=?,error=?,updated_at=?
	  WHERE child_session_id=? AND child_request_id=? AND state IN ('provisioning','running','collecting')`,
		status, result, string(encoded), reason, time.Now().UnixMilli(), childSessionID, childRequestID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (s *Store) MarkDeliveryQueued(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE session_delegations SET delivery_state='queued',updated_at=?
	  WHERE id=? AND state='terminal' AND delivery_state='pending'`, time.Now().UnixMilli(), id)
	return err
}

func (s *Store) MarkDelivered(ctx context.Context, parentSessionID, parentRequestID string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE session_delegations SET delivery_state='delivered',updated_at=?
	  WHERE parent_session_id=? AND parent_request_id=? AND delivery_state='queued'`,
		time.Now().UnixMilli(), parentSessionID, parentRequestID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func (s *Store) MarkDeliveryFailed(ctx context.Context, parentSessionID, parentRequestID, reason string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE session_delegations SET delivery_state='failed',delivery_error=?,updated_at=?
	  WHERE parent_session_id=? AND parent_request_id=? AND state='terminal' AND delivery_state IN ('pending','queued')`,
		reason, time.Now().UnixMilli(), parentSessionID, parentRequestID)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

func scanRecord(row interface{ Scan(...any) error }) (Record, error) {
	var r Record
	var artifacts string
	err := row.Scan(&r.ID, &r.ParentSessionID, &r.OriginRequestID, &r.ToolCallID, &r.IntentHash, &r.ChildSessionID, &r.ChildRequestID,
		&r.ChildName, &r.Cwd, &r.Instruction, &r.Model, &r.Effort, &r.Sandbox,
		&r.ParentRequestID, &r.State, &r.TerminalStatus, &r.Result, &artifacts, &r.Error,
		&r.DeliveryState, &r.DeliveryError, &r.CreatedAt, &r.UpdatedAt)
	if err == nil {
		err = json.Unmarshal([]byte(artifacts), &r.Artifacts)
	}
	return r, err
}

const columns = `id,parent_session_id,origin_request_id,tool_call_id,intent_hash,child_session_id,child_request_id,child_name,cwd,instruction,model,effort,sandbox,parent_request_id,state,
  terminal_status,result,artifacts_json,error,delivery_state,delivery_error,created_at,updated_at`

func (s *Store) ByChildRequest(ctx context.Context, sessionID, requestID string) (Record, bool, error) {
	r, err := scanRecord(s.db.QueryRowContext(ctx, `SELECT `+columns+` FROM session_delegations WHERE child_session_id=? AND child_request_id=?`, sessionID, requestID))
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	return r, err == nil, err
}

func (s *Store) ByOrigin(ctx context.Context, parentSessionID, originRequestID, toolCallID string) (Record, bool, error) {
	r, err := scanRecord(s.db.QueryRowContext(ctx, `SELECT `+columns+` FROM session_delegations
	  WHERE parent_session_id=? AND origin_request_id=? AND tool_call_id=?`, parentSessionID, originRequestID, toolCallID))
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	return r, err == nil, err
}

func (s *Store) ByID(ctx context.Context, id string) (Record, bool, error) {
	r, err := scanRecord(s.db.QueryRowContext(ctx, `SELECT `+columns+` FROM session_delegations WHERE id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	return r, err == nil, err
}

func (s *Store) ByParentRequest(ctx context.Context, sessionID, requestID string) (Record, bool, error) {
	r, err := scanRecord(s.db.QueryRowContext(ctx, `SELECT `+columns+` FROM session_delegations WHERE parent_session_id=? AND parent_request_id=?`, sessionID, requestID))
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	return r, err == nil, err
}

func (s *Store) Pending(ctx context.Context) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+columns+` FROM session_delegations WHERE state!='terminal' OR delivery_state IN ('pending','queued') ORDER BY created_at LIMIT 128`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ListByParent(ctx context.Context, sessionID string) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+columns+` FROM session_delegations WHERE parent_session_id=? ORDER BY created_at`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		r, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ActiveCountForParent(ctx context.Context, sessionID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM session_delegations WHERE parent_session_id=?
	  AND state IN ('provisioning','running')`, sessionID).Scan(&count)
	return count, err
}

// HasChildReceipt checks exact canonical delegation identity, not inferred history.
func (s *Store) HasChildReceipt(ctx context.Context, session, request string) (bool, error) {
	var found bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM session_delegations WHERE child_session_id=? AND child_request_id=?)`, session, request).Scan(&found)
	return found, err
}
