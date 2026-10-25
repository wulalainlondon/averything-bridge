package taskapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// Journal attaches transport intent/outbox/change metadata to an EXISTING
// canonical store connection. It never opens a database or owns task state.
type Journal struct {
	db    *sql.DB
	Store string
}
type IntentRecord struct {
	Key, Hash, ReceiptID, TaskID, NativeID, SessionID, RequestID, ScopeID string
	Generation                                                            uint64
	Path                                                                  string
	Metadata                                                              []byte
	CreatedAt                                                             int64
	Expired                                                               bool
}
type Change struct {
	Sequence                   uint64
	SessionID, RequestID, Kind string
	At                         int64
}

func InstallJournal(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS task_api_schema(version INTEGER NOT NULL); INSERT INTO task_api_schema(version) SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM task_api_schema);
 CREATE TABLE IF NOT EXISTS task_api_intents(namespace_key TEXT PRIMARY KEY,intent_hash TEXT NOT NULL,receipt_id TEXT NOT NULL UNIQUE,task_id TEXT NOT NULL,native_id TEXT NOT NULL,session_id TEXT NOT NULL,request_id TEXT NOT NULL,scope_id TEXT NOT NULL,generation INTEGER NOT NULL,path TEXT NOT NULL,metadata BLOB,created_at INTEGER NOT NULL);
 CREATE INDEX IF NOT EXISTS task_api_scope ON task_api_intents(scope_id,generation,task_id);
 CREATE TABLE IF NOT EXISTS task_api_outbox(namespace_key TEXT PRIMARY KEY,state TEXT NOT NULL,updated_at INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS task_api_changes(seq INTEGER PRIMARY KEY AUTOINCREMENT,session_id TEXT NOT NULL,request_id TEXT NOT NULL,kind TEXT NOT NULL,at INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS task_api_revisions(session_id TEXT NOT NULL,request_id TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(session_id,request_id));
 INSERT INTO task_api_revisions SELECT session_id,request_id,MAX(seq) FROM task_api_changes WHERE request_id!='' GROUP BY session_id,request_id ON CONFLICT(session_id,request_id) DO UPDATE SET revision=MAX(revision,excluded.revision);
 CREATE TABLE IF NOT EXISTS task_api_retention(id INTEGER PRIMARY KEY CHECK(id=1),floor INTEGER NOT NULL); INSERT OR IGNORE INTO task_api_retention VALUES(1,0);
 CREATE TABLE IF NOT EXISTS task_api_outcomes(namespace_key TEXT PRIMARY KEY,error BLOB);
 CREATE TABLE IF NOT EXISTS task_api_freezes(id TEXT PRIMARY KEY,scope_id TEXT NOT NULL,generation INTEGER NOT NULL,filter_hash TEXT NOT NULL,expires_at INTEGER NOT NULL,watermarks BLOB NOT NULL,rows BLOB NOT NULL);`)
	return err
}
func AttachedJournal(db *sql.DB, store string) *Journal { return &Journal{db, store} }
func (j *Journal) Transaction(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func scanIntent(row interface{ Scan(...any) error }) (IntentRecord, error) {
	var r IntentRecord
	err := row.Scan(&r.Key, &r.Hash, &r.ReceiptID, &r.TaskID, &r.NativeID, &r.SessionID, &r.RequestID, &r.ScopeID, &r.Generation, &r.Path, &r.Metadata, &r.CreatedAt)
	r.Expired = r.Metadata == nil
	return r, err
}

const intentColumns = "namespace_key,intent_hash,receipt_id,task_id,native_id,session_id,request_id,scope_id,generation,path,metadata,created_at"

func (j *Journal) ClaimTx(tx *sql.Tx, c AuthorizedCommand, r IntentRecord) (IntentRecord, bool, error) {
	key := c.Namespace.Key(c.Request.IdempotencyKey)
	previous, err := scanIntent(tx.QueryRow("SELECT "+intentColumns+" FROM task_api_intents WHERE namespace_key=?", key))
	if err == nil {
		if previous.Hash != c.IntentHash {
			return previous, false, Failure("idempotency_conflict", "known_receipt", "lookup_original")
		}
		if previous.Expired {
			return previous, false, Failure("key_expired", "known_receipt", "lookup_original")
		}
		return previous, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return r, false, err
	}
	r.Key = key
	r.Hash = c.IntentHash
	r.ScopeID = c.Caller.StableScopeID
	r.Generation = c.Caller.NamespaceGeneration
	r.Path = c.Locator.Path
	r.CreatedAt = time.Now().UnixMilli()
	_, err = tx.Exec(`INSERT INTO task_api_intents(`+intentColumns+`) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, r.Key, r.Hash, r.ReceiptID, r.TaskID, r.NativeID, r.SessionID, r.RequestID, r.ScopeID, r.Generation, r.Path, r.Metadata, r.CreatedAt)
	if err != nil {
		return r, false, err
	}
	_, err = tx.Exec(`INSERT INTO task_api_outbox VALUES(?,'prepared',?)`, key, r.CreatedAt)
	if err == nil {
		err = ChangeTx(tx, r.SessionID, r.RequestID, "intent_prepared")
	}
	return r, true, err
}
func (j *Journal) Lookup(ctx context.Context, n Namespace, key string) (IntentRecord, error) {
	r, err := scanIntent(j.db.QueryRowContext(ctx, "SELECT "+intentColumns+" FROM task_api_intents WHERE namespace_key=?", n.Key(key)))
	if errors.Is(err, sql.ErrNoRows) {
		return r, Failure("not_found", "known_none", "lookup_original")
	}
	if err == nil && r.Expired {
		return r, Failure("key_expired", "known_receipt", "lookup_original")
	}
	return r, err
}
func (j *Journal) Task(ctx context.Context, scope string, generation uint64, id string) (IntentRecord, error) {
	r, err := scanIntent(j.db.QueryRowContext(ctx, "SELECT "+intentColumns+" FROM task_api_intents WHERE scope_id=? AND generation=? AND task_id=? ORDER BY created_at LIMIT 1", scope, generation, id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, Failure("not_found", "known_none", "lookup_original")
	}
	return r, err
}
func (j *Journal) List(ctx context.Context, scope string, generation uint64) ([]IntentRecord, error) {
	rows, err := j.db.QueryContext(ctx, "SELECT "+intentColumns+" FROM task_api_intents WHERE scope_id=? AND generation=? ORDER BY task_id,created_at", scope, generation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []IntentRecord{}
	seen := map[string]bool{}
	for rows.Next() {
		r, err := scanIntent(rows)
		if err != nil {
			return nil, err
		}
		if !r.Expired && !seen[r.TaskID] {
			out = append(out, r)
			seen[r.TaskID] = true
		}
	}
	return out, rows.Err()
}
func (j *Journal) Outbox(ctx context.Context, key, state string) error {
	_, err := j.db.ExecContext(ctx, "UPDATE task_api_outbox SET state=?,updated_at=? WHERE namespace_key=?", state, time.Now().UnixMilli(), key)
	return err
}
func ChangeTx(tx *sql.Tx, session, request, kind string) error {
	result, err := tx.Exec(`INSERT INTO task_api_changes(session_id,request_id,kind,at) VALUES(?,?,?,?)`, session, request, kind, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	if request != "" {
		sequence, e := result.LastInsertId()
		if e != nil {
			return e
		}
		_, err = tx.Exec("INSERT INTO task_api_revisions VALUES(?,?,?) ON CONFLICT(session_id,request_id) DO UPDATE SET revision=MAX(revision,excluded.revision)", session, request, sequence)
	}
	return err
}
func (j *Journal) Changes(ctx context.Context, after uint64, limit int) ([]Change, uint64, error) {
	var floor uint64
	if err := j.db.QueryRowContext(ctx, "SELECT floor FROM task_api_retention WHERE id=1").Scan(&floor); err != nil {
		return nil, 0, err
	}
	if after < floor {
		return nil, floor, Failure("cursor_expired", "known_none", "refresh_snapshot")
	}
	rows, err := j.db.QueryContext(ctx, "SELECT seq,session_id,request_id,kind,at FROM task_api_changes WHERE seq>? ORDER BY seq LIMIT ?", after, limit)
	if err != nil {
		return nil, floor, err
	}
	defer rows.Close()
	out := []Change{}
	for rows.Next() {
		var r Change
		if err := rows.Scan(&r.Sequence, &r.SessionID, &r.RequestID, &r.Kind, &r.At); err != nil {
			return nil, floor, err
		}
		out = append(out, r)
	}
	return out, floor, rows.Err()
}
func (j *Journal) Watermark(ctx context.Context) (uint64, error) {
	var n uint64
	err := j.db.QueryRowContext(ctx, "SELECT MAX(COALESCE((SELECT MAX(seq) FROM task_api_changes),0),(SELECT floor FROM task_api_retention WHERE id=1))").Scan(&n)
	return n, err
}
func (j *Journal) Freeze(ctx context.Context, scope ReadScope, f Freeze, rows []SnapshotRow) error {
	wm, err := json.Marshal(f.Watermarks)
	if err != nil {
		return err
	}
	data, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	_, err = j.db.ExecContext(ctx, "INSERT INTO task_api_freezes VALUES(?,?,?,?,?,?,?)", f.ID, scope.StableScopeID, scope.NamespaceGeneration, scope.FilterHash, f.ExpiresAt, wm, data)
	return err
}
func (j *Journal) Frozen(ctx context.Context, scope ReadScope, f Freeze) ([]SnapshotRow, error) {
	var data, wm []byte
	err := j.db.QueryRowContext(ctx, "SELECT rows,watermarks FROM task_api_freezes WHERE id=? AND scope_id=? AND generation=? AND filter_hash=? AND expires_at>?", f.ID, scope.StableScopeID, scope.NamespaceGeneration, scope.FilterHash, time.Now().UnixMilli()).Scan(&data, &wm)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, Failure("cursor_expired", "known_none", "refresh_snapshot")
	}
	if err != nil {
		return nil, err
	}
	expected, _ := json.Marshal(f.Watermarks)
	if string(expected) != string(wm) {
		return nil, Failure("cursor_expired", "known_none", "refresh_snapshot")
	}
	var rows []SnapshotRow
	err = json.Unmarshal(data, &rows)
	return rows, err
}

// Terminal body expiration never removes intent/effect digest tombstones or
// prepared/unknown outboxes. Changes have a measurable retention floor.
func (j *Journal) Prune(ctx context.Context, now time.Time) error {
	return j.Transaction(ctx, func(tx *sql.Tx) error {
		cut := now.Add(-30 * 24 * time.Hour).UnixMilli()
		var floor uint64
		if err := tx.QueryRow("SELECT COALESCE(MAX(seq),0) FROM task_api_changes WHERE at<? OR seq<=(SELECT COALESCE(MAX(seq),0)-100000 FROM task_api_changes)", cut).Scan(&floor); err != nil {
			return err
		}
		if _, err := tx.Exec("DELETE FROM task_api_changes WHERE seq<=?", floor); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE task_api_retention SET floor=MAX(floor,?) WHERE id=1", floor); err != nil {
			return err
		}
		result, err := tx.Exec("UPDATE task_api_intents SET metadata=NULL WHERE metadata IS NOT NULL AND created_at<? AND namespace_key IN(SELECT namespace_key FROM task_api_outbox WHERE state='resolved')", now.Add(-90*24*time.Hour).UnixMilli())
		if err != nil {
			return err
		}
		if n, e := result.RowsAffected(); e != nil {
			return e
		} else if n > 0 {
			// Body expiry invalidates old event cursors. Otherwise a cached task
			// could retain a body after a silent membership change.
			if err = ChangeTx(tx, "", "", "receipt_expired"); err != nil {
				return err
			}
			if _, err = tx.Exec("UPDATE task_api_retention SET floor=MAX(floor,(SELECT COALESCE(MAX(seq),0) FROM task_api_changes)) WHERE id=1"); err != nil {
				return err
			}
		}
		_, err = tx.Exec("DELETE FROM task_api_freezes WHERE expires_at<=?", now.UnixMilli())
		return err
	})
}

func (j *Journal) Receipt(ctx context.Context, scope string, generation uint64, id string) (IntentRecord, error) {
	r, e := scanIntent(j.db.QueryRowContext(ctx, "SELECT "+intentColumns+" FROM task_api_intents WHERE scope_id=? AND generation=? AND receipt_id=?", scope, generation, id))
	if errors.Is(e, sql.ErrNoRows) {
		return r, Failure("not_found", "known_none", "lookup_original")
	}
	return r, e
}

func (j *Journal) Pending(ctx context.Context) ([]IntentRecord, error) {
	rows, err := j.db.QueryContext(ctx, "SELECT "+intentColumns+" FROM task_api_intents WHERE namespace_key IN(SELECT namespace_key FROM task_api_outbox WHERE state IN('prepared','queued')) ORDER BY created_at")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []IntentRecord{}
	for rows.Next() {
		r, err := scanIntent(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// Outcome is the immutable result of a mutation attempt, separate from current
// native task state. A refused cancel cannot later become a successful retry.
func (j *Journal) Outcome(ctx context.Context, key string) (*APIError, bool, error) {
	var raw []byte
	err := j.db.QueryRowContext(ctx, "SELECT error FROM task_api_outcomes WHERE namespace_key=?", key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if raw == nil {
		return nil, true, nil
	}
	var result APIError
	err = json.Unmarshal(raw, &result)
	return &result, true, err
}
func (j *Journal) SaveOutcome(ctx context.Context, key string, failure *APIError) error {
	return j.Transaction(ctx, func(tx *sql.Tx) error { return SaveOutcomeTx(tx, key, failure) })
}
func SaveOutcomeTx(tx *sql.Tx, key string, failure *APIError) error {
	var raw any
	if failure != nil {
		bytes, err := json.Marshal(failure)
		if err != nil {
			return err
		}
		raw = bytes
	}
	_, err := tx.Exec("INSERT INTO task_api_outcomes VALUES(?,?) ON CONFLICT(namespace_key) DO NOTHING", key, raw)
	if err != nil {
		return err
	}
	_, err = tx.Exec("UPDATE task_api_outbox SET state='resolved',updated_at=? WHERE namespace_key=?", time.Now().UnixMilli(), key)
	return err
}
func (j *Journal) TaskCommands(ctx context.Context, task string) ([]IntentRecord, error) {
	rows, err := j.db.QueryContext(ctx, "SELECT "+intentColumns+" FROM task_api_intents WHERE task_id=? ORDER BY created_at", task)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []IntentRecord{}
	for rows.Next() {
		r, err := scanIntent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// FreezeChecked commits the materialization only if this original store still
// has the validated event watermark. Capture verifies every store twice; a
// concurrent writer must emit its invalidation in its state transaction.
func (j *Journal) FreezeChecked(ctx context.Context, scope ReadScope, f Freeze, rows []SnapshotRow, expected uint64) error {
	wm, _ := json.Marshal(f.Watermarks)
	data, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	return j.Transaction(ctx, func(tx *sql.Tx) error {
		var n uint64
		if err := tx.QueryRow("SELECT MAX(COALESCE((SELECT MAX(seq) FROM task_api_changes),0),(SELECT floor FROM task_api_retention WHERE id=1))").Scan(&n); err != nil {
			return err
		}
		if n != expected {
			return Failure("busy", "known_none", "wait")
		}
		_, err := tx.Exec("INSERT INTO task_api_freezes VALUES(?,?,?,?,?,?,?)", f.ID, scope.StableScopeID, scope.NamespaceGeneration, scope.FilterHash, f.ExpiresAt, wm, data)
		return err
	})
}

// All is an internal canonical intent projection. The service must authorize
// every row with the original paired-human/session path before exposing it.
func (j *Journal) All(ctx context.Context) ([]IntentRecord, error) {
	rows, err := j.db.QueryContext(ctx, "SELECT "+intentColumns+" FROM task_api_intents WHERE metadata IS NOT NULL ORDER BY task_id,created_at")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []IntentRecord{}
	seen := map[string]bool{}
	for rows.Next() {
		r, e := scanIntent(rows)
		if e != nil {
			return nil, e
		}
		if !seen[r.TaskID] {
			out = append(out, r)
			seen[r.TaskID] = true
		}
	}
	return out, rows.Err()
}
func (j *Journal) FindTask(ctx context.Context, id string) (IntentRecord, error) {
	r, err := scanIntent(j.db.QueryRowContext(ctx, "SELECT "+intentColumns+" FROM task_api_intents WHERE task_id=? ORDER BY created_at LIMIT 1", id))
	if errors.Is(err, sql.ErrNoRows) {
		return r, Failure("not_found", "known_none", "lookup_original")
	}
	return r, err
}

func (j *Journal) OutboxState(ctx context.Context, key string) (string, error) {
	var state string
	err := j.db.QueryRowContext(ctx, "SELECT state FROM task_api_outbox WHERE namespace_key=?", key).Scan(&state)
	return state, err
}

// OriginalCommandForRequest resolves persisted request identity without
// substituting a task's first turn for a later accepted continuation.
func (j *Journal) OriginalCommandForRequest(ctx context.Context, session, request string) (IntentRecord, error) {
	return scanIntent(j.db.QueryRowContext(ctx, "SELECT "+intentColumns+" FROM task_api_intents WHERE session_id=? AND request_id=? ORDER BY rowid LIMIT 1", session, request))
}
