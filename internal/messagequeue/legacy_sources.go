package messagequeue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"time"
)

type LegacySourceIdentity struct {
	InstanceID string `json:"instance_id"`
	SessionID  string `json:"session_id"`
	ThreadID   string `json:"native_thread_id"`
	Revision   uint64 `json:"config_revision"`
}
type LegacySourceLink struct {
	ID           string               `json:"relation_id"`
	Source       LegacySourceIdentity `json:"source"`
	Target       LegacySourceIdentity `json:"target"`
	RequestID    string               `json:"request_id"`
	Hash         string               `json:"receipt_hash"`
	Actor        string               `json:"actor_scope_id"`
	Generation   uint64               `json:"-"`
	Kind         string               `json:"kind"`
	NativeOrigin string               `json:"native_origin_status"`
	Revoked      bool                 `json:"revoked"`
}

func installLegacySources(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS task_legacy_source_links(relation_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,request_id TEXT NOT NULL,actor TEXT NOT NULL,generation INTEGER NOT NULL,metadata BLOB NOT NULL); CREATE TABLE IF NOT EXISTS task_legacy_source_revocations(relation_id TEXT PRIMARY KEY,actor TEXT NOT NULL,generation INTEGER NOT NULL,at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS task_legacy_source_intake_receipts(namespace_key TEXT PRIMARY KEY,relation_id TEXT NOT NULL);
INSERT OR IGNORE INTO task_legacy_source_intake_receipts SELECT namespace_key,json_extract(metadata,'$.LegacyLinkID') FROM task_api_intents WHERE json_valid(metadata) AND json_extract(metadata,'$.LegacyLinkID') IS NOT NULL;`)
	return err
}
func (s *Store) LegacySourceLinks(ctx context.Context) ([]LegacySourceLink, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT l.metadata,l.generation,EXISTS(SELECT 1 FROM task_legacy_source_revocations r WHERE r.relation_id=l.relation_id) FROM task_legacy_source_links l ORDER BY l.rowid LIMIT 1001`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []LegacySourceLink{}
	for rows.Next() {
		var data []byte
		var link LegacySourceLink
		var revoked bool
		var generation uint64
		if err = rows.Scan(&data, &generation, &revoked); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &link); err != nil {
			return nil, err
		}
		link.Generation = generation
		link.Revoked = revoked
		out = append(out, link)
	}
	return out, rows.Err()
}

// Intake has its own authenticated operation intent. It never fabricates a
// create-dispatch caller, changes the original receipt/admission, or enqueues.
func (s *Store) ConfirmLegacySource(ctx context.Context, c taskapi.AuthorizedCommand, record taskapi.IntentRecord, link LegacySourceLink, queueRevision uint64) (taskapi.IntentRecord, error) {
	var out taskapi.IntentRecord
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		existing, created, err := s.TaskJournal().ClaimTx(tx, c, record)
		if err != nil {
			return err
		}
		out = existing
		if !created {
			return nil
		}
		var count int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM task_legacy_source_links`).Scan(&count); err != nil {
			return err
		}
		if count >= 1000 {
			return taskapi.Failure("busy", "known_none", "read_capabilities")
		}
		var formal int
		if err = tx.QueryRow(`SELECT COUNT(*) FROM task_api_intents i WHERE session_id=? AND request_id=? AND namespace_key!=? AND NOT EXISTS(SELECT 1 FROM task_legacy_source_intake_receipts l WHERE l.namespace_key=i.namespace_key)`, link.Target.SessionID, link.RequestID, out.Key).Scan(&formal); err != nil {
			return err
		}
		if formal > 0 {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		var revision uint64
		if err = tx.QueryRow(`SELECT revision FROM queue_sessions WHERE session_id=?`, link.Target.SessionID).Scan(&revision); err != nil {
			return err
		}
		if revision != queueRevision {
			return taskapi.Failure("stale_revision", "known_none", "refresh_identity")
		}
		original, found, err := lookup(tx, link.Target.SessionID, link.RequestID)
		if err != nil {
			return err
		}
		if !found || original.PayloadHash != link.Hash {
			return taskapi.Failure("stale_revision", "known_none", "refresh_identity")
		}
		var active int
		if err = tx.QueryRow(`SELECT COUNT(*) FROM task_legacy_source_links l WHERE l.session_id=? AND l.request_id=? AND NOT EXISTS(SELECT 1 FROM task_legacy_source_revocations r WHERE r.relation_id=l.relation_id)`, link.Target.SessionID, link.RequestID).Scan(&active); err != nil {
			return err
		}
		if active > 0 {
			return taskapi.Failure("idempotency_conflict", "known_none", "lookup_original")
		}
		raw, err := json.Marshal(link)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO task_legacy_source_links VALUES(?,?,?,?,?,?)`, link.ID, link.Target.SessionID, link.RequestID, link.Actor, link.Generation, raw); err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO task_legacy_source_intake_receipts VALUES(?,?)`, out.Key, link.ID); err != nil {
			return err
		}
		if err = taskapi.ChangeTx(tx, link.Target.SessionID, link.RequestID, "human_source_linked"); err != nil {
			return err
		}
		return taskapi.SaveOutcomeTx(tx, out.Key, nil)
	})
	return out, err
}
func (s *Store) RevokeLegacySource(ctx context.Context, c taskapi.AuthorizedCommand, record taskapi.IntentRecord, link LegacySourceLink) (taskapi.IntentRecord, error) {
	var out taskapi.IntentRecord
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		existing, created, err := s.TaskJournal().ClaimTx(tx, c, record)
		if err != nil {
			return err
		}
		out = existing
		if !created {
			return nil
		}
		if link.Actor != c.Caller.StableScopeID {
			return taskapi.Failure("permission", "known_none", "request_scope_change")
		}
		result, err := tx.Exec(`INSERT OR IGNORE INTO task_legacy_source_revocations VALUES(?,?,?,?)`, link.ID, c.Caller.StableScopeID, c.Caller.NamespaceGeneration, time.Now().UnixMilli())
		if err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO task_legacy_source_intake_receipts VALUES(?,?)`, out.Key, link.ID); err != nil {
			return err
		}
		count, _ := result.RowsAffected()
		if count > 0 {
			if err = taskapi.ChangeTx(tx, link.Target.SessionID, link.RequestID, "human_source_revoked"); err != nil {
				return err
			}
		}
		return taskapi.SaveOutcomeTx(tx, out.Key, nil)
	})
	return out, err
}
func (s *Store) LegacySourceLink(ctx context.Context, id string) (LegacySourceLink, error) {
	var raw []byte
	var out LegacySourceLink
	var generation uint64
	var revoked bool
	err := s.db.QueryRowContext(ctx, `SELECT metadata,generation,EXISTS(SELECT 1 FROM task_legacy_source_revocations r WHERE r.relation_id=l.relation_id) FROM task_legacy_source_links l WHERE relation_id=?`, id).Scan(&raw, &generation, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return out, taskapi.Failure("not_found", "known_none", "lookup_original")
	}
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(raw, &out)
	out.Generation = generation
	out.Revoked = revoked
	return out, err
}

// Exact original-store metadata, never runtime discovery or inferred source identity.
func (s *Store) HasFormalTaskSource(ctx context.Context, session, request string) (bool, error) {
	var known bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM task_api_intents i WHERE session_id=? AND request_id=? AND NOT EXISTS(SELECT 1 FROM task_legacy_source_intake_receipts l WHERE l.namespace_key=i.namespace_key))`, session, request).Scan(&known)
	return known, err
}
