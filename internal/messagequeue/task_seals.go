package messagequeue

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
)

type TaskSeal struct {
	ID, SessionID, RequestID, Hash, Text string
	Anchor                               json.RawMessage
}

func (s *Store) SealTask(ctx context.Context, seal TaskSeal) (TaskSeal, error) {
	digest := sha256.Sum256([]byte(seal.Text))
	if hex.EncodeToString(digest[:]) != seal.Hash {
		return TaskSeal{}, errors.New("seal content hash mismatch")
	}
	var result TaskSeal
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var conflict int
		if err := tx.QueryRow("SELECT (SELECT COUNT(*) FROM task_native_conflicts WHERE session_id=?1 AND request_id=?2)+(SELECT COUNT(*) FROM task_provider_conflicts WHERE session_id=?1 AND request_id=?2)", seal.SessionID, seal.RequestID).Scan(&conflict); err != nil {
			return err
		}
		if conflict > 0 {
			return errors.New("native evidence quarantined")
		}
		var stored TaskSeal
		err := tx.QueryRow("SELECT seal_id,session_id,request_id,hash,text,anchor FROM task_api_seals WHERE session_id=? AND request_id=?", seal.SessionID, seal.RequestID).Scan(&stored.ID, &stored.SessionID, &stored.RequestID, &stored.Hash, &stored.Text, &stored.Anchor)
		if err == nil {
			if stored.Hash != seal.Hash || string(stored.Anchor) != string(seal.Anchor) {
				return errors.New("immutable seal conflict")
			}
			result = stored
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err = tx.Exec("INSERT INTO task_api_seals VALUES(?,?,?,?,?,?)", seal.ID, seal.SessionID, seal.RequestID, seal.Hash, seal.Text, seal.Anchor); err != nil {
			return err
		}
		result = seal
		return taskapi.ChangeTx(tx, seal.SessionID, seal.RequestID, "final_sealed")
	})
	return result, err
}
func (s *Store) TaskSeal(ctx context.Context, session, request, id string) (TaskSeal, error) {
	var r TaskSeal
	err := s.db.QueryRowContext(ctx, "SELECT seal_id,session_id,request_id,hash,text,anchor FROM task_api_seals WHERE session_id=? AND request_id=? AND (?3='' OR seal_id=?3)", session, request, id).Scan(&r.ID, &r.SessionID, &r.RequestID, &r.Hash, &r.Text, &r.Anchor)
	if errors.Is(err, sql.ErrNoRows) {
		return r, taskapi.Failure("result_not_ready", "known_none", "wait")
	}
	return r, err
}
func (s *Store) TaskRevision(ctx context.Context, session, request string) (uint64, error) {
	var r uint64
	err := s.db.QueryRowContext(ctx, "SELECT COALESCE((SELECT revision FROM task_api_revisions WHERE session_id=? AND request_id=?),0)", session, request).Scan(&r)
	return r, err
}
