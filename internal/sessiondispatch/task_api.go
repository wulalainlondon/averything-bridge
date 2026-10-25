package sessiondispatch

import (
	"context"
	"database/sql"
	"encoding/json"
	"everything-go/internal/taskapi"
	"time"
)

func (s *Store) TaskJournal() *taskapi.Journal { return taskapi.AttachedJournal(s.db, "controller") }

func (s *Store) CreateAPI(ctx context.Context, c taskapi.AuthorizedCommand, intent taskapi.IntentRecord, r Record) (taskapi.IntentRecord, bool, error) {
	var result taskapi.IntentRecord
	created := false
	err := s.TaskJournal().Transaction(ctx, func(tx *sql.Tx) error {
		var err error
		result, created, err = s.TaskJournal().ClaimTx(tx, c, intent)
		if err != nil || !created {
			return err
		}
		var count int
		if err = tx.QueryRow(`SELECT COUNT(*) FROM session_dispatches WHERE parent_id=? AND state IN ('prepared','pending','accepted','queued','running','steering','steered','uncertain')`, r.ParentID).Scan(&count); err != nil {
			return err
		}
		if count >= 8 {
			return taskapi.Failure("busy", "known_none", "wait")
		}
		r.CreatedAt = time.Now().UnixMilli()
		r.UpdatedAt = r.CreatedAt
		r.State = "prepared"
		r.DeliveryState = "pending"
		raw, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO session_dispatches VALUES(?,?,?,?,?,?,?)`, r.ID, r.ParentID, r.OriginRequestID, r.ToolCallID, r.IntentHash, r.State, raw); err != nil {
			return err
		}
		return taskapi.ChangeTx(tx, r.SessionID, r.RequestID, "admission")
	})
	return result, created, err
}
