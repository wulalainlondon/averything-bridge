package messagequeue

import (
	"context"
	"database/sql"
	"errors"
	"everything-go/internal/taskapi"
)

type ProviderEvidence struct{ SessionID, RequestID, Backend, ConversationID, Token, TokenKind, MessageID, Text, Status string }

func (s *Store) ProviderEvidence(session, request string) (ProviderEvidence, bool, error) {
	var r ProviderEvidence
	err := s.db.QueryRow(`SELECT session_id,request_id,backend,conversation_id,token,token_kind,message_id,text,status FROM task_api_provider_evidence WHERE session_id=? AND request_id=?`, session, request).Scan(&r.SessionID, &r.RequestID, &r.Backend, &r.ConversationID, &r.Token, &r.TokenKind, &r.MessageID, &r.Text, &r.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return r, false, nil
	}
	return r, err == nil, err
}
func (s *Store) RecordProviderEvidence(r ProviderEvidence) error {
	if r.Backend != "claude" || r.ConversationID == "" || r.Token == "" || r.TokenKind != "message_uuid" {
		return errors.New("invalid provider evidence")
	}
	conflicted := false
	err := s.TaskJournal().Transaction(context.Background(), func(tx *sql.Tx) error {
		var old ProviderEvidence
		err := tx.QueryRow("SELECT backend,conversation_id,token,message_id,text,status FROM task_api_provider_evidence WHERE session_id=? AND request_id=?", r.SessionID, r.RequestID).Scan(&old.Backend, &old.ConversationID, &old.Token, &old.MessageID, &old.Text, &old.Status)
		if err == nil {
			conflict := old.Backend != r.Backend || old.ConversationID != r.ConversationID || old.Token != r.Token || old.Status != "" && (r.Status != "" && (old.Status != r.Status || old.Text != r.Text) || r.MessageID != "" && old.MessageID != r.MessageID)
			if conflict {
				conflicted = true
				if _, e := tx.Exec("INSERT OR IGNORE INTO task_provider_conflicts VALUES(?,?)", r.SessionID, r.RequestID); e != nil {
					return e
				}
				return taskapi.ChangeTx(tx, r.SessionID, r.RequestID, "provider_conflict")
			}
			if r.Status == "" || old.Status == r.Status && old.MessageID == r.MessageID && old.Text == r.Text {
				return nil
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = tx.Exec(`INSERT INTO task_api_provider_evidence VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id,request_id) DO UPDATE SET message_id=excluded.message_id,text=excluded.text,status=excluded.status`, r.SessionID, r.RequestID, r.Backend, r.ConversationID, r.Token, r.TokenKind, r.MessageID, r.Text, r.Status)
		if err != nil {
			return err
		}
		return taskapi.ChangeTx(tx, r.SessionID, r.RequestID, "provider_evidence")
	})
	if err == nil && conflicted {
		return errors.New("provider_evidence_conflict")
	}
	return err
}
