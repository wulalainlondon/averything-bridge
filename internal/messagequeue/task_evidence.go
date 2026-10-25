package messagequeue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"everything-go/internal/taskapi"
	"strings"
)

var ErrNativeConflict = errors.New("native_acceptance_conflict")

type NativeReceipt struct{ ThreadID, TurnID string }

func (s *Store) RecordNativeAcceptance(session, request, thread, turn string) error {
	if session == "" || request == "" || thread == "" || turn == "" {
		return errors.New("invalid native acceptance")
	}
	conflict := false
	err := s.TaskJournal().Transaction(context.Background(), func(tx *sql.Tx) error {
		var oldThread, oldTurn string
		err := tx.QueryRow(`SELECT thread_id,turn_id FROM task_native_acceptance WHERE session_id=? AND request_id=?`, session, request).Scan(&oldThread, &oldTurn)
		if err == nil {
			if oldThread == thread && oldTurn == turn {
				return nil
			}
			conflict = true
			if _, err = tx.Exec(`INSERT OR IGNORE INTO task_native_conflicts(session_id,request_id,thread_id,turn_id) VALUES(?,?,?,?)`, session, request, thread, turn); err != nil {
				return err
			}
			result, err := tx.Exec(`INSERT OR IGNORE INTO task_native_conflict_evidence(session_id,request_id,thread_id,turn_id) VALUES(?,?,?,?)`, session, request, thread, turn)
			if err != nil {
				return err
			}
			count, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if count == 0 {
				return nil
			}
			return taskapi.ChangeTx(tx, session, request, "native_conflict")
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO task_native_acceptance(session_id,request_id,thread_id,turn_id) VALUES(?,?,?,?)`, session, request, thread, turn); err != nil {
			return err
		}
		return taskapi.ChangeTx(tx, session, request, "native_acceptance")
	})
	if err == nil && conflict {
		return ErrNativeConflict
	}
	return err
}

// The one-statement view includes native and provider conflicts even if the
// acceptance tuple is absent. Absence, unavailable and conflict never alias.
func (s *Store) ExactNativeAcceptance(session, request string) (NativeReceipt, bool, error) {
	var thread, turn sql.NullString
	var found, conflict bool
	err := s.db.QueryRow(`SELECT (SELECT thread_id FROM task_native_acceptance WHERE session_id=?1 AND request_id=?2),(SELECT turn_id FROM task_native_acceptance WHERE session_id=?1 AND request_id=?2),EXISTS(SELECT 1 FROM task_native_acceptance WHERE session_id=?1 AND request_id=?2),EXISTS(SELECT 1 FROM task_native_conflicts WHERE session_id=?1 AND request_id=?2) OR EXISTS(SELECT 1 FROM task_provider_conflicts WHERE session_id=?1 AND request_id=?2)`, session, request).Scan(&thread, &turn, &found, &conflict)
	if err != nil {
		return NativeReceipt{}, false, err
	}
	if conflict {
		return NativeReceipt{}, false, ErrNativeConflict
	}
	if !found {
		return NativeReceipt{}, false, nil
	}
	if !thread.Valid || !turn.Valid || thread.String == "" || turn.String == "" {
		return NativeReceipt{}, false, errors.New("invalid canonical native tuple")
	}
	return NativeReceipt{thread.String, turn.String}, true, nil
}
func (s *Store) NativeAcceptance(session, request, thread string) string {
	out, found, err := s.ExactNativeAcceptance(session, request)
	if err == nil && found && out.ThreadID == thread {
		return out.TurnID
	}
	return ""
}

// OriginEntries queries typed admission payloads only, never runtime directories.
func (s *Store) OriginEntries(instance, parent string) ([]Entry, error) {
	rows, err := s.db.Query("SELECT q."+strings.ReplaceAll(columns, ",", ",q.")+` FROM queue_commands q JOIN task_admissions a ON q.session_id=a.session_id AND q.request_id=a.request_id WHERE a.origin_instance=? AND a.origin_session=? ORDER BY q.seq DESC LIMIT 100`, instance, parent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Entry{}
	for rows.Next() {
		e, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Admission survives terminal payload/attachment clearing in the same enqueue
// transaction. No attachment bytes or credentials enter this typed relation.
func saveTaskAdmission(tx *sql.Tx, e Entry) error {
	var raw struct {
		MessagePurpose     string          `json:"message_purpose,omitempty"`
		CallbackOnComplete *bool           `json:"callback_on_complete,omitempty"`
		CallbackBinding    string          `json:"callback_binding,omitempty"`
		OwnerDevice        string          `json:"owner_device"`
		Origin             json.RawMessage `json:"task_origin"`
		Target             json.RawMessage `json:"expected_target"`
		Content            string          `json:"content"`
	}
	if json.Unmarshal(e.Payload, &raw) != nil || raw.OwnerDevice == "" && len(raw.Origin) == 0 && raw.MessagePurpose == "" {
		return nil
	}
	var origin struct {
		InstanceID string `json:"instance_id"`
		SessionID  string `json:"session_id"`
	}
	if len(raw.Origin) > 0 {
		if err := json.Unmarshal(raw.Origin, &origin); err != nil {
			return err
		}
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO task_admissions(session_id,request_id,origin_instance,origin_session,metadata) VALUES(?,?,?,?,?)`, e.SessionID, e.RequestID, origin.InstanceID, origin.SessionID, data)
	return err
}
func (s *Store) TaskAdmission(session, request string) ([]byte, bool, error) {
	var data []byte
	err := s.db.QueryRow(`SELECT metadata FROM task_admissions WHERE session_id=? AND request_id=?`, session, request).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	return data, err == nil, err
}

func (s *Store) NativeConflict(session, request string) (bool, error) {
	var count int
	err := s.db.QueryRow("SELECT (SELECT COUNT(*) FROM task_native_conflicts WHERE session_id=?1 AND request_id=?2)+(SELECT COUNT(*) FROM task_provider_conflicts WHERE session_id=?1 AND request_id=?2)", session, request).Scan(&count)
	return count > 0, err
}

// Exact original acceptance tuple; never substitutes today's ResumeID.
func (s *Store) NativeExecution(session, request string) (string, string, bool) {
	if conflict, e := s.NativeConflict(session, request); e != nil || conflict {
		return "", "", false
	}
	var thread, turn string
	err := s.db.QueryRow("SELECT thread_id,turn_id FROM task_native_acceptance WHERE session_id=? AND request_id=?", session, request).Scan(&thread, &turn)
	return thread, turn, err == nil && thread != "" && turn != ""
}
