package messagequeue

import (
	"database/sql"
	"errors"
	"everything-go/internal/taskapi"
)

func (s *Store) TaskJournal() *taskapi.Journal { return taskapi.AttachedJournal(s.db, "ordinary") }

func (s *Store) CancelOrigin(session, request string) (State, error) {
	var state string
	err := s.db.QueryRow("SELECT from_state FROM task_cancel_origins WHERE session_id=? AND request_id=?", session, request).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return State(state), err
}
