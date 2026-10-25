package workitems

import (
	"context"
	"encoding/json"

	"everything-go/internal/coordination"
)

// Collaboration uses the existing work DB and its single-writer connection.
// Events live in the durable document; the wire exposes only a bounded tail.
func (s *Service) Collaboration(ctx context.Context) (coordination.State, error) {
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return coordination.State{}, err
	}
	defer tx.Rollback()
	state, _, err := readCollaboration(ctx, tx)
	if err != nil {
		return state, err
	}
	return state, tx.Commit()
}
func (s *Service) UpdateCollaboration(ctx context.Context, fn func(*coordination.State) error) (coordination.State, error) {
	state := coordination.NewState()
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return state, err
	}
	defer tx.Rollback()
	state, previous, err := readCollaboration(ctx, tx)
	if err != nil {
		return state, err
	}
	before, err := json.Marshal(state)
	if err != nil {
		return state, err
	}
	if err = fn(&state); err != nil {
		return state, err
	}
	after, err := json.Marshal(state)
	if err != nil {
		return state, err
	}
	if string(before) == string(after) {
		return state, nil
	}
	legacy, records, err := collaborationRecords(state)
	if err != nil {
		return state, err
	}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		return state, err
	}
	if err = s.store.writeCollaborationRecords(ctx, tx, previous, records, state); err != nil {
		return state, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO work_pm_state(id,payload) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", string(encoded)); err != nil {
		return state, err
	}
	err = tx.Commit()
	return state, err
}
