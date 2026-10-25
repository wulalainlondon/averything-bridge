package automation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
)

type EventAssignment struct {
	SourceInstanceID string   `json:"source_instance_id,omitempty"`
	RuleVersion      uint64   `json:"rule_version,omitempty"`
	ID               string   `json:"id"`
	EventIDs         []string `json:"event_ids"`
	ReprocessOf      []string `json:"reprocess_of,omitempty"`
	SessionID        string   `json:"session_id"`
	OwnerInstanceID  string   `json:"owner_instance_id"`
	ThreadID         string   `json:"thread_id"`
	ConfigRevision   uint64   `json:"config_revision"`
	OwnerDevice      string   `json:"owner_device"`
	Mode             string   `json:"mode"`
	RuleID           string   `json:"rule_id,omitempty"`
	Content          string   `json:"-"`
	Status           string   `json:"status"`
	CreatedAt        int64    `json:"created_at"`
}

func (s *Store) SaveEventAssignment(ctx context.Context, a EventAssignment) (EventAssignment, error) {
	if a.ID == "" || a.SessionID == "" || a.OwnerDevice == "" || len(a.EventIDs) == 0 || len(a.EventIDs) > 25 {
		return a, errors.New("invalid event assignment")
	}
	sort.Strings(a.ReprocessOf)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return a, err
	}
	defer tx.Rollback()
	var raw []byte
	err = tx.QueryRowContext(ctx, "SELECT payload FROM event_session_assignments WHERE id=?", a.ID).Scan(&raw)
	if err == nil {
		var previous EventAssignment
		if e := json.Unmarshal(raw, &previous); e != nil {
			return a, e
		}
		var content string
		if e := tx.QueryRowContext(ctx, "SELECT content FROM event_session_assignments WHERE id=?", a.ID).Scan(&content); e != nil {
			return a, e
		}
		previous.Content = content
		// Status and creation time are receipts, not caller intent.
		a.Status = previous.Status
		a.CreatedAt = previous.CreatedAt
		if !reflect.DeepEqual(a, previous) {
			return a, ErrConflict
		}
		return previous, nil
	}
	if err != sql.ErrNoRows {
		return a, err
	}
	// A new explicit reprocessing intent compares the current owners atomically.
	// Original receipts/queued requests remain immutable; implicit duplicates still conflict.
	parents := map[string]bool{}
	seenEvents := map[string]bool{}
	for _, id := range a.EventIDs {
		if id == "" || seenEvents[id] {
			return a, errors.New("invalid event IDs")
		}
		seenEvents[id] = true
		var owner string
		err = tx.QueryRowContext(ctx, "SELECT assignment_id FROM event_session_owners WHERE event_id=?", a.SourceInstanceID+"\x00"+id).Scan(&owner)
		if err == nil {
			parents[owner] = true
		} else if err != sql.ErrNoRows {
			return a, err
		}
	}
	expected := map[string]bool{}
	for _, id := range a.ReprocessOf {
		if id == "" || expected[id] {
			return a, ErrConflict
		}
		expected[id] = true
	}
	if !reflect.DeepEqual(parents, expected) {
		return a, ErrConflict
	}
	sort.Strings(a.ReprocessOf)
	a.CreatedAt = s.now().UnixMilli()
	a.Status = "accepted"
	raw, err = json.Marshal(a)
	if err != nil {
		return a, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO event_session_assignments(id,payload,content) VALUES(?,?,?)", a.ID, raw, a.Content)
	if err != nil {
		return a, err
	}
	for _, id := range a.EventIDs {
		if _, err = tx.ExecContext(ctx, "INSERT INTO event_session_owners(event_id,assignment_id) VALUES(?,?) ON CONFLICT(event_id) DO UPDATE SET assignment_id=excluded.assignment_id", a.SourceInstanceID+"\x00"+id, a.ID); err != nil {
			return a, err
		}
	}
	if _, err = s.bump(tx); err != nil {
		return a, err
	}
	err = tx.Commit()
	return a, err
}
func (s *Store) EventAssignments(ctx context.Context) ([]EventAssignment, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT payload,content FROM event_session_assignments ORDER BY rowid DESC LIMIT 1000")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []EventAssignment{}
	for rows.Next() {
		var a EventAssignment
		var raw []byte
		var content string
		if err = rows.Scan(&raw, &content); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		a.Content = content
		result = append(result, a)
	}
	return result, rows.Err()
}

func (s *Store) SaveSessionPolicy(ctx context.Context, id string, a EventAssignment) error {
	raw, err := json.Marshal(a)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, "INSERT INTO event_session_policies(id,payload) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", id, raw)
	return err
}
func (s *Store) SessionPolicy(ctx context.Context, id string) (EventAssignment, error) {
	var a EventAssignment
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT payload FROM event_session_policies WHERE id=?", id).Scan(&raw)
	if err != nil {
		return a, err
	}
	err = json.Unmarshal(raw, &a)
	return a, err
}
