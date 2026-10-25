package taskinteraction

import (
	"context"
	"errors"
)

func (s *Store) InstallControls() error {
	_, e := s.db.Exec("CREATE TABLE IF NOT EXISTS interaction_controls(id TEXT PRIMARY KEY,question TEXT NOT NULL,target TEXT NOT NULL,content_hash TEXT NOT NULL)")
	return e
}
func (s *Store) ReserveControl(ctx context.Context, id, question, target, hash string) error {
	_, e := s.db.ExecContext(ctx, "INSERT OR IGNORE INTO interaction_controls VALUES(?,?,?,?)", id, question, target, hash)
	if e != nil {
		return e
	}
	return s.CheckControl(ctx, id, question, target, hash)
}
func (s *Store) CheckControl(ctx context.Context, id, question, target, hash string) error {
	var q, t, h string
	if e := s.db.QueryRowContext(ctx, "SELECT question,target,content_hash FROM interaction_controls WHERE id=?", id).Scan(&q, &t, &h); e != nil {
		return e
	}
	if q != question || t != target || (hash != "" && h != hash) {
		return errors.New("interaction_control_binding_changed")
	}
	return nil
}
