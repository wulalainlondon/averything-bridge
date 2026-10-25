package eventinbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
)

// CommitSourceBatch owns the source checkpoint and normalized event evidence in
// one SQLite transaction. Cursors are opaque bytes; a caller may encrypt them.
func (s *Store) CommitSourceBatch(ctx context.Context, source, stream string, expected, next []byte, input []Input) ([]Event, error) {
	if source == "" || len(source) > 64 || stream == "" || len(stream) > 128 || len(next) > 16384 || len(input) > 100 {
		return nil, errors.New("invalid source batch")
	}
	if err := s.ensureCursors(ctx); err != nil {
		return nil, err
	}
	now := s.now().UnixMilli()
	normalized := make([]Input, len(input))
	for i, in := range input {
		if in.Source != source {
			return nil, errors.New("source batch scope mismatch")
		}
		var err error
		normalized[i], err = normalizeInput(in, now)
		if err != nil {
			return nil, err
		}
		if len(in.Attachments) != 0 {
			return nil, errors.New("batch attachments require separate materialization")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var current []byte
	err = tx.QueryRowContext(ctx, "SELECT cursor FROM external_source_cursors WHERE source=? AND stream=?", source, stream).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if !bytes.Equal(current, expected) {
		return nil, errors.New("source_cursor_conflict")
	}
	events := []Event{}
	for _, in := range normalized {
		result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO external_events(id,authority_instance_id,source,event_key,kind,severity,title,body,url,data_json,occurred_at,received_at,expires_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, "evt_"+randomHex(16), s.instanceID, in.Source, in.EventKey, in.Kind, in.Severity, in.Title, in.Body, in.URL, string(in.Data), in.OccurredAt, now, in.ExpiresAt)
		if err != nil {
			return nil, err
		}
		rows, _ := result.RowsAffected()
		if rows > 0 {
			event, err := getBySourceKey(ctx, tx, source, in.EventKey)
			if err != nil {
				return nil, err
			}
			events = append(events, event)
		}
	}
	// Cap each source separately; a busy account cannot evict another source here.
	if _, err = tx.ExecContext(ctx, "DELETE FROM external_events WHERE source=? AND (expires_at<=? OR sequence NOT IN (SELECT sequence FROM external_events WHERE source=? ORDER BY sequence DESC LIMIT 10000))", source, now, source); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO external_source_cursors(source,stream,cursor) VALUES(?,?,?) ON CONFLICT(source,stream) DO UPDATE SET cursor=excluded.cursor", source, stream, next); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return events, nil
}
func (s *Store) ensureCursors(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS external_source_cursors(source TEXT NOT NULL,stream TEXT NOT NULL,cursor BLOB NOT NULL,PRIMARY KEY(source,stream))")
	return err
}
func (s *Store) SourceCursor(ctx context.Context, source, stream string) ([]byte, error) {
	if err := s.ensureCursors(ctx); err != nil {
		return nil, err
	}
	var value []byte
	err := s.db.QueryRowContext(ctx, "SELECT cursor FROM external_source_cursors WHERE source=? AND stream=?", source, stream).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return value, err
}
