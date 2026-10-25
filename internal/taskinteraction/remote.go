package taskinteraction

import (
	"context"
	"database/sql"
	"errors"
)

func (s *Store) InstallRemote() error {
	_, e := s.db.Exec(`CREATE TABLE IF NOT EXISTS relay_receipts(id TEXT PRIMARY KEY,version INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS interaction_outbox(scope TEXT NOT NULL,key TEXT NOT NULL,hash TEXT NOT NULL,question TEXT NOT NULL,state TEXT NOT NULL,PRIMARY KEY(scope,key));`)
	return e
}
func (s *Store) Import(ctx context.Context, remote Record) (Record, error) {
	if remote.ID == "" || remote.Binding.Authority == "" || remote.Binding.ParentAuthority == remote.Binding.Authority {
		return Record{}, errors.New("invalid_remote_interaction")
	}
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Record{}, e
	}
	defer tx.Rollback()
	var b []byte
	e = tx.QueryRowContext(ctx, "SELECT data FROM questions WHERE id=?", remote.ID).Scan(&b)
	remote.AckToken = ""
	remote.OriginKey = "replica:" + remote.Binding.Authority + ":" + remote.ID
	if e == nil {
		old, e := decode(b)
		if e != nil {
			return old, e
		}
		if old.Binding != remote.Binding || old.IntentHash != remote.IntentHash {
			return old, errors.New("remote_interaction_conflict")
		}
		if remote.Version <= old.AuthorityVersion {
			return old, nil
		}
		sourceVersion := remote.Version
		remote.Version = old.Version + 1
		remote.AuthorityVersion = sourceVersion
		remote.Notification = old.Notification
		remote.NotificationRequest = old.NotificationRequest
		_, e = tx.ExecContext(ctx, "UPDATE questions SET version=?,data=? WHERE id=?", remote.Version, encode(remote), remote.ID)
		if e != nil {
			return old, e
		}
	} else if e == sql.ErrNoRows {
		remote.AuthorityVersion = remote.Version
		_, e = tx.ExecContext(ctx, "INSERT INTO questions VALUES(?,?,?,?,?,?,?)", remote.ID, remote.Binding.Child, remote.Binding.ChildRequest, remote.Binding.Parent, remote.OriginKey, remote.Version, encode(remote))
		if e != nil {
			return remote, e
		}
	} else {
		return remote, e
	}
	return remote, tx.Commit()
}
func (s *Store) RelayVersion(ctx context.Context, id string) uint64 {
	var v uint64
	_ = s.db.QueryRowContext(ctx, "SELECT version FROM relay_receipts WHERE id=?", id).Scan(&v)
	return v
}
func (s *Store) RelayAccepted(ctx context.Context, id string, v uint64) error {
	_, e := s.db.ExecContext(ctx, "INSERT INTO relay_receipts VALUES(?,?) ON CONFLICT(id) DO UPDATE SET version=MAX(version,excluded.version)", id, v)
	return e
}
func (s *Store) RemoteCommand(ctx context.Context, scope, key string) (string, bool, error) {
	var state string
	e := s.db.QueryRowContext(ctx, "SELECT state FROM interaction_outbox WHERE scope=? AND key=?", scope, key).Scan(&state)
	if e == sql.ErrNoRows {
		return "", false, nil
	}
	return state, e == nil, e
}
func (s *Store) RemoteComplete(ctx context.Context, scope, key string) error {
	_, e := s.db.ExecContext(ctx, "UPDATE interaction_outbox SET state='complete' WHERE scope=? AND key=?", scope, key)
	return e
}
func (s *Store) Command(ctx context.Context, scope, key string) (Record, bool, error) {
	var id string
	e := s.db.QueryRowContext(ctx, "SELECT question FROM commands WHERE scope=? AND key=?", scope, key).Scan(&id)
	if e == sql.ErrNoRows {
		return Record{}, false, nil
	}
	if e != nil {
		return Record{}, false, e
	}
	r, e := s.Get(ctx, id)
	return r, e == nil, e
}
func (s *Store) ReserveRemoteOnce(ctx context.Context, scope, key, hash, id string) (bool, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return false, e
	}
	defer tx.Rollback()
	result, e := tx.ExecContext(ctx, "INSERT OR IGNORE INTO interaction_outbox VALUES(?,?,?,?,'unknown')", scope, key, hash, id)
	if e != nil {
		return false, e
	}
	n, e := result.RowsAffected()
	if e != nil {
		return false, e
	}
	var h, q string
	if e = tx.QueryRowContext(ctx, "SELECT hash,question FROM interaction_outbox WHERE scope=? AND key=?", scope, key).Scan(&h, &q); e != nil {
		return false, e
	}
	if h != hash || q != id {
		return false, errors.New("idempotency_conflict")
	}
	return n == 1, tx.Commit()
}
