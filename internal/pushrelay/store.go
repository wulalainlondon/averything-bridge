package pushrelay

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"time"

	_ "modernc.org/sqlite"
)

var ErrDenied = errors.New("not_authorized")

type Store struct{ db *sql.DB }

func OpenStore(path string) (*Store, error) {
	if info, err := os.Lstat(path); err == nil && !info.Mode().IsRegular() {
		return nil, errors.New("relay database must be a regular file")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, errors.New("private relay directory unavailable")
	}
	info, err := os.Stat(filepath.Dir(path))
	if err != nil || (runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		return nil, errors.New("relay database requires a private directory (0700)")
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.New("relay database unavailable")
	}
	info, err = f.Stat()
	f.Close()
	if err != nil || !info.Mode().IsRegular() || (runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		return nil, errors.New("relay database must be private (0600)")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS bridges(id TEXT PRIMARY KEY, authority TEXT NOT NULL, disabled INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS devices(bridge TEXT NOT NULL, device TEXT NOT NULL, token TEXT NOT NULL, PRIMARY KEY(bridge,device));
CREATE TABLE IF NOT EXISTS live_activities(bridge TEXT NOT NULL, device TEXT NOT NULL, activity TEXT NOT NULL, token_hash TEXT NOT NULL, live_token TEXT NOT NULL, expires INTEGER NOT NULL, PRIMARY KEY(bridge,device));
CREATE TABLE IF NOT EXISTS challenges(id TEXT PRIMARY KEY, bridge TEXT NOT NULL, device TEXT NOT NULL, token TEXT NOT NULL, code_hash TEXT NOT NULL, expires INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS receipts(bridge TEXT NOT NULL, request TEXT NOT NULL, digest TEXT NOT NULL, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', updated INTEGER NOT NULL, PRIMARY KEY(bridge,request));
CREATE TABLE IF NOT EXISTS rates(bucket TEXT PRIMARY KEY, period INTEGER NOT NULL, count INTEGER NOT NULL, updated INTEGER NOT NULL);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	// No WAL: SQLite's rollback journal inherits the database's private mode.
	// The service needs a persistent volume; an ephemeral Cloud Run filesystem
	// is not a production store and must not be presented as one.
	return &Store{db: db}, nil
}
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) enroll(ctx context.Context, id, authority string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var existing string
	var disabled int
	err = tx.QueryRowContext(ctx, `SELECT authority,disabled FROM bridges WHERE id=?`, id).Scan(&existing, &disabled)
	if err == nil {
		if existing != authority || disabled != 0 {
			return ErrDenied
		}
		return tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM bridges`).Scan(&count); err != nil {
		return err
	}
	if count >= 10000 {
		return ErrDenied
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO bridges(id,authority) VALUES(?,?)`, id, authority)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) authority(ctx context.Context, id string) (string, error) {
	var authority string
	err := s.db.QueryRowContext(ctx, `SELECT authority FROM bridges WHERE id=? AND disabled=0`, id).Scan(&authority)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrDenied
	}
	return authority, err
}

// RevokeBridge is operator-only. It is never exposed on the public HTTP API.
func (s *Store) RevokeBridge(ctx context.Context, id string) error {
	result, err := s.db.ExecContext(ctx, `UPDATE bridges SET disabled=1 WHERE id=?`, id)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrDenied
	}
	return nil
}

func (s *Store) rate(ctx context.Context, bucket string, limit int, window time.Duration, now time.Time) (bool, error) {
	period := now.Unix() / int64(window/time.Second)
	var count int
	err := s.db.QueryRowContext(ctx, `INSERT INTO rates(bucket,period,count,updated) VALUES(?,?,1,?)
ON CONFLICT(bucket) DO UPDATE SET count=CASE WHEN period=excluded.period THEN count+1 ELSE 1 END,period=excluded.period,updated=excluded.updated RETURNING count`, bucket, period, now.Unix()).Scan(&count)
	return count <= limit, err
}

func (s *Store) challenge(ctx context.Context, id, bridge string, input ChallengeRequest, code string, expires int64) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO challenges(id,bridge,device,token,code_hash,expires) VALUES(?,?,?,?,?,?)`, id, bridge, input.DeviceID, input.Token, digest(id+":"+code), expires)
	return err
}

func (s *Store) confirm(ctx context.Context, bridge string, input ConfirmRequest, now int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var device, token, hash string
	var expires int64
	var attempts int
	err = tx.QueryRowContext(ctx, `SELECT device,token,code_hash,expires,attempts FROM challenges WHERE id=? AND bridge=?`, input.ChallengeID, bridge).Scan(&device, &token, &hash, &expires, &attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDenied
	}
	if err != nil {
		return err
	}
	if now >= expires || attempts >= 5 {
		return ErrDenied
	}
	if hash != digest(input.ChallengeID+":"+input.Code) {
		_, err = tx.ExecContext(ctx, `UPDATE challenges SET attempts=attempts+1 WHERE id=?`, input.ChallengeID)
		if err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		return ErrDenied
	}
	var devices int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM devices WHERE bridge=?`, bridge).Scan(&devices); err != nil {
		return err
	}
	if devices >= 64 {
		var exists int
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM devices WHERE bridge=? AND device=?`, bridge, device).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return ErrDenied
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO devices(bridge,device,token) VALUES(?,?,?) ON CONFLICT(bridge,device) DO UPDATE SET token=excluded.token`, bridge, device, token)
	if err != nil {
		return err
	}
	// Confirmation is single-use; other pending attempts for this device may
	// not later revert the binding to an older registration token.
	_, err = tx.ExecContext(ctx, `DELETE FROM challenges WHERE bridge=? AND device=?`, bridge, device)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) devices(ctx context.Context, bridge string) ([]DeviceStatus, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT device,token FROM devices WHERE bridge=? ORDER BY device`, bridge)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]DeviceStatus, 0)
	for rows.Next() {
		var id, token string
		if err = rows.Scan(&id, &token); err != nil {
			return nil, err
		}
		result = append(result, DeviceStatus{DeviceID: id, TokenHash: digest(token)})
	}
	return result, rows.Err()
}

func (s *Store) token(ctx context.Context, bridge, device, hash string) (string, error) {
	var token string
	err := s.db.QueryRowContext(ctx, `SELECT token FROM devices WHERE bridge=? AND device=?`, bridge, device).Scan(&token)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrDenied
	}
	if err != nil {
		return "", err
	}
	if digest(token) != hash {
		return "", ErrDenied
	}
	return token, nil
}
func (s *Store) revokeDevice(ctx context.Context, bridge, device, expectedHash string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var token string
	err = tx.QueryRowContext(ctx, `SELECT token FROM devices WHERE bridge=? AND device=?`, bridge, device).Scan(&token)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if digest(token) != expectedHash {
		return nil
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM devices WHERE bridge=? AND device=? AND token=?`, bridge, device, token); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM challenges WHERE bridge=? AND device=? AND token=?`, bridge, device, token); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM live_activities WHERE bridge=? AND device=? AND token_hash=?`, bridge, device, expectedHash); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) invalidateDevice(ctx context.Context, bridge, device, token string) error {
	// A response for an old registration must not revoke a newly verified
	// replacement token or erase the replacement's pending ownership proof.
	_, err := s.db.ExecContext(ctx, `DELETE FROM devices WHERE bridge=? AND device=? AND token=?`, bridge, device, token)
	return err
}

// reserve provides durable at-most-once submission. A crash/network ambiguity
// leaves 'sending'/'uncertain', never an automatic resend to Google. Only an
// explicit provider rejection (429/5xx response) can be retried with the same ID.
func (s *Store) reserve(ctx context.Context, bridge string, input SendRequest, now int64) (bool, ProviderResult, error) {
	body, err := jsonBytes(input)
	if err != nil {
		return false, ProviderResult{}, err
	}
	hash := digest(string(body))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, ProviderResult{}, err
	}
	defer tx.Rollback()
	var existing, status, reason string
	err = tx.QueryRowContext(ctx, `SELECT digest,status,error FROM receipts WHERE bridge=? AND request=?`, bridge, input.RequestID).Scan(&existing, &status, &reason)
	if err == nil {
		if existing != hash {
			return false, ProviderResult{}, errors.New("idempotency_conflict")
		}
		if status != Retryable {
			return false, ProviderResult{Status: status, Error: reason}, nil
		}
		_, err = tx.ExecContext(ctx, `UPDATE receipts SET status='sending',error='',updated=? WHERE bridge=? AND request=?`, now, bridge, input.RequestID)
	} else if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `INSERT INTO receipts(bridge,request,digest,status,updated) VALUES(?,?,?,'sending',?)`, bridge, input.RequestID, hash, now)
	}
	if err != nil {
		return false, ProviderResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return false, ProviderResult{}, err
	}
	return true, ProviderResult{}, nil
}
func (s *Store) finish(ctx context.Context, bridge, request string, result ProviderResult, now int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE receipts SET status=?,error=?,updated=? WHERE bridge=? AND request=?`, result.Status, result.Error, now, bridge, request)
	return err
}

func (s *Store) prune(ctx context.Context, now int64) error {
	// Retain receipt tombstones for 30 days. HTTP clients cannot request IDs
	// older than 24h (enforced at the server), so expiry cannot resurrect sends.
	for _, item := range []struct {
		query  string
		cutoff int64
	}{
		{`DELETE FROM challenges WHERE expires<?`, now - 3600},
		{`DELETE FROM receipts WHERE updated<?`, now - 30*24*3600},
		{`DELETE FROM rates WHERE updated<?`, now - 24*3600},
	} {
		if _, err := s.db.ExecContext(ctx, item.query, item.cutoff); err != nil {
			return err
		}
	}
	return nil
}
