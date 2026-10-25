// Package voicetransfer holds receipts outside the voice being retired.
// It never dispatches a voice, text turn, device action or recovered effect.
package voicetransfer

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"time"
)

type Target struct {
	Session  string `json:"session_id"`
	Thread   string `json:"thread_id"`
	Name     string `json:"name"`
	Revision uint64 `json:"config_revision"`
}
type Record struct {
	ID            string `json:"transfer_id"`
	Authority     string `json:"authority_instance_id"`
	Source        Target `json:"source"`
	Target        Target `json:"target"`
	SourceVoice   string `json:"source_voice_id"`
	Authorization string `json:"authorization,omitempty"`
	State         string `json:"state"`
	Revision      uint64 `json:"revision"`
	CreatedAt     int64  `json:"created_at"`
	UpdatedAt     int64  `json:"updated_at"`
	TargetVoice   string `json:"target_voice_id,omitempty"`
	Device        string `json:"-"`
	Request       string `json:"-"`
	Turn          string `json:"-"`
	Tool          string `json:"-"`
}

// DirectAuthorized is only for a fresh, provider-verified spoken transfer intent.
// A receipt read, legacy confirmation, or recovered operation cannot grant a new effect.
func (r Record) DirectAuthorized(now time.Time) bool {
	return r.Authorization == "voice_command" && r.State == "confirmation_required" && r.Revision == 1 && r.CreatedAt > 0 && now.UnixMilli() >= r.CreatedAt && now.Sub(time.UnixMilli(r.CreatedAt)) <= 30*time.Second
}

type Store struct{ db *sql.DB }

func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("voice_transfer_storage_required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "voice_transfers.sqlite")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	file.Close()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;
 CREATE TABLE IF NOT EXISTS voice_transfers(id TEXT PRIMARY KEY,device TEXT NOT NULL,state TEXT NOT NULL,revision INTEGER NOT NULL,caller TEXT NOT NULL,payload TEXT NOT NULL);
 CREATE UNIQUE INDEX IF NOT EXISTS voice_transfer_one_device ON voice_transfers(device) WHERE state NOT IN ('completed','cancelled','failed');
 UPDATE voice_transfers SET state='unknown_recovered',revision=revision+1,payload=json_set(payload,'$.state','unknown_recovered','$.revision',revision+1) WHERE state NOT IN ('completed','cancelled','failed','unknown_recovered');`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db}, nil
}
func (s *Store) Close() error { return s.db.Close() }
func ID(authority, request, tool string) string {
	sum := sha256.Sum256([]byte(authority + "\x00" + request + "\x00" + tool))
	return "vt_" + hex.EncodeToString(sum[:16])
}
func (s *Store) Get(ctx context.Context, id, device string) (Record, error) {
	var r Record
	var payload, caller string
	err := s.db.QueryRowContext(ctx, "SELECT payload,caller FROM voice_transfers WHERE id=? AND device=?", id, device).Scan(&payload, &caller)
	if err != nil {
		return r, errors.New("voice_transfer_not_found")
	}
	if err = json.Unmarshal([]byte(payload), &r); err != nil {
		return r, err
	}
	r.Device = device
	var c struct{ Request, Turn, Tool string }
	if err = json.Unmarshal([]byte(caller), &c); err != nil {
		return r, err
	}
	r.Request, r.Turn, r.Tool = c.Request, c.Turn, c.Tool
	return r, nil
}
func (s *Store) Create(ctx context.Context, r Record) (Record, error) {
	if old, err := s.Get(ctx, r.ID, r.Device); err == nil {
		if old.Source != r.Source || old.Target != r.Target || old.SourceVoice != r.SourceVoice || old.Request != r.Request || old.Tool != r.Tool || old.Authorization != r.Authorization {
			return Record{}, errors.New("voice_transfer_intent_conflict")
		}
		return old, nil
	}
	r.State = "confirmation_required"
	r.Revision = 1
	r.CreatedAt = time.Now().UnixMilli()
	r.UpdatedAt = r.CreatedAt
	data, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	caller, _ := json.Marshal(struct{ Request, Turn, Tool string }{r.Request, r.Turn, r.Tool})
	_, err = s.db.ExecContext(ctx, "INSERT INTO voice_transfers VALUES(?,?,?,?,?,?)", r.ID, r.Device, r.State, r.Revision, string(caller), string(data))
	if err != nil {
		return Record{}, errors.New("voice_transfer_pending_or_conflict")
	}
	return r, nil
}
func (s *Store) Advance(ctx context.Context, r Record, next, targetVoice string) (Record, error) {
	valid := map[string]map[string]bool{
		"unknown_recovered":     {"failed": true},
		"confirmation_required": {"confirmed": true, "cancelled": true, "failed": true},
		"confirmed":             {"a_released": true, "cancelled": true, "failed": true},
		"a_released":            {"starting_b": true, "cancelled": true, "failed": true},
		"starting_b":            {"completed": true, "cancelled": true, "failed": true},
	}
	if !valid[r.State][next] {
		return r, errors.New("voice_transfer_stage_invalid")
	}
	before := r.Revision
	r.Revision++
	r.State = next
	r.UpdatedAt = time.Now().UnixMilli()
	if targetVoice != "" {
		r.TargetVoice = targetVoice
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	x, err := s.db.ExecContext(ctx, "UPDATE voice_transfers SET state=?,revision=?,payload=? WHERE id=? AND device=? AND revision=?", next, r.Revision, string(payload), r.ID, r.Device, before)
	if err != nil {
		return Record{}, err
	}
	n, err := x.RowsAffected()
	if err != nil {
		return Record{}, err
	}
	if n != 1 {
		return Record{}, errors.New("voice_transfer_revision_conflict")
	}
	return r, nil
}
