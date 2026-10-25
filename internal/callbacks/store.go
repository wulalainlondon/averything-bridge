// Package callbacks persists explicit, device-bound task-report invitations.
// It never starts work or acquires media; those effects require an owning claim.
package callbacks

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
	"sync"
	"time"
)

const AnyRevision = ^uint64(0)

var ErrConflict = errors.New("callback_revision_conflict")
var ErrOperation = errors.New("callback_operation_conflict")
var ErrUnavailable = errors.New("callback_unavailable")

type Recipient struct {
	Device    string `json:"device"`
	Binding   string `json:"-"`
	Name      string `json:"name"`
	Inventory string `json:"inventory_id"`
}

// DiskRecipient avoids exposing a pairing fingerprint on the public wire.
type Report struct {
	ExecutionRequest string `json:"execution_request_id,omitempty"`
	SourceKind       string `json:"source_kind,omitempty"`
	SourceID         string `json:"source_id,omitempty"`
	SourceRevision   uint64 `json:"source_revision,omitempty"`
	ID               string `json:"id"`
	Hash             string `json:"hash"`
	Text             string `json:"text"`
	Status           string `json:"status"`
	At               int64  `json:"at"`
	Thread           string `json:"thread"`
	Turn             string `json:"turn"`
}
type Invitation struct {
	ClaimedAt       int64  `json:"claimed_at,omitempty"`
	Manual          bool   `json:"manual,omitempty"`
	AudioObserved   bool   `json:"audio_observed,omitempty"`
	ContentVerified bool   `json:"content_verified,omitempty"`
	ContentMismatch bool   `json:"content_mismatch,omitempty"`
	ID              string `json:"id"`
	Generation      uint64 `json:"generation"`
	State           string `json:"state"`
	Issued          int64  `json:"issued_at"`
	Expires         int64  `json:"expires_at"`
	Claim           string `json:"claim_operation,omitempty"`
	Voice           string `json:"voice_id,omitempty"`
	ReportOperation string `json:"report_operation,omitempty"`
	Reason          string `json:"reason,omitempty"`
	PresentedAt     int64  `json:"presented_at,omitempty"`
}
type Record struct {
	RequesterBinding  string      `json:"-"`
	RecipientEligible bool        `json:"recipient_eligible"`
	ID                string      `json:"id"`
	Revision          uint64      `json:"revision"`
	Authority         string      `json:"authority"`
	Session           string      `json:"session_id"`
	Request           string      `json:"task_request_id"`
	Name              string      `json:"session_name"`
	Thread            string      `json:"thread_id"`
	Epoch             string      `json:"session_epoch"`
	Config            uint64      `json:"config_revision"`
	Origin            string      `json:"origin_device"`
	Requester         string      `json:"requesting_device"`
	Recipient         Recipient   `json:"recipient"`
	Enabled           bool        `json:"enabled"`
	Failures          bool        `json:"include_failure"`
	State             string      `json:"state"`
	Report            *Report     `json:"report,omitempty"`
	Invite            *Invitation `json:"invitation,omitempty"`
	Updated           int64       `json:"updated_at"`
}
type diskRecord struct {
	Record
	Binding          string `json:"recipient_binding"`
	RequesterBinding string `json:"requester_binding"`
}
type Store struct {
	mu  sync.Mutex
	db  *sql.DB
	now func() time.Time
}

func Open(dir string) (*Store, error) {
	path := ":memory:"
	if dir != "" {
		if e := os.MkdirAll(dir, 0700); e != nil {
			return nil, e
		}
		path = filepath.Join(dir, "task_callbacks.sqlite")
		f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if e != nil {
			return nil, e
		}
		f.Close()
	}
	db, e := sql.Open("sqlite", path)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	_, e = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
 CREATE TABLE IF NOT EXISTS callbacks(id TEXT PRIMARY KEY,session TEXT NOT NULL,request TEXT NOT NULL,record BLOB NOT NULL);
 CREATE INDEX IF NOT EXISTS callbacks_source ON callbacks(session,request);
 CREATE INDEX IF NOT EXISTS callbacks_active_recipient ON callbacks(
 json_extract(record,'$.recipient.device'),json_extract(record,'$.recipient_binding'),
 json_extract(record,'$.enabled'),json_extract(record,'$.invitation.state'),json_extract(record,'$.invitation.expires_at'),id);
 CREATE TABLE IF NOT EXISTS callback_operations(id TEXT PRIMARY KEY,fingerprint TEXT NOT NULL,response BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS callback_delivery_history(group_key TEXT PRIMARY KEY,invitation TEXT NOT NULL);`)
	if e != nil {
		db.Close()
		return nil, e
	}
	return &Store{db: db, now: time.Now}, nil
}
func (s *Store) Close() error { return s.db.Close() }

// Operation reads an original receipt without re-running its mutation.
func (s *Store) Operation(ctx context.Context, id string) (Record, string, bool, error) {
	var fingerprint string
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT fingerprint,response FROM callback_operations WHERE id=?", id).Scan(&fingerprint, &b)
	if errors.Is(e, sql.ErrNoRows) {
		return Record{}, "", false, nil
	}
	if e != nil {
		return Record{}, "", false, e
	}
	r, e := decode(b)
	return r, fingerprint, true, e
}
func ID(parts ...string) string {
	b, _ := json.Marshal(parts)
	h := sha256.Sum256(b)
	return "cb_" + hex.EncodeToString(h[:24])
}
func Digest(text string) string { h := sha256.Sum256([]byte(text)); return hex.EncodeToString(h[:]) }
func encode(r Record) []byte {
	b, _ := json.Marshal(diskRecord{Record: r, Binding: r.Recipient.Binding, RequesterBinding: r.RequesterBinding})
	return b
}
func decode(b []byte) (Record, error) {
	var d diskRecord
	e := json.Unmarshal(b, &d)
	d.Record.Recipient.Binding = d.Binding
	d.Record.RequesterBinding = d.RequesterBinding
	if d.Record.RequesterBinding == "" && d.Record.Requester == d.Record.Recipient.Device {
		d.Record.RequesterBinding = d.Binding
	}
	return d.Record, e
}
func (s *Store) Get(ctx context.Context, id string) (Record, bool, error) {
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT record FROM callbacks WHERE id=?", id).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	if e != nil {
		return Record{}, false, e
	}
	r, e := decode(b)
	return r, true, e
}
func (s *Store) List(ctx context.Context) ([]Record, error) {
	rows, e := s.db.QueryContext(ctx, "SELECT record FROM callbacks ORDER BY id")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, e
		}
		r, e := decode(b)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ActiveInvitationPage omits report bodies in SQL before copying rows into Go.
// Historical policies never consume this reconnect page. The recipient and
// pairing-generation binding are server-derived, not caller-supplied grants.
const activeInvitationPageQuery = `SELECT json_remove(record,'$.report') FROM callbacks
 WHERE id > ? AND json_extract(record,'$.enabled')=1
 AND json_extract(record,'$.recipient.device')=? AND json_extract(record,'$.recipient_binding')=?
 AND json_extract(record,'$.invitation.state') IN ('delivery_pending','ringing')
 AND json_extract(record,'$.invitation.expires_at') > ? ORDER BY id LIMIT ?`

func (s *Store) ActiveInvitationPage(ctx context.Context, device, binding, after string, limit int, now int64) ([]Record, string, error) {
	if device == "" || binding == "" || len(after) > 200 || limit < 1 || limit > 64 {
		return nil, "", errors.New("invalid_callback_page")
	}
	rows, err := s.db.QueryContext(ctx, activeInvitationPageQuery, after, device, binding, now, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	out := make([]Record, 0, limit)
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, "", err
		}
		if len(out) == limit {
			return out, out[len(out)-1].ID, nil
		}
		r, err := decode(raw)
		if err != nil {
			return nil, "", err
		}
		out = append(out, r)
	}
	return out, "", rows.Err()
}

// Mutate commits the receipt and state together. Duplicate operation IDs return
// their original answer; they never execute fn twice, including after restart.
func (s *Store) Mutate(ctx context.Context, op, fingerprint, id string, expected uint64, fn func(*Record, bool) error) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Record{}, e
	}
	defer tx.Rollback()
	var oldFP string
	var b []byte
	e = tx.QueryRowContext(ctx, "SELECT fingerprint,response FROM callback_operations WHERE id=?", op).Scan(&oldFP, &b)
	if e == nil {
		if oldFP != fingerprint {
			return Record{}, ErrOperation
		}
		return decode(b)
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return Record{}, e
	}
	e = tx.QueryRowContext(ctx, "SELECT record FROM callbacks WHERE id=?", id).Scan(&b)
	exists := e == nil
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return Record{}, e
	}
	var r Record
	if exists {
		r, e = decode(b)
		if e != nil {
			return r, e
		}
	}
	if expected != AnyRevision && r.Revision != expected {
		return r, ErrConflict
	}
	if e = fn(&r, exists); e != nil {
		return r, e
	}
	r.ID = id
	if e = groupAutomaticInvitation(ctx, tx, &r); e != nil {
		return r, e
	}
	r.Revision++
	r.Updated = s.now().UnixMilli()
	b = encode(r)
	if _, e = tx.ExecContext(ctx, "INSERT INTO callbacks VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET record=excluded.record", id, r.Session, r.Request, b); e != nil {
		return r, e
	}
	if _, e = tx.ExecContext(ctx, "INSERT INTO callback_operations VALUES(?,?,?)", op, fingerprint, b); e != nil {
		return r, e
	}
	return r, tx.Commit()
}
func Terminal(state string) bool {
	switch state {
	case "declined", "missed", "expired", "revoked", "ended", "failed", "busy_deferred", "unsupported":
		return true
	}
	return false
}
func NewInvitation(r *Record, now int64, operation string) {
	g := uint64(1)
	if r.Invite != nil {
		g = r.Invite.Generation + 1
	}
	r.Invite = &Invitation{ID: ID(r.ID, operation), Generation: g, State: "delivery_pending", Issued: now, Expires: now + 60_000}
	r.State = "invitation_pending"
}
func Claim(r *Record, device, binding, invitation, operation string, now int64) error {
	if !r.Enabled || r.Invite == nil || r.Report == nil || r.Recipient.Device != device || r.Recipient.Binding != binding || r.Invite.ID != invitation {
		return ErrUnavailable
	}
	i := r.Invite
	if i.State != "delivery_pending" && i.State != "ringing" {
		return ErrUnavailable
	}
	if now >= i.Expires || Terminal(i.State) || i.Claim != "" {
		return ErrUnavailable
	}
	i.Claim = operation
	i.ClaimedAt = now
	i.State = "claimed"
	r.State = "connecting"
	return nil
}
func Cancel(r *Record) {
	r.Enabled = false
	r.State = "disabled"
	if r.Invite != nil {
		switch r.Invite.State {
		case "connected", "report_requested", "report_started", "report_dispatched", "report_audio_observed", "report_finished", "report_text_verified", "report_output_unverified", "report_delivery_unknown", "report_delivery_mismatch":
		default:
			r.Invite.State = "revoked"
		}
	}
}
