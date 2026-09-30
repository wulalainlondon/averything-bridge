// Package messagequeue owns durable receipts and payloads for ordinary chat
// messages. Actor callbacks are reconstructed only for entries still queued;
// a crash during execution/steering is quarantined, never blindly replayed.
package messagequeue

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type State string

const (
	Queued    State = "queued"
	Running   State = "running"
	Steering  State = "steering"
	Steered   State = "steered"
	Cancelled State = "cancelled"
	Completed State = "completed"
	Failed    State = "failed"
	Uncertain State = "uncertain"
)

var ErrConflict = errors.New("request ID already belongs to different message content")
var ErrRejected = errors.New("message request was permanently rejected")

type Entry struct {
	Sequence                         int64
	SessionID, RequestID             string
	State                            State
	Content                          string
	ImageCount                       int
	FileNames                        []string
	Payload                          []byte
	PayloadHash                      string
	Message, ActiveRequestID, TurnID string
	CreatedAt, UpdatedAt             int64
}
type Snapshot struct {
	Revision uint64
	Items    []Entry
}
type Store struct{ db *sql.DB }

func Open(dataDir string) (*Store, error) {
	path := ":memory:"
	if dataDir != "" {
		if err := os.MkdirAll(dataDir, 0700); err != nil {
			return nil, err
		}
		path = filepath.Join(dataDir, "message_queue.sqlite")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		file.Close()
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
 CREATE TABLE IF NOT EXISTS queue_sessions (session_id TEXT PRIMARY KEY, revision INTEGER NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS queue_commands (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, request_id TEXT NOT NULL,
 state TEXT NOT NULL, content TEXT NOT NULL, image_count INTEGER NOT NULL, file_names TEXT NOT NULL,
 payload_hash TEXT NOT NULL, message TEXT NOT NULL DEFAULT '',
 active_request_id TEXT NOT NULL DEFAULT '', turn_id TEXT NOT NULL DEFAULT '',
 created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, payload BLOB NOT NULL, UNIQUE(session_id, request_id));
 CREATE INDEX IF NOT EXISTS queue_pending ON queue_commands(session_id, state, seq);
 CREATE TABLE IF NOT EXISTS queue_rejections (session_id TEXT NOT NULL, request_id TEXT NOT NULL, reason TEXT NOT NULL, created_at INTEGER NOT NULL, PRIMARY KEY(session_id,request_id));`); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Touch(sessionID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = bump(tx, sessionID); err != nil {
		return err
	}
	return tx.Commit()
}

const columns = `seq,session_id,request_id,state,content,image_count,file_names,payload,payload_hash,message,active_request_id,turn_id,created_at,updated_at`

type scanner interface{ Scan(...any) error }

func scan(row scanner) (Entry, error) {
	var e Entry
	var names string
	err := row.Scan(&e.Sequence, &e.SessionID, &e.RequestID, &e.State, &e.Content, &e.ImageCount, &names, &e.Payload, &e.PayloadHash, &e.Message, &e.ActiveRequestID, &e.TurnID, &e.CreatedAt, &e.UpdatedAt)
	if err == nil {
		err = json.Unmarshal([]byte(names), &e.FileNames)
	}
	return e, err
}
func lookup(tx interface{ QueryRow(string, ...any) *sql.Row }, sessionID, requestID string) (Entry, bool, error) {
	e, err := scan(tx.QueryRow("SELECT "+columns+" FROM queue_commands WHERE session_id=? AND request_id=?", sessionID, requestID))
	if errors.Is(err, sql.ErrNoRows) {
		return Entry{}, false, nil
	}
	return e, err == nil, err
}
func (s *Store) Get(sessionID, requestID string) (Entry, bool, error) {
	return lookup(s.db, sessionID, requestID)
}
func bump(tx *sql.Tx, sessionID string) error {
	_, err := tx.Exec(`INSERT INTO queue_sessions(session_id,revision) VALUES(?,1) ON CONFLICT(session_id) DO UPDATE SET revision=revision+1`, sessionID)
	return err
}

func (s *Store) Enqueue(e Entry) (Entry, bool, error) {
	hash := sha256.Sum256(e.Payload)
	hashText := hex.EncodeToString(hash[:])
	// The caller may pin server-side admission metadata in Payload while
	// hashing only the immutable user intent (e.g. per-message settings).
	if e.PayloadHash != "" {
		if decoded, err := hex.DecodeString(e.PayloadHash); err != nil || len(decoded) != sha256.Size {
			return Entry{}, false, ErrConflict
		}
		hashText = e.PayloadHash
	}
	tx, err := s.db.Begin()
	if err != nil {
		return Entry{}, false, err
	}
	defer tx.Rollback()
	var rejection string
	if err := tx.QueryRow(`SELECT reason FROM queue_rejections WHERE session_id=? AND request_id=?`, e.SessionID, e.RequestID).Scan(&rejection); err == nil {
		return Entry{}, false, fmt.Errorf("%w: %s", ErrRejected, rejection)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Entry{}, false, err
	}
	if previous, found, err := lookup(tx, e.SessionID, e.RequestID); err != nil {
		return Entry{}, false, err
	} else if found {
		if previous.PayloadHash != hashText {
			return previous, false, ErrConflict
		}
		return previous, false, nil
	}
	if e.FileNames == nil {
		e.FileNames = []string{}
	}
	names, err := json.Marshal(e.FileNames)
	if err != nil {
		return Entry{}, false, err
	}
	now := time.Now().UnixMilli()
	_, err = tx.Exec(`INSERT INTO queue_commands(session_id,request_id,state,content,image_count,file_names,payload,payload_hash,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`, e.SessionID, e.RequestID, Queued, e.Content, e.ImageCount, string(names), e.Payload, hashText, now, now)
	if err != nil {
		return Entry{}, false, err
	}
	if err = bump(tx, e.SessionID); err != nil {
		return Entry{}, false, err
	}
	inserted, _, err := lookup(tx, e.SessionID, e.RequestID)
	if err != nil {
		return Entry{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return Entry{}, false, err
	}
	return inserted, true, nil
}

// Reject records a durable negative receipt, without ever creating a queued
// payload. A reconnect/lost-ACK retry cannot become authorized after takeover.
// Never overwrite the ownership of a message that was already accepted.
func (s *Store) Reject(sessionID, requestID, reason string) error {
	if sessionID == "" || requestID == "" {
		return errors.New("missing rejection identity")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, found, err := lookup(tx, sessionID, requestID); err != nil {
		return err
	} else if found {
		return nil
	}
	_, err = tx.Exec(`INSERT OR IGNORE INTO queue_rejections(session_id,request_id,reason,created_at) VALUES(?,?,?,?)`, sessionID, requestID, reason, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Transition(sessionID, requestID string, from []State, to State, message, activeRequestID, turnID string) (Entry, bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Entry{}, false, err
	}
	defer tx.Rollback()
	e, found, err := lookup(tx, sessionID, requestID)
	if err != nil || !found {
		return e, false, err
	}
	allowed := false
	for _, state := range from {
		if e.State == state {
			allowed = true
			break
		}
	}
	if !allowed {
		return e, false, nil
	}
	payload := e.Payload
	if to == Completed || to == Cancelled || to == Steered || to == Failed {
		payload = []byte{}
	}
	_, err = tx.Exec(`UPDATE queue_commands SET state=?,message=?,active_request_id=?,turn_id=?,updated_at=?,payload=? WHERE session_id=? AND request_id=?`, to, message, activeRequestID, turnID, time.Now().UnixMilli(), payload, sessionID, requestID)
	if err != nil {
		return Entry{}, false, err
	}
	if err = bump(tx, sessionID); err != nil {
		return Entry{}, false, err
	}
	e, _, err = lookup(tx, sessionID, requestID)
	if err != nil {
		return Entry{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return Entry{}, false, err
	}
	return e, true, nil
}

func (s *Store) Snapshot(sessionID string) (Snapshot, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Rollback()
	out := Snapshot{Items: []Entry{}}
	if err = tx.QueryRow(`SELECT revision FROM queue_sessions WHERE session_id=?`, sessionID).Scan(&out.Revision); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	// Do not materialize queued attachment blobs just to broadcast a preview.
	metadataColumns := strings.Replace(columns, "payload,payload_hash", "X'',payload_hash", 1)
	rows, err := tx.Query("SELECT "+metadataColumns+` FROM queue_commands WHERE session_id=? AND (state IN ('queued','running','steering','uncertain') OR seq IN (SELECT seq FROM queue_commands WHERE session_id=? ORDER BY seq DESC LIMIT 100)) ORDER BY seq`, sessionID, sessionID)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		e, err := scan(rows)
		if err != nil {
			rows.Close()
			return out, err
		}
		e.Payload = nil
		out.Items = append(out.Items, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func (s *Store) Queued() ([]Entry, error) {
	rows, err := s.db.Query("SELECT " + columns + ` FROM queue_commands WHERE state='queued' ORDER BY seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Entry{}
	for rows.Next() {
		e, err := scan(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (s *Store) Recover() error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT DISTINCT session_id FROM queue_commands WHERE state IN ('running','steering')`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE queue_commands SET state='uncertain',message='Bridge restarted before the result was confirmed; this message will not be sent again automatically',updated_at=? WHERE state IN ('running','steering')`, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err = bump(tx, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
