// Package taskinteraction owns durable question and answer receipts. It never
// sends model input; authority and native submission remain with the caller.
package taskinteraction

import (
	"bytes"
	"context"
	"crypto/rand"
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

type Binding struct {
	ParentRequest   string `json:"parent_request_id,omitempty"`
	Authority       string `json:"authority"`
	Kind            string `json:"kind"`
	RelationID      string `json:"relation_id"`
	LogicalID       string `json:"logical_id"`
	Child           string `json:"child_session_id"`
	ChildThread     string `json:"child_thread_id"`
	ChildRequest    string `json:"child_request_id"`
	ChildRevision   uint64 `json:"child_config_revision"`
	Parent          string `json:"parent_session_id"`
	ParentThread    string `json:"parent_thread_id"`
	ParentRevision  uint64 `json:"parent_config_revision"`
	ParentAuthority string `json:"parent_authority"`
}
type Record struct {
	ProducerRequest   string   `json:"producer_request_id"`
	SourceInteraction string   `json:"source_interaction_id,omitempty"`
	Route             []string `json:"route"`

	AuthorityVersion uint64 `json:"authority_version,omitempty"`

	NativePayload json.RawMessage `json:"native_question,omitempty"`

	ID                  string   `json:"interaction_id"`
	Binding             Binding  `json:"binding"`
	NativeCall          string   `json:"native_call_id"`
	NativeTurn          string   `json:"native_turn_id"`
	OriginKey           string   `json:"origin_key"`
	IntentHash          string   `json:"intent_hash"`
	Question            string   `json:"question"`
	Options             []string `json:"options"`
	Kind                string   `json:"kind"`
	Blocking            bool     `json:"blocking"`
	HumanOnly           bool     `json:"human_only"`
	NativeQuestion      string   `json:"native_question_id,omitempty"`
	QuestionRevision    uint64   `json:"question_revision"`
	Version             uint64   `json:"version"`
	State               string   `json:"state"`
	Notification        string   `json:"notification"`
	NotificationRequest string   `json:"notification_request_id,omitempty"`
	Answer              string   `json:"answer,omitempty"`
	Actor               string   `json:"answer_actor,omitempty"`
	ActorID             string   `json:"answer_actor_id,omitempty"`
	AnswerKey           string   `json:"answer_key,omitempty"`
	AnswerHash          string   `json:"answer_hash,omitempty"`
	AckToken            string   `json:"-"`
	AcknowledgedBy      string   `json:"acknowledged_by,omitempty"`
	Error               string   `json:"error,omitempty"`
	CreatedAt           int64    `json:"created_at"`
	UpdatedAt           int64    `json:"updated_at"`
}
type stored struct {
	Record
	Token string `json:"private_ack_token"`
}
type Store struct {
	db            *sql.DB
	pendingMu     sync.Mutex
	pendingCursor int64
}

func Open(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("interaction_storage_required")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	p := filepath.Join(dir, "task_interactions.sqlite")
	f, e := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	f.Close()
	db, e := sql.Open("sqlite", "file:"+p+"?_txlock=immediate&_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)")
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	_, e = db.Exec(`CREATE TABLE IF NOT EXISTS questions(id TEXT PRIMARY KEY, child TEXT NOT NULL, request TEXT NOT NULL, parent TEXT NOT NULL, origin TEXT NOT NULL UNIQUE, version INTEGER NOT NULL, data BLOB NOT NULL);
 CREATE INDEX IF NOT EXISTS questions_child ON questions(child,request);
 CREATE INDEX IF NOT EXISTS questions_parent ON questions(parent);
 CREATE TABLE IF NOT EXISTS commands(scope TEXT NOT NULL,key TEXT NOT NULL,hash TEXT NOT NULL,question TEXT NOT NULL,PRIMARY KEY(scope,key));`)
	if e != nil {
		db.Close()
		return nil, e
	}
	store := &Store{db: db}
	if e = store.InstallRemote(); e != nil {
		db.Close()
		return nil, e
	}
	if e = store.InstallControls(); e != nil {
		db.Close()
		return nil, e
	}
	return store, nil
}
func (s *Store) Close() error { return s.db.Close() }
func ID(prefix string) string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return prefix + hex.EncodeToString(b)
}
func Hash(v any) string {
	b, _ := json.Marshal(v)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func encode(r Record) []byte { b, _ := json.Marshal(stored{r, r.AckToken}); return b }
func decode(b []byte) (Record, error) {
	var r stored
	e := json.Unmarshal(b, &r)
	r.Record.AckToken = r.Token
	return r.Record, e
}
func (s *Store) Get(ctx context.Context, id string) (Record, error) {
	var b []byte
	e := s.db.QueryRowContext(ctx, "SELECT data FROM questions WHERE id=?", id).Scan(&b)
	if e != nil {
		return Record{}, e
	}
	return decode(b)
}
func (s *Store) Create(ctx context.Context, r Record) (Record, bool, error) {
	r.ID = ID("ti_")
	r.Version = 1
	r.QuestionRevision = 1
	r.State = "pending_parent"
	if r.HumanOnly {
		r.State = "pending_human"
	}
	r.Notification = "pending"
	r.CreatedAt = time.Now().UnixMilli()
	r.UpdatedAt = r.CreatedAt
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return r, false, e
	}
	defer tx.Rollback()
	var old []byte
	e = tx.QueryRowContext(ctx, "SELECT data FROM questions WHERE origin=?", r.OriginKey).Scan(&old)
	if e == nil {
		o, e := decode(old)
		if o.IntentHash != r.IntentHash {
			return o, false, errors.New("idempotency_conflict")
		}
		return o, false, e
	}
	if e != sql.ErrNoRows {
		return r, false, e
	}
	var pending int
	if e = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM questions WHERE child=? AND request=? AND json_extract(data,'$.state') IN ('pending_parent','pending_human','answer_reserved','awaiting_consumption')", r.Binding.Child, r.Binding.ChildRequest).Scan(&pending); e != nil {
		return r, false, e
	}
	if pending >= 8 {
		return r, false, errors.New("interaction_limit_requires_human")
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO questions VALUES(?,?,?,?,?,?,?)", r.ID, r.Binding.Child, r.Binding.ChildRequest, r.Binding.Parent, r.OriginKey, r.Version, encode(r))
	if e != nil {
		return r, false, e
	}
	return r, true, tx.Commit()
}
func (s *Store) List(ctx context.Context, session string, limit int) ([]Record, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, e := s.db.QueryContext(ctx, "SELECT data FROM questions WHERE child=? OR parent=? ORDER BY rowid DESC LIMIT ?", session, session, limit)
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

// OpenForSession is a complete pending snapshot, independent of the scheduler's
// rotating cursor and historical completed rows.
func (s *Store) OpenForSession(ctx context.Context, session string) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT data FROM questions WHERE (?='' OR child=? OR parent=?) AND json_extract(data,'$.state') IN ('pending_parent','pending_human') ORDER BY rowid DESC", session, session, session)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var data []byte
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		record, err := decode(data)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

// Pending rotates bounded pages so long-lived questions cannot starve newer
// tasks. The cursor is only a scheduling hint; receipts remain durable.
func (s *Store) Pending(ctx context.Context) ([]Record, error) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	rows, e := s.db.QueryContext(ctx, `SELECT rowid,data FROM questions WHERE rowid>? AND (json_extract(data,'$.state') NOT IN ('resolved','expired','cancelled','orphaned','native_reply_written') OR (COALESCE(json_extract(data,'$.authority_version'),0)=0 AND json_extract(data,'$.binding.parent_authority')!=json_extract(data,'$.binding.authority') AND version>COALESCE((SELECT version FROM relay_receipts WHERE relay_receipts.id=questions.id),0))  ) ORDER BY rowid LIMIT 256`, s.pendingCursor)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var b []byte
		var rowID int64
		if e = rows.Scan(&rowID, &b); e != nil {
			return nil, e
		}
		r, e := decode(b)
		if e != nil {
			return nil, e
		}
		out = append(out, r)
		s.pendingCursor = rowID
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if len(out) < 256 {
		s.pendingCursor = 0
	}
	return out, nil
}

// Change performs a state CAS, with optional command deduplication. Once an
// answer is reserved, competing actors cannot replace it, including after crash.
func (s *Store) Change(ctx context.Context, id, scope, key, hash string, fn func(*Record) error) (Record, error) {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return Record{}, e
	}
	defer tx.Rollback()
	var b []byte
	e = tx.QueryRowContext(ctx, "SELECT data FROM questions WHERE id=?", id).Scan(&b)
	if e != nil {
		return Record{}, e
	}
	r, e := decode(b)
	if e != nil {
		return r, e
	}
	if key != "" {
		var h, q string
		e = tx.QueryRowContext(ctx, "SELECT hash,question FROM commands WHERE scope=? AND key=?", scope, key).Scan(&h, &q)
		if e == nil {
			if h != hash || q != id {
				return r, errors.New("idempotency_conflict")
			}
			return r, nil
		}
		if e != sql.ErrNoRows {
			return r, e
		}
	}
	before := encode(r)
	if e = fn(&r); e != nil {
		return r, e
	}
	if bytes.Equal(before, encode(r)) && key == "" {
		return r, nil
	}
	old := r.Version
	r.Version++
	r.UpdatedAt = time.Now().UnixMilli()
	res, e := tx.ExecContext(ctx, "UPDATE questions SET version=?,data=? WHERE id=? AND version=?", r.Version, encode(r), id, old)
	if e != nil {
		return r, e
	}
	n, e := res.RowsAffected()
	if e != nil || n != 1 {
		return r, errors.New("interaction_conflict")
	}
	if key != "" {
		_, e = tx.ExecContext(ctx, "INSERT INTO commands VALUES(?,?,?,?)", scope, key, hash, id)
		if e != nil {
			return r, e
		}
	}
	return r, tx.Commit()
}
func (s *Store) Answer(ctx context.Context, id, scope, key, hash string, revision uint64, actor, actorID, answer string) (Record, error) {
	return s.Change(ctx, id, scope, key, hash, func(r *Record) error {
		if r.QuestionRevision != revision {
			return errors.New("question_revision_changed")
		}
		if r.State != "pending_parent" && r.State != "pending_human" {
			return errors.New("answer_already_reserved")
		}
		if r.HumanOnly && actor != "human" {
			return errors.New("human_answer_required")
		}
		if answer == "" || len(answer) > 16000 {
			return errors.New("invalid_answer")
		}
		r.Answer = answer
		r.Actor = actor
		r.ActorID = actorID
		r.AnswerKey = key
		r.AnswerHash = Hash(answer)
		r.AckToken = ID("ack_")
		r.State = "answer_reserved"
		return nil
	})
}
func (s *Store) Acknowledge(ctx context.Context, id, token, proof string) (Record, error) {
	return s.Change(ctx, id, "", "", "", func(r *Record) error {
		if token == "" || r.AckToken != token {
			return errors.New("answer_proof_mismatch")
		}
		if r.State == "resolved" {
			return nil
		}
		if r.State != "answer_reserved" && r.State != "awaiting_consumption" {
			return errors.New("answer_not_delivered")
		}
		r.State = "resolved"
		r.AcknowledgedBy = proof
		return nil
	})
}

func (s *Store) ListPage(ctx context.Context, sid, cursor string, limit int) ([]Record, string, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, e := s.db.QueryContext(ctx, "SELECT data FROM questions WHERE ( ?='' OR child=? OR parent=?) AND (?='' OR rowid<(SELECT rowid FROM questions WHERE id=?)) ORDER BY rowid DESC LIMIT ?", sid, sid, sid, cursor, cursor, limit+1)
	if e != nil {
		return nil, "", e
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, "", e
		}
		v, e := decode(b)
		if e != nil {
			return nil, "", e
		}
		out = append(out, v)
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		next = out[len(out)-1].ID
	}
	return out, next, rows.Err()
}
