package workspace

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db                *sql.DB
	root              string
	authority         string
	ownerID           string
	secret            []byte
	mu                sync.Mutex
	now               func() time.Time
	ownerGrantCurrent func(Grant) bool
}

// Open creates a new additive store. Unknown schema versions fail closed; it
// never repairs or overwrites an existing identity/database on a read error.
func Open(dataDir, authority string) (*Store, error) {
	if strings.TrimSpace(dataDir) == "" || authority == "" {
		return nil, errors.New("workspace_storage_unavailable")
	}
	root := filepath.Join(dataDir, "collaboration-workspace-v1")
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	if err := privatePath(root, true); err != nil {
		return nil, err
	}
	keyPath := filepath.Join(root, "cursor.key")
	if err := privatePath(keyPath, false); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	secret, err := os.ReadFile(keyPath)
	if os.IsNotExist(err) {
		// Never rotate an existing store's missing key: old cursors/invitations
		// and authentication receipts depend on that exact identity.
		if _, statErr := os.Stat(filepath.Join(root, "workspace.sqlite")); statErr == nil {
			return nil, errors.New("workspace_key_missing")
		}
		secret = make([]byte, 32)
		if _, err = rand.Read(secret); err != nil {
			return nil, err
		}
		f, e := os.OpenFile(keyPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return nil, e
		}
		_, err = f.Write(secret)
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}
	if err != nil || len(secret) != 32 {
		return nil, errors.New("workspace_key_invalid")
	}
	file := filepath.Join(root, "workspace.sqlite")
	if err := privatePath(file, false); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	_ = f.Close()
	db, err := sql.Open("sqlite", file)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	// Inspect before any DDL or journal-mode mutation. A newer binary's store
	// must remain untouched when this binary cannot understand its schema.
	if err = preflightIdentity(db, authority); err != nil {
		db.Close()
		return nil, err
	}
	if _, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
CREATE TABLE IF NOT EXISTS workspace_schema(version INTEGER NOT NULL);
INSERT INTO workspace_schema(version) SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM workspace_schema);
CREATE TABLE IF NOT EXISTS workspace_identity(authority TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS workspaces(id TEXT PRIMARY KEY, name TEXT NOT NULL, owner TEXT NOT NULL, revision INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS rooms(id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL, owner TEXT NOT NULL, revision INTEGER NOT NULL, acl_revision INTEGER NOT NULL, archived INTEGER NOT NULL, expires_at INTEGER NOT NULL, created_at INTEGER NOT NULL, project_id TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS rooms_workspace ON rooms(workspace_id,created_at,id);
CREATE TABLE IF NOT EXISTS push_endpoints(id TEXT PRIMARY KEY,principal_id TEXT NOT NULL,token_hash TEXT NOT NULL,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS push_subscriptions(endpoint_id TEXT NOT NULL,room_id TEXT NOT NULL,member_revision INTEGER NOT NULL,PRIMARY KEY(endpoint_id,room_id));
CREATE TABLE IF NOT EXISTS push_jobs(id TEXT PRIMARY KEY,endpoint_id TEXT NOT NULL,room_id TEXT NOT NULL,notification_id TEXT NOT NULL,member_revision INTEGER NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL,next_at INTEGER NOT NULL,provider_id TEXT NOT NULL,reason TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS push_due ON push_jobs(state,next_at);
CREATE TABLE IF NOT EXISTS room_preferences(room_id TEXT NOT NULL,principal_id TEXT NOT NULL,pinned INTEGER NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(room_id,principal_id));
CREATE TABLE IF NOT EXISTS members(room_id TEXT NOT NULL, principal_id TEXT NOT NULL, name TEXT NOT NULL, role TEXT NOT NULL, revision INTEGER NOT NULL, active INTEGER NOT NULL, PRIMARY KEY(room_id,principal_id));
CREATE INDEX IF NOT EXISTS member_scope ON members(principal_id,active,room_id);
CREATE TABLE IF NOT EXISTS credentials(token_hash TEXT PRIMARY KEY, principal_id TEXT NOT NULL, device_id TEXT NOT NULL, name TEXT NOT NULL, active INTEGER NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS invites(id TEXT PRIMARY KEY, room_id TEXT NOT NULL, role TEXT NOT NULL, expires_at INTEGER NOT NULL, used_by TEXT NOT NULL, credential_hash TEXT NOT NULL, revoked INTEGER NOT NULL, principal_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS invite_authorizations(invite_id TEXT PRIMARY KEY,payload BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS entities(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT NOT NULL UNIQUE,room_id TEXT NOT NULL,room_seq INTEGER NOT NULL,kind TEXT NOT NULL,revision INTEGER NOT NULL,created_by TEXT NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,hash TEXT NOT NULL,body BLOB NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS entity_room_seq ON entities(room_id,room_seq);
CREATE INDEX IF NOT EXISTS entity_page ON entities(room_id,kind,room_seq);
CREATE TABLE IF NOT EXISTS entity_versions(entity_id TEXT NOT NULL,room_id TEXT NOT NULL,kind TEXT NOT NULL,revision INTEGER NOT NULL,metadata BLOB NOT NULL,PRIMARY KEY(entity_id,revision));
CREATE TABLE IF NOT EXISTS read_messages(room_id TEXT NOT NULL,principal_id TEXT NOT NULL,message_id TEXT NOT NULL,sequence INTEGER NOT NULL,PRIMARY KEY(room_id,principal_id,message_id));
CREATE INDEX IF NOT EXISTS read_message_scope ON read_messages(room_id,principal_id,sequence);
CREATE TABLE IF NOT EXISTS mutations(principal_id TEXT NOT NULL,mutation_id TEXT NOT NULL,room_id TEXT NOT NULL,action TEXT NOT NULL,hash TEXT NOT NULL,result BLOB NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(principal_id,mutation_id));
CREATE TABLE IF NOT EXISTS events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,seq INTEGER NOT NULL,room_id TEXT NOT NULL,kind TEXT NOT NULL,entity_id TEXT NOT NULL,revision INTEGER NOT NULL,acl_revision INTEGER NOT NULL,actor TEXT NOT NULL,at INTEGER NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS event_scope ON events(room_id,seq);
CREATE TABLE IF NOT EXISTS grants(entity_id TEXT PRIMARY KEY,principal_id TEXT NOT NULL,device_id TEXT NOT NULL,credential_hash TEXT NOT NULL,is_owner INTEGER NOT NULL,acl_revision INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS native_jobs(task_id TEXT PRIMARY KEY,room_id TEXT NOT NULL,session_id TEXT NOT NULL,request_id TEXT NOT NULL UNIQUE,state TEXT NOT NULL,updated_at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS native_job_state ON native_jobs(state,updated_at);
CREATE TABLE IF NOT EXISTS room_sessions(session_id TEXT PRIMARY KEY,room_id TEXT NOT NULL,agent_id TEXT NOT NULL UNIQUE,exclusive INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS room_native_identity(session_id TEXT PRIMARY KEY,room_id TEXT NOT NULL,native_thread_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS schedule_due(schedule_id TEXT PRIMARY KEY,room_id TEXT NOT NULL,next_utc INTEGER NOT NULL,enabled INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS due_schedules ON schedule_due(enabled,next_utc);
CREATE TABLE IF NOT EXISTS effect_jobs(effect_id TEXT PRIMARY KEY,room_id TEXT NOT NULL,state TEXT NOT NULL,updated_at INTEGER NOT NULL);
`); err != nil {
		db.Close()
		return nil, err
	}
	var version int
	if err = db.QueryRow("SELECT version FROM workspace_schema").Scan(&version); err != nil || version != 1 {
		db.Close()
		return nil, errors.New("workspace_schema_unsupported")
	}
	if _, err = db.Exec("INSERT INTO workspace_identity(authority) SELECT ? WHERE NOT EXISTS(SELECT 1 FROM workspace_identity)", authority); err != nil {
		db.Close()
		return nil, err
	}
	if err = preflightIdentity(db, authority); err != nil {
		db.Close()
		return nil, err
	}
	if err = os.MkdirAll(filepath.Join(root, "blobs"), 0700); err != nil {
		db.Close()
		return nil, err
	}
	if err = privatePath(filepath.Join(root, "blobs"), true); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, root: root, authority: authority, ownerID: "owner:" + authority, secret: secret, now: time.Now}, nil
}

func privatePath(path string, directory bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || info.IsDir() != directory || (!directory && !info.Mode().IsRegular()) || info.Mode().Perm()&0077 != 0 {
		return errors.New("workspace_storage_permissions_invalid")
	}
	return nil
}

func preflightIdentity(db *sql.DB, authority string) error {
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='workspace_schema'").Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.New("workspace_schema_unsupported")
		}
		return nil
	}
	var version, rows int
	if err := db.QueryRow("SELECT COUNT(*),COALESCE(MAX(version),0) FROM workspace_schema").Scan(&rows, &version); err != nil || rows != 1 || version != 1 {
		return errors.New("workspace_schema_unsupported")
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='workspace_identity'").Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		var pinned string
		if err := db.QueryRow("SELECT COUNT(*),COALESCE(MAX(authority),'') FROM workspace_identity").Scan(&rows, &pinned); err != nil || rows != 1 || pinned != authority {
			return errors.New("workspace_authority_mismatch")
		}
		return nil
	}
	// Only the unreleased v1 layout lacks an identity row. Its original
	// workspace owner is sufficient to detect accidental reuse by another Go.
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='workspaces'").Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		if err := db.QueryRow("SELECT COUNT(*) FROM workspaces WHERE owner<>?", "owner:"+authority).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errors.New("workspace_authority_mismatch")
		}
	}
	return nil
}

// SetOwnerGrantValidator binds the host's current pairing authority at bootstrap.
// It is deliberately absent from the wire protocol. The callback must not query
// this store: authorization also runs inside its single-connection transaction.
func (s *Store) SetOwnerGrantValidator(validate func(Grant) bool) { s.ownerGrantCurrent = validate }
func (s *Store) Close() error                                     { return s.db.Close() }
func (s *Store) Owner(deviceID, tokenHash string) Actor {
	return Actor{ID: s.ownerID, Name: "擁有者", Owner: true, DeviceID: deviceID, CredentialHash: tokenHash}
}
func (s *Store) Authority() string { return s.authority }

func (s *Store) keyed(purpose, value string) string {
	m := hmac.New(sha256.New, s.secret)
	_, _ = io.WriteString(m, purpose+"\x00"+value)
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}
func (s *Store) entityID(kind string, a Actor, mutation string) string {
	return "cw_" + kind + "_" + s.keyed("entity", a.ID+"\x00"+mutation+"\x00"+kind)[:24]
}
func (s *Store) Authenticate(ctx context.Context, credential string) (Actor, error) {
	if len(credential) < 40 || len(credential) > 256 {
		return Actor{}, errors.New("workspace_unauthorized")
	}
	a := Actor{CredentialHash: digest([]byte(credential))}
	err := s.db.QueryRowContext(ctx, "SELECT principal_id,device_id,name FROM credentials WHERE token_hash=? AND active=1", a.CredentialHash).Scan(&a.ID, &a.DeviceID, &a.Name)
	if err != nil {
		return Actor{}, errors.New("workspace_unauthorized")
	}
	return a, nil
}

type sqlReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (s *Store) actorOK(ctx context.Context, q sqlReader, a Actor) error {
	if a.Owner {
		if a.ID == s.ownerID && a.DeviceID != "" && len(a.CredentialHash) == 64 &&
			(s.ownerGrantCurrent == nil || s.ownerGrantCurrent(Grant{PrincipalID: a.ID, DeviceID: a.DeviceID, CredentialHash: a.CredentialHash, Owner: true})) {
			return nil
		}
		return errors.New("workspace_unauthorized")
	}
	var id string
	if err := q.QueryRowContext(ctx, "SELECT principal_id FROM credentials WHERE token_hash=? AND active=1 AND device_id=?", a.CredentialHash, a.DeviceID).Scan(&id); err != nil || id != a.ID {
		return errors.New("workspace_unauthorized")
	}
	return nil
}
func roomByID(ctx context.Context, q sqlReader, id string) (Room, error) {
	var r Room
	err := q.QueryRowContext(ctx, "SELECT id,workspace_id,name,description,owner,revision,acl_revision,archived,expires_at,created_at,project_id FROM rooms WHERE id=?", id).Scan(&r.ID, &r.WorkspaceID, &r.Name, &r.Description, &r.Owner, &r.Revision, &r.ACLRevision, &r.Archived, &r.ExpiresAt, &r.CreatedAt, &r.ProjectID)
	if err != nil {
		return Room{}, errors.New("room_unavailable")
	}
	return r, nil
}
func (s *Store) access(ctx context.Context, q sqlReader, a Actor, id, right string) (Room, string, error) {
	if err := s.actorOK(ctx, q, a); err != nil {
		return Room{}, "", err
	}
	r, err := roomByID(ctx, q, id)
	if err != nil {
		return Room{}, "", err
	}
	var role string
	if err = q.QueryRowContext(ctx, "SELECT role FROM members WHERE room_id=? AND principal_id=? AND active=1", id, a.ID).Scan(&role); err != nil || !permits(role, right) {
		return Room{}, "", errors.New("room_permission_denied")
	}
	if right != "read" && right != "manage" && (r.Archived || (r.ExpiresAt > 0 && s.now().UnixMilli() >= r.ExpiresAt)) {
		return Room{}, "", errors.New("room_archived")
	}
	return r, role, nil
}
func (s *Store) Authorize(ctx context.Context, a Actor, room, right string) (Room, string, error) {
	return s.access(ctx, s.db, a, room, right)
}

const entityColumns = "id,kind,room_id,revision,room_seq,created_by,created_at,updated_at,hash,body"

func scanEntity(row interface{ Scan(...any) error }) (Entity, error) {
	var e Entity
	err := row.Scan(&e.ID, &e.Kind, &e.RoomID, &e.Revision, &e.Sequence, &e.CreatedBy, &e.CreatedAt, &e.UpdatedAt, &e.Hash, &e.Body)
	return e, err
}
func getEntity(ctx context.Context, q sqlReader, room, id string) (Entity, error) {
	e, err := scanEntity(q.QueryRowContext(ctx, "SELECT "+entityColumns+" FROM entities WHERE room_id=? AND id=?", room, id))
	if err != nil {
		return Entity{}, errors.New("entity_unavailable")
	}
	return e, nil
}
func bodyOf[T any](e Entity) (T, error) { var b T; err := json.Unmarshal(e.Body, &b); return b, err }

func (s *Store) put(ctx context.Context, tx *sql.Tx, a Actor, r Room, e Entity, body any) (Entity, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return Entity{}, err
	}
	now := s.now().UnixMilli()
	if len(b) > MaxPromptBytes*2 {
		return Entity{}, errors.New("entity_limit")
	}
	if e.Revision == 0 {
		e.CreatedAt = now
		e.CreatedBy = a.ID
		e.Revision = 1
		e.RoomID = r.ID
		if err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(room_seq),0)+1 FROM entities WHERE room_id=?", r.ID).Scan(&e.Sequence); err != nil {
			return Entity{}, err
		}
	} else {
		e.Revision++
	}
	e.UpdatedAt = now
	e.Hash = digest(b)
	e.Body = b
	_, err = tx.ExecContext(ctx, `INSERT INTO entities(id,room_id,room_seq,kind,revision,created_by,created_at,updated_at,hash,body) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET revision=excluded.revision,updated_at=excluded.updated_at,hash=excluded.hash,body=excluded.body`, e.ID, e.RoomID, e.Sequence, e.Kind, e.Revision, e.CreatedBy, e.CreatedAt, e.UpdatedAt, e.Hash, e.Body)
	if err != nil {
		return Entity{}, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT room_seq FROM entities WHERE id=?", e.ID).Scan(&e.Sequence); err != nil {
		return Entity{}, err
	}
	immutable, _ := json.Marshal(e)
	if _, err = tx.ExecContext(ctx, "INSERT INTO entity_versions VALUES(?,?,?,?,?)", e.ID, e.RoomID, e.Kind, e.Revision, immutable); err != nil {
		return Entity{}, err
	}
	if err = s.appendEvent(ctx, tx, r, e.Kind, e.ID, e.Revision, a.ID); err != nil {
		return Entity{}, err
	}
	return e, nil
}
func cas(c Command, e Entity) error {
	if c.ExpectedRevision == nil || *c.ExpectedRevision != e.Revision {
		return errors.New("revision_conflict")
	}
	return nil
}
func fresh(c Command) error {
	if c.ExpectedRevision != nil && *c.ExpectedRevision != 0 {
		return errors.New("revision_conflict")
	}
	return nil
}
func (s *Store) getForUpdate(ctx context.Context, tx *sql.Tx, c Command, kind string) (Entity, error) {
	e, err := getEntity(ctx, tx, c.RoomID, c.EntityID)
	if err != nil {
		return e, err
	}
	if e.Kind != kind {
		return Entity{}, errors.New("entity_kind_mismatch")
	}
	return e, cas(c, e)
}
func withEntity(r Result, e Entity, err error) (Result, error) {
	if err != nil {
		return r, err
	}
	r.Entity = &e
	return r, nil
}

// Apply commits a mutation and its receipt in one transaction. Receipt lookup
// always follows current authentication/ACL checks; revoked callers cannot use
// the ledger to recover private results from an earlier grant.
type CommitUncertain struct{ Cause error }

func (e *CommitUncertain) Error() string { return "workspace_mutation_commit_unknown" }
func (e *CommitUncertain) Unwrap() error { return e.Cause }

func ReadAction(action string) bool {
	switch action {
	case "list", "snapshot", "page", "get", "events", "search", "status", "read_attachment", "export_document", "tool_accounts", "tool_read", "agent_binding":
		return true
	}
	return false
}

func (s *Store) Apply(ctx context.Context, a Actor, c Command) (Result, error) {
	if c.Authority != s.authority {
		return Result{}, errors.New("workspace_wrong_authority")
	}
	if ReadAction(c.Action) {
		r, err := s.read(ctx, a, c)
		if err != nil {
			return Result{}, err
		}
		if c.Action != "status" {
			r.Action = c.Action
		}
		if err = s.actorOK(ctx, s.db, a); err != nil {
			return Result{}, err
		}
		if c.RoomID != "" {
			current, _, e := s.Authorize(ctx, a, c.RoomID, "read")
			if e != nil {
				return Result{}, e
			}
			if r.Snapshot != nil && current.ACLRevision != r.Snapshot.Room.ACLRevision {
				return Result{}, errors.New("acl_revision_conflict")
			}
		}
		for _, room := range r.Rooms {
			if _, _, e := s.Authorize(ctx, a, room.ID, "read"); e != nil {
				return Result{}, e
			}
		}
		return publicResult(a, r), nil
	}
	if len(c.MutationID) < 8 || len(c.MutationID) > 128 {
		return Result{}, errors.New("invalid_mutation")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	if err = s.actorOK(ctx, tx, a); err != nil {
		return Result{}, err
	}
	var room Room
	if c.RoomID != "" {
		room, _, err = s.access(ctx, tx, a, c.RoomID, "read")
		if err != nil {
			return Result{}, err
		}
	}
	raw, _ := json.Marshal(c)
	hash := digest(raw)
	var oldHash string
	var resultBytes []byte
	err = tx.QueryRowContext(ctx, "SELECT hash,result FROM mutations WHERE principal_id=? AND mutation_id=?", a.ID, c.MutationID).Scan(&oldHash, &resultBytes)
	if err == nil {
		if oldHash != hash {
			return Result{}, errors.New("idempotency_conflict")
		}
		var r Result
		if err = json.Unmarshal(resultBytes, &r); err != nil {
			return Result{}, err
		}
		if r.Room != nil {
			if _, _, err = s.access(ctx, tx, a, r.Room.ID, "read"); err != nil {
				return Result{}, err
			}
		}
		if err = tx.Commit(); err != nil {
			return Result{}, &CommitUncertain{err}
		}
		return publicResult(a, s.decorateInvite(a, r)), nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Result{}, err
	}
	if c.RoomID != "" && (c.ExpectedACL == nil || *c.ExpectedACL != room.ACLRevision) {
		return Result{}, errors.New("acl_revision_conflict")
	}
	r, err := s.mutate(ctx, tx, a, room, c)
	if err != nil {
		return Result{}, err
	}
	r.Action = c.Action
	r.MutationID = c.MutationID
	resultBytes, err = json.Marshal(r)
	if err != nil {
		return Result{}, err
	}
	roomID := c.RoomID
	if r.Room != nil {
		roomID = r.Room.ID
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO mutations VALUES(?,?,?,?,?,?,?)", a.ID, c.MutationID, roomID, c.Action, hash, resultBytes, s.now().UnixMilli()); err != nil {
		return Result{}, err
	}
	if err = tx.Commit(); err != nil {
		return Result{}, &CommitUncertain{err}
	}
	return publicResult(a, s.decorateInvite(a, r)), nil
}
func (s *Store) decorateInvite(a Actor, r Result) Result {
	if r.Invite != nil && !r.Invite.Revoked && r.Invite.UsedBy == "" && r.Invite.ExpiresAt > s.now().UnixMilli() {
		r.InviteCode = r.Invite.ID + "." + s.keyed("invite", r.Invite.ID)
	}
	return r
}

type cursor struct {
	Room, Actor, Kind, Thread string
	ACL                       uint64
	Before                    int64
}

func (s *Store) encodeCursor(v cursor) (string, error) {
	b, _ := json.Marshal(v)
	block, err := aes.NewCipher(s.secret)
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	sealed := aead.Seal(nonce, nonce, b, []byte("bridge-workspace-cursor-v1"))
	return "wc1." + base64.RawURLEncoding.EncodeToString(sealed), nil
}
func (s *Store) decodeCursor(token string) (cursor, error) {
	var c cursor
	if !strings.HasPrefix(token, "wc1.") {
		return c, errors.New("cursor_invalid")
	}
	sealed, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, "wc1."))
	if err != nil {
		return c, errors.New("cursor_invalid")
	}
	block, err := aes.NewCipher(s.secret)
	if err != nil {
		return c, errors.New("cursor_invalid")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(sealed) < aead.NonceSize() {
		return c, errors.New("cursor_invalid")
	}
	b, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], []byte("bridge-workspace-cursor-v1"))
	if err != nil {
		return c, errors.New("cursor_invalid")
	}
	if json.Unmarshal(b, &c) != nil {
		return c, errors.New("cursor_invalid")
	}
	return c, nil
}

func (s *Store) Page(ctx context.Context, a Actor, c Command) (Page, error) {
	r, _, err := s.Authorize(ctx, a, c.RoomID, "read")
	if err != nil {
		return Page{}, err
	}
	return s.page(ctx, s.db, a, r, c)
}
func (s *Store) page(ctx context.Context, q sqlReader, a Actor, r Room, c Command) (Page, error) {
	allowed := map[string]bool{"message": true, "agent": true, "document": true, "document_version": true, "document_proposal": true, "task": true, "workflow": true, "review": true, "knowledge": true, "decision": true, "attachment": true, "media_comment": true, "schedule": true, "occurrence": true, "effect": true, "notification": true, "reaction": true, "research_source": true, "human_todo": true}
	if !allowed[c.Kind] {
		return Page{}, errors.New("entity_kind_invalid")
	}
	limit := c.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	before := int64(9223372036854775807)
	if c.Cursor != "" {
		p, err := s.decodeCursor(c.Cursor)
		if err != nil {
			return Page{}, err
		}
		if p.Room != r.ID || p.Actor != a.ID || p.Kind != c.Kind || p.Thread != c.ThreadID || p.ACL != r.ACLRevision {
			return Page{}, errors.New("cursor_scope_changed")
		}
		before = p.Before
	}
	query := "SELECT " + entityColumns + " FROM entities WHERE room_id=? AND kind=? AND room_seq<?"
	args := []any{r.ID, c.Kind, before}
	if c.Kind == "notification" {
		query += " AND json_extract(body,'$.principal_id')=?"
		args = append(args, a.ID)
	}
	if c.ThreadID != "" {
		if c.Kind == "document_version" {
			query += " AND json_extract(body,'$.document_id')=?"
		} else {
			if c.Kind != "message" && c.Kind != "task" && c.Kind != "workflow" {
				return Page{}, errors.New("thread_filter_invalid")
			}
			query += " AND json_extract(body,'$.thread_id')=?"
		}
		args = append(args, c.ThreadID)
	}
	query += " ORDER BY room_seq DESC LIMIT ?"
	args = append(args, limit+1)
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()
	page := Page{Items: []Entity{}}
	bytes := 0
	for rows.Next() {
		e, err := scanEntity(rows)
		if err != nil {
			return Page{}, err
		}
		if len(page.Items) == limit || (bytes+len(e.Body) > 1024*1024 && len(page.Items) > 0) {
			page.HasMore = true
			break
		}
		bytes += len(e.Body)
		page.Items = append(page.Items, publicEntity(a, e))
	}
	if err = rows.Err(); err != nil {
		return Page{}, err
	}
	rows.Close()
	if c.Kind == "message" {
		for _, e := range page.Items {
			var seen bool
			if err = q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM read_messages WHERE room_id=? AND principal_id=? AND message_id=?)", r.ID, a.ID, e.ID).Scan(&seen); err != nil {
				return Page{}, err
			}
			if seen {
				page.ReadMessageIDs = append(page.ReadMessageIDs, e.ID)
			}
		}
	}
	if page.HasMore && len(page.Items) > 0 {
		page.Cursor, err = s.encodeCursor(cursor{r.ID, a.ID, c.Kind, c.ThreadID, r.ACLRevision, page.Items[len(page.Items)-1].Sequence})
		if err != nil {
			return Page{}, err
		}
	}
	return page, nil
}
func publicEntity(a Actor, e Entity) Entity {
	if !a.Owner && (e.Kind == "agent" || e.Kind == "task") {
		var b map[string]any
		if json.Unmarshal(e.Body, &b) == nil {
			delete(b, "session_id")
			delete(b, "native_thread_id")
			delete(b, "config_revision")
			delete(b, "role")
			e.Body, _ = json.Marshal(b)
			e.Hash = digest(e.Body)
		}
	}
	return e
}
func publicResult(a Actor, r Result) Result {
	if r.Entity != nil {
		e := publicEntity(a, *r.Entity)
		r.Entity = &e
	}
	return r
}

func (s *Store) saveGrant(ctx context.Context, tx *sql.Tx, id string, a Actor, acl uint64) error {
	_, err := tx.ExecContext(ctx, "INSERT INTO grants VALUES(?,?,?,?,?,?)", id, a.ID, a.DeviceID, a.CredentialHash, a.Owner, acl)
	return err
}
func grant(ctx context.Context, q sqlReader, id string) (Grant, error) {
	var g Grant
	err := q.QueryRowContext(ctx, "SELECT principal_id,device_id,credential_hash,is_owner,acl_revision FROM grants WHERE entity_id=?", id).Scan(&g.PrincipalID, &g.DeviceID, &g.CredentialHash, &g.Owner, &g.ACLRevision)
	return g, err
}
func actorForGrant(g Grant) Actor {
	return Actor{ID: g.PrincipalID, Owner: g.Owner, DeviceID: g.DeviceID, CredentialHash: g.CredentialHash}
}
func (s *Store) notify(ctx context.Context, tx *sql.Tx, a Actor, r Room, principal, kind, id string) error {
	if principal == a.ID {
		return nil
	}
	e := Entity{ID: "cw_notice_" + s.keyed("notice", r.ID+"/"+principal+"/"+kind+"/"+id)[:24], Kind: "notification"}
	if _, err := getEntity(ctx, tx, r.ID, e.ID); err == nil {
		return nil
	}
	_, err := s.put(ctx, tx, a, r, e, Notification{PrincipalID: principal, Kind: kind, EntityID: id})
	if err != nil {
		return err
	}
	return s.queuePush(ctx, tx, r, principal, e.ID)
}
func stableRequest(id string) string { return "r_" + strings.TrimPrefix(id, "cw_task_") }
func (s *Store) scopeError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("workspace: %w", err)
}

func (s *Store) appendEvent(ctx context.Context, tx *sql.Tx, r Room, kind, id string, revision uint64, actor string) error {
	var seq int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq),0)+1 FROM events WHERE room_id=?", r.ID).Scan(&seq); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, "INSERT INTO events(seq,room_id,kind,entity_id,revision,acl_revision,actor,at) VALUES(?,?,?,?,?,?,?,?)", seq, r.ID, kind, id, revision, r.ACLRevision, actor, s.now().UnixMilli())
	return err
}
