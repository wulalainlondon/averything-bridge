package linebridge

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

type Store struct {
	db        *sql.DB
	aead      cipher.AEAD
	key       []byte
	Authority string
	// Installed once by the operator-configured service before it is published.
	voiceDefaultAllowed func(context.Context, Caller, string, string) bool
	mu                  sync.Mutex
	release             func()
}

func OpenStore(dir, authority string, key []byte) (*Store, error) {
	if authority == "" || len(key) != 32 {
		return nil, Fail("line_store_configuration_invalid")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	release, err := lockOwner(filepath.Join(dir, "owner.lock"))
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			release()
		}
	}()
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "line.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;
 CREATE TABLE IF NOT EXISTS line_accounts(id TEXT PRIMARY KEY,label TEXT NOT NULL,enabled INTEGER NOT NULL,revision INTEGER NOT NULL,auth_state TEXT NOT NULL,owner TEXT NOT NULL,data_dir TEXT NOT NULL,helper TEXT NOT NULL,principal TEXT UNIQUE,created_at INTEGER NOT NULL,profile_name TEXT NOT NULL DEFAULT '');
 CREATE TABLE IF NOT EXISTS line_grants(account TEXT NOT NULL,subject TEXT NOT NULL,scope TEXT NOT NULL,PRIMARY KEY(account,subject,scope),FOREIGN KEY(account) REFERENCES line_accounts(id));
 CREATE TABLE IF NOT EXISTS line_targets(ref TEXT PRIMARY KEY,account TEXT NOT NULL,external TEXT NOT NULL,name TEXT NOT NULL,kind TEXT NOT NULL,UNIQUE(account,external),FOREIGN KEY(account) REFERENCES line_accounts(id));
 CREATE TABLE IF NOT EXISTS line_ops(id TEXT PRIMARY KEY,account TEXT NOT NULL,target TEXT NOT NULL,kind TEXT NOT NULL,state TEXT NOT NULL,effect TEXT NOT NULL,remote_id TEXT NOT NULL DEFAULT '',code TEXT NOT NULL DEFAULT '',subject TEXT NOT NULL,intent TEXT NOT NULL,fingerprint TEXT NOT NULL,binding INTEGER NOT NULL,content BLOB NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,due_at INTEGER NOT NULL DEFAULT 0,timezone TEXT NOT NULL DEFAULT '',late_window INTEGER NOT NULL DEFAULT 0,revision INTEGER NOT NULL,UNIQUE(account,kind,intent));
 CREATE TABLE IF NOT EXISTS line_message_refs(ref TEXT PRIMARY KEY,account TEXT NOT NULL,target TEXT NOT NULL,external BLOB NOT NULL,FOREIGN KEY(account) REFERENCES line_accounts(id));
 CREATE TABLE IF NOT EXISTS line_subscriptions(id TEXT PRIMARY KEY,account TEXT NOT NULL,target TEXT NOT NULL,subject TEXT NOT NULL,enabled INTEGER NOT NULL,UNIQUE(account,target,subject));
 CREATE TABLE IF NOT EXISTS line_calls(id TEXT PRIMARY KEY,account TEXT NOT NULL,target TEXT NOT NULL,subject TEXT NOT NULL,intent TEXT NOT NULL,fingerprint TEXT NOT NULL,state TEXT NOT NULL,code TEXT NOT NULL DEFAULT '',created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,binding INTEGER NOT NULL,UNIQUE(account,intent));
 CREATE TABLE IF NOT EXISTS line_call_handoffs(call_id TEXT PRIMARY KEY,device TEXT NOT NULL,session_id TEXT NOT NULL,voice_id TEXT NOT NULL,expires_at INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS line_call_events(seq INTEGER PRIMARY KEY AUTOINCREMENT,call_id TEXT NOT NULL,event BLOB NOT NULL,created_at INTEGER NOT NULL);
 CREATE UNIQUE INDEX IF NOT EXISTS line_one_reserved_or_active_call ON line_calls((1)) WHERE state IN ('handoff_pending','preparing','prepared','dispatching','ringing','connected');
 CREATE TABLE IF NOT EXISTS line_resources(ref TEXT PRIMARY KEY,account TEXT NOT NULL,subject TEXT NOT NULL,name TEXT NOT NULL,mime TEXT NOT NULL,size INTEGER NOT NULL,sha256 TEXT NOT NULL,expires_at INTEGER NOT NULL,data BLOB NOT NULL,created_at INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS line_meta(key TEXT PRIMARY KEY,value BLOB NOT NULL);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS line_health_observations(account TEXT PRIMARY KEY,checked_at INTEGER NOT NULL,state TEXT NOT NULL,code TEXT NOT NULL,binding_revision INTEGER NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS line_target_aliases(account TEXT NOT NULL,subject TEXT NOT NULL,alias_key TEXT NOT NULL,target TEXT NOT NULL,PRIMARY KEY(account,subject,alias_key),FOREIGN KEY(account) REFERENCES line_accounts(id));
 CREATE TABLE IF NOT EXISTS line_grant_revisions(account TEXT PRIMARY KEY,revision INTEGER NOT NULL);`); err != nil {
		db.Close()
		return nil, err
	}
	healthColumns, healthErr := db.Query("PRAGMA table_info(line_health_observations)")
	if healthErr != nil {
		db.Close()
		return nil, healthErr
	}
	hasHealthBinding := false
	for healthColumns.Next() {
		var cid, notnull, pk int
		var name, kind string
		var value any
		if healthErr = healthColumns.Scan(&cid, &name, &kind, &notnull, &value, &pk); healthErr != nil {
			break
		}
		if name == "binding_revision" {
			hasHealthBinding = true
		}
	}
	if healthErr == nil {
		healthErr = healthColumns.Err()
	}
	healthColumns.Close()
	if healthErr != nil {
		db.Close()
		return nil, healthErr
	}
	if !hasHealthBinding {
		if _, healthErr = db.Exec("ALTER TABLE line_health_observations ADD COLUMN binding_revision INTEGER NOT NULL DEFAULT 0"); healthErr != nil {
			db.Close()
			return nil, healthErr
		}
	}
	columns, e := db.Query("PRAGMA table_info(line_accounts)")
	if e != nil {
		db.Close()
		return nil, e
	}
	hasProfile := false
	for columns.Next() {
		var cid, notnull, pk int
		var name, kind string
		var value any
		if e = columns.Scan(&cid, &name, &kind, &notnull, &value, &pk); e != nil {
			columns.Close()
			db.Close()
			return nil, e
		}
		if name == "profile_name" {
			hasProfile = true
		}
	}
	if e = columns.Close(); e != nil {
		db.Close()
		return nil, e
	}
	if !hasProfile {
		if _, e = db.Exec("ALTER TABLE line_accounts ADD COLUMN profile_name TEXT NOT NULL DEFAULT ''"); e != nil {
			db.Close()
			return nil, e
		}
	}
	_ = os.Chmod(path, 0600)
	s := &Store{db: db, aead: aead, key: append([]byte(nil), key...), Authority: authority, release: release}
	var check []byte
	err = db.QueryRow("SELECT value FROM line_meta WHERE key='key_check'").Scan(&check)
	if errors.Is(err, sql.ErrNoRows) {
		check, err = s.seal("meta", "line-store-v1")
		if err == nil {
			_, err = db.Exec("INSERT INTO line_meta VALUES('key_check',?)", check)
		}
	} else if err == nil {
		var value string
		err = s.unseal("meta", check, &value)
		if value != "line-store-v1" {
			err = Fail("line_store_key_invalid")
		}
	}
	if err != nil {
		db.Close()
		return nil, Fail("line_store_key_invalid")
	}
	if _, err = db.Exec("UPDATE line_ops SET state='unknown',effect='unknown',code='line_restart_during_dispatch',revision=revision+1 WHERE state='dispatching'"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err = db.Exec("UPDATE line_calls SET state='unknown',code='line_restart_during_call',updated_at=? WHERE state IN ('handoff_pending','preparing','prepared','dispatching','ringing','connected')", nowMS()); err != nil {
		db.Close()
		return nil, err
	}
	success = true
	return s, nil
}
func (s *Store) Close() error {
	err := s.db.Close()
	if s.release != nil {
		s.release()
	}
	for i := range s.key {
		s.key[i] = 0
	}
	return err
}
func newID(prefix string) string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b)
}
func (s *Store) digest(v string) string {
	h := hmac.New(sha256.New, s.key)
	h.Write([]byte(s.Authority + "\x00" + v))
	return hex.EncodeToString(h.Sum(nil))
}
func (s *Store) seal(account string, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	n := make([]byte, s.aead.NonceSize())
	if _, err = rand.Read(n); err != nil {
		return nil, err
	}
	return s.aead.Seal(n, n, b, []byte(s.Authority+"\x00"+account)), nil
}
func (s *Store) unseal(account string, b []byte, v any) error {
	n := s.aead.NonceSize()
	if len(b) < n {
		return Fail("line_store_corrupt")
	}
	plain, err := s.aead.Open(nil, b[:n], b[n:], []byte(s.Authority+"\x00"+account))
	if err != nil {
		return Fail("line_store_corrupt")
	}
	return json.Unmarshal(plain, v)
}

var scopes = []string{"status", "directory", "messages.read", "messages.send", "events", "schedule", "call", "mark_read", "resources"}

func (s *Store) CreateAccount(ctx context.Context, label, owner, dir, helper string) (Account, error) {
	if strings.TrimSpace(label) == "" || len(label) > 128 || owner == "" || !filepath.IsAbs(dir) || !filepath.IsAbs(helper) {
		return Account{}, Fail("line_account_invalid")
	}
	a := Account{ID: newID("ln_"), Label: strings.TrimSpace(label), Enabled: true, Revision: 1, AuthState: "unpaired", Owner: owner, DataDir: dir, KeychainHelper: helper, CreatedAt: nowMS()}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return a, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, "INSERT INTO line_accounts(id,label,enabled,revision,auth_state,owner,data_dir,helper,principal,created_at) VALUES(?,?,?,?,?,?,?,?,NULL,?)", a.ID, a.Label, 1, a.Revision, a.AuthState, owner, dir, helper, a.CreatedAt)
	if err != nil {
		return a, err
	}
	for _, scope := range scopes {
		if _, err = tx.ExecContext(ctx, "INSERT INTO line_grants VALUES(?,?,?)", a.ID, owner, scope); err != nil {
			return a, err
		}
	}
	return a, tx.Commit()
}
func accountScan(scan interface{ Scan(...any) error }) (Account, error) {
	var a Account
	var p sql.NullString
	err := scan.Scan(&a.ID, &a.Label, &a.Enabled, &a.Revision, &a.AuthState, &a.Owner, &a.DataDir, &a.KeychainHelper, &p, &a.CreatedAt, &a.ProfileName)
	a.Principal = p.String
	return a, err
}
func (s *Store) Account(ctx context.Context, id string) (Account, error) {
	a, err := accountScan(s.db.QueryRowContext(ctx, "SELECT * FROM line_accounts WHERE id=?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return a, Fail("line_account_not_found")
	}
	return a, err
}
func (s *Store) Allowed(ctx context.Context, c Caller, account, scope string) bool {
	var n int
	if c.Subject == "" {
		return false
	}
	if s.db.QueryRowContext(ctx, "SELECT count(*) FROM line_grants WHERE account=? AND subject=? AND scope=?", account, c.Subject, scope).Scan(&n) != nil {
		return false
	}
	return n == 1 || (s.voiceDefaultAllowed != nil && s.voiceDefaultAllowed(ctx, c, account, scope))
}
func (s *Store) Accounts(ctx context.Context, c Caller) ([]Account, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT * FROM line_accounts ORDER BY created_at")
	if err != nil {
		return nil, err
	}
	// Finish the scan before evaluating access: the store intentionally owns a
	// single connection, and inherited access revalidates the account binding.
	all := []Account{}
	for rows.Next() {
		a, e := accountScan(rows)
		if e != nil {
			rows.Close()
			return nil, e
		}
		all = append(all, a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := []Account{}
	for _, a := range all {
		if s.Allowed(ctx, c, a.ID, "status") {
			out = append(out, a)
		}
	}
	return out, nil
}
func (s *Store) Grant(ctx context.Context, c Caller, account, subject string, allowed []string) error {
	if subject == "" {
		return Fail("line_forbidden")
	}
	for _, scope := range allowed {
		valid := false
		for _, x := range scopes {
			if x == scope {
				valid = true
			}
		}
		if !valid {
			return Fail("line_scope_invalid")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var owner string
	if err = tx.QueryRowContext(ctx, "SELECT owner FROM line_accounts WHERE id=?", account).Scan(&owner); err != nil {
		return err
	}
	if owner != c.Subject {
		return Fail("line_forbidden")
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM line_grants WHERE account=? AND subject=?", account, subject); err != nil {
		return err
	}
	for _, scope := range allowed {
		if _, err = tx.ExecContext(ctx, "INSERT INTO line_grants VALUES(?,?,?)", account, subject, scope); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO line_grant_revisions VALUES(?,2) ON CONFLICT(account) DO UPDATE SET revision=revision+1", account); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) BindPrincipal(ctx context.Context, id, principal string) error {
	if !strings.HasPrefix(principal, "u") || len(principal) != 33 {
		return Fail("line_principal_invalid")
	}
	fp := s.digest(principal)
	a, err := s.Account(ctx, id)
	if err != nil {
		return err
	}
	if a.Principal != "" && a.Principal != fp {
		_, _ = s.db.ExecContext(ctx, "UPDATE line_accounts SET auth_state='principal_mismatch' WHERE id=?", id)
		return Fail("line_principal_mismatch")
	}
	_, err = s.db.ExecContext(ctx, "UPDATE line_accounts SET principal=?,auth_state='ready' WHERE id=?", fp, id)
	if err != nil {
		return Fail("line_principal_already_managed")
	}
	return nil
}
func (s *Store) SetAuth(ctx context.Context, id, state string) {
	_, _ = s.db.ExecContext(ctx, "UPDATE line_accounts SET auth_state=? WHERE id=?", state, id)
}
func (s *Store) SetEnabled(ctx context.Context, c Caller, id string, enabled bool) error {
	result, err := s.db.ExecContext(ctx, "UPDATE line_accounts SET enabled=?,revision=revision+1 WHERE id=? AND owner=?", enabled, id, c.Subject)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return Fail("line_forbidden")
	}
	return nil
}
func (s *Store) SaveTarget(ctx context.Context, account, external, name, kind string) (Target, error) {
	if external == "" || name == "" || len(external) > 128 {
		return Target{}, Fail("line_target_invalid")
	}
	ref := "lt_" + s.digest(account + "\x00" + external)[:32]
	_, err := s.db.ExecContext(ctx, "INSERT INTO line_targets VALUES(?,?,?,?,?) ON CONFLICT(account,external) DO UPDATE SET name=excluded.name,kind=excluded.kind", ref, account, external, name, kind)
	return Target{Ref: ref, Account: account, Name: name, Kind: kind, ExternalID: external}, err
}
func (s *Store) Target(ctx context.Context, account, ref string) (Target, error) {
	var t Target
	err := s.db.QueryRowContext(ctx, "SELECT ref,account,external,name,kind FROM line_targets WHERE ref=? AND account=?", ref, account).Scan(&t.Ref, &t.Account, &t.ExternalID, &t.Name, &t.Kind)
	if errors.Is(err, sql.ErrNoRows) {
		return t, Fail("line_target_not_found")
	}
	return t, err
}
func scanOp(scan interface{ Scan(...any) error }, s *Store) (Operation, error) {
	var o Operation
	var b []byte
	err := scan.Scan(&o.ID, &o.Account, &o.Target, &o.Kind, &o.State, &o.Effect, &o.RemoteID, &o.Code, &o.Subject, &o.Intent, &o.Fingerprint, &o.BindingRevision, &b, &o.CreatedAt, &o.UpdatedAt, &o.DueAt, &o.Timezone, &o.LateWindow, &o.Revision)
	if err == nil {
		err = s.unseal(o.Account, b, &o.Content)
	}
	return o, err
}
func (s *Store) Operation(ctx context.Context, id string) (Operation, error) {
	o, err := scanOp(s.db.QueryRowContext(ctx, "SELECT * FROM line_ops WHERE id=?", id), s)
	if errors.Is(err, sql.ErrNoRows) {
		return o, Fail("line_operation_not_found")
	}
	return o, err
}
func (s *Store) LookupIntent(ctx context.Context, c Caller, a Account, t Target, content Content, intent string, due int64, zone string, late int64) (Operation, bool, error) {
	o, err := scanOp(s.db.QueryRowContext(ctx, "SELECT * FROM line_ops WHERE account=? AND kind='send_message' AND intent=?", a.ID, intent), s)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, false, nil
	}
	if err != nil {
		return Operation{}, false, err
	}
	if o.Subject != c.Subject {
		return Operation{}, true, Fail("line_forbidden")
	}
	fingerprint := s.digest(string(raw([]any{a.ID, a.Principal, a.Revision, t.Ref, content, due, zone, late})))
	if fingerprint != o.Fingerprint {
		return Operation{}, true, Fail("line_intent_conflict")
	}
	return o, true, nil
}
func (s *Store) Enqueue(ctx context.Context, c Caller, a Account, t Target, content Content, intent string, due int64, zone string, late int64) (Operation, error) {
	if len(intent) < 8 || len(intent) > 128 || (strings.TrimSpace(content.Text) == "" && content.ResourceRef == "") || len(content.Text) > 20000 {
		return Operation{}, Fail("line_send_invalid")
	}
	fingerprint := s.digest(string(raw([]any{a.ID, a.Principal, a.Revision, t.Ref, content, due, zone, late})))
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, e := scanOp(s.db.QueryRowContext(ctx, "SELECT * FROM line_ops WHERE account=? AND kind='send_message' AND intent=?", a.ID, intent), s)
	if e == nil {
		if existing.Fingerprint != fingerprint {
			return existing, Fail("line_intent_conflict")
		}
		if existing.Subject != c.Subject {
			return existing, Fail("line_forbidden")
		}
		return existing, nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return Operation{}, e
	}
	state := "queued"
	if due > 0 {
		state = "scheduled"
	}
	o := Operation{ID: newID("lo_"), Account: a.ID, Target: t.Ref, Kind: "send_message", State: state, Effect: "not_dispatched", Subject: c.Subject, Intent: intent, Fingerprint: fingerprint, BindingRevision: a.Revision, Content: content, CreatedAt: nowMS(), UpdatedAt: nowMS(), DueAt: due, Timezone: zone, LateWindow: late, Revision: 1}
	b, err := s.seal(a.ID, content)
	if err != nil {
		return o, err
	}
	_, err = s.db.ExecContext(ctx, "INSERT INTO line_ops VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", o.ID, o.Account, o.Target, o.Kind, o.State, o.Effect, "", "", o.Subject, o.Intent, o.Fingerprint, o.BindingRevision, b, o.CreatedAt, o.UpdatedAt, due, zone, late, o.Revision)
	return o, err
}
func (s *Store) Transition(ctx context.Context, id, from, to, effect, code, remote string) (bool, error) {
	r, e := s.db.ExecContext(ctx, "UPDATE line_ops SET state=?,effect=?,code=?,remote_id=?,updated_at=?,revision=revision+1 WHERE id=? AND state=?", to, effect, code, remote, nowMS(), id, from)
	if e != nil {
		return false, e
	}
	n, e := r.RowsAffected()
	return n == 1, e
}

// A scheduler/dispatcher snapshot must not overwrite a newer human edit.
func (s *Store) transitionRevision(ctx context.Context, op Operation, to, effect, code string) (bool, error) {
	r, e := s.db.ExecContext(ctx, "UPDATE line_ops SET state=?,effect=?,code=?,updated_at=?,revision=revision+1 WHERE id=? AND state=? AND revision=?", to, effect, code, nowMS(), op.ID, op.State, op.Revision)
	if e != nil {
		return false, e
	}
	n, e := r.RowsAffected()
	return n == 1, e
}
func (s *Store) Pending(ctx context.Context, now int64) ([]Operation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT * FROM line_ops WHERE id IN (SELECT id FROM (SELECT id,ROW_NUMBER() OVER(PARTITION BY account ORDER BY CASE WHEN state='queued' THEN 0 ELSE 1 END,due_at,created_at) AS position FROM line_ops WHERE state='queued' OR (state='scheduled' AND due_at<=?)) WHERE position=1) ORDER BY created_at LIMIT 100`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Operation{}
	for rows.Next() {
		o, e := scanOp(rows, s)
		if e != nil {
			return nil, e
		}
		out = append(out, o)
	}
	return out, rows.Err()
}
func (s *Store) Operations(ctx context.Context, c Caller, account string) ([]Operation, error) {
	a, err := s.Account(ctx, account)
	if err != nil {
		return nil, err
	}
	query := "SELECT * FROM line_ops WHERE account=? AND subject=? ORDER BY created_at DESC LIMIT 100"
	arguments := []any{account, c.Subject}
	if c.Admin && a.Owner == c.Subject {
		query = "SELECT * FROM line_ops WHERE account=? ORDER BY created_at DESC LIMIT 100"
		arguments = []any{account}
	}
	rows, err := s.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Operation{}
	for rows.Next() {
		o, e := scanOp(rows, s)
		if e != nil {
			return nil, e
		}
		out = append(out, o)
	}
	return out, rows.Err()
}
func (s *Store) Cancel(ctx context.Context, c Caller, id string, revision uint64) error {
	o, err := s.Operation(ctx, id)
	if err != nil {
		return err
	}
	a, _ := s.Account(ctx, o.Account)
	if o.Subject != c.Subject && !(c.Admin && a.Owner == c.Subject) {
		return Fail("line_forbidden")
	}
	r, err := s.db.ExecContext(ctx, "UPDATE line_ops SET state='cancelled',updated_at=?,revision=revision+1 WHERE id=? AND revision=? AND state IN ('queued','scheduled','held_late','held_binding','held_unavailable','failed_before_effect')", nowMS(), id, revision)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return Fail("line_operation_not_cancellable")
	}
	return nil
}

func (s *Store) BindIdentity(ctx context.Context, id string, identity Identity) error {
	if err := s.BindPrincipal(ctx, id, identity.Principal); err != nil {
		return err
	}
	name := []rune(identity.Name)
	if len(name) > 128 {
		name = name[:128]
	}
	_, err := s.db.ExecContext(ctx, "UPDATE line_accounts SET profile_name=? WHERE id=?", string(name), id)
	return err
}

// Read a CLI intent under its original account/subject. It does not resume an
// account, dispatch work or assume a missing row means definite non-execution.
func (s *Store) IntentReceipt(ctx context.Context, c Caller, account, intent string) (Operation, bool, error) {
	if !s.Allowed(ctx, c, account, "status") {
		return Operation{}, false, Fail("line_forbidden")
	}
	rows, e := s.db.QueryContext(ctx, "SELECT id FROM line_ops WHERE account=? AND subject=? AND intent=? LIMIT 2", account, c.Subject, intent)
	if e != nil {
		return Operation{}, false, e
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return Operation{}, false, e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return Operation{}, false, e
	}
	if len(ids) == 0 {
		return Operation{}, false, nil
	}
	if len(ids) != 1 {
		return Operation{}, false, Fail("line_intent_ambiguous")
	}
	op, e := s.Operation(ctx, ids[0])
	return op, e == nil, e
}
