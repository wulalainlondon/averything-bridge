package linebridge

import (
	"context"
	"database/sql"
	"errors"
	"sort"
)

// Management is an original-owner view. A paired device being Admin never
// permits it to inspect another owner's sharing or to become that owner.
type Access struct {
	VoiceDispatch VoiceDispatchAccess `json:"voice_dispatch"`
	Subject       string              `json:"subject_ref"`
	Role          string              `json:"role"`
	CanManage     bool                `json:"can_manage"`
	Scopes        []string            `json:"scopes"`
	Limits        map[string]int      `json:"limits"`
}
type Grant struct {
	Subject string   `json:"subject_ref"`
	Scopes  []string `json:"scopes"`
}
type Grants struct {
	Revision uint64  `json:"revision"`
	Items    []Grant `json:"items"`
}
type HealthObservation struct {
	CheckedAt       int64  `json:"checked_at"`
	BindingRevision uint64 `json:"binding_revision"`
	State           string `json:"state"`
	Code            string `json:"code,omitempty"`
}

func (s *Service) Access(ctx context.Context, c Caller, id string) (Access, error) {
	if s.config.SubjectAllowed != nil && !s.config.SubjectAllowed(c.Subject) {
		return Access{}, Fail("line_caller_revoked")
	}
	if !s.store.Allowed(ctx, c, id, "status") {
		return Access{}, Fail("line_forbidden")
	}
	a, err := s.store.Account(ctx, id)
	if err != nil {
		return Access{}, err
	}
	out := Access{Subject: c.Subject, Role: "shared", CanManage: c.Admin && a.Owner == c.Subject, Scopes: []string{}, Limits: map[string]int{"find_targets": 50, "find_openchats": 20, "get_messages": 100}, VoiceDispatch: s.voiceDispatchAccess(ctx, c, id)}
	if out.CanManage {
		out.Role = "owner"
	}
	for _, scope := range scopes {
		if !s.config.BlockedScopes[scope] && s.store.Allowed(ctx, c, id, scope) {
			out.Scopes = append(out.Scopes, scope)
		}
	}
	sort.Strings(out.Scopes)
	return out, nil
}
func (s *Service) requireOwner(ctx context.Context, c Caller, id string) error {
	if !c.Admin || (s.config.SubjectAllowed != nil && !s.config.SubjectAllowed(c.Subject)) {
		return Fail("line_admin_required")
	}
	a, err := s.store.Account(ctx, id)
	if err != nil {
		return err
	}
	if a.Owner != c.Subject {
		return Fail("line_forbidden")
	}
	return nil
}
func (s *Service) Grants(ctx context.Context, c Caller, id string) (Grants, error) {
	if err := s.requireOwner(ctx, c, id); err != nil {
		return Grants{}, err
	}
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return Grants{}, err
	}
	defer tx.Rollback()
	var revision uint64
	if _, err = tx.ExecContext(ctx, "INSERT INTO line_grant_revisions VALUES(?,1) ON CONFLICT(account) DO NOTHING", id); err != nil {
		return Grants{}, err
	}
	if err = tx.QueryRowContext(ctx, "SELECT revision FROM line_grant_revisions WHERE account=?", id).Scan(&revision); err != nil {
		return Grants{}, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT subject,scope FROM line_grants WHERE account=? ORDER BY subject,scope LIMIT 501", id)
	if err != nil {
		return Grants{}, err
	}
	items := []Grant{}
	count := 0
	for rows.Next() {
		var subject, scope string
		if err = rows.Scan(&subject, &scope); err != nil {
			rows.Close()
			return Grants{}, err
		}
		count++
		if count > 500 {
			rows.Close()
			return Grants{}, Fail("line_result_limit")
		}
		if len(items) == 0 || items[len(items)-1].Subject != subject {
			items = append(items, Grant{Subject: subject, Scopes: []string{}})
		}
		items[len(items)-1].Scopes = append(items[len(items)-1].Scopes, scope)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return Grants{}, err
	}
	if err = tx.Commit(); err != nil {
		return Grants{}, err
	}
	return Grants{Revision: revision, Items: items}, nil
}
func (s *Service) SetGrants(ctx context.Context, c Caller, r Request) (Grants, error) {
	if err := s.requireOwner(ctx, c, r.Account); err != nil {
		return Grants{}, err
	}
	if r.Subject == "" || r.Subject == c.Subject || s.config.SubjectAllowed == nil || !s.config.SubjectAllowed(r.Subject) {
		return Grants{}, Fail("line_subject_invalid")
	}
	seen := map[string]bool{}
	for _, scope := range r.Scopes {
		valid := false
		for _, allowed := range scopes {
			valid = valid || scope == allowed
		}
		if !valid || seen[scope] || s.config.BlockedScopes[scope] {
			return Grants{}, Fail("line_scope_invalid")
		}
		seen[scope] = true
	}
	if len(r.Scopes) > 0 && !seen["status"] {
		return Grants{}, Fail("line_status_scope_required")
	}
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return Grants{}, err
	}
	defer tx.Rollback()
	// Check owner and revision in the same transaction as the actual grant change.
	result, err := tx.ExecContext(ctx, "UPDATE line_grant_revisions SET revision=revision+1 WHERE account=? AND revision=? AND EXISTS(SELECT 1 FROM line_accounts WHERE id=? AND owner=?)", r.Account, r.Revision, r.Account, c.Subject)
	if err != nil {
		return Grants{}, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return Grants{}, err
	}
	if n != 1 {
		return Grants{}, Fail("line_grant_revision_conflict")
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM line_grants WHERE account=? AND subject=?", r.Account, r.Subject); err != nil {
		return Grants{}, err
	}
	sort.Strings(r.Scopes)
	for _, scope := range r.Scopes {
		if _, err = tx.ExecContext(ctx, "INSERT INTO line_grants VALUES(?,?,?)", r.Account, r.Subject, scope); err != nil {
			return Grants{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return Grants{}, err
	}
	return s.Grants(ctx, c, r.Account)
}
func (s *Service) ListSubscriptions(ctx context.Context, c Caller, id string) ([]map[string]any, error) {
	if _, err := s.Access(ctx, c, id); err != nil {
		return nil, err
	}
	a, err := s.store.Account(ctx, id)
	if err != nil {
		return nil, err
	}
	query := "SELECT id,target,subject,enabled FROM line_subscriptions WHERE account=?"
	args := []any{id}
	if !c.Admin || a.Owner != c.Subject {
		query += " AND subject=?"
		args = append(args, c.Subject)
	}
	rows, err := s.store.db.QueryContext(ctx, query+" ORDER BY id LIMIT 501", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var ref, target, subject string
		var enabled bool
		if err = rows.Scan(&ref, &target, &subject, &enabled); err != nil {
			return nil, err
		}
		if len(out) == 500 {
			return nil, Fail("line_result_limit")
		}
		out = append(out, map[string]any{"subscription_id": ref, "account_ref": id, "target_ref": target, "subject_ref": subject, "enabled": enabled})
	}
	return out, rows.Err()
}
func (s *Store) healthObservation(ctx context.Context, id string) (*HealthObservation, error) {
	v := &HealthObservation{}
	err := s.db.QueryRowContext(ctx, "SELECT checked_at,state,code,binding_revision FROM line_health_observations WHERE account=? AND binding_revision=(SELECT revision FROM line_accounts WHERE id=?)", id, id).Scan(&v.CheckedAt, &v.State, &v.Code, &v.BindingRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return v, err
}
func (s *Store) recordHealth(ctx context.Context, id, state, code string) error {
	_, err := s.db.ExecContext(ctx, "INSERT INTO line_health_observations(account,checked_at,state,code,binding_revision) SELECT ?,?,?,?,revision FROM line_accounts WHERE id=? ON CONFLICT(account) DO UPDATE SET checked_at=excluded.checked_at,state=excluded.state,code=excluded.code,binding_revision=excluded.binding_revision", id, nowMS(), state, code, id)
	return err
}
func (s *Service) Health(ctx context.Context, c Caller, id string) (Status, error) {
	if _, err := s.account(ctx, c, id, "status"); err != nil {
		return Status{}, err
	}
	_, d, err := s.driver(ctx, c, id, "status")
	if err == nil {
		var identity Identity
		err = d.Invoke(ctx, "health", nil, &identity)
		if err == nil {
			err = s.store.BindIdentity(ctx, id, identity)
		}
	}
	state, code := "verified", ""
	if err != nil {
		state, code = "failed", SafeCode(err)
	}
	if saveErr := s.store.recordHealth(context.Background(), id, state, code); saveErr != nil {
		return Status{}, saveErr
	}
	status, statusErr := s.Status(ctx, c, id)
	if statusErr != nil {
		return status, statusErr
	}
	status.Connectivity = state
	return status, err
}
