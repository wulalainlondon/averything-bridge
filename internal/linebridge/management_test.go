package linebridge

import (
	"context"
	"testing"
)

func TestManagementOwnerAndRevisionAreIndependentOfAdminFlag(t *testing.T) {
	ctx := context.Background()
	store, s, _, owner := fixture(t)
	a, err := s.Create(ctx, owner, "owner", false)
	if err != nil {
		t.Fatal(err)
	}
	s.config.SubjectAllowed = func(subject string) bool {
		return subject == owner.Subject || subject == "session:chosen" || subject == "device:other"
	}
	if _, err = s.Grants(ctx, Caller{Subject: "device:other", Admin: true}, a.ID); SafeCode(err) != "line_forbidden" {
		t.Fatal("other paired administrator inspected grants", err)
	}
	g, err := s.Grants(ctx, owner, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	r := Request{Account: a.ID, Subject: "session:chosen", Scopes: []string{"status", "directory", "messages.read"}, Revision: g.Revision}
	changed, err := s.SetGrants(ctx, owner, r)
	if err != nil || changed.Revision != g.Revision+1 {
		t.Fatal(changed, err)
	}
	if _, err = s.SetGrants(ctx, owner, r); SafeCode(err) != "line_grant_revision_conflict" {
		t.Fatal("stale screen changed grants", err)
	}
	access, err := s.Access(ctx, Caller{Subject: r.Subject}, a.ID)
	if err != nil || access.CanManage || access.Role != "shared" || len(access.Scopes) != 3 {
		t.Fatal(access, err)
	}
	if access.Limits["find_targets"] != 50 || access.Limits["find_openchats"] != 20 {
		t.Fatal(access.Limits)
	}
	// Existing owner entry remains compatible, and invalidates a stale new UI.
	if err = store.Grant(ctx, owner, a.ID, r.Subject, []string{"status"}); err != nil {
		t.Fatal(err)
	}
	r.Revision = changed.Revision
	if _, err = s.SetGrants(ctx, owner, r); SafeCode(err) != "line_grant_revision_conflict" {
		t.Fatal("legacy update bypassed revision", err)
	}
	fresh, err := s.Grants(ctx, owner, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	r.Revision = fresh.Revision
	r.Scopes = nil
	if _, err = s.SetGrants(ctx, owner, r); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Access(ctx, Caller{Subject: r.Subject}, a.ID); SafeCode(err) != "line_forbidden" {
		t.Fatal("revocation did not take effect", err)
	}
}
func TestManagementRejectsSelfRevokedAndDisabledScopes(t *testing.T) {
	ctx := context.Background()
	_, s, _, owner := fixture(t)
	a, err := s.Create(ctx, owner, "one", false)
	if err != nil {
		t.Fatal(err)
	}
	s.config.SubjectAllowed = func(subject string) bool { return subject == owner.Subject || subject == "session:chosen" }
	g, err := s.Grants(ctx, owner, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, subject := range []string{owner.Subject, "session:revoked"} {
		if _, err = s.SetGrants(ctx, owner, Request{Account: a.ID, Subject: subject, Revision: g.Revision, Scopes: []string{"status"}}); err == nil {
			t.Fatal("invalid subject accepted")
		}
	}
	s.config.BlockedScopes = map[string]bool{"call": true}
	for _, scopes := range [][]string{{"status", "call"}, {"status", "status"}, {"directory"}, {"invalid"}} {
		if _, err = s.SetGrants(ctx, owner, Request{Account: a.ID, Subject: "session:chosen", Revision: g.Revision, Scopes: scopes}); err == nil {
			t.Fatal("invalid scopes accepted", scopes)
		}
	}
	fresh, _ := s.Grants(ctx, owner, a.ID)
	if fresh.Revision != g.Revision {
		t.Fatal("invalid request changed revision")
	}
}
func TestManagementHealthSnapshotNeverStartsRuntimeOrCallsNetwork(t *testing.T) {
	ctx := context.Background()
	store, s, f, owner := fixture(t)
	a, err := s.Create(ctx, owner, "one", false)
	if err != nil {
		t.Fatal(err)
	}
	status, err := s.Status(ctx, owner, a.ID)
	if err != nil || status.LastHealth != nil || len(f.drivers) != 0 {
		t.Fatal(status, err)
	}
	if err = store.recordHealth(ctx, a.ID, "verified", ""); err != nil {
		t.Fatal(err)
	}
	status, err = s.Status(ctx, owner, a.ID)
	if err != nil || status.LastHealth == nil || status.LastHealth.State != "verified" || status.LastHealth.CheckedAt == 0 || status.Connectivity != "not_checked" || len(f.drivers) != 0 {
		t.Fatal(status, err)
	}
	// Fixture deliberately refuses health. An earlier success must not remain current.
	if _, err = s.Health(ctx, owner, a.ID); err == nil {
		t.Fatal("fixture should refuse live health")
	}
	status, err = s.Status(ctx, owner, a.ID)
	if err != nil || status.LastHealth == nil || status.LastHealth.State != "failed" || status.LastHealth.Code == "" {
		t.Fatal(status, err)
	}
}
func TestManagementSubscriptionsDoNotLeakOtherSubjects(t *testing.T) {
	ctx := context.Background()
	store, s, _, owner := fixture(t)
	a, err := s.Create(ctx, owner, "one", false)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Grant(ctx, owner, a.ID, "session:chosen", []string{"status"}); err != nil {
		t.Fatal(err)
	}
	for i, subject := range []string{owner.Subject, "session:chosen", "session:other"} {
		if _, err = store.db.ExecContext(ctx, "INSERT INTO line_subscriptions VALUES(?,?,?,?,?)", string(rune('a'+i)), a.ID, "target", subject, i != 2); err != nil {
			t.Fatal(err)
		}
	}
	items, err := s.ListSubscriptions(ctx, Caller{Subject: "session:chosen"}, a.ID)
	if err != nil || len(items) != 1 || items[0]["subject_ref"] != "session:chosen" {
		t.Fatal(items, err)
	}
	items, err = s.ListSubscriptions(ctx, owner, a.ID)
	if err != nil || len(items) != 3 {
		t.Fatal(items, err)
	}
	if _, err = s.ListSubscriptions(ctx, Caller{Subject: "session:other"}, a.ID); SafeCode(err) != "line_forbidden" {
		t.Fatal(err)
	}
}

func TestManagementHealthIsScopedToOriginalAccountBinding(t *testing.T) {
	ctx := context.Background()
	store, s, _, owner := fixture(t)
	a, err := s.Create(ctx, owner, "binding", false)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.recordHealth(ctx, a.ID, "verified", ""); err != nil {
		t.Fatal(err)
	}
	if _, err = store.db.ExecContext(ctx, "UPDATE line_accounts SET revision=revision+1 WHERE id=?", a.ID); err != nil {
		t.Fatal(err)
	}
	status, err := s.Status(ctx, owner, a.ID)
	if err != nil || status.LastHealth != nil {
		t.Fatal("stale binding health reused", status, err)
	}
}
