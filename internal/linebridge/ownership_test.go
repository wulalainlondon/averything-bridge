package linebridge

import (
	"context"
	"testing"
	"time"
)

func TestOwnerTransferRequiresDisabledExactBindingAndRevokesSharing(t *testing.T) {
	store, service, _, owner := fixture(t)
	ctx := context.Background()
	replacement := Caller{Subject: "device:replacement", Admin: true}
	service.config.SubjectAllowed = func(subject string) bool { return subject == owner.Subject || subject == replacement.Subject }
	a, target := accountFixture(t, service, owner, "Personal")
	if err := store.Grant(ctx, owner, a.ID, "session:previous", []string{"status", "messages.read"}); err != nil {
		t.Fatal(err)
	}
	op, err := service.Send(ctx, owner, a.ID, target.Ref, Content{Text: "scheduled fixture"}, "owner-transfer-fixture", time.Now().Add(time.Hour).UnixMilli(), "UTC", 0)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Method: "transfer_owner", Account: a.ID, Subject: replacement.Subject, Revision: a.Revision}
	if _, err = service.Execute(ctx, owner, request); err == nil {
		t.Fatal("enabled account transferred")
	}
	if err = service.SetEnabled(ctx, owner, a.ID, false); err != nil {
		t.Fatal(err)
	}
	disabled, _ := store.Account(ctx, a.ID)
	if _, err = service.Execute(ctx, owner, request); err == nil {
		t.Fatal("stale revision transferred")
	}
	request.Revision = disabled.Revision
	for _, subject := range []string{"device:unpaired", "session:previous", owner.Subject} {
		invalid := request
		invalid.Subject = subject
		if _, err = service.Execute(ctx, owner, invalid); err == nil {
			t.Fatalf("invalid replacement accepted: %s", subject)
		}
	}
	if _, err = service.Execute(ctx, Caller{Subject: owner.Subject}, request); err == nil {
		t.Fatal("non-admin transferred")
	}
	if _, err = service.Execute(ctx, replacement, request); err == nil {
		t.Fatal("another paired device transferred")
	}
	if _, err = store.db.ExecContext(ctx, "INSERT INTO line_subscriptions VALUES('fixture-sub',?,?,?,1)", a.ID, target.Ref, owner.Subject); err != nil {
		t.Fatal(err)
	}
	result, err := service.Execute(ctx, owner, request)
	if err != nil {
		t.Fatal(err)
	}
	got := result.(Account)
	if got.Enabled || got.Owner != replacement.Subject || got.Revision != disabled.Revision+1 || got.ID != a.ID || got.DataDir != a.DataDir || got.Principal != a.Principal {
		t.Fatalf("account identity not preserved: %+v", got)
	}
	for _, subject := range []string{owner.Subject, "session:previous"} {
		if store.Allowed(ctx, Caller{Subject: subject}, a.ID, "status") {
			t.Fatal("previous sharing survived transfer")
		}
	}
	for _, scope := range scopes {
		if !store.Allowed(ctx, replacement, a.ID, scope) {
			t.Fatalf("new owner missing %s", scope)
		}
	}
	if err = store.SetEnabled(ctx, owner, a.ID, true); err == nil {
		t.Fatal("old owner re-enabled account")
	}
	if err = store.Grant(ctx, owner, a.ID, owner.Subject, scopes); err == nil {
		t.Fatal("old owner restored grant")
	}
	if subscriptions, err := store.subscriptions(ctx, a.ID); err != nil || len(subscriptions) != 0 {
		t.Fatal("previous subscriptions survived transfer")
	}
	retained, err := store.Operation(ctx, op.ID)
	if err != nil || retained.Subject != owner.Subject || retained.BindingRevision == got.Revision || retained.State != op.State {
		t.Fatal("old operation was rewritten or rebound")
	}
	if err = service.SetEnabled(ctx, replacement, a.ID, true); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerTransferRejectsRevokedCurrentOwner(t *testing.T) {
	store, service, _, owner := fixture(t)
	ctx := context.Background()
	a, _ := accountFixture(t, service, owner, "Personal")
	if err := service.SetEnabled(ctx, owner, a.ID, false); err != nil {
		t.Fatal(err)
	}
	a, _ = store.Account(ctx, a.ID)
	service.config.SubjectAllowed = func(subject string) bool { return subject == "device:replacement" }
	if _, err := service.Execute(ctx, owner, Request{Method: "transfer_owner", Account: a.ID, Subject: "device:replacement", Revision: a.Revision}); err == nil {
		t.Fatal("revoked owner transferred account")
	}
}
