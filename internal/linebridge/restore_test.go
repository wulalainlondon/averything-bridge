package linebridge

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
)

type restoreDriver struct {
	principal string
	calls     atomic.Int32
	invalid   atomic.Bool
}

func (d *restoreDriver) Close() error { return nil }
func (d *restoreDriver) Invoke(_ context.Context, method string, _ any, out any) error {
	d.calls.Add(1)
	if method != "resume" && method != "health" {
		d.invalid.Store(true)
		return Fail("line_unexpected_effect")
	}
	b, _ := json.Marshal(Identity{Principal: d.principal, Name: "unit fixture"})
	return json.Unmarshal(b, out)
}

type restoreFactory struct{ driver *restoreDriver }

func (f restoreFactory) Start(context.Context, Account, string, func(RuntimeEvent)) (Driver, error) {
	return f.driver, nil
}
func TestRestoreOnlyPreviouslyBoundEnabledAccountsWithoutGrantOrSend(t *testing.T) {
	store, service, _, owner := fixture(t)
	ctx := context.Background()
	a, e := store.CreateAccount(ctx, "fixture", owner.Subject, "/fixture/profile", "/fixture/helper")
	if e != nil {
		t.Fatal(e)
	}
	principal := "u" + strings.Repeat("b", 32)
	if e = store.BindIdentity(ctx, a.ID, Identity{Principal: principal, Name: "fixture"}); e != nil {
		t.Fatal(e)
	}
	driver := &restoreDriver{principal: principal}
	service.factory = restoreFactory{driver}
	status := service.restoreReady(ctx)
	if status.State != "complete" || status.Ready != 1 || status.Verified != 1 || driver.calls.Load() != 2 || driver.invalid.Load() {
		t.Fatal(status)
	}
	accounts, e := store.Accounts(ctx, owner)
	if e != nil || len(accounts) != 1 || accounts[0].ID != a.ID {
		t.Fatal(accounts, e)
	}
}
func TestRestoreDoesNotPairUnboundOrDisabledAccounts(t *testing.T) {
	store, service, _, owner := fixture(t)
	ctx := context.Background()
	a, e := store.CreateAccount(ctx, "unbound", owner.Subject, "/fixture/profile", "/fixture/helper")
	if e != nil {
		t.Fatal(e)
	}
	driver := &restoreDriver{}
	service.factory = restoreFactory{driver}
	status := service.restoreReady(ctx)
	if status.Ready != 0 || driver.calls.Load() != 0 {
		t.Fatal(status)
	}
	if e = store.BindIdentity(ctx, a.ID, Identity{Principal: "u" + strings.Repeat("c", 32)}); e != nil {
		t.Fatal(e)
	}
	if e = store.SetEnabled(ctx, owner, a.ID, false); e != nil {
		t.Fatal(e)
	}
	status = service.restoreReady(ctx)
	if status.Ready != 0 || driver.calls.Load() != 0 {
		t.Fatal(status)
	}
}
func TestRestoreRefusesChangedIdentityWithoutReplacingOriginalPrincipal(t *testing.T) {
	store, service, _, owner := fixture(t)
	ctx := context.Background()
	a, e := store.CreateAccount(ctx, "fixture", owner.Subject, "/fixture/profile", "/fixture/helper")
	if e != nil {
		t.Fatal(e)
	}
	if e = store.BindIdentity(ctx, a.ID, Identity{Principal: "u" + strings.Repeat("a", 32)}); e != nil {
		t.Fatal(e)
	}
	before, _ := store.Account(ctx, a.ID)
	driver := &restoreDriver{principal: "u" + strings.Repeat("b", 32)}
	service.factory = restoreFactory{driver}
	status := service.restoreReady(ctx)
	after, _ := store.Account(ctx, a.ID)
	if status.Verified != 0 || status.State != "partial" || before.Principal != after.Principal || driver.invalid.Load() {
		t.Fatal(status)
	}
}
