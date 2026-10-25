package linebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeFactory struct {
	mu      sync.Mutex
	drivers map[string]*fakeDriver
}
type fakeDriver struct {
	principal string
	sends     atomic.Int64
	closed    atomic.Bool
	failure   error
}

func (f *fakeFactory) Start(_ context.Context, a Account, _ string, _ func(RuntimeEvent)) (Driver, error) {
	d := &fakeDriver{principal: "u" + strings.TrimPrefix(a.ID, "ln_")}
	f.mu.Lock()
	f.drivers[a.ID] = d
	f.mu.Unlock()
	return d, nil
}
func (d *fakeDriver) Close() error { d.closed.Store(true); return nil }
func (d *fakeDriver) Invoke(_ context.Context, method string, input, out any) error {
	if d.closed.Load() {
		return Fail("line_runtime_stopped")
	}
	var value any
	switch method {
	case "resume":
		value = Identity{Principal: d.principal, Name: "fixture"}
	case "find_targets":
		value = []map[string]string{{"id": "u" + strings.Repeat("1", 32), "name": "Same Name", "kind": "user"}}
	case "get_message_page":
		value = map[string]any{"messages": []Message{{ID: "m1", Text: "fixture", ContentType: "NONE", Timestamp: 100}}, "scanned": 1, "next_cursor": "fixture-next", "coverage": "upstream_page"}
	case "get_messages":
		value = []Message{{ID: "m1", Text: "fixture", ContentType: "NONE"}}
	case "send_message":
		d.sends.Add(1)
		if d.failure != nil {
			return d.failure
		}
		value = map[string]string{"message_id": "remote-fixture"}
	default:
		return Fail("line_fixture_method_invalid")
	}
	if out != nil {
		return json.Unmarshal(raw(value), out)
	}
	return nil
}
func fixture(t *testing.T) (*Store, *Service, *fakeFactory, Caller) {
	t.Helper()
	store, err := OpenStore(t.TempDir(), "fixture-authority", bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeFactory{drivers: map[string]*fakeDriver{}}
	s := NewService(store, f, Config{DataDir: t.TempDir(), KeychainHelper: "/fixture/helper"})
	t.Cleanup(func() { s.Close(); store.Close() })
	return store, s, f, Caller{Subject: "device:owner", Admin: true}
}
func accountFixture(t *testing.T, s *Service, c Caller, label string) (Account, Target) {
	t.Helper()
	ctx := context.Background()
	a, err := s.Create(ctx, c, label, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Resume(ctx, c, a.ID); err != nil {
		t.Fatal(err)
	}
	targets, err := s.Find(ctx, c, a.ID, "Same", 10)
	if err != nil {
		t.Fatal(err)
	}
	a, _ = s.store.Account(ctx, a.ID)
	return a, targets[0]
}
func terminal(t *testing.T, s *Service, c Caller, id string) Operation {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		o, e := s.Receipt(context.Background(), c, id)
		if e != nil {
			t.Fatal(e)
		}
		if o.State != "queued" && o.State != "dispatching" {
			return o
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("operation did not terminate")
	return Operation{}
}
func TestAccountsTargetsAndReceiptsStayIsolated(t *testing.T) {
	_, s, _, owner := fixture(t)
	a, ta := accountFixture(t, s, owner, "A")
	b, tb := accountFixture(t, s, owner, "B")
	if ta.Ref == tb.Ref {
		t.Fatal("target reference crosses accounts")
	}
	other := Caller{Subject: "device:other"}
	list, _ := s.store.Accounts(context.Background(), other)
	if len(list) != 0 {
		t.Fatal("ungranted account leaked")
	}
	if _, err := s.Read(context.Background(), owner, a.ID, tb.Ref, "", 3); err == nil {
		t.Fatal("cross-account target accepted")
	}
	op, err := s.Send(context.Background(), owner, a.ID, ta.Ref, Content{Text: "private fixture"}, "intent-one", 0, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if terminal(t, s, owner, op.ID).State != "accepted_by_line" {
		t.Fatal("missing LINE acknowledgement")
	}
	if _, err = s.Receipt(context.Background(), other, op.ID); err == nil {
		t.Fatal("receipt exposed to another caller")
	}
	if err = s.SetEnabled(context.Background(), owner, b.ID, false); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Read(context.Background(), owner, a.ID, ta.Ref, "", 1); err != nil {
		t.Fatal("stopping B affected A:", err)
	}
}
func TestUnknownEffectNeverAutomaticallyResends(t *testing.T) {
	_, s, f, c := fixture(t)
	a, target := accountFixture(t, s, c, "A")
	f.drivers[a.ID].failure = &Failure{Code: "line_delivery_unknown"}
	op, err := s.Send(context.Background(), c, a.ID, target.Ref, Content{Text: "same fixture"}, "intent-unknown", 0, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := terminal(t, s, c, op.ID); got.State != "unknown" {
		t.Fatalf("got %s", got.State)
	}
	for i := 0; i < 3; i++ {
		same, err := s.Send(context.Background(), c, a.ID, target.Ref, Content{Text: "same fixture"}, "intent-unknown", 0, "", 0)
		if err != nil || same.ID != op.ID {
			t.Fatal("original receipt not reused", err)
		}
	}
	if f.drivers[a.ID].sends.Load() != 1 {
		t.Fatal("ambiguous effect replayed")
	}
	if _, err = s.Send(context.Background(), c, a.ID, target.Ref, Content{Text: "changed"}, "intent-unknown", 0, "", 0); err == nil {
		t.Fatal("idempotency conflict accepted")
	}
}
func TestScheduledSenderStaysFixedAndCancellationWinsBeforeDispatch(t *testing.T) {
	_, s, f, c := fixture(t)
	a, target := accountFixture(t, s, c, "A")
	_, _ = accountFixture(t, s, c, "B")
	due := nowMS() + 60000
	op, err := s.Send(context.Background(), c, a.ID, target.Ref, Content{Text: "scheduled"}, "intent-schedule", due, "Asia/Taipei", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.store.Cancel(context.Background(), c, op.ID, op.Revision); err != nil {
		t.Fatal(err)
	}
	s.Tick(context.Background(), due)
	if f.drivers[a.ID].sends.Load() != 0 {
		t.Fatal("cancelled operation sent")
	}
	second, err := s.Send(context.Background(), c, a.ID, target.Ref, Content{Text: "held"}, "intent-overdue", due, "Asia/Taipei", 1000)
	if err != nil {
		t.Fatal(err)
	}
	s.Tick(context.Background(), due+2000)
	got, _ := s.Receipt(context.Background(), c, second.ID)
	if got.State != "held_late" {
		t.Fatal("overdue operation dispatched", got.State)
	}
}
func TestPrincipalDriftAndDuplicatePrincipalAreRejected(t *testing.T) {
	store, s, f, c := fixture(t)
	a, _ := accountFixture(t, s, c, "A")
	b, _ := accountFixture(t, s, c, "B")
	if err := store.BindPrincipal(context.Background(), b.ID, f.drivers[a.ID].principal); err == nil {
		t.Fatal("principal change accepted")
	}
	if err := store.BindPrincipal(context.Background(), a.ID, "u"+strings.Repeat("f", 32)); err == nil {
		t.Fatal("principal drift accepted")
	}
	third, err := s.Create(context.Background(), c, "C", false)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BindPrincipal(context.Background(), third.ID, f.drivers[a.ID].principal); err == nil {
		t.Fatal("same principal gained second owner")
	}
}
func TestSensitivePayloadIsEncryptedAndAccountProjectionOmitsPrivateMetadata(t *testing.T) {
	store, s, _, c := fixture(t)
	a, target := accountFixture(t, s, c, "A")
	phrase := "private-message-fixture-123"
	op, err := s.Send(context.Background(), c, a.ID, target.Ref, Content{Text: phrase}, "intent-encrypted", nowMS()+60000, "Asia/Taipei", 0)
	if err != nil {
		t.Fatal(err)
	}
	var ciphertext []byte
	if err = store.db.QueryRow("SELECT content FROM line_ops WHERE id=?", op.ID).Scan(&ciphertext); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ciphertext, []byte(phrase)) {
		t.Fatal("content stored in plaintext")
	}
	projection := string(raw(a))
	for _, private := range []string{a.DataDir, a.KeychainHelper, a.Principal, "principal", "owner"} {
		if private != "" && strings.Contains(projection, private) {
			t.Fatal("private account metadata in projection", private)
		}
	}
}
func TestRestartMarksInFlightUnknownAndRetainsQueuedAccountBinding(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{8}, 32)
	store, err := OpenStore(dir, "fixture", key)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a, err := store.CreateAccount(ctx, "A", "owner", filepath.Join(dir, "account"), "/fixture/helper")
	if err != nil {
		t.Fatal(err)
	}
	_ = store.BindPrincipal(ctx, a.ID, "u"+strings.Repeat("a", 32))
	a, _ = store.Account(ctx, a.ID)
	target, _ := store.SaveTarget(ctx, a.ID, "u"+strings.Repeat("b", 32), "friend", "user")
	op, err := store.Enqueue(ctx, Caller{Subject: "owner"}, a, target, Content{Text: "fixture"}, "restart-intent", 0, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = store.Transition(ctx, op.ID, "queued", "dispatching", "pending", "", "")
	store.Close()
	resumed, err := OpenStore(dir, "fixture", key)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	got, err := resumed.Operation(ctx, op.ID)
	if err != nil || got.State != "unknown" || got.Account != a.ID {
		t.Fatal("restart replay risk", got.State, err)
	}
}
func TestStrictRequestAndSafeErrors(t *testing.T) {
	for _, v := range []string{`{"method":"accounts","credential":"secret"}`, `{"method":"accounts"} garbage`, `{"method":"accounts"}{}`} {
		if _, err := DecodeRequest([]byte(v)); err == nil {
			t.Fatal("invalid envelope accepted")
		}
	}
	if SafeCode(errors.New("Authorization: secret")) != "line_operation_failed" {
		t.Fatal("raw error exposed")
	}
	if _, err := DecodeRequest([]byte(`{"method":"accounts"}`)); err != nil {
		t.Fatal(err)
	}
}
