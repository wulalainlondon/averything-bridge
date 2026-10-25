package linebridge

import (
	"context"
	"encoding/json"
	"testing"
)

type routeFactory struct{ base *fakeFactory }
type routeDriver struct {
	Driver
	account string
}

func (f routeFactory) Start(ctx context.Context, a Account, epoch string, emit func(RuntimeEvent)) (Driver, error) {
	d, err := f.base.Start(ctx, a, epoch, emit)
	return routeDriver{d, a.ID}, err
}
func (d routeDriver) Invoke(ctx context.Context, method string, in, out any) error {
	if method == "prepare_call" {
		var args map[string]string
		_ = json.Unmarshal(raw(in), &args)
		return json.Unmarshal(raw(CallMaterial{Route: json.RawMessage(`{"test":"private-short-lived-route"}`), Principal: "u" + d.account[3:], Target: args["target"]}), out)
	}
	return d.Driver.Invoke(ctx, method, in, out)
}
func TestCallLeaseConsumesIntentAndChecksBindingBeforeInvite(t *testing.T) {
	_, s, f, owner := fixture(t)
	s.factory = routeFactory{f}
	ctx := context.Background()
	a, ta := accountFixture(t, s, owner, "A")
	b, tb := accountFixture(t, s, owner, "B")
	r, material, err := s.PrepareCall(ctx, owner, a.ID, ta.Ref, "call-fixture-one")
	if err != nil || r.State != "prepared" || len(material.Route) == 0 {
		t.Fatal(r, err)
	}
	if _, _, err = s.PrepareCall(ctx, owner, b.ID, tb.Ref, "call-fixture-two"); SafeCode(err) != "line_call_busy" {
		t.Fatal("parallel call admitted", err)
	}
	if err = s.ValidatePreparedCall(ctx, owner, r.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.ValidatePreparedCall(ctx, owner, r.ID); err == nil {
		t.Fatal("lease consumed twice")
	}
	if err = s.UpdateCall(ctx, r.ID, "closed", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.PrepareCall(ctx, owner, a.ID, ta.Ref, "call-fixture-one"); SafeCode(err) != "line_call_intent_consumed" {
		t.Fatal("terminal intent replayed", err)
	}
	r, _, err = s.PrepareCall(ctx, owner, b.ID, tb.Ref, "call-fixture-two")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SetEnabled(ctx, owner, b.ID, false); err != nil {
		t.Fatal(err)
	}
	if err = s.ValidatePreparedCall(ctx, owner, r.ID); err == nil {
		t.Fatal("disabled account reached invite")
	}
	if _, err = s.CallReceipt(ctx, Caller{Subject: "device:other"}, r.ID); err == nil {
		t.Fatal("call receipt crossed caller")
	}
}
