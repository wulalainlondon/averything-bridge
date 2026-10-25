package linebridge

import (
	"context"
	"testing"
)

func TestHandoffReceiptBindsOriginalSessionPhoneAccountAndIntent(t *testing.T) {
	store, s, f, owner := fixture(t)
	s.factory = routeFactory{f}
	ctx := context.Background()
	a, target := accountFixture(t, s, owner, "A")
	original := Caller{Subject: "session:fixture-session"}
	if err := store.Grant(ctx, owner, a.ID, original.Subject, []string{"status", "call"}); err != nil {
		t.Fatal(err)
	}
	r, err := s.ReserveHandoff(ctx, original, owner.Subject, "fixture-session", "original-ai-voice", a.ID, target.Ref, "native-tool-call-one")
	if err != nil || r.State != "handoff_pending" {
		t.Fatal(r, err)
	}
	replay, err := s.ReserveHandoff(ctx, original, owner.Subject, "fixture-session", "original-ai-voice", a.ID, target.Ref, "native-tool-call-one")
	if err != nil || replay.ID != r.ID {
		t.Fatal("reservation replay changed ID", err)
	}
	if _, _, err = s.PrepareHandoffCall(ctx, owner, r.ID, "wrong-session", a.ID, target.Ref); err == nil {
		t.Fatal("handoff moved to another conversation")
	}
	if err = store.Grant(ctx, owner, a.ID, "device:other", []string{"status", "call"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.PrepareHandoffCall(ctx, Caller{Subject: "device:other", Admin: true}, r.ID, "fixture-session", a.ID, target.Ref); err == nil {
		t.Fatal("handoff moved to another authorized phone")
	}
	prepared, _, err := s.PrepareHandoffCall(ctx, owner, r.ID, "fixture-session", a.ID, target.Ref)
	if err != nil || prepared.ID != r.ID {
		t.Fatal("preparation lost original call identity", err)
	}
	if _, _, err = s.PrepareHandoffCall(ctx, owner, r.ID, "fixture-session", a.ID, target.Ref); err == nil {
		t.Fatal("handoff consumed twice")
	}
	if err = s.ValidatePreparedCall(ctx, owner, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CallReceipt(ctx, original, r.ID); err != nil {
		t.Fatal("original model conversation cannot query receipt", err)
	}
}
func TestCancelledAndExpiredHandoffsNeverAcquireRoute(t *testing.T) {
	store, s, f, owner := fixture(t)
	s.factory = routeFactory{f}
	ctx := context.Background()
	a, target := accountFixture(t, s, owner, "A")
	original := Caller{Subject: "session:fixture-session"}
	_ = store.Grant(ctx, owner, a.ID, original.Subject, []string{"status", "call"})
	r, err := s.ReserveHandoff(ctx, original, owner.Subject, "fixture-session", "voice", a.ID, target.Ref, "cancel-native-call")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CancelCall(ctx, original, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.PrepareHandoffCall(ctx, owner, r.ID, "fixture-session", a.ID, target.Ref); err == nil {
		t.Fatal("cancelled handoff resumed")
	}
	r, err = s.ReserveHandoff(ctx, original, owner.Subject, "fixture-session", "voice", a.ID, target.Ref, "expire-native-call")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = store.db.Exec("UPDATE line_call_handoffs SET expires_at=0 WHERE call_id=?", r.ID)
	s.expireHandoffs(ctx)
	receipt, err := s.CallReceipt(ctx, original, r.ID)
	if err != nil || receipt.State != "closed" || receipt.Code != "line_handoff_expired" {
		t.Fatal(receipt, err)
	}
}
