package linebridge

import (
	"context"
	"testing"
)

func TestMessageReferencesRejectAccountAndRoomSubstitution(t *testing.T) {
	store, s, _, owner := fixture(t)
	ctx := context.Background()
	a, ta := accountFixture(t, s, owner, "A")
	b, tb := accountFixture(t, s, owner, "B")
	messages, err := s.Read(ctx, owner, a.ID, ta.Ref, "", 3)
	if err != nil || len(messages) != 1 {
		t.Fatal(err)
	}
	ref := messages[0].ID
	if ref == "m1" {
		t.Fatal("provider ID exposed")
	}
	if _, err = store.ResolveMessage(ctx, b.ID, tb.Ref, ref); err == nil {
		t.Fatal("cross-account reply accepted")
	}
	other, err := store.SaveTarget(ctx, a.ID, "u222", "other room", "user")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.ResolveMessage(ctx, a.ID, other.Ref, ref); err == nil {
		t.Fatal("cross-room reply accepted")
	}
	if _, err = s.Send(ctx, owner, b.ID, tb.Ref, Content{Text: "fixture", ReplyTo: ref}, "reply-test-intent", 0, "", 0); err == nil {
		t.Fatal("invalid reply reached outbox")
	}
	op, err := s.Send(ctx, owner, a.ID, ta.Ref, Content{Text: "fixture", ReplyTo: ref}, "reply-test-intent", 0, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	result := terminal(t, s, owner, op.ID)
	if result.State != "accepted_by_line" || result.RemoteID == "remote-fixture" {
		t.Fatal("scoped acknowledgment missing")
	}
}
