package callbacks

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func groupFixture(id string) Record {
	return Record{ID: id, Authority: "host", Session: "session", Request: id, Epoch: "epoch", Thread: "thread", Config: 3, Recipient: Recipient{Device: "phone", Binding: "binding"}, Enabled: true, Report: &Report{ExecutionRequest: "root", Hash: "hash", Turn: "turn"}}
}
func TestAutomaticDeliveryGroupIsAtomicAndPreservesRequests(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprint(i)
			_, e := s.Mutate(ctx, "seal"+id, id, id, 0, func(r *Record, _ bool) error { *r = groupFixture(id); NewInvitation(r, 1, id); return nil })
			if e != nil {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	rows, e := s.List(ctx)
	if e != nil {
		t.Fatal(e)
	}
	count := 0
	for _, r := range rows {
		if r.Invite.State == "delivery_pending" {
			count++
		}
		if r.Request != r.ID || !r.Enabled {
			t.Fatal("demand overwritten", r)
		}
	}
	if count != 1 {
		t.Fatal("duplicate automatic calls", count)
	}
}
func TestGroupCancellationBeforePresentationCanReleaseButAfterCannot(t *testing.T) {
	s, _ := Open(t.TempDir())
	defer s.Close()
	ctx := context.Background()
	seed := func(id string) Record {
		r, e := s.Mutate(ctx, "seed"+id, id, id, 0, func(r *Record, _ bool) error { *r = groupFixture(id); NewInvitation(r, 1, id); return nil })
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	a := seed("a")
	b := seed("b")
	if b.Invite.State != "group_waiting" {
		t.Fatal(b)
	}
	_, e := s.Mutate(ctx, "cancel-a", "cancel", "a", a.Revision, func(r *Record, _ bool) error { Cancel(r); return nil })
	if e != nil {
		t.Fatal(e)
	}
	b, e = s.Mutate(ctx, "promote-b", "promote", "b", b.Revision, func(r *Record, _ bool) error { r.Invite.State = "delivery_pending"; return nil })
	if e != nil || b.Invite.State != "delivery_pending" {
		t.Fatal(b, e)
	}
	b, e = s.Mutate(ctx, "present-b", "present", "b", b.Revision, func(r *Record, _ bool) error { r.Invite.PresentedAt = 10; Cancel(r); return nil })
	if e != nil {
		t.Fatal(e)
	}
	c := seed("c")
	if c.Invite.State != "group_waiting" {
		t.Fatal("presented report repeated after cancel", c)
	}
}
func TestDeliveryGroupsDoNotCrossRecipientsTurnsOrManualIntent(t *testing.T) {
	a := groupFixture("a")
	b := groupFixture("b")
	if !SameDelivery(a, b) {
		t.Fatal("same root not grouped")
	}
	b.Recipient.Binding = "rotated"
	if SameDelivery(a, b) {
		t.Fatal("recipient boundary")
	}
	b = groupFixture("b")
	b.Report.Turn = "next"
	if SameDelivery(a, b) {
		t.Fatal("turn boundary")
	}
	s, _ := Open(t.TempDir())
	defer s.Close()
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		r, e := s.Mutate(ctx, id, id, id, 0, func(r *Record, _ bool) error {
			*r = groupFixture(id)
			NewInvitation(r, 1, id)
			r.Invite.Manual = id == "b"
			return nil
		})
		if e != nil || r.Invite.State != "delivery_pending" {
			t.Fatal("explicit manual repeat lost", r, e)
		}
	}
}

func TestDeliveryAttemptSurvivesRestartAndNewAutomaticGeneration(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	ctx := context.Background()
	r, e := s.Mutate(ctx, "first", "first", "a", 0, func(r *Record, _ bool) error { *r = groupFixture("a"); NewInvitation(r, 1, "first"); return nil })
	if e != nil {
		t.Fatal(e)
	}
	r, e = s.Mutate(ctx, "shown", "shown", "a", r.Revision, func(r *Record, _ bool) error { r.Invite.PresentedAt = 10; Cancel(r); return nil })
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, _ = Open(dir)
	defer s.Close()
	r, e = s.Mutate(ctx, "newauto", "newauto", "a", r.Revision, func(r *Record, _ bool) error { r.Enabled = true; NewInvitation(r, 100, "newauto"); return nil })
	if e != nil || r.Invite.State != "group_waiting" {
		t.Fatal("history lost", r, e)
	}
	r, e = s.Mutate(ctx, "manual", "manual", "a", r.Revision, func(r *Record, _ bool) error { NewInvitation(r, 200, "manual"); r.Invite.Manual = true; return nil })
	if e != nil || r.Invite.State != "delivery_pending" {
		t.Fatal("explicit repeat lost", r, e)
	}
}

func TestGroupedInvitationCannotBeClaimed(t *testing.T) {
	r := groupFixture("a")
	NewInvitation(&r, 100, "a")
	r.Invite.State = "group_waiting"
	if Claim(&r, "phone", "binding", r.Invite.ID, "claim", 101) == nil {
		t.Fatal("grouped invitation claimed")
	}
}
