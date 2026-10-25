package callbacks

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

func TestDurableOperationAndDeviceClaim(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	id := ID("authority", "epoch", "request")
	calls := 0
	create := func(r *Record, _ bool) error {
		calls++
		*r = Record{Session: "s", Request: "r", Enabled: true, Recipient: Recipient{Device: "phone-a", Binding: "paired-generation-a"}, Report: &Report{Text: "result"}}
		NewInvitation(r, 1000, "enable")
		return nil
	}
	r, e := s.Mutate(ctx, "enable", "fingerprint", id, 0, create)
	if e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, _ = Open(dir)
	defer s.Close()
	same, e := s.Mutate(ctx, "enable", "fingerprint", id, 0, create)
	if e != nil || calls != 1 || same.Revision != 1 {
		t.Fatalf("replay %v %d %+v", e, calls, same)
	}
	if _, e = s.Mutate(ctx, "enable", "changed", id, 1, create); !errors.Is(e, ErrOperation) {
		t.Fatal("operation collision accepted", e)
	}
	if Claim(&r, "phone-b", "paired-generation-a", r.Invite.ID, "accept", 2000) == nil {
		t.Fatal("other phone claimed")
	}
	if Claim(&r, "phone-a", "new-pairing", r.Invite.ID, "accept", 2000) == nil {
		t.Fatal("new install claimed")
	}
	r, e = s.Mutate(ctx, "accept", "accept-fp", id, 1, func(r *Record, _ bool) error {
		return Claim(r, "phone-a", "paired-generation-a", r.Invite.ID, "accept", 2000)
	})
	if e != nil || r.Invite.State != "claimed" {
		t.Fatal(e, r)
	}
	b, _ := json.Marshal(r)
	if string(b) == "" || contains(string(b), "paired-generation-a") {
		t.Fatal("binding leaked on wire")
	}
}
func contains(s, x string) bool {
	for i := 0; i+len(x) <= len(s); i++ {
		if s[i:i+len(x)] == x {
			return true
		}
	}
	return false
}
func TestCancelAndClaimRaceHasOneWinner(t *testing.T) {
	s, _ := Open("")
	defer s.Close()
	ctx := context.Background()
	r, _ := s.Mutate(ctx, "set", "set", "id", 0, func(r *Record, _ bool) error {
		*r = Record{Enabled: true, Recipient: Recipient{Device: "a", Binding: "b"}, Report: &Report{Text: "x"}}
		NewInvitation(r, 1000, "set")
		return nil
	})
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for _, action := range []string{"cancel", "accept"} {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			_, e := s.Mutate(ctx, a, a, "id", 1, func(x *Record, _ bool) error {
				if a == "cancel" {
					Cancel(x)
					return nil
				}
				return Claim(x, "a", "b", r.Invite.ID, "claim", 2000)
			})
			if e == nil {
				mu.Lock()
				wins++
				mu.Unlock()
			} else if !errors.Is(e, ErrConflict) {
				t.Error(e)
			}
		}(action)
	}
	wg.Wait()
	if wins != 1 {
		t.Fatal(wins)
	}
}
func TestLateExpiredCancelledAndClaimNotReplayed(t *testing.T) {
	r := Record{Enabled: true, Recipient: Recipient{Device: "a", Binding: "b"}, Report: &Report{Text: "result"}}
	NewInvitation(&r, 1000, "op")
	if Claim(&r, "a", "b", r.Invite.ID, "expired", 61_000) == nil {
		t.Fatal("expiry ignored")
	}
	Cancel(&r)
	if Claim(&r, "a", "b", r.Invite.ID, "late", 2000) == nil {
		t.Fatal("cancel ignored")
	}
	r.Enabled = true
	NewInvitation(&r, 2000, "new-user-action")
	if e := Claim(&r, "a", "b", r.Invite.ID, "claim", 3000); e != nil {
		t.Fatal(e)
	}
	if Claim(&r, "a", "b", r.Invite.ID, "other", 3001) == nil {
		t.Fatal("claimed twice")
	}
}
