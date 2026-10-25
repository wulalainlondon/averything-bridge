package delegation

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestContinuityStoreRoundLimitsSixSixtyFourOneTwentyEight(t *testing.T) {
	ctx := context.Background()
	s := capacityStore(t)
	base := capacityRecord("parent", "root")
	admit(t, s, base)
	for i := 1; i < 6; i++ {
		admit(t, s, capacityRecord("parent", fmt.Sprint("other-", i)))
	}
	previous := base.ID
	for i := 1; i <= 64; i++ {
		r := capacityRecord("parent", fmt.Sprint("round-", i))
		r.LogicalID = base.ID
		r.PreviousID = previous
		r.ChildSessionID = base.ChildSessionID
		admit(t, s, r)
		previous = r.ID
	}
	n := count(t, s, "parent")
	if n.Active != 6 || n.Queued != 64 || n.Reserved != 70 {
		t.Fatal(n)
	}
	refused := capacityRecord("parent", "round-overflow")
	refused.LogicalID = base.ID
	refused.PreviousID = previous
	refused.ChildSessionID = base.ChildSessionID
	if e := s.CreateBounded(ctx, refused); !errors.Is(e, ErrQueueLimit) {
		t.Fatal("round escaped queued64", e)
	}
	if got := count(t, s, "parent"); got != n {
		t.Fatal("capacity leaked on refusal", got)
	}
	// Each continuation reserves its own future return. Terminal pending returns
	// consume delivery128 regardless of the logical-task/session grouping.
	s = capacityStore(t)
	previous = ""
	var root string
	for i := 0; i < 128; i++ {
		r := capacityRecord("parent", fmt.Sprint("terminal-", i))
		if i == 0 {
			root = r.ID
		} else {
			r.LogicalID = root
			r.PreviousID = previous
			r.ChildSessionID = "child_terminal-0"
		}
		admit(t, s, r)
		finish(t, s, r)
		previous = r.ID
	}
	n = count(t, s, "parent")
	if n.Active != 0 || n.Queued != 0 || n.Delivery != 128 || n.Reserved != 128 {
		t.Fatal(n)
	}
	refused = capacityRecord("parent", "return-overflow")
	refused.LogicalID = root
	refused.PreviousID = previous
	refused.ChildSessionID = "child_terminal-0"
	if e := s.CreateBounded(ctx, refused); !errors.Is(e, ErrDeliveryLimit) {
		t.Fatal("round escaped delivery128", e)
	}
}
func TestContinuityStoreSerializedValidationRollbackAndCollectingFence(t *testing.T) {
	ctx := context.Background()
	s := capacityStore(t)
	base := capacityRecord("parent", "root")
	admit(t, s, base)
	r := capacityRecord("parent", "fix")
	r.LogicalID = base.ID
	r.PreviousID = base.ID
	r.ChildSessionID = base.ChildSessionID
	calls := 0
	if _, e := s.Continue(ctx, r, func() error {
		calls++
		if calls == 2 {
			return errors.New("grant revoked at commit")
		}
		return nil
	}); e == nil {
		t.Fatal("revoked grant committed")
	}
	if _, found, _ := s.ByID(ctx, r.ID); found {
		t.Fatal("revocation left accepted row")
	}
	if got := count(t, s, "parent"); got.Active != 1 || got.Reserved != 1 {
		t.Fatal("rollback leaked capacity", got)
	}
	if e := s.MarkCollecting(ctx, base.ID, "completed"); e != nil {
		t.Fatal(e)
	}
	admit(t, s, r)
	if promoted, e := s.TryPromote(ctx, r.ID); e != nil || promoted {
		t.Fatal("same child overtook unsealed final", promoted, e)
	}
	finish(t, s, base)
	if promoted, e := s.TryPromote(ctx, r.ID); e != nil || !promoted {
		t.Fatal("sealed previous did not free same child", promoted, e)
	}
}
func TestContinuityAuthorityAndNativeThreadNeverRebind(t *testing.T) {
	ctx := context.Background()
	s := capacityStore(t)
	if e := s.BindAuthority(ctx, "original-instance"); e != nil {
		t.Fatal(e)
	}
	if e := s.BindAuthority(ctx, "original-instance"); e != nil {
		t.Fatal(e)
	}
	if e := s.BindAuthority(ctx, "foreign-instance"); e == nil {
		t.Fatal("foreign database authority adopted")
	}
	if e := s.PinNativeThread(ctx, "child", "original-thread", 7); e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		thread string
		rev    uint64
	}{{"foreign-thread", 7}, {"original-thread", 8}} {
		if e := s.PinNativeThread(ctx, "child", tc.thread, tc.rev); e == nil {
			t.Fatal("thread/config rebound")
		}
	}
	thread, rev, found, e := s.NativeThread(ctx, "child")
	if e != nil || !found || thread != "original-thread" || rev != 7 {
		t.Fatal(thread, rev, found, e)
	}
}
