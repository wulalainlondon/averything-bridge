package delegation

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestReauthorizePairAtomicRollbackCASAndOriginalBinding(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ctx := context.Background()
	for _, id := range []string{"owner", "qa"} {
		r := Record{ID: id, ParentSessionID: "parent", OriginRequestID: "req-" + id, ToolCallID: "call-" + id, ChildSessionID: "child-" + id, ChildRequestID: "exec-" + id, ParentRequestID: "return-" + id, SourceThread: "parent-thread", SourceRevision: 1}
		if e = s.CreateBounded(ctx, r); e != nil {
			t.Fatal(e)
		}
		if _, e = s.db.Exec(`UPDATE session_delegations SET state='terminal',delivery_state='delivered' WHERE id=?`, id); e != nil {
			t.Fatal(e)
		}
	}
	pair := [2]SourceEpoch{{LogicalID: "owner", Peer: "qa", Parent: "parent", Authority: "authority", Thread: "parent-thread", FromRevision: 1, ToRevision: 2, Epoch: 1}, {LogicalID: "qa", Peer: "owner", Parent: "parent", Authority: "authority", Thread: "parent-thread", FromRevision: 1, ToRevision: 2, Epoch: 1}}
	n := 0
	e = s.ReauthorizePair(ctx, pair, func() error {
		n++
		if n == 2 {
			return errors.New("drift during commit")
		}
		return nil
	})
	if e == nil {
		t.Fatal("commit accepted after drift")
	}
	for _, id := range []string{"owner", "qa"} {
		if _, found, e := s.SourceEpoch(ctx, id); e != nil || found {
			t.Fatal("partial epoch after rollback", id, e)
		}
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- s.ReauthorizePair(ctx, pair, func() error { return nil }) }()
	}
	wg.Wait()
	close(results)
	successes := 0
	for e := range results {
		if e == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatal("CAS must accept exactly once", successes)
	}
	for _, id := range []string{"owner", "qa"} {
		_, rev, found, e := s.SourceBinding(ctx, id)
		if e != nil || !found || rev != 1 {
			t.Fatal("original binding rewritten", id, rev, e)
		}
		a, found, e := s.SourceEpoch(ctx, id)
		if e != nil || !found || a.ToRevision != 2 || a.Epoch != 1 {
			t.Fatal("missing atomic epoch", id, a, e)
		}
	}
}
