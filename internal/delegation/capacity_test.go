package delegation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"everything-go/internal/taskapi"
)

func capacityRecord(parent, id string) Record {
	return Record{ID: id, ParentSessionID: parent, OriginRequestID: "origin", ToolCallID: id, IntentHash: id, ChildSessionID: "child_" + id, ChildRequestID: "work_" + id, ParentRequestID: "return_" + id, ChildName: "Synthetic", Cwd: "/fixture", Instruction: "Synthetic", SourceThread: "thread", SourceRevision: 7}
}
func capacityStore(t *testing.T) *Store {
	t.Helper()
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func admit(t *testing.T, s *Store, r Record) {
	t.Helper()
	if e := s.CreateBounded(context.Background(), r); e != nil {
		t.Fatal(e)
	}
}
func finish(t *testing.T, s *Store, r Record) {
	t.Helper()
	if changed, e := s.Complete(context.Background(), r.ChildSessionID, r.ChildRequestID, "completed", "Synthetic final", "", nil); e != nil || !changed {
		t.Fatal(changed, e)
	}
}
func count(t *testing.T, s *Store, parent string) Counts {
	t.Helper()
	n, e := s.CountsForParent(context.Background(), parent)
	if e != nil {
		t.Fatal(e)
	}
	return n
}
func configure(t *testing.T, s *Store, l Limits) {
	t.Helper()
	if e := s.ConfigureCapacity(context.Background(), CapacityConfig{Default: l}); e != nil {
		t.Fatal(e)
	}
}

func TestCapacityCrossConnectionAtomicAdmissionAndParents(t *testing.T) {
	dir := t.TempDir()
	a, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	var wg sync.WaitGroup
	out := make(chan error, 100)
	for k := 0; k < 100; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			s := a
			if k%2 == 1 {
				s = b
			}
			out <- s.CreateBounded(context.Background(), capacityRecord("p", fmt.Sprint(k)))
		}(k)
	}
	wg.Wait()
	close(out)
	accepted, refused := 0, 0
	for e := range out {
		if e == nil {
			accepted++
		} else if errors.Is(e, ErrQueueLimit) {
			refused++
		} else {
			t.Fatal(e)
		}
	}
	if accepted != 70 || refused != 30 {
		t.Fatalf("accepted=%d refused=%d", accepted, refused)
	}
	if n := count(t, a, "p"); n.Active != 6 || n.Queued != 64 || n.Reserved != 70 {
		t.Fatal(n)
	}
	admit(t, b, capacityRecord("other", "other"))
	if n := count(t, a, "other"); n.Active != 1 {
		t.Fatal(n)
	}
}
func TestCapacityFIFOAgainstFreshArrivalsAndConcurrentPromotion(t *testing.T) {
	s := capacityStore(t)
	configure(t, s, Limits{1, 10, 20})
	ctx := context.Background()
	first := capacityRecord("p", "first")
	old := capacityRecord("p", "z-old")
	newer := capacityRecord("p", "a-new")
	admit(t, s, first)
	admit(t, s, old)
	admit(t, s, newer)
	finish(t, s, first)
	admit(t, s, capacityRecord("p", "fresh"))
	if ok, e := s.TryPromote(ctx, newer.ID); e != nil || ok {
		t.Fatal("younger bypassed oldest", ok, e)
	}
	var wg sync.WaitGroup
	out := make(chan bool, 20)
	for k := 0; k < 20; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, e := s.TryPromote(ctx, old.ID)
			if e != nil {
				t.Error(e)
			}
			out <- ok
		}()
	}
	wg.Wait()
	close(out)
	n := 0
	for ok := range out {
		if ok {
			n++
		}
	}
	if n != 1 {
		t.Fatal("promotion effects", n)
	}
	if n := count(t, s, "p"); n.Active != 1 || n.Queued != 2 || n.Delivery != 1 {
		t.Fatal(n)
	}
}
func TestCapacityDeliveryReservationSurvivesEveryTerminal(t *testing.T) {
	s := capacityStore(t)
	configure(t, s, Limits{2, 8, 4})
	ctx := context.Background()
	records := []Record{}
	for k := 0; k < 4; k++ {
		r := capacityRecord("p", fmt.Sprint(k))
		admit(t, s, r)
		records = append(records, r)
	}
	if e := s.CreateBounded(ctx, capacityRecord("p", "full")); !errors.Is(e, ErrDeliveryLimit) {
		t.Fatal(e)
	}
	finish(t, s, records[0])
	finish(t, s, records[1])
	if e := s.MarkDeliveryQueued(ctx, records[0].ID); e != nil {
		t.Fatal(e)
	}
	for _, r := range records[2:] {
		if ok, e := s.TryPromote(ctx, r.ID); e != nil || !ok {
			t.Fatal(ok, e)
		}
		finish(t, s, r)
	}
	if n := count(t, s, "p"); n.Active != 0 || n.Delivery != 4 || n.Reserved != 4 {
		t.Fatal(n)
	}
	// A terminal result does not consume execution, but unacknowledged delivery
	// still refuses new promises at the independent result bound.
	if e := s.CreateBounded(ctx, capacityRecord("p", "blocked")); !errors.Is(e, ErrDeliveryLimit) {
		t.Fatal(e)
	}
	if ok, e := s.MarkDelivered(ctx, "p", records[0].ParentRequestID); e != nil || !ok {
		t.Fatal(ok, e)
	}
	admit(t, s, capacityRecord("p", "next"))
	if n := count(t, s, "p"); n.Active != 1 || n.Delivery != 3 {
		t.Fatal(n)
	}
}
func TestCapacityNativeTerminalReleasesSlotBeforeSeal(t *testing.T) {
	s := capacityStore(t)
	configure(t, s, Limits{1, 2, 4})
	ctx := context.Background()
	a, b := capacityRecord("p", "a"), capacityRecord("p", "b")
	admit(t, s, a)
	admit(t, s, b)
	if e := s.MarkCollecting(ctx, a.ID, "completed"); e != nil {
		t.Fatal(e)
	}
	if n := count(t, s, "p"); n.Active != 0 || n.Delivery != 1 || n.Reserved != 2 {
		t.Fatal(n)
	}
	if ok, e := s.TryPromote(ctx, b.ID); e != nil || !ok {
		t.Fatal(ok, e)
	}
	finish(t, s, a)
	if ok, e := s.Complete(ctx, a.ChildSessionID, a.ChildRequestID, "failed", "overwrite", "", nil); e != nil || ok {
		t.Fatal(ok, e)
	}
	r, _, _ := s.ByID(ctx, a.ID)
	if r.Result != "Synthetic final" {
		t.Fatal(r)
	}
}
func TestCapacityRestartPreservesQueueReservationsBindingAndUnknownRunning(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	configure(t, s, Limits{1, 2, 4})
	ctx := context.Background()
	a, b := capacityRecord("p", "a"), capacityRecord("p", "b")
	admit(t, s, a)
	admit(t, s, b)
	s.MarkRunning(ctx, a.ID)
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if n := count(t, s, "p"); n.Active != 1 || n.Queued != 1 {
		t.Fatal(n)
	}
	if ok, e := s.TryPromote(ctx, b.ID); e != nil || ok {
		t.Fatal("restart freed ambiguous active work", ok, e)
	}
	thread, revision, bound, e := s.SourceBinding(ctx, b.ID)
	if e != nil || !bound || thread != "thread" || revision != 7 {
		t.Fatal(thread, revision, bound, e)
	}
	if ok, e := s.NativeCommitment(ctx, b.ID); e != nil || !ok {
		t.Fatal(ok, e)
	}
	finish(t, s, a)
	if ok, e := s.TryPromote(ctx, b.ID); e != nil || !ok {
		t.Fatal(ok, e)
	}
}
func TestCapacityFairCursorPast128AndContinuousArrival(t *testing.T) {
	s := capacityStore(t)
	ctx := context.Background()
	for i := 0; i < 260; i++ {
		admit(t, s, capacityRecord(fmt.Sprint(i), fmt.Sprint(i)))
	}
	seen := map[string]bool{}
	for cycle := 0; cycle < 3; cycle++ {
		rows, e := s.PendingBatch(ctx, 128)
		if e != nil {
			t.Fatal(e)
		}
		for _, r := range rows {
			seen[r.ID] = true
		}
		// Arrivals during a cycle cannot push the wrap point indefinitely forward.
		admit(t, s, capacityRecord("arrive", fmt.Sprintf("arrive%d", cycle)))
	}
	for i := 0; i < 260; i++ {
		if !seen[fmt.Sprint(i)] {
			t.Fatal("starved", i)
		}
	}
	rows, e := s.PendingBatch(ctx, 128)
	if e != nil || len(rows) != 128 || rows[0].ID != "0" {
		t.Fatal("cycle did not wrap under arrivals", len(rows), e)
	}
}
func TestCapacityConfigurationShrinkAndParentOverride(t *testing.T) {
	s := capacityStore(t)
	ctx := context.Background()
	if e := s.ConfigureCapacity(ctx, CapacityConfig{Default: DefaultLimits, Parents: map[string]Limits{"p": {2, 2, 5}}}); e != nil {
		t.Fatal(e)
	}
	a, b, c := capacityRecord("p", "a"), capacityRecord("p", "b"), capacityRecord("p", "c")
	admit(t, s, a)
	admit(t, s, b)
	admit(t, s, c)
	configure(t, s, Limits{1, 2, 2})
	if ok, e := s.TryPromote(ctx, c.ID); e != nil || ok {
		t.Fatal(ok, e)
	}
	if e := s.CreateBounded(ctx, capacityRecord("p", "new")); !errors.Is(e, ErrDeliveryLimit) {
		t.Fatal(e)
	}
	// Shrinking never rewrites/interrupts admitted work.
	if n := count(t, s, "p"); n.Active != 2 || n.Queued != 1 {
		t.Fatal(n)
	}
	for _, raw := range []string{strings.Repeat(" ", 65537), `{"default":{"active":6,"queued":2,"delivery":8}}` + strings.Repeat(" ", 65537) + `{}`, `{"default":{"active":0,"queued":2,"delivery":4}}`, `{"default":{"active":6,"queued":2,"delivery":4}}`, `{"default":{"active":6,"queued":2,"delivery":8},"typo":1}`, `{"default":{"active":6,"queued":2,"delivery":8}} {}`} {
		if _, e := ReadCapacityConfig(strings.NewReader(raw)); e == nil {
			t.Fatal("invalid config accepted", raw)
		}
	}
}
func TestCapacityAPICancellationAtomicWithPromotion(t *testing.T) {
	for iteration := 0; iteration < 16; iteration++ {
		t.Run(fmt.Sprint(iteration), func(t *testing.T) {
			s := capacityStore(t)
			ctx := context.Background()
			configure(t, s, Limits{1, 2, 4})
			first := capacityRecord("parent", "first")
			admit(t, s, first)
			c, i, r := apiAdmissionFixture()
			stored, _, e := s.CreateAPI(ctx, c, i, r, "codex")
			if e != nil {
				t.Fatal(e)
			}
			revision, _ := s.Revision(ctx, r.ChildSessionID, r.ChildRequestID)
			cancel := c
			cancel.Namespace.Operation = "cancel"
			cancel.Namespace.TaskID = stored.TaskID
			cancel.Request.Operation = "cancel"
			cancel.Request.IdempotencyKey = "cancel-key"
			cancel.IntentHash = "cancel-hash"
			intent := stored
			intent.ReceiptID = "cancel-receipt"
			finish(t, s, first)
			var wg sync.WaitGroup
			wg.Add(2)
			var promoted bool
			var promoteErr, cancelErr error
			var cancelled taskapi.IntentRecord
			go func() { defer wg.Done(); promoted, promoteErr = s.TryPromote(ctx, r.ID) }()
			go func() {
				defer wg.Done()
				cancelled, cancelErr = s.CancelQueuedAPI(ctx, cancel, intent, stored, revision)
			}()
			wg.Wait()
			if promoteErr != nil {
				t.Fatal(promoteErr)
			}
			if promoted == (cancelErr == nil) {
				t.Fatalf("promotion=%v cancelErr=%v", promoted, cancelErr)
			}
			got, _, e := s.ByID(ctx, r.ID)
			if e != nil {
				t.Fatal(e)
			}
			if cancelErr == nil {
				if got.State != "terminal" || got.Error != "cancelled_before_start" {
					t.Fatal(got)
				}
				again, e := s.CancelQueuedAPI(ctx, cancel, intent, stored, revision)
				if e != nil || again.ReceiptID != cancelled.ReceiptID {
					t.Fatal("cancel replay lost", e)
				}
				if failure, done, e := s.TaskJournal().Outcome(ctx, cancelled.Key); e != nil || !done || failure != nil {
					t.Fatal(failure, done, e)
				}
			} else if got.State != "provisioning" {
				t.Fatal(got)
			}
		})
	}
}
func TestCapacityAPIBackpressureRollsBackReceiptAndAuthority(t *testing.T) {
	s := capacityStore(t)
	configure(t, s, Limits{1, 0, 1})
	admit(t, s, capacityRecord("parent", "full"))
	c, i, r := apiAdmissionFixture()
	if _, created, e := s.CreateAPI(context.Background(), c, i, r, "codex"); e == nil || created {
		t.Fatal(created, e)
	}
	for _, table := range []string{"task_api_intents", "task_api_outbox", "task_api_child_profiles"} {
		var n int
		s.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n)
		if n != 0 {
			t.Fatal(table, n)
		}
	}
	if _, found, _ := s.ByID(context.Background(), r.ID); found {
		t.Fatal("refused work persisted")
	}
}

func TestCapacityContinuationSameChildWaitsWithoutBlockingEligibleSibling(t *testing.T) {
	s := capacityStore(t)
	ctx := context.Background()
	configure(t, s, Limits{2, 8, 12})
	c, i, first := apiAdmissionFixture()
	if _, _, e := s.CreateAPI(ctx, c, i, first, "codex"); e != nil {
		t.Fatal(e)
	}
	follow := capacityRecord("parent", "follow")
	follow.ChildSessionID = first.ChildSessionID
	follow.DeliveryState = "not_requested"
	follow.ExpectedThread = "native-original"
	follow.ExpectedRevision = 1
	appendCommand := c
	appendCommand.Namespace.Operation = "append"
	appendCommand.Namespace.TaskID = i.TaskID
	appendCommand.Request.Operation = "append"
	appendCommand.Request.IdempotencyKey = "follow-key"
	appendCommand.IntentHash = "follow-hash"
	appendIntent := i
	appendIntent.RequestID = follow.ChildRequestID
	appendIntent.ReceiptID = "follow-receipt"
	stored, created, e := s.CreateAPIContinuation(ctx, appendCommand, appendIntent, follow)
	if e != nil || !created {
		t.Fatal(created, e)
	}
	row, _, _ := s.ByID(ctx, follow.ID)
	if row.State != "queued" || row.DeliveryState != "not_requested" {
		t.Fatal(row)
	}
	sibling := capacityRecord("parent", "sibling")
	admit(t, s, sibling)
	if ok, e := s.TryPromote(ctx, sibling.ID); e != nil || !ok {
		t.Fatal("same-child head blocked eligible sibling", ok, e)
	}
	if ok, e := s.TryPromote(ctx, follow.ID); e != nil || ok {
		t.Fatal("same child double execution", ok, e)
	}
	finish(t, s, first)
	if ok, e := s.TryPromote(ctx, follow.ID); e != nil || !ok {
		t.Fatal(ok, e)
	}
	resolved, e := s.TaskJournal().OriginalCommandForRequest(ctx, follow.ChildSessionID, follow.ChildRequestID)
	if e != nil || resolved.ReceiptID != stored.ReceiptID {
		t.Fatal("continuation resolved first task turn", resolved, e)
	}
	finish(t, s, follow)
	if n := count(t, s, "parent"); n.Active != 1 || n.Delivery != 1 || n.Reserved != 2 {
		t.Fatal("continuation requested duplicate parent return", n)
	}
}

func TestCapacityCancelledAPIReopenDoesNotResurrect(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	configure(t, s, Limits{1, 2, 4})
	admit(t, s, capacityRecord("parent", "busy"))
	c, i, r := apiAdmissionFixture()
	original, _, e := s.CreateAPI(ctx, c, i, r, "codex")
	if e != nil {
		t.Fatal(e)
	}
	revision, _ := s.Revision(ctx, r.ChildSessionID, r.ChildRequestID)
	cancel := c
	cancel.Namespace.Operation = "cancel"
	cancel.Namespace.TaskID = original.TaskID
	cancel.Request.Operation = "cancel"
	cancel.Request.IdempotencyKey = "cancel-reopen"
	cancel.IntentHash = "cancel-reopen-hash"
	intent := original
	intent.ReceiptID = "cancel-reopen-receipt"
	cancelled, e := s.CancelQueuedAPI(ctx, cancel, intent, original, revision)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	replay, created, e := s.CreateAPI(ctx, c, i, r, "codex")
	if e != nil || created || replay.ReceiptID != original.ReceiptID {
		t.Fatal(replay, created, e)
	}
	again, e := s.CancelQueuedAPI(ctx, cancel, intent, original, revision)
	if e != nil || again.ReceiptID != cancelled.ReceiptID {
		t.Fatal(again, e)
	}
	if ok, e := s.TryPromote(ctx, r.ID); e != nil || ok {
		t.Fatal("cancelled task promoted after reopen", ok, e)
	}
	if n := count(t, s, "parent"); n.Active != 1 || n.Queued != 0 || n.Delivery != 1 {
		t.Fatal(n)
	}
	if failure, done, e := s.TaskJournal().Outcome(ctx, again.Key); e != nil || !done || failure != nil {
		t.Fatal(failure, done, e)
	}
}
