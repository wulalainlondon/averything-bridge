package delegation

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestDelegationStoreSeparatesResultAndDelivery(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	ctx := context.Background()
	r := Record{ID: "d1", ParentSessionID: "parent", ChildSessionID: "child", ChildRequestID: "work", ParentRequestID: "return",
		ChildName: "Check", Cwd: "/workspace", Instruction: "Verify", Sandbox: "read-only"}
	if err := store.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRunning(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	changed, err := store.Complete(ctx, "child", "work", "completed", "Final answer", "", []string{"/workspace/report.md"})
	if err != nil || !changed {
		t.Fatalf("first completion: changed=%v err=%v", changed, err)
	}
	changed, err = store.Complete(ctx, "child", "work", "completed", "duplicate", "", nil)
	if err != nil || changed {
		t.Fatalf("duplicate completion: changed=%v err=%v", changed, err)
	}
	record, found, err := store.ByChildRequest(ctx, "child", "work")
	if err != nil || !found || record.Result != "Final answer" || record.DeliveryState != "pending" || len(record.Artifacts) != 1 {
		t.Fatalf("pending result: %+v found=%v err=%v", record, found, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	record, found, err = store.ByChildRequest(ctx, "child", "work")
	if err != nil || !found || record.Result != "Final answer" || record.DeliveryState != "pending" || len(record.Artifacts) != 1 {
		t.Fatalf("result lost after restart: %+v found=%v err=%v", record, found, err)
	}
	if err := store.MarkDeliveryQueued(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	changed, err = store.MarkDelivered(ctx, "parent", "return")
	if err != nil || !changed {
		t.Fatalf("delivery: changed=%v err=%v", changed, err)
	}
	changed, err = store.MarkDelivered(ctx, "parent", "return")
	if err != nil || changed {
		t.Fatalf("duplicate delivery: changed=%v err=%v", changed, err)
	}
	if pending, err := store.Pending(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
}

func TestCreateBoundedSerializesConcurrentParentBudget(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var wg sync.WaitGroup
	outcomes := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("d%d", i)
			outcomes <- store.CreateBounded(context.Background(), Record{ID: id, ParentSessionID: "parent", ParentRequestID: "return_" + id,
				OriginRequestID: "parent-turn", ToolCallID: id, IntentHash: id,
				ChildSessionID: "child_" + id, ChildRequestID: "work_" + id, ChildName: "Check", Cwd: "/workspace", Instruction: "Verify"}, 3)
		}(i)
	}
	wg.Wait()
	close(outcomes)
	succeeded, limited := 0, 0
	for err := range outcomes {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrParentLimit):
			limited++
		default:
			t.Fatal(err)
		}
	}
	if succeeded != 3 || limited != 1 {
		t.Fatalf("succeeded=%d limited=%d", succeeded, limited)
	}
}

func TestFailedParentDeliveryIsRetainedWithoutAutomaticResubmit(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	r := Record{ID: "d1", ParentSessionID: "parent", ChildSessionID: "child", ChildRequestID: "work", ParentRequestID: "return",
		ChildName: "Check", Cwd: "/workspace", Instruction: "Verify"}
	if err := store.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(ctx, "child", "work", "completed", "Result", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDeliveryQueued(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	changed, err := store.MarkDeliveryFailed(ctx, "parent", "return", "uncertain")
	if err != nil || !changed {
		t.Fatalf("failed delivery: changed=%v err=%v", changed, err)
	}
	got, found, err := store.ByParentRequest(ctx, "parent", "return")
	if err != nil || !found || got.Result != "Result" || got.DeliveryError != "uncertain" || got.DeliveryState != "failed" {
		t.Fatalf("retained: %+v found=%v err=%v", got, found, err)
	}
	if pending, err := store.Pending(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("failed delivery should await human reconciliation: %+v err=%v", pending, err)
	}
}
