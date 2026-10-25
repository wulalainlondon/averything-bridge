package delegation

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSessionSourcesReadOnlyScopeAndEvidence(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	for _, id := range []string{"one", "two"} {
		if err := store.Create(ctx, Record{ID: id, ParentSessionID: "parent", ChildSessionID: id, ChildRequestID: "r_" + id, ParentRequestID: "return_" + id, ChildName: "same", Cwd: "/synthetic", Instruction: "private instruction never projected"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Complete(ctx, "one", "r_one", "completed", "synthetic native final", "", nil); err != nil {
		t.Fatal(err)
	}
	before, _, err := store.ByID(ctx, "one")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.SessionSources(ctx, []string{"one", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ChildSessionID != "one" || !rows[0].FinalObserved || rows[0].DeliveryState != "pending" {
		t.Fatalf("%+v", rows)
	}
	after, _, err := store.ByID(ctx, "one")
	if err != nil || before.UpdatedAt != after.UpdatedAt || after.DeliveryState != before.DeliveryState {
		t.Fatal("projection mutated ledger")
	}
	rows, err = store.SessionSources(ctx, []string{"two"})
	if err != nil || len(rows) != 1 || rows[0].FinalObserved {
		t.Fatalf("running final inferred: %+v %v", rows, err)
	}
	ids := make([]string, 450)
	for i := range ids {
		ids[i] = "missing"
	}
	ids[449] = "one"
	rows, err = store.SessionSources(ctx, ids)
	if err != nil || len(rows) != 1 {
		t.Fatalf("batch lost data: %+v %v", rows, err)
	}
}

func TestSessionSourcesHonorsDeadlineWithoutMutatingLedger(t *testing.T) {
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = store.SessionSources(ctx, []string{"public-child"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("query did not honor caller deadline: %v", err)
	}
}
