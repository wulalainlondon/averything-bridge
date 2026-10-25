package delegation

import (
	"context"
	"everything-go/internal/taskapi"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func apiAdmissionFixture() (taskapi.AuthorizedCommand, taskapi.IntentRecord, Record) {
	c := taskapi.AuthorizedCommand{Namespace: taskapi.Namespace{Authority: "fixture", StableScopeID: "session:parent", Path: "delegation", Operation: "create_dispatch"}, Request: taskapi.Request{Operation: "create_dispatch", IdempotencyKey: "fixture-same-key"}, IntentHash: "fixture-hash", Revalidate: func(context.Context) error { return nil }}
	i := taskapi.IntentRecord{ReceiptID: "fixture-receipt", TaskID: "fixture-task", SessionID: "child", RequestID: "work", NativeID: "d1", Metadata: []byte(`{}`)}
	r := Record{ID: "d1", ParentSessionID: "parent", ChildSessionID: "child", ChildRequestID: "work", ParentRequestID: "return", ChildName: "Fixture", Cwd: "/fixture", Instruction: "Fixture", Sandbox: "read-only"}
	return c, i, r
}
func TestCreateAPICallerRevocationRollsBackAllCanonicalFacts(t *testing.T) {
	for _, boundary := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(boundary), func(t *testing.T) {
			s, e := Open(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			c, i, r := apiAdmissionFixture()
			checks := 0
			if boundary == 0 {
				c.Revalidate = nil
			} else {
				c.Revalidate = func(context.Context) error {
					checks++
					if checks == boundary {
						return taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
					}
					return nil
				}
			}
			receipt, created, e := s.CreateAPI(context.Background(), c, i, r, "codex")
			if e == nil || created || receipt.ReceiptID != "" {
				t.Fatal("revoked admission acknowledged", receipt, created, e)
			}
			for _, table := range []string{"task_api_intents", "task_api_outbox", "task_api_changes", "task_api_revisions", "session_delegations", "task_api_child_profiles"} {
				var n int
				if e = s.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); e != nil || n != 0 {
					t.Fatal("rollback lost canonical atomicity", table, n, e)
				}
			}
		})
	}
}
func TestCreateAPISameKeyConcurrentOneEffectAndReopen(t *testing.T) {
	dir := t.TempDir()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	c, i, r := apiAdmissionFixture()
	var effects atomic.Int32
	var wg sync.WaitGroup
	for k := 0; k < 8; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, created, e := s.CreateAPI(context.Background(), c, i, r, "codex")
			if e != nil || got.ReceiptID != i.ReceiptID {
				t.Error("same original receipt lost", e)
			}
			if created {
				effects.Add(1)
			}
		}()
	}
	wg.Wait()
	if effects.Load() != 1 {
		t.Fatal("duplicate effect", effects.Load())
	}
	s.Close()
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	got, created, e := s.CreateAPI(context.Background(), c, i, r, "codex")
	if e != nil || created || got.ReceiptID != i.ReceiptID {
		t.Fatal("reopen replayed admission", e)
	}
	c.IntentHash = "different-intent"
	_, created, e = s.CreateAPI(context.Background(), c, i, r, "codex")
	if e == nil || created {
		t.Fatal("same-key different intent not conflict")
	}
}
