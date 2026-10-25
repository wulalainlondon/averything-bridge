package messagequeue

import (
	"context"
	"database/sql"
	"errors"
	"everything-go/internal/taskapi"
	"path/filepath"
	"testing"
)

func TestCombinedNativeConflictMigrationBothHistoricalShapes(t *testing.T) {
	for _, shape := range []string{"owner", "queue2", "api4"} {
		t.Run(shape, func(t *testing.T) {
			dir := t.TempDir()
			if shape != "owner" {
				db, err := sql.Open("sqlite", filepath.Join(dir, "message_queue.sqlite"))
				if err != nil {
					t.Fatal(err)
				}
				extra := ""
				values := "'s','legacy'"
				if shape == "api4" {
					extra = ",thread_id TEXT NOT NULL,turn_id TEXT NOT NULL"
					values += ",'original-conflicting-thread','original-conflicting-turn'"
				}
				if _, err = db.Exec("CREATE TABLE task_native_conflicts(session_id TEXT NOT NULL,request_id TEXT NOT NULL" + extra + ",PRIMARY KEY(session_id,request_id)); INSERT INTO task_native_conflicts VALUES(" + values + ")"); err != nil {
					t.Fatal(err)
				}
				db.Close()
			}
			store, err := Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			if shape != "owner" {
				if _, found, err := store.ExactNativeAcceptance("s", "legacy"); found || !errors.Is(err, ErrNativeConflict) {
					t.Fatal("old conflict became absence", found, err)
				}
			}
			if err = store.RecordNativeAcceptance("s", "r", "original", "first"); err != nil {
				t.Fatal(err)
			}
			if err = store.RecordNativeAcceptance("s", "r", "wrong", "conflict1"); !errors.Is(err, ErrNativeConflict) {
				t.Fatal(err)
			}
			if err = store.RecordNativeAcceptance("s", "r", "wrong2", "conflict2"); !errors.Is(err, ErrNativeConflict) {
				t.Fatal(err)
			}
			var facts int
			store.db.QueryRow("SELECT COUNT(*) FROM task_native_conflict_evidence WHERE session_id='s' AND request_id='r'").Scan(&facts)
			if facts != 2 {
				t.Fatal("conflict facts lost", facts)
			}
			changes, _, _ := store.TaskJournal().Changes(context.Background(), 0, 100)
			before := len(changes)
			store.RecordNativeAcceptance("s", "r", "wrong2", "conflict2")
			changes, _, _ = store.TaskJournal().Changes(context.Background(), 0, 100)
			if len(changes) != before {
				t.Fatal("duplicate conflict event")
			}
			store.Close()
			store, err = Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if _, found, err := store.ExactNativeAcceptance("s", "r"); found || !errors.Is(err, ErrNativeConflict) {
				t.Fatal("conflict lost on reopen")
			}
			var thread, turn string
			store.db.QueryRow("SELECT thread_id,turn_id FROM task_native_acceptance WHERE session_id='s' AND request_id='r'").Scan(&thread, &turn)
			if thread != "original" || turn != "first" {
				t.Fatal("first native tuple changed")
			}
			if shape == "api4" {
				store.db.QueryRow("SELECT thread_id,turn_id FROM task_native_conflicts WHERE session_id='s' AND request_id='legacy'").Scan(&thread, &turn)
				if thread != "original-conflicting-thread" || turn != "original-conflicting-turn" {
					t.Fatal("API4 facts changed")
				}
			}
		})
	}
}
func TestCommonCancelSavesOriginTaskRevisionChangeAndOutcomeAtomically(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e, _, err := s.Enqueue(Entry{SessionID: "s", RequestID: "r", Payload: []byte(`{"content":"waiting"}`)})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := s.Snapshot("s")
	revision := snapshot.Revision
	before, _, _ := s.TaskJournal().Changes(context.Background(), 0, 100)
	from := before[len(before)-1].Sequence
	if _, changed, err := s.CancelWaitingAPI("s", "r", e.PayloadHash, &revision, "isolated-outcome"); !changed || err != nil {
		t.Fatal(changed, err)
	}
	origin, err := s.CancelOrigin("s", "r")
	if err != nil || origin != Queued {
		t.Fatal(origin, err)
	}
	changes, _, err := s.TaskJournal().Changes(context.Background(), from, 100)
	if err != nil || len(changes) != 2 {
		t.Fatal(changes, err)
	} // cancelled + queue snapshot bump
	hasCancel := false
	for _, change := range changes {
		if change.RequestID == "r" && change.Kind == "cancelled" {
			hasCancel = true
		}
	}
	if !hasCancel {
		t.Fatal("cancel event omitted")
	}
	if failure, done, err := s.TaskJournal().Outcome(context.Background(), "isolated-outcome"); failure != nil || !done || err != nil {
		t.Fatal(failure, done, err)
	}
	var taskRevision uint64
	s.db.QueryRow("SELECT revision FROM task_api_revisions WHERE session_id='s' AND request_id='r'").Scan(&taskRevision)
	if taskRevision <= from {
		t.Fatal("task revision did not advance")
	}
}
func TestProviderConflictWithoutNativeTupleBlocksCommonCancel(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e, _, _ := s.Enqueue(Entry{SessionID: "s", RequestID: "r", Payload: []byte(`{}`)})
	// Isolated negative fixture on canonical provider conflict port data.
	s.TaskJournal().Transaction(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec("INSERT INTO task_provider_conflicts VALUES('s','r')")
		if err != nil {
			return err
		}
		return taskapi.ChangeTx(tx, "s", "r", "provider_conflict")
	})
	if _, found, err := s.ExactNativeAcceptance("s", "r"); found || !errors.Is(err, ErrNativeConflict) {
		t.Fatal("provider conflict became absence")
	}
	if _, changed, err := s.CancelWaiting("s", "r", e.PayloadHash, nil); changed || err != nil {
		t.Fatal("conflict cancelled", changed, err)
	}
}

func TestCombinedCancelBarrierLateConflictRevisionAndTransactionalRollback(t *testing.T) {
	for _, condition := range []string{"native-before-commit", "conflict-before-commit", "stale-queue-revision", "abort-origin-commit"} {
		t.Run(condition, func(t *testing.T) {
			s, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			entry, _, err := s.Enqueue(Entry{SessionID: "s", RequestID: "r", Payload: []byte(`{"content":"fixture"}`)})
			if err != nil {
				t.Fatal(err)
			}
			snap, _ := s.Snapshot("s")
			revision := snap.Revision
			switch condition {
			case "native-before-commit":
				if err := s.RecordNativeAcceptance("s", "r", "thread", "turn"); err != nil {
					t.Fatal(err)
				}
			case "conflict-before-commit":
				s.RecordNativeAcceptance("s", "r", "thread", "turn")
				s.RecordNativeAcceptance("s", "r", "other", "conflict")
			case "stale-queue-revision":
				s.Enqueue(Entry{SessionID: "s", RequestID: "new-arrival", Payload: []byte(`{}`)})
			case "abort-origin-commit":
				if _, err := s.db.Exec(`CREATE TRIGGER fixture_abort_cancel BEFORE INSERT ON task_cancel_origins BEGIN SELECT RAISE(ABORT,'fixture rollback'); END`); err != nil {
					t.Fatal(err)
				}
			}
			before, _ := s.Snapshot("s")
			changes, _, _ := s.TaskJournal().Changes(context.Background(), 0, 100)
			floor := changes[len(changes)-1].Sequence
			_, changed, cancelErr := s.CancelWaitingAPI("s", "r", entry.PayloadHash, &revision, "isolated-cancel-outcome")
			if changed {
				t.Fatal("blocked cancellation committed")
			}
			if condition == "abort-origin-commit" && cancelErr == nil {
				t.Fatal("injected rollback not reported")
			}
			original, _, _ := s.Get("s", "r")
			after, _ := s.Snapshot("s")
			if original.State != Queued || after.Revision != before.Revision {
				t.Fatal("partial cancelled effect", original, after)
			}
			if origin, _ := s.CancelOrigin("s", "r"); origin != "" {
				t.Fatal("cancel origin survived rollback")
			}
			afterChanges, _, _ := s.TaskJournal().Changes(context.Background(), floor, 100)
			if len(afterChanges) != 0 {
				t.Fatal("false cancellation event", afterChanges)
			}
			if _, done, _ := s.TaskJournal().Outcome(context.Background(), "isolated-cancel-outcome"); done {
				t.Fatal("false success outcome")
			}
		})
	}
}
