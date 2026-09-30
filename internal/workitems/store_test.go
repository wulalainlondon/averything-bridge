package workitems

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir(), "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.now = func() time.Time { return time.UnixMilli(1_800_000_000_000) }
	return s
}

func seedItem(t *testing.T, s *Store, projectID, itemID string) WorkItem {
	t.Helper()
	ctx := context.Background()
	if _, err := s.CreateProject(ctx, CreateProjectInput{ID: projectID, Name: projectID}); err != nil {
		t.Fatal(err)
	}
	item, err := s.CreateItem(ctx, CreateItemInput{ID: itemID, ProjectID: projectID, Title: "Build native board", Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func move(t *testing.T, s *Store, item WorkItem, lifecycle Lifecycle, actor ActorType) WorkItem {
	t.Helper()
	item, err := s.MoveItem(context.Background(), MoveItemInput{ID: item.ID, ExpectedVersion: item.Version,
		Lifecycle: lifecycle, Actor: Actor{Type: actor}})
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func TestLifecycleRequiresHumanAcceptance(t *testing.T) {
	s := openTestStore(t)
	item := seedItem(t, s, "p1", "i1")
	item = move(t, s, item, LifecycleReady, ActorUser)
	_, item, err := s.LinkSession(context.Background(), LinkSessionInput{ID: "accept-link", WorkItemID: item.ID,
		SessionID: "accept-session", ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	item = move(t, s, item, LifecycleActive, ActorAgent)
	item = move(t, s, item, LifecycleReview, ActorAgent)

	_, err = s.MoveItem(context.Background(), MoveItemInput{ID: item.ID, ExpectedVersion: item.Version,
		Lifecycle: LifecycleDone, Actor: Actor{Type: ActorAgent}})
	if !errors.Is(err, ErrHumanRequired) {
		t.Fatalf("agent done error=%v, want ErrHumanRequired", err)
	}

	accepted, err := s.DecideReview(context.Background(), ReviewDecisionInput{WorkItemID: item.ID,
		ExpectedVersion: item.Version, Decision: "accept", Actor: Actor{Type: ActorMobile}})
	if err != nil {
		t.Fatal(err)
	}
	item = accepted.Item
	if item.AcceptedAt == nil || item.Lifecycle != LifecycleDone {
		t.Fatalf("human acceptance not persisted: %+v", item)
	}
	reopened, err := s.DecideReview(context.Background(), ReviewDecisionInput{WorkItemID: item.ID,
		ExpectedVersion: item.Version, Decision: "reopen", Feedback: "new evidence", Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	item = reopened.Item
	if item.AcceptedAt != nil {
		t.Fatal("reopened item retained accepted_at")
	}
}

func TestOptimisticConflictReturnsCurrentItem(t *testing.T) {
	s := openTestStore(t)
	item := seedItem(t, s, "p1", "i1")
	newTitle := "Updated"
	updated, err := s.UpdateItem(context.Background(), UpdateItemInput{ID: item.ID,
		ExpectedVersion: item.Version, Title: &newTitle, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.UpdateItem(context.Background(), UpdateItemInput{ID: item.ID,
		ExpectedVersion: item.Version, Title: &newTitle, Actor: Actor{Type: ActorUser}})
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.Current.Version != updated.Version {
		t.Fatalf("conflict=%v current=%+v", err, conflict)
	}
}

func TestOneActiveWorkItemPerSession(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	i1 := seedItem(t, s, "p1", "i1")
	i2, err := s.CreateItem(ctx, CreateItemInput{ID: "i2", ProjectID: "p1", Title: "Second"})
	if err != nil {
		t.Fatal(err)
	}
	_, i1, err = s.LinkSession(ctx, LinkSessionInput{WorkItemID: i1.ID, SessionID: "s1",
		ExpectedVersion: i1.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.LinkSession(ctx, LinkSessionInput{WorkItemID: i2.ID, SessionID: "s1",
		ExpectedVersion: i2.Version, Actor: Actor{Type: ActorUser}})
	if !errors.Is(err, ErrSessionLinked) {
		t.Fatalf("link error=%v, want ErrSessionLinked", err)
	}
}

func TestDependencyCycleAndCrossProjectRejected(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	a := seedItem(t, s, "p1", "a")
	b, _ := s.CreateItem(ctx, CreateItemInput{ID: "b", ProjectID: "p1", Title: "B"})
	c, _ := s.CreateItem(ctx, CreateItemInput{ID: "c", ProjectID: "p1", Title: "C"})
	other := seedItem(t, s, "p2", "other")

	a, err := s.AddDependency(ctx, AddDependencyInput{WorkItemID: a.ID, DependsOnID: b.ID,
		ExpectedVersion: a.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	b, err = s.AddDependency(ctx, AddDependencyInput{WorkItemID: b.ID, DependsOnID: c.ID,
		ExpectedVersion: b.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.AddDependency(ctx, AddDependencyInput{WorkItemID: c.ID, DependsOnID: a.ID,
		ExpectedVersion: c.Version, Actor: Actor{Type: ActorUser}})
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("cycle error=%v", err)
	}
	_, err = s.AddDependency(ctx, AddDependencyInput{WorkItemID: a.ID, DependsOnID: other.ID,
		ExpectedVersion: a.Version, Actor: Actor{Type: ActorUser}})
	if !errors.Is(err, ErrCrossProject) {
		t.Fatalf("cross-project error=%v", err)
	}
}

func TestRestartPreservesSnapshotAndRevision(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(ctx, CreateProjectInput{ID: "p1", Name: "Project"})
	if err != nil {
		t.Fatal(err)
	}
	item, err := s.CreateItem(ctx, CreateItemInput{ID: "i1", ProjectID: project.ID, Title: "Persist me"})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Projects) != 1 || len(snap.Items) != 1 || snap.Items[0].ID != item.ID || snap.Revision < 2 {
		t.Fatalf("restart snapshot=%+v", snap)
	}
	if _, err := filepath.Abs(filepath.Join(dir, "everything_go_work_items.db")); err != nil {
		t.Fatal(err)
	}
}

func TestImportSessionDraftIsAtomicAndSemanticallyIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	result, err := s.ImportSessionDraft(ctx, ImportSessionDraftInput{
		ProjectID: "p-import", ProjectName: "Bridge", WorkspacePath: "/repo/bridge",
		WorkItemID: "wi-import", SessionID: "session-1", ThreadIDSnapshot: "thread-1",
		Title: "Ship session import", Outcome: "Existing work is visible",
		NextStep: "Confirm the draft", AcceptanceCriteria: "Session opens from the item",
		Actor: Actor{Type: ActorMobile, DeviceID: "phone"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AlreadyLinked || result.Item.Lifecycle != LifecycleInbox || result.Link.SessionID != "session-1" {
		t.Fatalf("import result=%+v", result)
	}
	if result.Item.Outcome != "Existing work is visible" || result.Item.NextStep != "Confirm the draft" || result.Item.AcceptanceCriteria != "Session opens from the item" {
		t.Fatalf("semantic draft fields=%+v", result.Item)
	}
	before, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := s.ImportSessionDraft(ctx, ImportSessionDraftInput{
		ProjectID: "other-project", ProjectName: "Duplicate", WorkItemID: "other-item",
		SessionID: "session-1", Title: "Must not duplicate", Actor: Actor{Type: ActorDesktop},
	})
	if err != nil {
		t.Fatal(err)
	}
	after, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.AlreadyLinked || retry.Item.ID != result.Item.ID || after.Revision != before.Revision || len(after.Items) != 1 || len(after.Projects) != 1 {
		t.Fatalf("idempotent retry=%+v before=%+v after=%+v", retry, before, after)
	}
}

func TestImportSessionDraftRollsBackProjectOnItemFailure(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	_ = seedItem(t, s, "existing-project", "duplicate-item")
	_, err := s.ImportSessionDraft(ctx, ImportSessionDraftInput{
		ProjectID: "must-rollback", ProjectName: "Rollback", WorkItemID: "duplicate-item",
		SessionID: "session-rollback", Title: "Draft", Actor: Actor{Type: ActorUser},
	})
	if err == nil {
		t.Fatal("duplicate item import unexpectedly succeeded")
	}
	snapshot, snapErr := s.Snapshot(ctx)
	if snapErr != nil {
		t.Fatal(snapErr)
	}
	for _, project := range snapshot.Projects {
		if project.ID == "must-rollback" {
			t.Fatalf("failed import left project behind: %+v", snapshot)
		}
	}
	for _, link := range snapshot.SessionLinks {
		if link.SessionID == "session-rollback" {
			t.Fatalf("failed import left link behind: %+v", snapshot)
		}
	}
}

func TestRelatedEntitiesAreTransactionalAndAppearInDeltas(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	item := seedItem(t, s, "p1", "i1")
	link, item, err := s.LinkSession(ctx, LinkSessionInput{ID: "link1", WorkItemID: item.ID,
		SessionID: "s1", ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	changes, _, _, err := s.ChangesSince(ctx, item.ActivityRevision-1, 10)
	if err != nil || len(changes) != 1 || !strings.Contains(string(changes[0].Payload), `"link"`) {
		t.Fatalf("link delta=%+v err=%v", changes, err)
	}
	link, item, err = s.UnlinkSession(ctx, UnlinkSessionInput{LinkID: link.ID,
		ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil || link.UnlinkedAt == nil {
		t.Fatalf("unlink=%+v item=%+v err=%v", link, item, err)
	}

	comment, item, err := s.AddComment(ctx, AddCommentInput{ID: "comment1", WorkItemID: item.ID,
		ExpectedVersion: item.Version, Body: "  ready for review  ", Actor: Actor{Type: ActorMobile, DeviceID: "phone"}})
	if err != nil || comment.Body != "ready for review" {
		t.Fatalf("comment=%+v err=%v", comment, err)
	}
	comment, item, err = s.EditComment(ctx, EditCommentInput{CommentID: comment.ID,
		ExpectedVersion: item.Version, Body: "approved soon", Actor: Actor{Type: ActorMobile, DeviceID: "phone"}})
	if err != nil || comment.EditedAt == nil {
		t.Fatalf("edited comment=%+v err=%v", comment, err)
	}
	item, err = s.ArchiveItem(ctx, ArchiveItemInput{ID: item.ID, ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil || item.ArchivedAt == nil {
		t.Fatalf("archive=%+v err=%v", item, err)
	}
	item, err = s.ArchiveItem(ctx, ArchiveItemInput{ID: item.ID, ExpectedVersion: item.Version, Restore: true, Actor: Actor{Type: ActorUser}})
	if err != nil || item.ArchivedAt != nil {
		t.Fatalf("restore=%+v err=%v", item, err)
	}
	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.SessionLinks) != 0 || len(snap.Comments) != 1 || snap.Comments[0].Body != "approved soon" {
		t.Fatalf("snapshot after related mutations=%+v", snap)
	}
}

func TestAttachmentRefsAreDurableAndRemovalIsDeltaVisible(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	item := seedItem(t, s, "p1", "i1")
	ref, item, err := s.AddAttachment(ctx, AddAttachmentInput{ID: "wa1", WorkItemID: item.ID,
		AttachmentID: "att1", DisplayName: "proof.png", ExpectedVersion: item.Version,
		Actor: Actor{Type: ActorMobile, DeviceID: "phone"}})
	if err != nil || ref.AttachmentID != "att1" {
		t.Fatalf("ref=%+v item=%+v err=%v", ref, item, err)
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil || len(snapshot.Attachments) != 1 || snapshot.Attachments[0].ID != ref.ID {
		t.Fatalf("snapshot attachments=%+v err=%v", snapshot.Attachments, err)
	}
	ref, item, err = s.RemoveAttachment(ctx, RemoveAttachmentInput{AttachmentRefID: ref.ID,
		ExpectedVersion: item.Version, Actor: Actor{Type: ActorMobile, DeviceID: "phone"}})
	if err != nil || ref.RemovedAt == nil {
		t.Fatalf("removed ref=%+v item=%+v err=%v", ref, item, err)
	}
	changes, _, _, err := s.ChangesSince(ctx, item.ActivityRevision-1, 10)
	if err != nil || len(changes) != 1 || !strings.Contains(string(changes[0].Payload), `"removed_at"`) {
		t.Fatalf("removal delta=%+v err=%v", changes, err)
	}
}

func TestSchemaV1MigrationBacksUpAndAddsAttachmentTombstone(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "everything_go_work_items.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE work_schema(version INTEGER NOT NULL);
		INSERT INTO work_schema(version) VALUES(1);
		CREATE TABLE work_item_attachments(
		id TEXT PRIMARY KEY, work_item_id TEXT NOT NULL, comment_id TEXT,
		attachment_id TEXT NOT NULL, display_name TEXT NOT NULL DEFAULT '',
		sort_key INTEGER NOT NULL, created_at INTEGER NOT NULL);`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var version int
	if err := s.db.QueryRow("SELECT version FROM work_schema").Scan(&version); err != nil || version != schemaVersion {
		t.Fatalf("version=%d err=%v", version, err)
	}
	var column string
	if err := s.db.QueryRow(`SELECT name FROM pragma_table_info('work_item_attachments') WHERE name='removed_at'`).Scan(&column); err != nil || column != "removed_at" {
		t.Fatalf("column=%q err=%v", column, err)
	}
	if _, err := os.Stat(path + ".pre-v2.bak"); err != nil {
		t.Fatalf("migration backup missing: %v", err)
	}
}

func TestSchemaV2MigrationBacksUpAndAddsOutcomeFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "everything_go_work_items.db")
	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE work_schema(version INTEGER NOT NULL);
		INSERT INTO work_schema(version) VALUES(2);
		CREATE TABLE work_items(id TEXT PRIMARY KEY, project_id TEXT NOT NULL, title TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '', lifecycle TEXT NOT NULL, priority TEXT NOT NULL,
		sort_key INTEGER NOT NULL, version INTEGER NOT NULL, activity_revision INTEGER NOT NULL DEFAULT 0,
		blocked_reason_code TEXT NOT NULL DEFAULT '', blocked_note TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL, accepted_at INTEGER, cancelled_at INTEGER, archived_at INTEGER);`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, column := range []string{"outcome", "next_step", "acceptance_criteria"} {
		var found string
		if err := s.db.QueryRow(`SELECT name FROM pragma_table_info('work_items') WHERE name=?`, column).Scan(&found); err != nil || found != column {
			t.Fatalf("column %q missing: found=%q err=%v", column, found, err)
		}
	}
	if _, err := os.Stat(path + ".pre-v3.bak"); err != nil {
		t.Fatalf("v2 migration backup missing: %v", err)
	}
}

func TestExplicitRunProjectsToReviewButOrdinaryLinkedSessionDoesNot(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	item := seedItem(t, s, "p1", "i1")
	item = move(t, s, item, LifecycleReady, ActorUser)
	_, item, err := s.LinkSession(ctx, LinkSessionInput{ID: "link1", WorkItemID: item.ID,
		SessionID: "s1", ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := s.AdvanceRun(ctx, "s1", "ordinary", "succeeded", "")
	if err != nil || ordinary.Changed {
		t.Fatalf("ordinary linked turn changed work: %+v err=%v", ordinary, err)
	}
	run, item, err := s.StartRun(ctx, StartRunInput{ID: "run1", WorkItemID: item.ID,
		SessionID: "s1", RequestID: "req1", Kind: "implementation", ExpectedVersion: item.Version,
		Actor: Actor{Type: ActorMobile}})
	if err != nil || run.Status != "queued" || item.Lifecycle != LifecycleActive {
		t.Fatalf("start run=%+v item=%+v err=%v", run, item, err)
	}
	update, err := s.AdvanceRun(ctx, "s1", "req1", "running", "")
	if err != nil || !update.Changed || update.Item.Lifecycle != LifecycleActive {
		t.Fatalf("running update=%+v err=%v", update, err)
	}
	update, err = s.AdvanceRun(ctx, "s1", "req1", "succeeded", "")
	if err != nil || !update.Changed || update.Item.Lifecycle != LifecycleReview || update.Attention != "review_ready" {
		t.Fatalf("success update=%+v err=%v", update, err)
	}
	owned, err := s.OwnsRequest(ctx, "s1", "req1")
	if err != nil || !owned {
		t.Fatalf("terminal run lost request ownership: owned=%v err=%v", owned, err)
	}
	owned, err = s.OwnsRequest(ctx, "s1", "ordinary")
	if err != nil || owned {
		t.Fatalf("ordinary request claimed by work: owned=%v err=%v", owned, err)
	}
	snap, err := s.Snapshot(ctx)
	if err != nil || len(snap.Runs) != 1 || snap.Runs[0].FinishedAt == nil {
		t.Fatalf("run snapshot=%+v err=%v", snap.Runs, err)
	}
}

func TestFailedExplicitRunCreatesAttentionWithoutCompletingWork(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	item := seedItem(t, s, "p1", "i1")
	item = move(t, s, item, LifecycleReady, ActorUser)
	_, item, _ = s.LinkSession(ctx, LinkSessionInput{ID: "link1", WorkItemID: item.ID,
		SessionID: "s1", ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	_, item, err := s.StartRun(ctx, StartRunInput{ID: "run1", WorkItemID: item.ID,
		SessionID: "s1", RequestID: "req1", ExpectedVersion: item.Version, Actor: Actor{Type: ActorMobile}})
	if err != nil {
		t.Fatal(err)
	}
	update, err := s.AdvanceRun(ctx, "s1", "req1", "failed", "tool failed")
	if err != nil || update.Item.Lifecycle != LifecycleActive || update.Attention != "execution_failed" ||
		update.Item.BlockedReasonCode != "execution_failed" {
		t.Fatalf("failure projection=%+v err=%v", update, err)
	}
}
