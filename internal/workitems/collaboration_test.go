package workitems

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"everything-go/internal/coordination"
)

func TestCollaborationDurableAndRollback(t *testing.T) {
	dir := t.TempDir()
	svc, err := OpenService(dir, "authority")
	if err != nil {
		t.Fatal(err)
	}
	state, err := svc.UpdateCollaboration(context.Background(), func(s *coordination.State) error {
		_, err := s.Apply(coordination.Principal{Human: true, ID: "human"}, coordination.Command{Action: "create", ProjectID: "p1", Name: "P", Cwd: "/p", MutationID: "c"}, 1)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.UpdateCollaboration(context.Background(), func(s *coordination.State) error { s.Revision = 999; return errors.New("rollback") })
	if err == nil {
		t.Fatal("expected rollback")
	}
	svc.Close()
	svc, err = OpenService(dir, "authority")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	restored, err := svc.Collaboration(context.Background())
	if err != nil || restored.Revision != state.Revision || restored.Projects["p1"].ProfileID != coordination.ProfileID {
		t.Fatalf("restore %+v %v", restored, err)
	}
}
func TestCollaborationMigrationBacksUpV5(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, "i")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE work_schema SET version=5"); err != nil {
		t.Fatal(err)
	}
	path := s.dbPath
	s.Close()
	s, err = Open(dir, "i")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = os.Stat(path + ".pre-v6.bak"); err != nil {
		t.Fatal("missing backup", err)
	}
	var version int
	if err = s.db.QueryRow("SELECT version FROM work_schema").Scan(&version); err != nil || version != schemaVersion {
		t.Fatal(version, err)
	}
}

func TestCollaborationV2NormalizedPersistence(t *testing.T) {
	dir := t.TempDir()
	svc, err := OpenService(dir, "authority")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	state, err := svc.UpdateCollaboration(ctx, func(s *coordination.State) error {
		_, err := s.ApplyCollaboration(coordination.Principal{Human: true, ID: "human"}, coordination.CollaborationCommand{Command: coordination.Command{Action: "create", ProjectID: "v2", Name: "V2", Cwd: "/v2", MutationID: "create"}}, 1000)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	var legacy string
	if err = svc.store.db.QueryRow("SELECT payload FROM work_pm_state WHERE id=1").Scan(&legacy); err != nil {
		t.Fatal(err)
	}
	var old coordination.State
	if err = json.Unmarshal([]byte(legacy), &old); err != nil {
		t.Fatal(err)
	}
	if _, exists := old.Projects["v2"]; exists {
		t.Fatal("v2 has a second writable legacy copy")
	}
	var count int
	if err = svc.store.db.QueryRow("SELECT COUNT(*) FROM work_collaboration_records WHERE kind='project' AND id='v2'").Scan(&count); err != nil || count != 1 {
		t.Fatal("normalized project missing", err, count)
	}
	_, err = svc.UpdateCollaboration(ctx, func(s *coordination.State) error {
		s.Projects["v2"] = coordination.Project{}
		return errors.New("rollback")
	})
	if err == nil {
		t.Fatal("rollback was ignored")
	}
	svc.Close()
	svc, err = OpenService(dir, "authority")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	restored, err := svc.Collaboration(ctx)
	if err != nil || restored.Revision != state.Revision || restored.Projects["v2"].EngineVersion != 2 {
		t.Fatal("v2 restore", err)
	}
}

func TestCollaborationMigrationBacksUpV6(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, "i")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec("UPDATE work_schema SET version=6"); err != nil {
		t.Fatal(err)
	}
	path := s.dbPath
	s.Close()
	s, err = Open(dir, "i")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = os.Stat(path + ".pre-v7.bak"); err != nil {
		t.Fatal("missing v6 backup", err)
	}
}

func TestCollaborationV2SQLFailureRollsBackWorkAndNormalizedRecords(t *testing.T) {
	svc, err := OpenService(t.TempDir(), "authority")
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	ctx := context.Background()
	item := seedItem(t, svc.store, "p", "work")
	_, err = svc.UpdateCollaboration(ctx, func(s *coordination.State) error {
		s.Projects["p"] = coordination.Project{ID: "p", EngineVersion: 2}
		s.Tasks["task"] = coordination.Task{ID: "task", ProjectID: "p", WorkItemID: item.ID, State: "proposed"}
		s.Revision++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := svc.GetItem(ctx, item.ID)
	beforeRevision, _ := svc.WorkRevision(ctx)
	// This is the last SQL write, after normalized records, Work lifecycle,
	// activity and change-feed writes have already happened in the transaction.
	_, err = svc.store.db.Exec(`CREATE TRIGGER fail_collaboration_commit BEFORE UPDATE ON work_pm_state BEGIN SELECT RAISE(ABORT, 'injected final write failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.UpdateCollaboration(ctx, func(s *coordination.State) error {
		task := s.Tasks["task"]
		task.State = "cancelled"
		s.Tasks[task.ID] = task
		s.Revision++
		return nil
	})
	if err == nil {
		t.Fatal("injected SQL failure did not abort")
	}
	after, err := svc.GetItem(ctx, item.ID)
	if err != nil || after.Lifecycle != before.Lifecycle || after.Version != before.Version {
		t.Fatalf("Work escaped rollback: %+v %v", after, err)
	}
	afterRevision, _ := svc.WorkRevision(ctx)
	if afterRevision != beforeRevision {
		t.Fatal("change feed escaped rollback")
	}
	state, err := svc.Collaboration(ctx)
	if err != nil || state.Tasks["task"].State != "proposed" {
		t.Fatalf("normalized record escaped rollback: %+v %v", state.Tasks["task"], err)
	}
}
