package delegation

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

func TestR2ContinuityBindingImmutableDurable(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	c := ContinuityCommand{Parent: "owned", Key: "key", Hash: "outer-hash", Action: "continue", EffectTaskID: "task", FormalBinding: []byte(`{"request":"exact-original","hash":"inner"}`)}
	if fresh, e := s.ReserveContinuityCommand(ctx, c); e != nil || !fresh {
		t.Fatal(e)
	}
	other := c
	other.FormalBinding = []byte(`{"request":"foreign"}`)
	if fresh, e := s.ReserveContinuityCommand(ctx, other); e != nil || fresh {
		t.Fatal("overwrote reservation", e)
	}
	if e := s.FinishContinuityCommand(ctx, c, "completed", []byte(`{"receipt":"original"}`), ""); e != nil {
		t.Fatal(e)
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	s, e = Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	got, found, e := s.ContinuityCommand(ctx, c.Parent, c.Key)
	if e != nil || !found || got.State != "completed" || string(got.FormalBinding) != string(c.FormalBinding) {
		t.Fatal("binding not immutable/durable", got, e)
	}
}
func TestR2ContinuityBindingLegacyMigration(t *testing.T) {
	dir := t.TempDir()
	db, e := sql.Open("sqlite", filepath.Join(dir, "everything_go_delegations.db"))
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Exec(`CREATE TABLE delegation_continuity_commands(parent TEXT NOT NULL, command_key TEXT NOT NULL,intent_hash TEXT NOT NULL,action TEXT NOT NULL,origin_request TEXT NOT NULL,origin_call TEXT NOT NULL,state TEXT NOT NULL,result BLOB,error_code TEXT NOT NULL DEFAULT '',effect_task_id TEXT NOT NULL DEFAULT '',PRIMARY KEY(parent,command_key)); INSERT INTO delegation_continuity_commands(parent,command_key,intent_hash,action,origin_request,origin_call,state,effect_task_id) VALUES('old','key','hash','continue','req','call','reserved','task')`)
	if e != nil {
		t.Fatal(e)
	}
	db.Close()
	s, e := Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	got, found, e := s.ContinuityCommand(context.Background(), "old", "key")
	if e != nil || !found || len(got.FormalBinding) != 0 || got.EffectTaskID != "task" {
		t.Fatal("legacy row forged or lost", got, e)
	}
}
