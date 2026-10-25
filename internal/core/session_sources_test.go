package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/delegation"
	"everything-go/internal/protocol"
	"everything-go/internal/runtimejournal"
	"everything-go/internal/session"
	"os"
	"strings"
	"testing"
)

func TestSessionSourceProjectionScopeFailureAndIdentity(t *testing.T) {
	store, err := delegation.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err = store.Create(ctx, delegation.Record{ID: "dg", ParentSessionID: "parent", ChildSessionID: "child", ChildRequestID: "r_child", ParentRequestID: "return", ChildName: "QA", Cwd: "/synthetic", Instruction: "do not expose"}); err != nil {
		t.Fatal(err)
	}
	registry := session.NewRegistry()
	registry.Create("child", "public child", "/synthetic", "codex", "synthetic-model", "read-only", "12345678-1234-1234-1234-123456789abc")
	runtimes := runtimejournal.New(t.TempDir())
	runtimes.Update("child", "running", "another_original_request", 0, "", "")
	h := &Hub{delegations: store, registry: registry, runtimes: runtimes}
	rows := []protocol.SessionSummary{{ID: "child"}, {ID: "legacy"}}
	h.projectSessionSources(rows)
	r := rows[0].SessionSource.Relationships[0]
	if rows[0].SessionSource.NativeThreadID != "12345678-1234-1234-1234-123456789abc" {
		t.Fatal("authorized native identity missing")
	}
	if rows[0].SessionSource.Origin != "delegation" || r.ParentAvailable || r.ParentSessionID != "" || r.QA != "unknown" || r.Final != "unknown" {
		t.Fatalf("%+v", r)
	}
	if r.Execution != "unknown" {
		t.Fatal("unrelated runtime request claimed execution")
	}
	runtimes.Update("child", "queued", "r_child", 1, "", "")
	h.projectSessionSources(rows)
	if rows[0].SessionSource.Relationships[0].Execution != "queued" {
		t.Fatal("exact queue evidence missing")
	}
	runtimes.Update("child", "running", "r_child", 0, "", "")
	h.projectSessionSources(rows)
	if rows[0].SessionSource.Relationships[0].Execution != "running" {
		t.Fatal("exact running evidence missing")
	}
	runtimes.Update("child", "running", "human_new_request", 0, "", "")
	if rows[1].SessionSource.Origin != "unknown" {
		t.Fatal("absence inferred as human")
	}
	rows = append(rows, protocol.SessionSummary{ID: "parent"})
	h.projectSessionSources(rows)
	if _, err := store.Complete(ctx, "child", "r_child", "completed", "public synthetic native final", "", nil); err != nil {
		t.Fatal(err)
	}
	h.projectSessionSources(rows)
	raw, err := json.Marshal(protocol.NewSessionsList(rows))
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := scopeSessionIDsJSON(raw, "synthetic-a")
	if err != nil {
		t.Fatal(err)
	}
	if destination := os.Getenv("BRIDGE_SOURCE_FIXTURE_OUT"); destination != "" {
		if err := os.WriteFile(destination, scoped, 0600); err != nil {
			t.Fatal(err)
		}
	}
	var value map[string]any
	if err = json.Unmarshal(scoped, &value); err != nil {
		t.Fatal(err)
	}
	items := value["sessions"].([]any)
	parent := items[2].(map[string]any)["id"]
	child := items[0].(map[string]any)["session_source"].(map[string]any)["relationships"].([]any)[0].(map[string]any)
	if child["parent_session_id"] != parent || child["parent_available"] != true {
		t.Fatal("parent not scoped like session ID")
	}
	if strings.Contains(string(scoped), "do not expose") {
		t.Fatal("private text exposed")
	}
	store.Close()
	h.projectSessionSources(rows)
	if rows[0].SessionSource.Origin != "unknown" || len(rows[0].SessionSource.Relationships) != 0 {
		t.Fatal("DB failure retained stale classification")
	}
}
