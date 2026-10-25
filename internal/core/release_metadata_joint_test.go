package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/delegation"
	"os"
	"testing"
)

// This fixture is supplemental owned review evidence, outside candidate source.
func TestReleaseJointMetadataRoutesAndScopedSource(t *testing.T) {
	h, _ := newTestHub(t)
	dir := t.TempDir()
	h.registry.Create("root", "same name", dir, backend.Codex, "", "", "native-root")
	h.registry.Create("child", "same name", dir, backend.Codex, "", "", "root")
	store, err := delegation.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	h.delegations = store
	rec := delegation.Record{ID: "fixture-dg", ParentSessionID: "root", OriginRequestID: "r_origin", ChildSessionID: "child", ChildRequestID: "r_child", ParentRequestID: "r_return", ChildName: "same name", Cwd: dir, Instruction: "PRIVATE_JOINT_INSTRUCTION"}
	if err = store.Create(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	before, _, err := store.ByID(context.Background(), rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	p := &visibilityProvider{}
	h.SetExecutor(&visibilityExecutor{fakeExec: fakeExec{sink: h}, p: p})
	c := sharedReadClient(t, h, "fixture-reader")
	route(h, c, `{"type":"request_sessions_list"}`)
	list := waitForType(t, c, "sessions_list")
	rows := list["sessions"].([]any)
	mapped := 0
	for _, raw := range rows {
		row := raw.(map[string]any)
		source := row["session_source"].(map[string]any)
		if source["origin"] != "delegation" {
			continue
		}
		rels := source["relationships"].([]any)
		if len(rels) != 1 {
			t.Fatal("lost mapping")
		}
		rel := rels[0].(map[string]any)
		if rel["child_request_id"] != "r_child" || rel["origin_request_id"] != "r_origin" || rel["qa"] != "unknown" || rel["lineage_scope"] != "authorized_ledger" {
			t.Fatal("mapping/QA widened", rel)
		}
		if rel["root_session_id"] != rel["parent_session_id"] || rel["lineage_status"] != "known" {
			t.Fatal("lineage drift", rel)
		}
		mapped++
	}
	if mapped != 1 {
		t.Fatal("new producer hook missing")
	}
	route(h, c, `{"type":"request_plugin_metadata_status"}`)
	status := waitForType(t, c, "plugin_metadata_status")
	for _, axis := range []string{"installed", "enabled", "loaded", "discovery"} {
		if status["host"].(map[string]any)[axis] != "unknown" {
			t.Fatal("host proof fabricated")
		}
	}
	route(h, c, `{"type":"request_internal_agent_observation","session_id":"child","authority":"forged"}`)
	obs := waitForType(t, c, "internal_agent_observation")
	if p.gotAuthority != "i1" || p.gotSession != "child" || obs["coverage"] != "partial_provider_child_cache" {
		t.Fatal("observer scope/partial drift", obs)
	}
	after, _, _ := store.ByID(context.Background(), rec.ID)
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if string(a) != string(b) {
		t.Fatal("read modified ledger")
	}
	if out := os.Getenv("RELEASE_METADATA_FIXTURE_OUT"); out != "" {
		wire, _ := json.Marshal(map[string]any{"status": status, "observation": obs, "sessions": list})
		if err = os.WriteFile(out, wire, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
