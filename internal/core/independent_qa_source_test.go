package core

import (
	"context"
	"everything-go/internal/delegation"
	"testing"
)

func TestIndependentSourceScopeAndOwner(t *testing.T) {
	h, c, root, outside := rootScopeFixture(t)
	store, openErr := delegation.Open(t.TempDir())
	if openErr != nil {
		t.Fatal(openErr)
	}
	h.delegations = store
	t.Cleanup(func() { store.Close() })
	h.registry.Create("inside-child", "same", root, "codex", "", "read-only", "")
	h.registry.Create("outside-parent", "same", outside, "codex", "", "read-only", "")
	err := h.delegations.Create(context.Background(), delegation.Record{ID: "dg-public", ParentSessionID: "outside-parent", OriginRequestID: "r_owner_scope", ChildSessionID: "inside-child", ChildRequestID: "r_public", ParentRequestID: "return_public", ChildName: "same", Cwd: root, Instruction: "public synthetic"})
	if err != nil {
		t.Fatal(err)
	}
	route(h, c, `{"type":"request_sessions_list"}`)
	event := waitForType(t, c, "sessions_list")
	rows := event["sessions"].([]any)
	if len(rows) != 1 {
		t.Fatalf("restricted list: %+v", rows)
	}
	src := rows[0].(map[string]any)["session_source"].(map[string]any)
	rel := src["relationships"].([]any)[0].(map[string]any)
	if rel["parent_available"] != false || rel["parent_session_id"] != nil || rel["root_session_id"] != nil || rel["origin_request_id"] != nil {
		t.Fatalf("out of scope parent exposed: %+v", rel)
	}
	h.cfg.RootDir = ""
	route(h, c, `{"type":"request_sessions_list"}`)
	event = waitForType(t, c, "sessions_list")
	rows = event["sessions"].([]any)
	if len(rows) != 2 {
		t.Fatalf("owner lost visible sessions: %+v", rows)
	}
	found := false
	for _, v := range rows {
		row := v.(map[string]any)
		src := row["session_source"].(map[string]any)
		rs := src["relationships"].([]any)
		if len(rs) > 0 {
			rel = rs[0].(map[string]any)
			if rel["parent_available"] != true || rel["parent_session_id"] != "outside-parent" {
				t.Fatalf("owner source unavailable: %+v", rel)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("owner cannot see mapped child")
	}
	for _, probe := range []bool{true, false} {
		denied := newTestClient(h)
		denied.inventoryProbe = probe
		denied.enrollmentOnly = !probe
		route(h, denied, `{"type":"request_sessions_list"}`)
		if event := waitForType(t, denied, "error"); event["message"] == "" {
			t.Fatal("missing explicit denial")
		}
	}
}
