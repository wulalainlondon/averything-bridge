package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/delegation"
	"everything-go/internal/protocol"
	"os"
	"strings"
	"testing"
)

func TestDispatcherRequestBoundLineage(t *testing.T) {
	visible := map[string]int{"root": 0, "root2": 1, "direct": 2, "child": 3, "cycle": 4}
	base := delegation.SessionSource{ID: "child-a", ParentSessionID: "direct", ChildSessionID: "child", ChildRequestID: "work-child", OriginRequestID: "work-direct-a"}
	first := delegation.SessionSource{ID: "direct-a", ParentSessionID: "root", ChildSessionID: "direct", ChildRequestID: "work-direct-a", OriginRequestID: "human-origin"}
	second := delegation.SessionSource{ID: "direct-b", ParentSessionID: "root2", ChildSessionID: "direct", ChildRequestID: "work-direct-b", OriginRequestID: "human-other"}
	tests := []struct {
		name         string
		row          delegation.SessionSource
		incoming     []delegation.SessionSource
		root, status string
	}{
		{"exact first origin", base, []delegation.SessionSource{first, second}, "root", "known"},
		{"same direct session different origin", func() delegation.SessionSource { v := base; v.OriginRequestID = "work-direct-b"; return v }(), []delegation.SessionSource{first, second}, "root2", "known"},
		{"later manual turn never borrows direct ancestor", func() delegation.SessionSource { v := base; v.OriginRequestID = "human-later"; return v }(), []delegation.SessionSource{first, second}, "", "unknown"},
		{"legacy no request binding", func() delegation.SessionSource { v := base; v.OriginRequestID = ""; return v }(), []delegation.SessionSource{first}, "", "unknown"},
		{"invisible root", base, []delegation.SessionSource{func() delegation.SessionSource { v := first; v.ParentSessionID = "outside"; return v }()}, "", "incomplete"},
		{"duplicate ambiguous producer", base, []delegation.SessionSource{first, first}, "", "ambiguous"},
		{"cycle", base, []delegation.SessionSource{func() delegation.SessionSource { v := first; v.ParentSessionID = "child"; return v }()}, "", "cycle"},
		{"self parent", func() delegation.SessionSource { v := base; v.ParentSessionID = "child"; return v }(), nil, "", "cycle"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byChild := map[string][]delegation.SessionSource{"direct": tt.incoming}
			root, status := sessionSourceRoot(tt.row, byChild, visible)
			if root != tt.root || status != tt.status {
				t.Fatalf("got %q %q want %q %q", root, status, tt.root, tt.status)
			}
		})
	}
}

func TestDispatcherMetadataProjectionEveryOriginAndScope(t *testing.T) {
	store, err := delegation.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	records := []delegation.Record{
		{ID: "direct-first", ParentSessionID: "root", OriginRequestID: "human-root", ChildSessionID: "direct", ChildRequestID: "direct-first-request", ParentRequestID: "return-direct-first"},
		{ID: "direct-second", ParentSessionID: "root2", OriginRequestID: "human-root2", ChildSessionID: "direct", ChildRequestID: "direct-second-request", ParentRequestID: "return-direct-second"},
		{ID: "child-first", ParentSessionID: "direct", OriginRequestID: "direct-first-request", ChildSessionID: "child", ChildRequestID: "child-first-request", ParentRequestID: "return-child-first"},
		{ID: "child-second", ParentSessionID: "direct", OriginRequestID: "direct-second-request", ChildSessionID: "child", ChildRequestID: "child-second-request", ParentRequestID: "return-child-second"},
		{ID: "child-other", ParentSessionID: "root2", OriginRequestID: "human-other", ChildSessionID: "child", ChildRequestID: "child-other-request", ParentRequestID: "return-child-other"},
	}
	for _, r := range records {
		r.ChildName = "public"
		r.Cwd = "/public"
		r.Instruction = "PRIVATE_INSTRUCTION"
		if err := store.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	before, _, _ := store.ByID(ctx, "child-first")
	h := &Hub{delegations: store}
	rows := []protocol.SessionSummary{{ID: "root"}, {ID: "root2"}, {ID: "direct"}, {ID: "child"}, {ID: "legacy"}}
	h.projectSessionSources(rows)
	origins := map[string]protocol.SessionSourceRelationship{}
	for _, r := range rows[3].SessionSource.Relationships {
		origins[r.DelegationID] = r
	}
	if len(origins) != 3 || origins["child-first"].RootSessionID != "root" || origins["child-second"].RootSessionID != "root2" || origins["child-other"].ParentSessionID != "root2" {
		t.Fatalf("origins %+v", origins)
	}
	raw, err := json.Marshal(protocol.NewSessionsList(rows))
	if err != nil {
		t.Fatal(err)
	}
	scoped, err := scopeSessionIDsJSON(raw, "host-with-same-names")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(scoped), `"root_session_id":"sk1:host-with-same-names:root"`) || !strings.Contains(string(scoped), `"parent_session_id":"sk1:host-with-same-names:direct"`) {
		t.Fatalf("unscoped wire %s", scoped)
	}
	if strings.Contains(string(scoped), "PRIVATE_INSTRUCTION") || strings.Contains(string(scoped), "source_thread") || strings.Contains(string(scoped), "grants") {
		t.Fatal("private metadata leaked")
	}
	if out := os.Getenv("DISPATCHER_SOURCE_FIXTURE_OUT"); out != "" {
		if err := os.WriteFile(out, scoped, 0600); err != nil {
			t.Fatal(err)
		}
	}
	after, _, _ := store.ByID(ctx, "child-first")
	beforeJSON, _ := json.Marshal(before)
	afterJSON, _ := json.Marshal(after)
	if string(beforeJSON) != string(afterJSON) {
		t.Fatal("projection changed ledger")
	}
	restricted := []protocol.SessionSummary{{ID: "direct"}, {ID: "child"}}
	h.projectSessionSources(restricted)
	for _, r := range restricted[1].SessionSource.Relationships {
		if r.RootSessionID != "" {
			t.Fatal("invisible root leaked")
		}
		if r.DelegationID == "child-other" && (r.ParentSessionID != "" || r.OriginRequestID != "") {
			t.Fatal("invisible direct leaked")
		}
	}
}
