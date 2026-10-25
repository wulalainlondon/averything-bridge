package core

import (
	"encoding/json"
	"testing"
)

func TestHistoryReadAnnouncesExistingThreadWithoutStartingWork(t *testing.T) {
	for _, backend := range []string{"codex", "claude"} {
		t.Run(backend, func(t *testing.T) {
			h, _ := newTestHub(t)
			s := h.registry.Create("s_original", "Original", "/work", backend, "", "", "thread-original")
			before := s.Snapshot()
			c := newTestClient(h)
			route(h, c, `{"type":"request_history","session_id":"s_original","mode":"snapshot"}`)
			event := waitForType(t, c, "session_uuid")
			if event["session_id"] != s.ID || event["claude_uuid"] != "thread-original" {
				t.Fatalf("wrong identity: %#v", event)
			}
			after := s.Snapshot()
			if len(h.registry.List()) != 1 || after.ResumeID != before.ResumeID || after.State != before.State {
				t.Fatal("history read changed development work or created a session")
			}
		})
	}
}

func TestSessionSummaryCarriesCurrentRegisteredThreadIncludingEmpty(t *testing.T) {
	h, _ := newTestHub(t)
	for _, backend := range []string{"codex", "claude"} {
		h.registry.Create("s_"+backend, backend, "/work", backend, "", "", "native-"+backend)
	}
	h.registry.Create("s_empty", "Empty", "/work", "claude", "", "", "")
	before := len(h.registry.List())
	rows := h.sessionSummaries()
	if len(rows) != 3 {
		t.Fatalf("rows=%d", len(rows))
	}
	for _, row := range rows {
		expected := "native-" + row.Backend
		if row.ID == "s_empty" {
			expected = ""
		}
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		var wire map[string]any
		if err := json.Unmarshal(raw, &wire); err != nil {
			t.Fatal(err)
		}
		if got, exists := wire["claude_uuid"]; !exists || got != expected {
			t.Fatalf("identity %s: %#v", row.ID, wire["claude_uuid"])
		}
	}
	if len(h.registry.List()) != before {
		t.Fatal("summary created development work")
	}
}
