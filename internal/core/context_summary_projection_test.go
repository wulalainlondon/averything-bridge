package core

import (
	"encoding/json"
	"testing"
)

func TestSessionSummaryProjectsReportedContextToExistingClientFields(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("qa-context", "QA", "/qa", "claude", "sonnet", "read-only", "")
	s.SetContext(42000, 1000000)
	rows := h.sessionSummaries()
	data, err := json.Marshal(h.client.SessionsList(rows))
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		Sessions []map[string]any `json:"sessions"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatal(err)
	}
	if len(event.Sessions) != 1 || event.Sessions[0]["context_used"] != float64(42000) || event.Sessions[0]["context_max"] != float64(1000000) {
		t.Fatal("context did not reach the existing client wire contract")
	}
}

func TestContextSummaryDistinguishesKnownZeroUnknownAndInvalid(t *testing.T) {
	for _, tc := range []struct {
		name              string
		set               bool
		used, max         int
		wantUsed, wantMax bool
		value             int
	}{
		{"unavailable", false, 0, 0, false, false, 0},
		{"known_zero", true, 0, 1000000, true, true, 0},
		{"window_only_unknown_usage", true, -1, 1000000, false, true, 0},
		{"invalid_negative", true, -1, -1, false, false, 0},
		{"positive", true, 42000, 1000000, true, true, 42000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHub(t)
			s := h.registry.Create("qa", "QA", "/qa", "claude", "sonnet", "read-only", "")
			if tc.set {
				s.SetContext(tc.used, tc.max)
			}
			data, _ := json.Marshal(h.sessionSummaries()[0])
			var wire map[string]any
			_ = json.Unmarshal(data, &wire)
			used, hasUsed := wire["context_used"]
			_, hasMax := wire["context_max"]
			if hasUsed != tc.wantUsed || hasMax != tc.wantMax || (hasUsed && used != float64(tc.value)) {
				t.Fatal("context presence/zero contract lost")
			}
		})
	}
}

func TestContextSummaryKnownZeroReplacesPositiveAndNegativeDoesNotEraseIt(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("qa", "QA", "/qa", "claude", "sonnet", "read-only", "")
	s.SetContext(42000, 1000000)
	s.SetContext(0, 1000000)
	s.SetContext(-9, -9)
	row := h.sessionSummaries()[0]
	if row.ContextUsed == nil || *row.ContextUsed != 0 || row.ContextMax == nil || *row.ContextMax != 1000000 {
		t.Fatal("known zero was dropped or invalid update replaced it")
	}
}
