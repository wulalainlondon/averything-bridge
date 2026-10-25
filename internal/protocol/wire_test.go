package protocol

import (
	"encoding/json"
	"testing"

	"everything-go/internal/workitems"
)

func TestParseInboundBasicEnvelope(t *testing.T) {
	in, err := ParseInbound([]byte(`{"type":"message","session_id":"s1","request_id":"r1","content":"hi"}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if in.Type != "message" || in.SessionID != "s1" || in.RequestID != "r1" || in.Content != "hi" {
		t.Fatalf("unexpected parse: %+v", in)
	}
}

func TestParseInboundPinnedHiddenAreTristate(t *testing.T) {
	// Absent → nil pointer (distinguishes "unset" from "false").
	in, _ := ParseInbound([]byte(`{"type":"set_session_meta","session_id":"s1"}`))
	if in.Pinned != nil || in.Hidden != nil {
		t.Fatal("absent pinned/hidden must be nil")
	}
	in2, _ := ParseInbound([]byte(`{"type":"set_session_meta","session_id":"s1","pinned":true,"hidden":false}`))
	if in2.Pinned == nil || !*in2.Pinned {
		t.Fatal("pinned:true should parse to *bool(true)")
	}
	if in2.Hidden == nil || *in2.Hidden {
		t.Fatal("hidden:false should parse to *bool(false)")
	}
}

func TestParseInboundSearchFilters(t *testing.T) {
	in, err := ParseInbound([]byte(`{"type":"request_search","query":"foo","limit":20,"offset":5,"filters":{"role":"user","exclude_subagents":true,"max_per_session":2}}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if in.Query != "foo" || in.Limit != 20 || in.Offset != 5 {
		t.Fatalf("bad search envelope: %+v", in)
	}
	if in.Filters == nil || in.Filters.Role != "user" || !in.Filters.ExcludeSubagents || in.Filters.MaxPerSession != 2 {
		t.Fatalf("bad filters: %+v", in.Filters)
	}
}

func TestParseInboundSessionListIncludeSubagents(t *testing.T) {
	in, err := ParseInbound([]byte(`{"type":"request_session_list","include_subagents":true}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !in.IncludeSubagents {
		t.Fatalf("include_subagents should parse true: %+v", in)
	}
}

func TestParseInboundCodexGoalFields(t *testing.T) {
	in, err := ParseInbound([]byte(`{"type":"codex_goal_set","session_id":"s1","objective":"ship","status":"active","token_budget":1234}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if in.Objective != "ship" || in.Status != "active" {
		t.Fatalf("goal strings not parsed: %+v", in)
	}
	if in.TokenBudget == nil || *in.TokenBudget != 1234 {
		t.Fatalf("token_budget not parsed: %+v", in.TokenBudget)
	}
}

func TestParseInboundBadJSON(t *testing.T) {
	if _, err := ParseInbound([]byte(`{not json`)); err == nil {
		t.Fatal("expected error on malformed JSON")
	}
}

// marshalKeys returns the set of top-level JSON keys for an event value.
func marshalKeys(t *testing.T, v any) (map[string]any, string) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m, string(b)
}

func TestOutboundEventSchemas(t *testing.T) {
	cases := []struct {
		name     string
		event    any
		wantType string
		wantKeys []string
	}{
		{"hello_ack", NewClaimAck(), "claim_ack", []string{"type", "is_locked", "locked_to_me"}},
		{"pong", NewPong(), "pong", []string{"type"}},
		{"text_chunk", NewTextChunk("s1", "r1", "hi"), "text_chunk", []string{"type", "session_id", "request_id", "content"}},
		{"done", NewDone("s1", "r1"), "done", []string{"type", "session_id", "request_id"}},
		{"session_uuid", NewSessionUUID("s1", "uuid-x"), "session_uuid", []string{"type", "session_id", "claude_uuid"}},
		{"tool_start", NewToolStart("s1", "r1", "t1", "Bash", "ls"), "tool_start", []string{"type", "session_id", "tool_use_id", "name", "command"}},
		{"goal_update", NewGoalUpdate("s1", Goal{ThreadID: "t1", Objective: "ship", Status: "active"}), "goal_update", []string{"type", "session_id", "goal"}},
		{"goal_cleared", NewGoalCleared("s1"), "goal_cleared", []string{"type", "session_id"}},
		{"unclaim_ack", NewUnclaimAck(false), "unclaim_ack", []string{"type", "is_locked"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, raw := marshalKeys(t, tc.event)
			if m["type"] != tc.wantType {
				t.Fatalf("type = %v, want %v (%s)", m["type"], tc.wantType, raw)
			}
			for _, k := range tc.wantKeys {
				if _, ok := m[k]; !ok {
					t.Fatalf("missing key %q in %s", k, raw)
				}
			}
		})
	}
}

func TestUsageReportNullableWindows(t *testing.T) {
	// A nil window must serialize as JSON null (the app distinguishes null vs object).
	rep := NewUsageReport(nil, nil, nil)
	_, raw := marshalKeys(t, rep)
	if want := `"five_hour":null`; !contains(raw, want) {
		t.Fatalf("nil five_hour should be null; got %s", raw)
	}

	util := 0.42
	resets := "2026-01-01T00:00:00Z"
	rep2 := NewUsageReport(&UsageWindow{Utilization: &util, ResetsAt: &resets}, nil, nil)
	_, raw2 := marshalKeys(t, rep2)
	if !contains(raw2, `"utilization":0.42`) {
		t.Fatalf("utilization not serialized: %s", raw2)
	}
}

func TestSessionsListNeverNull(t *testing.T) {
	// An empty list must serialize as [] not null, so the app can iterate safely.
	_, raw := marshalKeys(t, NewSessionsList(nil))
	if !contains(raw, `"sessions":[]`) {
		t.Fatalf("empty sessions should be []; got %s", raw)
	}
}

func TestWorkSnapshotNeverUsesNullCollections(t *testing.T) {
	_, raw := marshalKeys(t, NewWorkSnapshot(workitems.DeviceSnapshot{}))
	for _, want := range []string{`"projects":[]`, `"items":[]`, `"session_links":[]`, `"dependencies":[]`, `"comments":[]`} {
		if !contains(raw, want) {
			t.Fatalf("work snapshot missing %s: %s", want, raw)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
