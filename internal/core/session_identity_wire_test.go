package core

import (
	"encoding/json"
	"testing"
)

func TestScopeSessionIDsJSONScopesSessionEventsAndSummaryIDs(t *testing.T) {
	raw := []byte(`{"type":"sessions_list","sessions":[{"id":"same","name":"A","is_streaming":false,"created_at":1}],"unrelated":{"id":"keep"}}`)
	data, err := scopeSessionIDsJSON(raw, "wulala")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	sessions := got["sessions"].([]any)
	first := sessions[0].(map[string]any)
	if first["id"] != "sk1:wulala:same" {
		t.Fatalf("summary id=%v", first["id"])
	}
	if got["authority_instance_id"] != "wulala" {
		t.Fatalf("authority=%v", got["authority_instance_id"])
	}
	if got["unrelated"].(map[string]any)["id"] != "keep" {
		t.Fatal("scoped unrelated id")
	}
}

func TestProtocolV3AddsExplicitUnixMillisecondTimestampAliases(t *testing.T) {
	data, err := scopeSessionIDsJSON([]byte(`{"type":"sessions_list","sessions":[{"id":"s","created_at":123,"last_activity":1788280200.123}]}`), "wulala")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	session := got["sessions"].([]any)[0].(map[string]any)
	if session["created_at_unix_ms"] != float64(123000) || session["last_activity_unix_ms"] != float64(1788280200123) {
		t.Fatalf("timestamps=%s", data)
	}
}

func TestScopeSessionIDsAddsDomainSchemaVersions(t *testing.T) {
	for _, raw := range []string{
		`{"type":"session_runtime","session_id":"s","revision":1}`,
		`{"type":"work_snapshot","revision":1}`,
		`{"type":"external_event_snapshot","revision":1}`,
		`{"type":"media","session_id":"s","path":"/tmp/a.png"}`,
	} {
		data, err := scopeSessionIDsJSON([]byte(raw), "wulala")
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		if got["schema_version"] != float64(3) {
			t.Fatalf("event=%s got=%s", raw, data)
		}
	}
}

func TestScopeSessionIDsJSONScopesNestedReplayAndLists(t *testing.T) {
	raw := []byte(`{"type":"offline_replay_batch","events":[{"type":"done","session_id":"same"},{"type":"work","session_ids":["same"]}]}`)
	data, err := scopeSessionIDsJSON(raw, "morrie")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	events := got["events"].([]any)
	if events[0].(map[string]any)["session_id"] != "sk1:morrie:same" {
		t.Fatalf("nested session=%v", events[0])
	}
	ids := events[1].(map[string]any)["session_ids"].([]any)
	if ids[0] != "sk1:morrie:same" {
		t.Fatalf("nested session ids=%v", ids)
	}
}
