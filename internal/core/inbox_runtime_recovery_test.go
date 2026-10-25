package core

import (
	"database/sql"
	"encoding/json"
	"everything-go/internal/automation"
	"path/filepath"
	"testing"
)

func TestRuntimeRefreshIncludesLegacyCompatibleRoute(t *testing.T) {
	h, _ := newTestHub(t)
	dir := t.TempDir()
	auto := attachAutomationStore(t, h, dir)
	_, _, err := auto.UpsertRoute(t.Context(), automation.Route{ID: "legacy", Name: "Legacy review", Enabled: true, SourcePattern: "judicial.ops.health", KindPattern: "ops.hourly", SessionID: "original-session", HandlingMode: automation.AnalyzeForReview, RunKind: "research", ReviewOnly: true}, 0)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "everything_go_automation.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`UPDATE external_event_routes SET handling_mode='review' WHERE id='legacy'`); err != nil {
		t.Fatal(err)
	}
	c := newTestClient(h)
	route(h, c, `{"type":"request_runtime_snapshot"}`)
	waitForType(t, c, "session_runtime_snapshot")
	event := waitForType(t, c, "automation_snapshot")
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	// Public synthetic payload only; the external check runs the installed phone
	// schema over these actual server-serialized recovery bytes.
	t.Logf("PHONE_SCHEMA_FIXTURE=%s", data)
}

func TestRuntimeRefreshRestoresAutomationForReattachedWebView(t *testing.T) {
	h, _ := newTestHub(t)
	attachAutomationStore(t, h, t.TempDir())
	c := newTestClient(h)
	route(h, c, `{"type":"request_runtime_snapshot"}`)
	waitForType(t, c, "session_runtime_snapshot")
	event := waitForType(t, c, "automation_snapshot")
	snapshot, ok := event["snapshot"].(map[string]any)
	if !ok || snapshot["inbox_version"] != float64(2) {
		t.Fatalf("reattached phone retained obsolete capability: %+v", event)
	}
}

func TestRuntimeRefreshKeepsAutomationScopedToRequestingClient(t *testing.T) {
	h, _ := newTestHub(t)
	attachAutomationStore(t, h, t.TempDir())
	a, b := newTestClient(h), newTestClient(h)
	route(h, a, `{"type":"request_runtime_snapshot"}`)
	waitForType(t, a, "session_runtime_snapshot")
	waitForType(t, a, "automation_snapshot")
	select {
	case frame := <-b.send:
		t.Fatalf("another client received this refresh: %s", frame)
	default:
	}
}

func TestUnpairedAndProbeRuntimeRefreshCannotReadAutomation(t *testing.T) {
	for _, probe := range []bool{false, true} {
		h, _ := newTestHub(t)
		attachAutomationStore(t, h, t.TempDir())
		c := newTestClient(h)
		c.inventoryProbe = probe
		c.enrollmentOnly = !probe
		route(h, c, `{"type":"request_runtime_snapshot"}`)
		waitForType(t, c, "error")
		select {
		case frame := <-c.send:
			t.Fatalf("unpaired/probe received snapshot: %s", frame)
		default:
		}
	}
}

func TestRuntimeRefreshWithoutAutomationStillReturnsRuntime(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	route(h, c, `{"type":"request_runtime_snapshot"}`)
	waitForType(t, c, "session_runtime_snapshot")
	select {
	case frame := <-c.send:
		t.Fatalf("unexpected extra disclosure without automation: %s", frame)
	default:
	}
}
