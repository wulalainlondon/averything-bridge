package core

import "testing"

func TestNewSessionRejectsMissingOrWhitespaceIDBeforeCreatingRegistryEntry(t *testing.T) {
	for _, raw := range []string{`{"type":"new_session","request_id":"r_test_request","name":"QA","backend":"claude"}`, `{"type":"new_session","session_id":"  ","request_id":"r_test_request","name":"QA","backend":"claude"}`} {
		h, _ := newTestHub(t)
		c := newTestClient(h)
		route(h, c, raw)
		e := waitForType(t, c, "error")
		if e["request_id"] != "r_test_request" {
			t.Fatal(e)
		}
		if len(h.registry.List()) != 0 {
			t.Fatal("invalid input created a session")
		}
	}
}
