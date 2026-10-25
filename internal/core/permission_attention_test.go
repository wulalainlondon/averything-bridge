package core

import (
	"encoding/json"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"sync/atomic"
	"testing"
	"time"
)

func TestPermissionWrongResponderGetsOnlyItsCommandError(t *testing.T) {
	h, _ := newTestHub(t)
	wrong := newTestClient(h)
	wrong.deviceID = "synthetic-other"
	ready := make(chan string, 1)
	var terminal atomic.Int32
	h.perms = governance.NewPermissionManager(func(e any) {
		switch x := e.(type) {
		case protocol.PermissionRequest:
			ready <- x.RequestID
		case protocol.PermissionResult:
			terminal.Add(1)
		}
	}, "enforce")
	done := make(chan bool, 1)
	go func() { done <- h.perms.Request("synthetic-owner", "fixture.none", "t", "j", "", "low", "s1") }()
	id := <-ready
	body, _ := json.Marshal(map[string]any{"type": "permission_response", "request_id": id, "decision": "approve"})
	route(h, wrong, string(body))
	err := waitForType(t, wrong, "error")
	if err["request_id"] != id || err["command_type"] != "permission_response" || err["code"] != "permission_response_rejected" {
		t.Fatal("uncorrelated command error")
	}
	if terminal.Load() != 0 {
		t.Fatal("foreign responder broadcast a false terminal")
	}
	select {
	case <-done:
		t.Fatal("foreign response decided original gate")
	default:
	}
	if e := h.perms.Resolve(id, "approve", "synthetic-owner"); e != nil {
		t.Fatal(e)
	}
	select {
	case allowed := <-done:
		if !allowed {
			t.Fatal("owner not approved")
		}
	case <-time.After(time.Second):
		t.Fatal("owner response did not complete")
	}
	if terminal.Load() != 1 {
		t.Fatal("expected exactly one real terminal")
	}
}
