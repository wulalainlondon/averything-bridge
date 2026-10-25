package core

import (
	"encoding/json"
	"testing"
	"time"
)

func TestMachineResourceParsingPreservesUnknownAndReservedSpace(t *testing.T) {
	a, b, c := parseResourceDF("Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/test 1000 600 350 64% /home\n")
	if a == nil || *a != 1024000 || *b != 614400 || *c != 358400 {
		t.Fatal(a, b, c)
	}
	for _, s := range []string{"", "header\n/dev/test 0 0 0 0% /", "header\n/dev/test 100 200 20 0% /"} {
		a, _, _ = parseResourceDF(s)
		if a != nil {
			t.Fatal("invalid disk data accepted")
		}
	}
	data := "Mach Virtual Memory Statistics: (page size of 16384 bytes)\nPages active: 10.\nPages wired down: 5.\nPages occupied by compressor: 2.\n"
	n := parseDarwinResourceMemory(100*16384, data)
	if n == nil || *n != 17*16384 {
		t.Fatal(n)
	}
	if parseDarwinResourceMemory(100*16384, "unknown") != nil {
		t.Fatal("unknown RAM became zero")
	}
}

func TestMachineResourceReadIsCorrelatedAndDoesNotCreateATurn(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.InstanceID = "resource-fixture"
	c := newTestClient(h)
	route(h, c, `{"type":"request_status","request_id":"resource:fixture"}`)
	select {
	case frame := <-c.send:
		var event struct {
			Type      string `json:"type"`
			RequestID string `json:"request_id"`
			Status    struct {
				Resources machineResourceSnapshot `json:"machine_resources"`
			} `json:"status"`
		}
		if err := json.Unmarshal(frame, &event); err != nil {
			t.Fatal(err)
		}
		if event.Type != "status_result" || event.RequestID != "resource:fixture" || event.Status.Resources.InstanceID != "resource-fixture" || event.Status.Resources.SampledAtMS <= 0 {
			t.Fatalf("wrong resource envelope: %+v", event)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bounded metadata read did not return")
	}
	if len(h.registry.List()) != 0 {
		t.Fatal("metadata read created a conversation")
	}
}
