package core

import (
	"testing"
	"time"

	"everything-go/internal/fcm"
	"everything-go/internal/protocol"
)

func TestRuntimeDeliveryAndReadArePerDevice(t *testing.T) {
	h, _ := newTestHub(t)
	h.registry.Create("s1", "one", t.TempDir(), "codex", "", "", "")
	desktop := attachmentClient(h, "desktop")
	phone := attachmentClient(h, "phone")

	h.Emit(protocol.NewDone("s1", "r1"))
	desktopRuntime := waitForType(t, desktop, "session_runtime")
	phoneRuntime := waitForType(t, phone, "session_runtime")
	if desktopRuntime["unread"] != float64(1) || phoneRuntime["unread"] != float64(1) {
		t.Fatalf("initial unread desktop=%v phone=%v", desktopRuntime, phoneRuntime)
	}
	revision := uint64(desktopRuntime["revision"].(float64))
	route(h, desktop, `{"type":"session_runtime_ack","session_id":"s1","revision":`+formatUint(revision)+`,"read":true}`)
	select {
	case data := <-desktop.send:
		t.Fatalf("runtime ACK must be one-way, got echo %s", data)
	case <-time.After(50 * time.Millisecond):
	}
	desktopSnapshot := h.runtimeSnapshot("desktop")
	if len(desktopSnapshot.Items) != 1 || desktopSnapshot.Items[0].Unread != 0 {
		t.Fatalf("desktop unread after ACK=%+v", desktopSnapshot.Items)
	}

	phoneSnapshot := h.runtimeSnapshot("phone")
	if len(phoneSnapshot.Items) != 1 || phoneSnapshot.Items[0].Unread != 1 {
		t.Fatalf("desktop ACK consumed phone state: %+v", phoneSnapshot.Items)
	}
}

func formatUint(v uint64) string {
	if v == 0 {
		return "0"
	}
	buf := [20]byte{}
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

func TestRuntimeSnapshotSurvivesOtherOnlineClient(t *testing.T) {
	h, _ := newTestHub(t)
	h.registry.Create("s1", "one", t.TempDir(), "codex", "", "", "")
	desktop := attachmentClient(h, "desktop")
	h.Emit(protocol.NewDone("s1", "r1"))
	_ = waitForType(t, desktop, "done")
	_ = waitForType(t, desktop, "session_runtime")

	phone := h.runtimeSnapshot("phone")
	if len(phone.Items) != 1 || phone.Items[0].Phase != "completed" || phone.Items[0].Unread != 1 {
		t.Fatalf("offline phone did not recover terminal snapshot: %+v", phone.Items)
	}
}

func TestRuntimeStatusPushUsesAuthoritativeRevisionAndSessionName(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.InstanceID = "wulala"
	h.cfg.InstanceName = "Wulala"
	h.registry.Create("s1", "Release QA", t.TempDir(), "codex", "", "", "")
	type pushed struct {
		instanceID, instanceName, sessionID, name, phase, stage, message, activeRequestID string
		revision                                                                          uint64
		updatedAt, activeStartedAt                                                        int64
		queueLength                                                                       int
	}
	ch := make(chan pushed, 1)
	h.runtimeStatusPush = func(instanceID, instanceName, sessionID, name, phase, stage, message string,
		revision uint64, updatedAt, activeStartedAt int64, activeRequestID string, queueLength int, _ fcm.ReplyAction) {
		ch <- pushed{instanceID, instanceName, sessionID, name, phase, stage, message, activeRequestID, revision, updatedAt, activeStartedAt, queueLength}
	}
	h.updateRuntime("s1", "running", "r1", 0, "", "")
	select {
	case got := <-ch:
		if got.instanceID != "wulala" || got.instanceName != "Wulala" || got.sessionID != "s1" || got.name != "Release QA" ||
			got.phase != "running" || got.activeRequestID != "r1" || got.revision == 0 || got.updatedAt == 0 || got.activeStartedAt == 0 {
			t.Fatalf("runtime status projection=%+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime status push was not projected")
	}
}
