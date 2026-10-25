package core

import (
	"fmt"
	"testing"
	"time"

	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
)

func sharedReadClient(t *testing.T, h *Hub, device string) *Client {
	t.Helper()
	token := "qa-read-" + device
	h.pairing.OpenEnrollment(time.Minute)
	if err := h.pairing.Claim(token, device); err != nil {
		t.Fatal(err)
	}
	c := attachmentClient(h, device)
	c.readIdentity.Store(&pairedReadIdentity{token: token, deviceID: device})
	c.supportsSessionReadSync.Store(true)
	return c
}

func markReadFrame(h *Hub, session, epoch string, revision uint64) string {
	token, _ := h.runtimes.ReadBoundaryToken(session, epoch, revision)
	return fmt.Sprintf(`{"type":"session_mark_read","session_id":%q,"read_epoch":%q,"revision":%d,"read_token":%q}`, session, epoch, revision, token)
}

func TestSharedReadBroadcastsAcrossPairedClientsWithoutAckLoop(t *testing.T) {
	h, _ := newTestHub(t)
	h.registry.Create("s1", "one", t.TempDir(), "codex", "", "", "")
	desktop, phone := sharedReadClient(t, h, "desktop"), sharedReadClient(t, h, "phone")
	legacy := attachmentClient(h, "legacy")
	h.Emit(protocol.NewDone("s1", "r1"))
	view := waitForType(t, desktop, "session_runtime")
	_ = waitForType(t, phone, "session_runtime")
	_ = waitForType(t, legacy, "session_runtime")
	epoch := view["read_epoch"].(string)
	revision := uint64(view["revision"].(float64))
	route(h, desktop, markReadFrame(h, "s1", epoch, revision))
	for _, c := range []*Client{desktop, phone} {
		read := waitForType(t, c, "session_read_state")
		if read["unread"] != float64(0) || read["runtime_revision"] != float64(revision) {
			t.Fatalf("read projection: %+v", read)
		}
	}
	if got := h.runtimeSnapshotForClient(phone).Items[0]; got.Unread != 0 || !got.HistoryReconcile || !got.DeliveryPending {
		t.Fatalf("shared read consumed phone delivery/history: %+v", got)
	}
	if got := h.runtimeSnapshotForClient(legacy).Items[0]; got.Unread != 1 || got.ReadEpoch != "" {
		t.Fatalf("legacy projection changed: %+v", got)
	}
	route(h, desktop, markReadFrame(h, "s1", epoch, revision))
	_ = waitForType(t, desktop, "session_read_state") // bounded duplicate confirmation
	route(h, phone, fmt.Sprintf(`{"type":"session_runtime_ack","session_id":"s1","revision":%d,"read":false}`, revision))
	for _, c := range []*Client{phone, legacy} {
		select {
		case event := <-c.send:
			t.Fatalf("duplicate/receipt echo: %s", event)
		default:
		}
	}
}

func TestSharedReadRejectsUnpairedRevokedAndLegacyClients(t *testing.T) {
	h, _ := newTestHub(t)
	h.registry.Create("s1", "one", t.TempDir(), "codex", "", "", "")
	owner := sharedReadClient(t, h, "desktop")
	h.Emit(protocol.NewDone("s1", "r1"))
	view := waitForType(t, owner, "session_runtime")
	frame := markReadFrame(h, "s1", view["read_epoch"].(string), uint64(view["revision"].(float64)))
	for _, c := range []*Client{attachmentClient(h, "unpaired"), attachmentClient(h, "legacy")} {
		c.supportsSessionReadSync.Store(true) // a hello flag is not membership
		route(h, c, frame)
		if rejected := waitForType(t, c, "session_read_rejected"); rejected["reason"] != "unauthorized" {
			t.Fatalf("unpaired accepted: %+v", rejected)
		}
	}
	if err := h.pairing.Unclaim("qa-read-desktop"); err != nil {
		t.Fatal(err)
	}
	route(h, owner, frame)
	if rejected := waitForType(t, owner, "session_read_rejected"); rejected["reason"] != "unauthorized" {
		t.Fatalf("revoked accepted: %+v", rejected)
	}
}

func TestSharedReadOldBoundaryDoesNotConsumeConcurrentReply(t *testing.T) {
	h, _ := newTestHub(t)
	h.registry.Create("s1", "one", t.TempDir(), "codex", "", "", "")
	reader := sharedReadClient(t, h, "desktop")
	h.Emit(protocol.NewDone("s1", "r1"))
	first := waitForType(t, reader, "session_runtime")
	h.updateRuntime("s1", "running", "r2", 0, "", "")
	h.Emit(protocol.NewDone("s1", "r2"))
	_ = waitForType(t, reader, "done")
	_ = waitForType(t, reader, "session_runtime")
	route(h, reader, markReadFrame(h, "s1", first["read_epoch"].(string), uint64(first["revision"].(float64))))
	if state := waitForType(t, reader, "session_read_state"); state["unread"] != float64(1) {
		t.Fatalf("new reply lost: %+v", state)
	}
}

func TestHistoryReadBoundaryIsCapturedBeforeAsyncLoad(t *testing.T) {
	h, _ := newTestHub(t)
	h.registry.Create("s1", "one", t.TempDir(), "codex", "", "", "")
	reader := sharedReadClient(t, h, "desktop")
	h.Emit(protocol.NewDone("s1", "r1"))
	first := waitForType(t, reader, "session_runtime")
	request := h.captureHistoryReadBoundary(reader, clientproto.Command{Kind: "request_history", SessionID: "s1"})
	h.updateRuntime("s1", "running", "r2", 0, "", "")
	h.Emit(protocol.NewDone("s1", "r2"))
	epoch, revision, token := h.confirmHistoryReadBoundary(reader, request)
	if token != request.ReadToken || epoch != first["read_epoch"] || revision != uint64(first["revision"].(float64)) {
		t.Fatalf("history promoted to concurrent result: %s %d", epoch, revision)
	}
	page := h.captureHistoryReadBoundary(reader, clientproto.Command{Kind: "request_history", SessionID: "s1", Before: "older-message"})
	if page.ReadEpoch != "" || page.Revision != 0 {
		t.Fatal("older page acquired a read boundary")
	}
	bad := h.captureHistoryReadBoundary(reader, clientproto.Command{Kind: "request_history", SessionID: "s1", ReadEpoch: "wrong", Revision: 999})
	if bad.ReadEpoch != "" || bad.Revision != 0 {
		t.Fatal("stale/future history boundary accepted")
	}
}
