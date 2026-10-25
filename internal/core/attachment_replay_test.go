package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func attachmentClient(h *Hub, device string) *Client {
	c := newTestClient(h)
	c.clientID = device + "-client"
	c.deviceID = device
	c.supportsReplayAck = true
	h.registerLatest(c)
	return c
}

func attachmentFromBatch(t *testing.T, c *Client) (map[string]any, string) {
	t.Helper()
	batch := waitForType(t, c, "offline_replay_batch")
	events, ok := batch["events"].([]any)
	if !ok || len(events) == 0 {
		t.Fatalf("invalid attachment batch: %v", batch)
	}
	event, ok := events[0].(map[string]any)
	if !ok {
		t.Fatalf("invalid nested attachment: %v", events[0])
	}
	batchID, _ := batch["batch_id"].(string)
	if !strings.HasPrefix(batchID, "attachment-") {
		t.Fatalf("unexpected batch id %q", batchID)
	}
	return event, batchID
}

func ackAttachment(t *testing.T, h *Hub, c *Client, batchID string) {
	t.Helper()
	route(h, c, `{"type":"offline_replay_ack","batch_id":"`+batchID+`"}`)
}

func assertNoOutbound(t *testing.T, c *Client) {
	t.Helper()
	select {
	case data := <-c.send:
		var event map[string]any
		_ = json.Unmarshal(data, &event)
		t.Fatalf("unexpected outbound event: %v", event)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAttachmentDeliveryIsPerDevice(t *testing.T) {
	h, _ := newTestHub(t)
	desktop := attachmentClient(h, "desktop")
	phone := attachmentClient(h, "phone")
	path := filepath.Join(t.TempDir(), "image.png")
	if err := os.WriteFile(path, []byte("png"), 0o600); err != nil {
		t.Fatal(err)
	}

	h.Emit(protocol.Media{Type: "media", SessionID: "s1", RequestID: "r1", MediaType: "image", Path: path})
	de, desktopBatch := attachmentFromBatch(t, desktop)
	pe, phoneBatch := attachmentFromBatch(t, phone)
	if de["attachment_id"] == "" || de["attachment_id"] != pe["attachment_id"] {
		t.Fatalf("devices received different canonical identity: desktop=%v phone=%v", de, pe)
	}
	ackAttachment(t, h, desktop, desktopBatch)
	if pending := h.attachments.Pending("phone", "s1", 10); len(pending) != 1 {
		t.Fatalf("desktop ACK consumed phone delivery: %+v", pending)
	}
	ackAttachment(t, h, phone, phoneBatch)
	h.startAttachmentReplay(phone, "s1")
	assertNoOutbound(t, phone)
}

func TestOfflineDeviceReconnectAndRepeatedReconnectDedupes(t *testing.T) {
	h, _ := newTestHub(t)
	desktop := attachmentClient(h, "desktop")
	path := filepath.Join(t.TempDir(), "offline.png")
	if err := os.WriteFile(path, []byte("png"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.Emit(protocol.Media{Type: "media", SessionID: "s1", RequestID: "r1", MediaType: "image", Path: path})
	_, desktopBatch := attachmentFromBatch(t, desktop)
	ackAttachment(t, h, desktop, desktopBatch)

	phone := attachmentClient(h, "phone")
	h.startAttachmentReplay(phone, "s1")
	pe, phoneBatch := attachmentFromBatch(t, phone)
	if pe["session_id"] != "s1" || pe["request_id"] != "r1" {
		t.Fatalf("wrong replayed attachment: %v", pe)
	}
	ackAttachment(t, h, phone, phoneBatch)
	phone.shutdown()
	h.removeClient(phone)
	reconnected := attachmentClient(h, "phone")
	h.startAttachmentReplay(reconnected, "s1")
	assertNoOutbound(t, reconnected)
}

func TestAttachmentReplaySurvivesRestartAndRegeneratesURL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restart.png")
	if err := os.WriteFile(path, []byte("png"), 0o600); err != nil {
		t.Fatal(err)
	}
	reg := session.NewRegistry()
	h1 := NewHub(reg, Config{InstanceID: "i1", DataDir: dir}, governance.NewPairing(filepath.Join(dir, "pairing.json")), 8766)
	h1.SetTunnelURL("https://old.example")
	h1.Emit(protocol.Media{Type: "media", SessionID: "s1", RequestID: "r1", MediaType: "image", Path: path})

	h2 := NewHub(session.NewRegistry(), Config{InstanceID: "i1", DataDir: dir}, governance.NewPairing(filepath.Join(dir, "pairing.json")), 8766)
	h2.SetTunnelURL("https://new.example")
	phone := attachmentClient(h2, "phone")
	h2.startAttachmentReplay(phone, "s1")
	event, batchID := attachmentFromBatch(t, phone)
	if url, _ := event["url"].(string); !strings.HasPrefix(url, "https://new.example/media/") || strings.Contains(url, "old.example") {
		t.Fatalf("replay did not regenerate URL: %q", url)
	}
	ackAttachment(t, h2, phone, batchID)
}

func TestMissingAttachmentDoesNotBlockLaterReplayOrCrossSessions(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.png")
	valid := filepath.Join(dir, "valid.pdf")
	other := filepath.Join(dir, "other.pdf")
	if err := os.WriteFile(missing, []byte("png"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(valid, []byte("pdf"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(other, []byte("pdf"), 0o600); err != nil {
		t.Fatal(err)
	}
	h, _ := newTestHub(t)
	h.Emit(protocol.Media{Type: "media", SessionID: "s1", RequestID: "r1", MediaType: "image", Path: missing})
	h.Emit(protocol.Document{Type: "document", SessionID: "s1", RequestID: "r2", Path: valid, Title: "valid.pdf", DocType: "pdf"})
	h.Emit(protocol.Document{Type: "document", SessionID: "s2", RequestID: "r3", Path: other, Title: "other.pdf", DocType: "pdf"})
	if err := os.Remove(missing); err != nil {
		t.Fatal(err)
	}
	phone := attachmentClient(h, "phone")
	h.startAttachmentReplay(phone, "s1")
	batch := waitForType(t, phone, "offline_replay_batch")
	events := batch["events"].([]any)
	if len(events) != 2 {
		t.Fatalf("events=%v", events)
	}
	first := events[0].(map[string]any)
	second := events[1].(map[string]any)
	if first["status"] != "missing" || first["url"] != "" || first["session_id"] != "s1" {
		t.Fatalf("missing attachment state=%v", first)
	}
	if second["status"] != "available" || second["session_id"] != "s1" || second["type"] != "document" {
		t.Fatalf("later attachment corrupted=%v", second)
	}
	batchID, _ := batch["batch_id"].(string)
	ackAttachment(t, h, phone, batchID)
	assertNoOutbound(t, phone)

	// Restoring s1 must not send or ACK s2 before s2 history exists locally.
	h.startAttachmentReplay(phone, "s2")
	otherEvent, otherBatch := attachmentFromBatch(t, phone)
	if otherEvent["session_id"] != "s2" || otherEvent["request_id"] != "r3" {
		t.Fatalf("cross-session replay=%v", otherEvent)
	}
	ackAttachment(t, h, phone, otherBatch)
}

func TestTextAttachmentDoneOrdering(t *testing.T) {
	h, _ := newTestHub(t)
	phone := attachmentClient(h, "phone")
	dir := t.TempDir()
	path := filepath.Join(dir, "ordered.png")
	if err := os.WriteFile(path, []byte("png"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.registry.Create("s1", "ordered", dir, "codex", "", "", "")
	h.Emit(protocol.NewTextChunk("s1", "r1", "created "+path))
	h.Emit(protocol.NewDone("s1", "r1"))

	// Authoritative runtime snapshots may be interleaved with content events;
	// they must not change the content/attachment/terminal order itself.
	_ = waitForType(t, phone, "text_chunk")
	_ = waitForType(t, phone, "offline_replay_batch")
	_ = waitForType(t, phone, "done")
}
