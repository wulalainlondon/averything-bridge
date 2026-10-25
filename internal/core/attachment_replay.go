package core

import (
	"log"
	"os"
	"strings"
	"time"

	"everything-go/internal/attachmentjournal"
	"everything-go/internal/protocol"
)

type attachmentReplayLease struct {
	owner     *Client
	sessionID string
	batchID   string
	records   []attachmentjournal.Record
}

func attachmentReplayKey(deviceID, sessionID string) string {
	return deviceID + "\x00" + sessionID
}

func (h *Hub) emitAttachment(event any) {
	// Fresh room workers have no host-file output authority. Room attachments
	// are saved through the scoped hash/blob gateway, never the global media
	// URL registry or per-device legacy attachment journal.
	if h.workspaceRoomNativeEvent(event) {
		logOutbound(workspaceWire{Type: "workspace_artifact_requires_room_gateway"})
		return
	}
	record, added := h.attachments.Add(event)
	if !added {
		return
	}
	log.Printf("[attachment] registered id=%s session=%s request=%s seq=%d path=%q",
		record.AttachmentID, record.SessionID, record.RequestID, record.Sequence, record.Path)
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		h.startAttachmentReplay(c, record.SessionID)
	}
}

// startAttachmentReplay owns a lease per stable device identity. Different
// devices can progress independently, while reconnecting the same device keeps
// retrying its unacknowledged canonical records.
func (h *Hub) startAttachmentReplay(c *Client, sessionID string) {
	if c.deviceID == "" || sessionID == "" {
		return
	}
	key := attachmentReplayKey(c.deviceID, sessionID)
	if !c.supportsReplayAck {
		// Compatibility path for old clients. Delivery is best-effort because they
		// cannot ACK, but it remains isolated to this device.
		for _, record := range h.attachments.Pending(c.deviceID, sessionID, replayBatchSize) {
			c.enqueueEvent(h.materializeAttachment(record))
			h.attachments.Ack(c.deviceID, []string{record.AttachmentID})
		}
		return
	}

	h.attachmentReplayMu.Lock()
	if lease := h.attachmentReplays[key]; lease != nil {
		if lease.owner == c {
			h.attachmentReplayMu.Unlock()
			return
		}
		select {
		case <-lease.owner.quit:
			delete(h.attachmentReplays, key)
		default:
			h.attachmentReplayMu.Unlock()
			return
		}
	}
	records := h.attachments.Pending(c.deviceID, sessionID, replayBatchSize)
	if len(records) == 0 {
		h.attachmentReplayMu.Unlock()
		return
	}
	lease := &attachmentReplayLease{owner: c, sessionID: sessionID, batchID: "attachment-" + randomID(), records: records}
	h.attachmentReplays[key] = lease
	h.attachmentReplayMu.Unlock()
	h.sendAttachmentLease(c, lease)
}

func (h *Hub) sendAttachmentLease(c *Client, lease *attachmentReplayLease) {
	events := make([]any, 0, len(lease.records))
	for _, record := range lease.records {
		events = append(events, h.materializeAttachment(record))
	}
	remaining := len(h.attachments.Pending(c.deviceID, lease.sessionID, replayBatchSize*2)) - len(lease.records)
	if remaining < 0 {
		remaining = 0
	}
	log.Printf("[attachment-replay] send batch=%s device=%s count=%d remaining=%d",
		lease.batchID, c.deviceID, len(events), remaining)
	c.enqueueEvent(protocol.NewOfflineReplayBatch(lease.batchID, events, remaining))
	time.AfterFunc(replayAckTimeout, func() { h.retryAttachmentReplay(c, lease.batchID) })
}

func (h *Hub) retryAttachmentReplay(c *Client, batchID string) {
	h.attachmentReplayMu.Lock()
	var key string
	var lease *attachmentReplayLease
	for candidate, active := range h.attachmentReplays {
		if active.owner == c && active.batchID == batchID {
			key, lease = candidate, active
			break
		}
	}
	if lease == nil || lease.owner != c || lease.batchID != batchID {
		h.attachmentReplayMu.Unlock()
		return
	}
	select {
	case <-c.quit:
		delete(h.attachmentReplays, key)
		h.attachmentReplayMu.Unlock()
		return
	default:
	}
	h.attachmentReplayMu.Unlock()
	h.sendAttachmentLease(c, lease)
}

// ackAttachmentReplay returns true for every attachment-prefixed batch,
// including stale ACKs, so it can never fall through and mutate the unrelated
// global offline journal.
func (h *Hub) ackAttachmentReplay(c *Client, batchID string) bool {
	if !strings.HasPrefix(batchID, "attachment-") {
		return false
	}
	h.attachmentReplayMu.Lock()
	var key string
	var lease *attachmentReplayLease
	for candidate, active := range h.attachmentReplays {
		if active.owner == c && active.batchID == batchID {
			key, lease = candidate, active
			break
		}
	}
	if lease == nil || lease.owner != c || lease.batchID != batchID {
		h.attachmentReplayMu.Unlock()
		log.Printf("[attachment-replay] ignored stale ack batch=%s device=%s", batchID, c.deviceID)
		return true
	}
	delete(h.attachmentReplays, key)
	h.attachmentReplayMu.Unlock()
	ids := make([]string, len(lease.records))
	for i, record := range lease.records {
		ids[i] = record.AttachmentID
	}
	committed := h.attachments.Ack(c.deviceID, ids)
	log.Printf("[attachment-replay] ack batch=%s device=%s committed=%d", batchID, c.deviceID, committed)
	h.startAttachmentReplay(c, lease.sessionID)
	return true
}

func (h *Hub) releaseAttachmentReplay(c *Client) {
	if c.deviceID == "" {
		return
	}
	h.attachmentReplayMu.Lock()
	for key, lease := range h.attachmentReplays {
		if lease.owner == c {
			delete(h.attachmentReplays, key)
		}
	}
	h.attachmentReplayMu.Unlock()
}

func (h *Hub) materializeAttachment(record attachmentjournal.Record) any {
	status := "available"
	url := h.mediaScan.LocalURL(record.Path)
	if info, err := os.Stat(record.Path); err != nil || info.IsDir() {
		status = "missing"
		url = ""
	}
	if record.Kind == "document" {
		return protocol.Document{
			Type: "document", SessionID: record.SessionID, RequestID: record.RequestID,
			Path: record.Path, URL: url, Title: record.DisplayName, DocType: record.DocumentType,
			AttachmentID: record.AttachmentID, MIMEType: record.MIMEType, CreatedAt: record.CreatedAt,
			Sequence: record.Sequence, Status: status,
		}
	}
	return protocol.Media{
		Type: "media", SessionID: record.SessionID, RequestID: record.RequestID,
		MediaType: record.MediaType, Path: record.Path, URL: url,
		AttachmentID: record.AttachmentID, MIMEType: record.MIMEType, DisplayName: record.DisplayName,
		CreatedAt: record.CreatedAt, Sequence: record.Sequence, Status: status,
	}
}
