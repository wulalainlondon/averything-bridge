package core

import (
	"log"
	"time"

	"everything-go/internal/protocol"
)

const (
	replayBatchSize     = 64
	replayBatchMaxBytes = 256 * 1024
	replayAckTimeout    = 10 * time.Second
	legacyReplayPace    = 20 * time.Millisecond
)

type replayLease struct {
	owner   *Client
	batchID string
	events  []any
	count   int
}

// startOfflineReplay allows exactly one in-flight batch globally. The journal
// is only committed by ackOfflineReplay, so app backgrounding or a Tailscale
// flap resends the same batch instead of losing the unqueued tail.
func (h *Hub) startOfflineReplay(c *Client) {
	if !c.supportsReplayAck {
		h.replayMu.Lock()
		if h.replayLease != nil {
			h.replayMu.Unlock()
			return
		}
		h.replayLease = &replayLease{owner: c, batchID: "legacy-" + randomID()}
		h.replayMu.Unlock()
		go h.replayOfflineLegacy(c)
		return
	}

	h.replayMu.Lock()
	if h.replayLease != nil {
		if h.replayLease.owner == c {
			h.replayMu.Unlock()
			return
		}
		select {
		case <-h.replayLease.owner.quit:
			h.replayLease = nil
		default:
			h.replayMu.Unlock()
			return
		}
	}
	events := h.offline.PeekSized(replayBatchSize, replayBatchMaxBytes)
	if len(events) == 0 {
		h.replayMu.Unlock()
		return
	}
	lease := &replayLease{owner: c, batchID: randomID(), events: events, count: len(events)}
	h.replayLease = lease
	remaining := h.offline.Len() - len(events)
	h.replayMu.Unlock()

	log.Printf("[replay] send batch=%s client=%s count=%d remaining=%d", lease.batchID, c.clientID, lease.count, remaining)
	c.enqueueEvent(protocol.NewOfflineReplayBatch(lease.batchID, lease.events, remaining))
	time.AfterFunc(replayAckTimeout, func() { h.retryOfflineReplay(c, lease.batchID) })
}

func (h *Hub) retryOfflineReplay(c *Client, batchID string) {
	h.replayMu.Lock()
	lease := h.replayLease
	if lease == nil || lease.owner != c || lease.batchID != batchID {
		h.replayMu.Unlock()
		return
	}
	select {
	case <-c.quit:
		h.replayLease = nil
		h.replayMu.Unlock()
		return
	default:
	}
	remaining := h.offline.Len() - lease.count
	if remaining < 0 {
		remaining = 0
	}
	event := protocol.NewOfflineReplayBatch(lease.batchID, lease.events, remaining)
	h.replayMu.Unlock()
	log.Printf("[replay] ack timeout; resend batch=%s client=%s count=%d", batchID, c.clientID, lease.count)
	c.enqueueEvent(event)
	time.AfterFunc(replayAckTimeout, func() { h.retryOfflineReplay(c, batchID) })
}

func (h *Hub) ackOfflineReplay(c *Client, batchID string) {
	if batchID == "" {
		return
	}
	if h.ackAttachmentReplay(c, batchID) {
		return
	}
	h.replayMu.Lock()
	lease := h.replayLease
	if lease == nil || lease.owner != c || lease.batchID != batchID {
		h.replayMu.Unlock()
		log.Printf("[replay] ignored stale ack batch=%s client=%s", batchID, c.clientID)
		return
	}
	h.replayLease = nil
	h.replayMu.Unlock()

	committed := h.offline.Commit(lease.count)
	log.Printf("[replay] ack batch=%s client=%s committed=%d remaining=%d", batchID, c.clientID, committed, h.offline.Len())
	h.startOfflineReplay(c)
}

func (h *Hub) releaseReplayLease(c *Client) {
	h.replayMu.Lock()
	if h.replayLease != nil && h.replayLease.owner == c {
		log.Printf("[replay] release unacked batch=%s client=%s", h.replayLease.batchID, c.clientID)
		h.replayLease = nil
	}
	h.replayMu.Unlock()
}

// Legacy clients cannot ACK. Throttle individual events through the write
// queue and commit only what was accepted, avoiding the former all-at-once
// 2045→1024 overflow. New release clients always use the reliable path above.
func (h *Hub) replayOfflineLegacy(c *Client) {
	defer h.releaseReplayLease(c)
	for c.live() {
		events := h.offline.Peek(1)
		if len(events) == 0 {
			return
		}
		data, err := marshalEvent(events[0], c.wireAuthority())
		if err != nil {
			log.Printf("[replay] legacy marshal failed: %v", err)
			h.offline.Commit(1)
			continue
		}
		select {
		case c.send <- data:
			h.offline.Commit(1)
		case <-c.quit:
			return
		}
		time.Sleep(legacyReplayPace)
	}
}
