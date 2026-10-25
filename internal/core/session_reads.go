package core

import (
	"errors"
	"log"

	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/runtimejournal"
)

type pairedReadIdentity struct{ token, deviceID string }

func (h *Hub) sharedReadAuthorized(c *Client) bool {
	identity := c.readIdentity.Load()
	return identity != nil && c.supportsSessionReadSync.Load() &&
		h.pairing.MatchesDevice(identity.token, identity.deviceID) && h.runtimes.SharedReadsAvailable()
}

func (h *Hub) sharedReadDevices() []string {
	bindings := h.pairing.DeviceBindings()
	ids := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		if binding.DeviceID != "" {
			ids = append(ids, binding.DeviceID)
		}
	}
	return ids
}

func (h *Hub) runtimeViewsForClient(c *Client, ids []string) []runtimejournal.View {
	if h.sharedReadAuthorized(c) {
		identity := c.readIdentity.Load()
		if views, err := h.runtimes.SharedSnapshot(identity.deviceID, ids, h.sharedReadDevices()); err == nil {
			return views
		} else {
			log.Printf("[shared-read] snapshot unavailable: %v", err)
		}
	}
	return h.runtimes.Snapshot(c.deviceID, ids)
}

func (h *Hub) runtimeSnapshotForClient(c *Client) protocol.SessionRuntimeSnapshot {
	snapshot := h.runtimeSnapshot(c.deviceID)
	if !h.sharedReadAuthorized(c) {
		return snapshot
	}
	ids := make([]string, 0, len(snapshot.Items))
	for _, runtime := range snapshot.Items {
		ids = append(ids, runtime.SessionID)
	}
	views := h.runtimeViewsForClient(c, ids)
	snapshot.Items = make([]protocol.SessionRuntime, 0, len(views))
	for _, view := range views {
		snapshot.Items = append(snapshot.Items, runtimeEvent(view))
	}
	return snapshot
}

func readStateEvent(state runtimejournal.SharedReadState) protocol.SessionReadState {
	return protocol.SessionReadState{Type: "session_read_state", SessionID: state.SessionID,
		ReadEpoch: state.ReadEpoch, ReadRevision: state.ReadRevision, ReadVersion: state.ReadVersion,
		RuntimeRevision: state.RuntimeRevision, Unread: state.Unread, LastCompletedRevision: state.LastCompletedRevision}
}

// Capture before asynchronous history work, and bind its cache key to that
// boundary. A completion arriving during a slow history load must not be
// consumed by the old response. Pagination never supplies a readable boundary.
func (h *Hub) captureHistoryReadBoundary(c *Client, cmd clientproto.Command) clientproto.Command {
	if !h.sharedReadAuthorized(c) || cmd.Before != "" {
		cmd.ReadEpoch, cmd.Revision = "", 0
		return cmd
	}
	views := h.runtimeViewsForClient(c, []string{cmd.SessionID})
	if len(views) != 1 || views[0].ReadEpoch == "" {
		cmd.ReadEpoch, cmd.Revision = "", 0
		return cmd
	}
	current := views[0]
	if cmd.ReadEpoch == "" {
		cmd.ReadEpoch, cmd.Revision = current.ReadEpoch, current.Revision
	}
	if cmd.ReadEpoch != current.ReadEpoch || cmd.Revision == 0 || cmd.Revision > current.Revision {
		cmd.ReadEpoch, cmd.Revision = "", 0
	}
	if cmd.ReadEpoch != "" {
		cmd.ReadToken, _ = h.runtimes.ReadBoundaryToken(cmd.SessionID, cmd.ReadEpoch, cmd.Revision)
	}
	return cmd
}

func (h *Hub) confirmHistoryReadBoundary(c *Client, cmd clientproto.Command) (string, uint64, string) {
	if !h.sharedReadAuthorized(c) || cmd.ReadEpoch == "" || cmd.Revision == 0 || cmd.Before != "" {
		return "", 0, ""
	}
	views := h.runtimeViewsForClient(c, []string{cmd.SessionID})
	if len(views) != 1 || views[0].ReadEpoch != cmd.ReadEpoch || views[0].Revision < cmd.Revision {
		return "", 0, ""
	}
	token, err := h.runtimes.ReadBoundaryToken(cmd.SessionID, cmd.ReadEpoch, cmd.Revision)
	if err != nil || token == "" || token != cmd.ReadToken {
		return "", 0, ""
	}
	return cmd.ReadEpoch, cmd.Revision, token
}

func (h *Hub) handleSharedRead(c *Client, cmd clientproto.Command) {
	reject := func(reason string, retryable bool) {
		token := ""
		if len(cmd.ReadToken) == 64 {
			token = cmd.ReadToken
		}
		c.enqueueEvent(protocol.SessionReadRejected{Type: "session_read_rejected", SessionID: cmd.SessionID,
			ReadEpoch: cmd.ReadEpoch, ReadToken: token, Revision: cmd.Revision, Reason: reason, Retryable: retryable})
	}
	if !h.sharedReadAuthorized(c) {
		reject("unauthorized", false)
		return
	}
	if _, exists := h.registry.Get(cmd.SessionID); !exists {
		reject("unknown_session", false)
		return
	}
	state, changed, err := h.runtimes.MarkSharedRead(cmd.SessionID, cmd.ReadEpoch, cmd.Revision, cmd.ReadToken)
	if err != nil {
		switch {
		case errors.Is(err, runtimejournal.ErrSharedReadStaleEpoch):
			reject("stale_epoch", false)
		case errors.Is(err, runtimejournal.ErrSharedReadInvalidRevision):
			reject("invalid_revision", false)
		case errors.Is(err, runtimejournal.ErrSharedReadStaleBoundary):
			reject("stale_boundary", false)
		case errors.Is(err, runtimejournal.ErrSharedReadUnknownSession):
			reject("unknown_session", false)
		default:
			log.Printf("[shared-read] persistence failed: %v", err)
			reject("persist_failed", true)
		}
		return
	}
	if !changed {
		// A single idempotent confirmation lets a reconnecting sender retire a
		// durable pending read. Receiving this event never triggers another ACK.
		c.enqueueEvent(readStateEvent(state))
		return
	}
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()
	for _, client := range clients {
		if h.sharedReadAuthorized(client) {
			client.enqueueEvent(readStateEvent(state))
		}
	}
}
