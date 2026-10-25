package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/sessiondispatch"
	"io"
	"strings"
)

func controllerClientContext(c *Client) context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}

func (h *Hub) CloseSessionDispatches() error {
	if h.dispatches == nil {
		return nil
	}
	return h.dispatches.Close()
}
func (h *Hub) sendControllerStatus(c *Client, cmd clientproto.Command) {
	if h.dispatches == nil {
		return
	}
	parent, ok := h.registry.Get(cmd.SessionID)
	if !ok || !h.controllerInScope(parent) {
		return
	}
	g, e := h.dispatches.Grant(controllerClientContext(c), parent.ID)
	if e != nil {
		return
	}
	records, _ := h.dispatches.List(controllerClientContext(c), parent.ID)
	records = briefDispatches(records)
	peers := []map[string]any{}
	for id, peer := range h.relayPeers {
		inbound, _ := h.dispatches.Grant(controllerClientContext(c), "peer:"+id)
		peers = append(peers, map[string]any{"instance_id": id, "instance_name": peer.InstanceName, "incoming": inbound})
	}
	c.enqueueEvent(map[string]any{"type": "session_controller_status", "session_id": parent.ID, "request_id": cmd.RequestID, "grant": g, "dispatches": records, "peers": peers, "instance_id": h.cfg.InstanceID})
}
func (h *Hub) setControllerGrant(c *Client, cmd clientproto.Command) {
	reject := func(e error) {
		c.enqueueEvent(map[string]any{"type": "session_controller_result", "session_id": cmd.SessionID, "request_id": cmd.RequestID, "accepted": false, "error": e.Error()})
	}
	if h.dispatches == nil {
		reject(errors.New("controller_unavailable"))
		return
	}
	parent, ok := h.registry.Get(cmd.SessionID)
	if !ok || !h.controllerInScope(parent) || parent.Backend() != backend.Codex || strings.HasPrefix(parent.ID, "s_dg_") || !h.controls.MobileMayWrite(parent.ID) {
		reject(errors.New("controller_parent_forbidden"))
		return
	}
	if policy, e := h.PMConfiguration(parent.ID); e != nil || policy != nil {
		reject(errors.New("controller_managed_parent_forbidden"))
		return
	}
	var input struct {
		Grant  sessiondispatch.Grant `json:"grant"`
		PeerID string                `json:"peer_id,omitempty"`
	}
	decoder := json.NewDecoder(strings.NewReader(cmd.Data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		reject(errors.New("controller_grant_invalid"))
		return
	}
	if len(input.Grant.Instances) > 12 || len(input.Grant.Sessions) > 100 {
		reject(errors.New("controller_grant_limit"))
		return
	}
	for _, id := range input.Grant.Instances {
		if _, ok := h.relayPeers[id]; !ok {
			reject(errors.New("controller_peer_unconfigured"))
			return
		}
	}
	for _, key := range input.Grant.Sessions {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 || parts[1] == "" || len(key) > 320 {
			reject(errors.New("controller_session_scope_invalid"))
			return
		}
		if parts[0] != h.cfg.InstanceID {
			if _, ok := h.relayPeers[parts[0]]; !ok {
				reject(errors.New("controller_peer_unconfigured"))
				return
			}
		}
	}
	key := parent.ID
	if input.PeerID != "" {
		if _, ok := h.relayPeers[input.PeerID]; !ok {
			reject(errors.New("controller_peer_unconfigured"))
			return
		}
		key = "peer:" + input.PeerID
	}
	grant, e := h.dispatches.SetGrant(controllerClientContext(c), key, input.Grant, cmd.Revision)
	if e != nil {
		reject(e)
		return
	}
	c.enqueueEvent(map[string]any{"type": "session_controller_result", "session_id": parent.ID, "request_id": cmd.RequestID, "accepted": true, "grant": grant, "peer_id": input.PeerID})
	h.sendControllerStatus(c, cmd)
}
