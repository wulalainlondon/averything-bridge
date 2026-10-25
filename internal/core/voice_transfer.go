package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/session"
	"everything-go/internal/voicetransfer"
	"strings"
	"time"
)

func transferTarget(s *session.Session) voicetransfer.Target {
	snap := s.Snapshot()
	return voicetransfer.Target{Session: s.ID, Thread: s.ResumeID(), Name: snap.Name, Revision: s.SettingsSnapshot().ConfigRevision}
}
func (h *Hub) validTransferTarget(s *session.Session) bool {
	if s == nil || s.Backend() != backend.Codex || s.ResumeID() == "" || !h.controllerInScope(s) || s.Snapshot().Hidden || s.State() == session.Closed || !h.controls.MobileMayWrite(s.ID) || strings.HasPrefix(s.ID, "s_dg_") || strings.HasPrefix(s.ID, "pm_") {
		return false
	}
	p, e := h.PMConfiguration(s.ID)
	return e == nil && p == nil
}
func (h *Hub) transferClient(caller backend.SessionControlCaller) (*Client, error) {
	if caller.VoiceID == "" || h.controllerCaller(caller) != nil {
		return nil, errors.New("voice_transfer_live_caller_required")
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	for c, call := range h.voiceClients {
		if c.live() && !call.stopping && call.session == caller.Parent && call.voiceID == caller.VoiceID && call.externalMusicPlatform == "android" && h.pairedTaskDevice(c) != "" {
			return c, nil
		}
	}
	return nil, errors.New("voice_transfer_live_caller_required")
}
func (h *Hub) requestVoiceTransfer(ctx context.Context, caller backend.SessionControlCaller, input backend.SessionControlRequest) (any, error) {
	return h.requestVoiceTransferWithID(ctx, caller, input, voicetransfer.ID(h.cfg.InstanceID, caller.RequestID, caller.ToolCallID))
}
func (h *Hub) requestVoiceTransferWithID(ctx context.Context, caller backend.SessionControlCaller, input backend.SessionControlRequest, intentID string) (any, error) {
	if h.transfers == nil {
		return nil, errors.New("voice_transfer_unavailable")
	}
	c, err := h.transferClient(caller)
	if err != nil {
		return nil, err
	}
	if input.Action == "get_voice_transfer" {
		r, err := h.transfers.Get(ctx, input.DispatchID, h.pairedTaskDevice(c))
		if err != nil || r.Source.Session != caller.Parent.ID {
			return nil, errors.New("voice_transfer_not_found")
		}
		return r, nil
	}
	if input.InstanceID != "" && input.InstanceID != h.cfg.InstanceID {
		return nil, errors.New("voice_transfer_cross_bridge_unsupported")
	}
	grant, err := h.dispatches.Grant(ctx, caller.Parent.ID)
	if err != nil || !grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, input.SessionID) {
		return nil, errors.New("voice_transfer_target_not_granted")
	}
	target, ok := h.registry.Get(input.SessionID)
	if !ok || !h.validTransferTarget(target) || target.ResumeID() != input.ExpectedThreadID || target.SettingsSnapshot().ConfigRevision != input.ExpectedConfigRevision {
		return nil, errors.New("voice_transfer_target_changed_or_forbidden")
	}
	if target.ID == caller.Parent.ID {
		return map[string]any{"state": "already_on_target", "effect": false}, nil
	}
	h.voiceMu.Lock()
	busy := false
	for _, call := range h.voiceClients {
		if call.session == target {
			busy = true
		}
	}
	h.voiceMu.Unlock()
	if busy {
		return nil, errors.New("voice_transfer_target_has_call")
	}
	r, err := h.transfers.Create(ctx, voicetransfer.Record{ID: intentID, Authority: h.cfg.InstanceID, Source: transferTarget(caller.Parent), Target: transferTarget(target), SourceVoice: caller.VoiceID, Authorization: "voice_command", Device: h.pairedTaskDevice(c), Request: caller.RequestID, Turn: caller.TurnID, Tool: caller.ToolCallID})
	if err == nil {
		h.transferEvent(c, r, "")
	}
	return r, err
}
func (h *Hub) transferEvent(c *Client, r voicetransfer.Record, request string) {
	c.enqueueEvent(map[string]any{"type": "voice_transfer", "authority_instance_id": h.cfg.InstanceID, "session_id": r.Source.Session, "request_id": request, "operation": r})
}
func (h *Hub) validateTransfer(ctx context.Context, r voicetransfer.Record) error {
	s, ok := h.registry.Get(r.Source.Session)
	t, found := h.registry.Get(r.Target.Session)
	if !ok || !found || !h.validTransferTarget(s) || !h.validTransferTarget(t) || s.ResumeID() != r.Source.Thread || t.ResumeID() != r.Target.Thread || s.SettingsSnapshot().ConfigRevision != r.Source.Revision || t.SettingsSnapshot().ConfigRevision != r.Target.Revision {
		return errors.New("voice_transfer_source_or_target_changed")
	}
	g, err := h.dispatches.Grant(ctx, s.ID)
	if err != nil || !g.Allows(h.cfg.InstanceID, h.cfg.InstanceID, t.ID) {
		return errors.New("voice_transfer_target_not_granted")
	}
	return nil
}
func (h *Hub) handleVoiceTransfer(c *Client, cmd clientproto.Command) {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	device := h.pairedTaskDevice(c)
	if h.transfers == nil || device == "" {
		c.enqueueEvent(h.client.Error(cmd.SessionID, "voice_transfer_unavailable", "轉接來源尚未確認"))
		return
	}
	var input struct {
		ID              string `json:"transfer_id"`
		Action          string `json:"action"`
		Revision        uint64 `json:"revision"`
		TargetVoice     string `json:"target_voice_id"`
		BeforeStop      bool   `json:"before_stop"`
		LocalReleased   bool   `json:"native_release_confirmed"`
		NativeConnected bool   `json:"native_connected"`
		Automatic       bool   `json:"automatic"`
	}
	if len(cmd.Data) > 4096 || json.Unmarshal([]byte(cmd.Data), &input) != nil {
		c.enqueueEvent(h.client.Error(cmd.SessionID, "voice_transfer_request_invalid", "轉接參數無效"))
		return
	}
	r, err := h.transfers.Get(ctx, input.ID, device)
	if err != nil || r.Source.Session != cmd.SessionID {
		c.enqueueEvent(h.client.Error(cmd.SessionID, "voice_transfer_not_found", "找不到此裝置的轉接紀錄"))
		return
	}
	if input.Action == "get" {
		h.transferEvent(c, r, cmd.RequestID)
		return
	}
	if input.Revision != r.Revision {
		h.transferEvent(c, r, cmd.RequestID)
		return
	}
	if input.Action != "cancel" && input.Action != "fail" {
		err = h.validateTransfer(ctx, r)
	}
	next := ""
	if err == nil {
		h.voiceMu.Lock()
		aLive, bOther, bExact := false, false, false
		for vc, call := range h.voiceClients {
			if call.session.ID == r.Source.Session && call.voiceID == r.SourceVoice {
				aLive = true
			}
			if call.session.ID == r.Target.Session {
				if h.pairedTaskDevice(vc) == device && call.voiceID == input.TargetVoice && !call.stopping {
					bExact = true
				} else {
					bOther = true
				}
			}
		}
		h.voiceMu.Unlock()
		switch input.Action {
		case "confirm":
			if input.Automatic && !r.DirectAuthorized(time.Now()) {
				err = errors.New("voice_transfer_direct_authorization_expired")
			} else if r.State != "confirmation_required" || !aLive || bOther {
				err = errors.New("voice_transfer_call_changed")
			} else {
				next = "confirmed"
			}
		case "released":
			if r.State != "confirmed" || aLive || !input.LocalReleased {
				err = errors.New("voice_transfer_release_not_confirmed")
			} else {
				next = "a_released"
			}
		case "starting":
			if r.State != "a_released" || aLive || bOther {
				err = errors.New("voice_transfer_call_changed")
			} else {
				next = "starting_b"
			}
		case "connected":
			if r.State != "starting_b" || aLive || bOther || !bExact || !input.NativeConnected {
				err = errors.New("voice_transfer_connection_not_confirmed")
			} else {
				next = "completed"
			}
		case "cancel", "fail":
			if r.State == "confirmed" && aLive && input.BeforeStop && input.Action == "cancel" {
				next = "cancelled"
			} else if r.State != "confirmation_required" && (aLive || bOther || bExact || !input.LocalReleased) {
				err = errors.New("voice_transfer_settlement_required")
			} else if input.Action == "cancel" {
				next = "cancelled"
			} else {
				next = "failed"
			}
		default:
			err = errors.New("voice_transfer_action_invalid")
		}
	}
	if err == nil {
		updated, advanceErr := h.transfers.Advance(ctx, r, next, input.TargetVoice)
		err = advanceErr
		if err == nil {
			r = updated
		}
	}
	if err != nil {
		c.enqueueEvent(map[string]any{"type": "voice_transfer", "authority_instance_id": h.cfg.InstanceID, "session_id": r.Source.Session, "request_id": cmd.RequestID, "operation": r, "error": err.Error()})
		return
	}
	h.transferEvent(c, r, cmd.RequestID)
}
