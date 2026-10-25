package core

import (
	"everything-go/internal/clientproto"
	"everything-go/internal/session"
	"time"
)

type voiceStartPermit struct {
	device, session, thread, voice string
	owner                          *session.Session
	revision                       uint64
	expires                        time.Time
}

// A foreground native reconnect obtains the current source, never a cached
// navigation target or another device's voice lease. Preparing starts no work.
func (h *Hub) handleVoicePrepare(c *Client, cmd clientproto.Command) {
	refuse := func(code, message string) {
		c.enqueueEvent(map[string]any{"type": "error", "session_id": cmd.SessionID, "request_id": cmd.RequestID, "code": code, "message": message})
	}
	device := h.pairedTaskDevice(c)
	s, ok := h.registry.Get(cmd.SessionID)
	if device == "" || !ok || !h.validTransferTarget(s) || !voiceIdentifier.MatchString(cmd.VoiceID) || !voiceIdentifier.MatchString(cmd.RequestID) {
		refuse("voice_prepare_forbidden", "無法準備這個原對話")
		return
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	for vc, call := range h.voiceClients {
		if call.session == s || h.pairedTaskDevice(vc) == device {
			refuse("voice_prepare_busy", "這個對話仍有通話尚未結束")
			return
		}
	}
	if h.voicePermits == nil {
		h.voicePermits = map[string]voiceStartPermit{}
	}
	for id, permit := range h.voicePermits {
		if time.Now().After(permit.expires) {
			delete(h.voicePermits, id)
		}
	}
	if len(h.voicePermits) >= 128 {
		refuse("voice_prepare_busy", "目前準備中的通話過多，請稍後再試")
		return
	}
	id := randomID() + randomID()
	expires := time.Now().Add(15 * time.Second)
	h.voicePermits[id] = voiceStartPermit{owner: s, device: device, session: s.ID, thread: s.ResumeID(), voice: cmd.VoiceID, revision: s.SettingsSnapshot().ConfigRevision, expires: expires}
	c.enqueueEventUnlogged(map[string]any{"type": "voice_prepared", "session_id": s.ID, "authority_instance_id": h.cfg.InstanceID, "request_id": cmd.RequestID, "voice_id": cmd.VoiceID, "thread_id": s.ResumeID(), "config_revision": s.SettingsSnapshot().ConfigRevision, "name": s.Snapshot().Name, "permit": id, "expires_at": expires.UnixMilli()})
}
func (h *Hub) consumeVoicePermit(c *Client, s *session.Session, cmd clientproto.Command) bool {
	if cmd.VoiceStartPermit != "" && !h.validTransferTarget(s) {
		return false
	}
	if cmd.VoiceStartPermit == "" {
		return true
	} // Existing ordinary clients remain compatible.
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	permit, ok := h.voicePermits[cmd.VoiceStartPermit]
	if !ok || permit.owner != s || h.pairedTaskDevice(c) != permit.device || permit.session != s.ID || permit.thread != s.ResumeID() || permit.revision != s.SettingsSnapshot().ConfigRevision || permit.voice != cmd.VoiceID || !time.Now().Before(permit.expires) {
		return false
	}
	delete(h.voicePermits, cmd.VoiceStartPermit)
	return true
}
