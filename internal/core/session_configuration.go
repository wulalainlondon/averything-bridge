package core

import (
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/session"
	"slices"
)

func (h *Hub) setNextMessageConfiguration(c *Client, cmd clientproto.Command, s *session.Session) {
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	before := s.SettingsSnapshot()
	reject := func(reason string) {
		c.enqueueEvent(h.client.SessionConfigResult(s.ID, cmd.MutationID, false, reason, before))
	}
	if before.Backend != backend.Codex {
		reject("next_message_settings_unsupported_backend")
		return
	}
	if !h.controls.MobileMayWrite(s.ID) {
		reject("session_controlled_by_desktop")
		return
	}
	if reason := sessionConfigValidationError(cmd); reason != "" {
		reject(reason)
		return
	}
	if cmd.Backend != "" && cmd.Backend != before.Backend || cmd.Sandbox != "" && cmd.Sandbox != before.Sandbox {
		reject("next_message_cannot_change_backend_or_permissions")
		return
	}
	if cmd.CollaborationMode != nil || cmd.Personality != nil {
		reject("next_message_supports_model_effort_speed_only")
		return
	}
	next := session.ConfigurationFrom(before)
	if cmd.ModelSet || cmd.Model != "" {
		next.Model = cmd.Model
	}
	if cmd.EffortSet {
		next.Effort = cmd.Effort
	}
	if cmd.ServiceTier != nil {
		next.ServiceTier = *cmd.ServiceTier
	}
	if next.ServiceTier != "" && !slices.Contains([]string{"fast", "flex", "priority", "standard"}, next.ServiceTier) {
		reject("invalid_service_tier")
		return
	}
	after, err := s.SetFutureConfiguration(next, cmd.ExpectedConfigRevision)
	if err != nil {
		reject(err.Error())
		return
	}
	if err := h.registry.PersistDurably(); err != nil {
		s.RestoreFutureConfiguration(before)
		reject("config_persist_failed")
		return
	}
	result := h.client.SessionConfigResult(s.ID, cmd.MutationID, true, "", after)
	result.EffectiveBoundary = "new_messages_only"
	h.Emit(result)
}
