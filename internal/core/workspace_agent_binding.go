package core

import (
	"context"
	"encoding/json"
	"errors"

	"everything-go/internal/session"
	"everything-go/internal/workspace"
)

func (h *Hub) workspaceAgentOriginalTarget(ctx context.Context, a workspace.Agent, s *session.Session) error {
	v := s.SettingsSnapshot()
	expected := a.ThreadID
	if expected == "" {
		expected = h.workspace.KnownThread(ctx, s.ID)
	}
	if v.ConfigRevision != a.Config || string(v.Backend) != a.Backend || v.Model != a.Model || v.Sandbox != a.Sandbox || expected != s.ResumeID() {
		return errors.New("original_agent_binding_changed")
	}
	return h.workspaceNativeWritePolicy(s)
}

// Actual owner-only metadata for an edit, never a permission to replace the
// original thread/config or inherit another room's native context.
func (h *Hub) workspaceAgentBinding(ctx context.Context, a workspace.Actor, c workspace.Command) (workspace.Result, error) {
	if !a.Owner {
		return workspace.Result{}, errors.New("paired_owner_required")
	}
	if _, _, err := h.workspace.Authorize(ctx, a, c.RoomID, "manage"); err != nil {
		return workspace.Result{}, err
	}
	e, err := h.workspace.Lookup(ctx, a, c.RoomID, c.EntityID)
	if err != nil {
		return workspace.Result{}, err
	}
	var agent workspace.Agent
	if e.Kind != "agent" || json.Unmarshal(e.Body, &agent) != nil {
		return workspace.Result{}, errors.New("agent_unavailable")
	}
	s, ok := h.registry.Get(agent.SessionID)
	if !ok {
		return workspace.Result{}, errors.New("original_target_unavailable")
	}
	if err = h.workspaceAgentOriginalTarget(ctx, agent, s); err != nil {
		return workspace.Result{}, err
	}
	return workspace.Result{Action: c.Action, ToolData: map[string]any{"agent_id": e.ID, "entity_revision": e.Revision, "session_id": s.ID, "native_thread_id": s.ResumeID(), "config_revision": s.SettingsSnapshot().ConfigRevision, "profile_id": agent.ProfileID, "name": agent.Name, "description": agent.Description, "backend": agent.Backend, "model": agent.Model, "disabled": agent.Disabled}}, nil
}
