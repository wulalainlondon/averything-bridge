package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/session"
	"everything-go/internal/toolenv"
)

type toolEnvironmentEvent struct {
	Type      string             `json:"type"`
	SessionID string             `json:"session_id"`
	Authority string             `json:"authority_instance_id"`
	RequestID string             `json:"request_id,omitempty"`
	Snapshot  *toolenv.Snapshot  `json:"snapshot,omitempty"`
	Plan      *toolenv.Plan      `json:"plan,omitempty"`
	Operation *toolenv.Operation `json:"operation,omitempty"`
	Error     string             `json:"error_code,omitempty"`
}

func (h *Hub) rejectToolMaintenanceWrite(c *Client, cmd clientproto.Command) bool {
	if !h.toolRepairRunning.Load() {
		return false
	}
	switch cmd.Kind {
	case "message", "steer_message", "promote_queued_message", "resume_session_queue", "switch_session_config", "set_effort", "close_session", "clear_session", "fork_session", "handoff_to_desktop", "reclaim_from_desktop", "new_session", "codex_goal_set", "codex_goal_clear":
	default:
		return false
	}
	if cmd.SessionID != "" {
		if s, ok := h.registry.Get(cmd.SessionID); ok && s.Backend() != backend.Codex {
			return false
		}
	}
	if cmd.Kind == "new_session" && cmd.Backend != "" && cmd.Backend != backend.Codex {
		return false
	}
	const message = "Tool maintenance is pending; this new command was not submitted."
	if cmd.Kind == "switch_session_config" {
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			c.enqueueEvent(h.client.SessionConfigResult(cmd.SessionID, cmd.MutationID, false, "session_busy", s.Snapshot()))
		}
	} else if cmd.RequestID != "" && (cmd.Kind == "message" || cmd.Kind == "steer_message") {
		// Correlate delivery rejection with the NEW request. An uncorrelated
		// error can incorrectly terminate an older chat in released clients.
		c.enqueueEvent(backend.NewError(cmd.SessionID, cmd.RequestID, "session_busy", message))
	} else {
		c.enqueueEvent(backend.NewSessionWarning(cmd.SessionID, message))
	}
	return true
}

func (h *Hub) handleToolEnvironment(c *Client, cmd clientproto.Command) {
	e := toolEnvironmentEvent{Type: "tool_environment_snapshot", SessionID: cmd.SessionID, Authority: h.cfg.InstanceID, RequestID: cmd.RequestID}
	if cmd.Kind == "apply_tool_environment_repair" || cmd.Kind == "cancel_tool_environment_repair" || cmd.Kind == "request_tool_environment_operation" {
		e.Type = "tool_environment_operation"
	}
	fail := func(code string) { e.Error = code; c.enqueueEventUnlogged(e) }
	if c.enrollmentOnly || c.deviceID == "" {
		fail("pairing_required")
		return
	}
	s, ok := h.registry.Get(cmd.SessionID)
	if !ok {
		fail("no_session")
		return
	}
	if s.Backend() != backend.Codex || s.ResumeID() == "" {
		fail("backend_unsupported")
		return
	}
	p, ok := h.exec.(backend.ToolEnvironmentExecutor)
	if !ok {
		fail("backend_unsupported")
		return
	}
	id := toolenv.Identity{Authority: h.cfg.InstanceID, Device: c.deviceID, Session: s.ID, Thread: s.ResumeID()}
	req := toolenv.Request{}
	if cmd.ToolEnvironment != nil {
		req = *cmd.ToolEnvironment
	}
	if len(req.Token) > 128 || len(req.OperationID) > 100 || len(req.Action) > 20 {
		fail("invalid_request")
		return
	}
	if cmd.Kind == "request_tool_environment_operation" {
		o, err := h.toolOperations.Get(id, req.OperationID)
		if err != nil {
			fail(toolenv.Code(err))
			return
		}
		e.Operation = &o
		c.enqueueEventUnlogged(e)
		return
	}
	if cmd.Kind != "request_tool_environment" && !h.controls.MobileMayWrite(s.ID) {
		fail("session_controlled_by_desktop")
		return
	}
	if cmd.Kind == "cancel_tool_environment_repair" {
		o, err := h.toolOperations.Transition(id, req.OperationID, "waiting_idle", "cancelled", "cancelled_before_apply", "none", "", "")
		if err != nil {
			fail(toolenv.Code(err))
			return
		}
		e.Operation = &o
		c.enqueueEventUnlogged(e)
		return
	}
	select {
	case h.storm.heavySem <- struct{}{}:
	default:
		fail("inspection_busy")
		return
	}
	go func() {
		defer func() { <-h.storm.heavySem }()
		ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()
		snapshot, err := p.InspectToolEnvironment(ctx, s, req.Refresh || cmd.Kind != "request_tool_environment")
		if err != nil {
			fail(toolenv.Code(err))
			return
		}
		if s.ResumeID() != id.Thread {
			fail("environment_changed")
			return
		}
		meta := s.Snapshot()
		control := h.controls.Get(s.ID)
		bindingBytes, _ := json.Marshal([]any{meta.Cwd, meta.Backend, meta.Model, meta.Effort, meta.Sandbox, meta.ServiceTier, meta.CollaborationMode, meta.Personality, control.Owner, control.State, control.UpdatedAt, snapshot.Services, snapshot.ReloadAllowed, snapshot.ForkAllowed})
		bindingHash := sha256.Sum256(bindingBytes)
		snapshot.Binding = hex.EncodeToString(bindingHash[:])
		e.Snapshot = &snapshot
		switch cmd.Kind {
		case "request_tool_environment":
			c.enqueueEventUnlogged(e)
		case "prepare_tool_environment_repair":
			plan, err := h.toolOperations.Prepare(id, req.Action, snapshot)
			if err != nil {
				fail(toolenv.Code(err))
				return
			}
			e.Plan = &plan
			c.enqueueEventUnlogged(e)
		case "apply_tool_environment_repair":
			if !h.controls.MobileMayWrite(s.ID) {
				fail("session_controlled_by_desktop")
				return
			}
			o, start, err := h.toolOperations.Begin(id, req, snapshot.Generation, snapshot.Binding)
			if err != nil {
				fail(toolenv.Code(err))
				return
			}
			e.Operation = &o
			c.enqueueEventUnlogged(e)
			if start {
				h.runToolRepair(c, s, p, o)
			}
		}
	}()
}

func (h *Hub) toolLocalBusy() bool {
	for _, s := range h.registry.List() {
		if s.Backend() == backend.Codex && (s.IsStreaming() || s.QueueLen() > 0) {
			return true
		}
	}
	return false
}

func (h *Hub) runToolRepair(c *Client, s *session.Session, p backend.ToolEnvironmentExecutor, o toolenv.Operation) {
	if !h.toolRepairRunning.CompareAndSwap(false, true) {
		o, _ = h.toolOperations.Transition(o.Identity, o.ID, "waiting_idle", "failed", "operation_pending", "none", "", "")
		c.enqueueEventUnlogged(toolEnvironmentEvent{Type: "tool_environment_operation", SessionID: s.ID, Authority: h.cfg.InstanceID, Operation: &o})
		return
	}
	defer h.toolRepairRunning.Store(false)
	id := o.Identity
	finish := func(from, phase, reason, evidence, thread, newSession string) {
		updated, err := h.toolOperations.Transition(id, o.ID, from, phase, reason, evidence, thread, newSession)
		e := toolEnvironmentEvent{Type: "tool_environment_operation", SessionID: s.ID, Authority: h.cfg.InstanceID}
		if err != nil {
			e.Error = toolenv.Code(err)
		} else {
			e.Operation = &updated
		}
		c.enqueueEventUnlogged(e)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		current, err := h.toolOperations.Get(id, o.ID)
		if err != nil || current.Phase != "waiting_idle" {
			return
		}
		if s.ResumeID() != id.Thread || !h.controls.MobileMayWrite(s.ID) {
			finish("waiting_idle", "failed", "environment_changed", "none", "", "")
			return
		}
		if time.Now().After(deadline) {
			finish("waiting_idle", "cancelled", "idle_wait_expired", "none", "", "")
			return
		}
		if h.toolLocalBusy() {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		applying, err := h.toolOperations.Transition(id, o.ID, "waiting_idle", "applying", "checking_maintenance_window", "none", "", "")
		if err != nil {
			return
		}
		c.enqueueEventUnlogged(toolEnvironmentEvent{Type: "tool_environment_operation", SessionID: s.ID, Authority: h.cfg.InstanceID, Operation: &applying})
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		result, err := p.RepairToolEnvironment(ctx, s, o.Action, o.Generation)
		cancel()
		if err != nil {
			code := toolenv.Code(err)
			if code == "waiting_idle" {
				finish("applying", "waiting_idle", code, "none", "", "")
				time.Sleep(500 * time.Millisecond)
				continue
			}
			phase := "failed"
			if code == "mutation_outcome_unknown" {
				phase = "indeterminate"
			}
			if code == "verification_incomplete" {
				phase = "completed_unverified"
			}
			finish("applying", phase, code, "none", result.NewThread, "")
			return
		}
		finish("applying", "verifying", "checking_tool_inventory", "inventory", result.NewThread, "")
		var newSession string
		if result.NewThread != "" {
			parent := s.Snapshot()
			// The operation id fixes the local identity across reconnects. Never
			// replace the parent's ResumeID, copy its JSONL, or move queued input.
			newSession = "tool_fork_" + o.ID
			child := h.registry.Create(newSession, parent.Name+" (工具環境分支)", parent.Cwd, parent.Backend, parent.Model, parent.Sandbox, result.NewThread)
			child.SetEffort(parent.Effort)
			child.ApplyCodexSettings(&parent.ServiceTier, &parent.CollaborationMode, &parent.Personality)
			if err := h.registry.PersistDurably(); err != nil {
				finish("verifying", "indeterminate", "fork_registration_incomplete", "inventory", result.NewThread, newSession)
				return
			}
			c.enqueueEvent(h.client.SessionsList(h.sessionSummaries()))
		}
		if result.NewThread == "" {
			c.enqueueEventUnlogged(toolEnvironmentEvent{Type: "tool_environment_snapshot", SessionID: s.ID, Authority: h.cfg.InstanceID, Snapshot: &result.Snapshot})
		}
		finish("verifying", "completed_unverified", "browser_verification_required", "inventory", result.NewThread, newSession)
		return
	}
}
