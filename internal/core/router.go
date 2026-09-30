package core

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/runtime"
	"everything-go/internal/search"
	"everything-go/internal/session"
)

// truncate caps a preview string to n bytes (best-effort, byte-wise).
func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func sessionConfigValidationError(cmd clientproto.Command) string {
	if cmd.Backend != "" {
		switch cmd.Backend {
		case backend.Claude, backend.Codex, backend.Ollama, backend.RemoteWS:
		default:
			return "unsupported_backend"
		}
	}
	if cmd.Sandbox != "" {
		switch cmd.Sandbox {
		case "read-only", "workspace-write", "danger-full-access":
		default:
			return "invalid_sandbox"
		}
	}
	if cmd.EffortSet && cmd.Effort != "" && cmd.Effort != "auto" {
		switch cmd.Effort {
		case "low", "medium", "high", "xhigh", "max", "ultra":
		default:
			return "invalid_effort"
		}
	}
	return ""
}

// route dispatches an inbound frame on its envelope `type`. Transport-cheap
// commands are answered locally from the Hub's own state; the rest are forwarded
// to the Executor. The payload beyond {type, session_id} is only inspected by
// the specific handler that needs it.
func (h *Hub) route(ctx context.Context, c *Client, cmd clientproto.Command) {
	if c.inventoryProbe && cmd.Kind != "hello" && cmd.Kind != "claim_bridge" && cmd.Kind != "ping" {
		c.enqueueEvent(h.client.Error("", "", "Connection probes cannot issue application commands"))
		return
	}
	if c.enrollmentOnly && cmd.Kind != "hello" && cmd.Kind != "claim_bridge" && cmd.Kind != "ping" {
		c.enqueueEvent(h.client.Error("", "", "Pairing required before this device can use the bridge"))
		return
	}
	if h.rejectToolMaintenanceWrite(c, cmd) {
		return
	}
	if h.rejectPMCommand(c, cmd) {
		return
	}
	switch cmd.Kind {
	case "human_ai_collaboration":
		h.handleCollaboration(c, cmd)
	case "pm_collaboration":
		h.handlePM(c, cmd)
	case "hello":
		c.deviceID = cmd.DeviceID
		c.clientSurface = strings.ToLower(strings.TrimSpace(cmd.ClientSurface))
		c.protocolVersion = cmd.ProtocolVersion
		c.supportsReplayAck = cmd.ReplayAck
		if b := c.inventoryBinding.Load(); b != nil && b.deviceID == cmd.DeviceID && !cmd.ConnectionProbe && !c.inventoryProbe {
			promoted := *b
			promoted.probe = false
			c.inventoryBinding.Store(&promoted)
			h.observeClientInfo(c, cmd.DeviceID, cmd.ClientInfo)
		}
		// Latest-device-wins: evict any older client from the same device so the
		// half-disconnect storm can't pile up zombie clients (#1).
		if !c.enrollmentOnly && !c.inventoryProbe && !cmd.ConnectionProbe {
			h.registerLatest(c)
		}
		h.tunnelURLMu.RLock()
		tunnelURL := h.tunnelURL
		h.tunnelURLMu.RUnlock()
		helloInput := clientproto.HelloInput{
			ClientID: c.clientID, DeviceID: cmd.DeviceID,
			DeviceName: cmd.DeviceName, InstanceID: h.cfg.InstanceID, Gen: h.gen,
			IsLocked:     h.pairing.IsLocked(),
			LockedToMe:   h.pairing.LockedTo(cmd.AuthToken),
			PairingOpen:  h.pairing.EnrollmentOpen(),
			InstanceName: h.cfg.InstanceName,
		}
		if !c.enrollmentOnly && h.pmEnabled {
			helloInput.Capabilities = append(helloInput.Capabilities, "pm_collaboration_v1", "human_ai_collaboration_v2")
		}
		if !c.enrollmentOnly {
			if _, ok := h.exec.(backend.RecapExecutor); ok {
				helloInput.Capabilities = append(helloInput.Capabilities, "session_recap_v1")
			}
			if _, ok := h.exec.(backend.ToolEnvironmentExecutor); ok {
				helloInput.Capabilities = append(helloInput.Capabilities, "tool_environment_v1")
			}
			helloInput.RootDir = h.cfg.RootDir
			helloInput.DataDir = h.cfg.DataDir
			helloInput.LanIP = h.cfg.LanIP
			if h.cfg.ResolveLANIP != nil {
				helloInput.LanIP = h.cfg.ResolveLANIP()
			}
			helloInput.TunnelURL = tunnelURL
			helloInput.Backends = h.cfg.Backends
			if h.work != nil {
				// work_coordination_v1 is the stable product capability. Keep the
				// provisional work_items_v1 token during the released-client
				// migration window; both names address the same wire schema.
				helloInput.Capabilities = append(helloInput.Capabilities, "work_coordination_v1", "work_items_v1", "project_bootstrap_v1", "work_review_feedback_v1")
			}
			if h.events != nil {
				helloInput.Capabilities = append(helloInput.Capabilities, "event_inbox_v1")
			}
			if h.automation != nil {
				helloInput.Capabilities = append(helloInput.Capabilities, "external_automation_v1")
			}
		}
		if !c.enrollmentOnly && h.messageQueue != nil {
			helloInput.Capabilities = append(helloInput.Capabilities, "message_queue_v1", "queue_message_detail_v1")
		}
		if !c.enrollmentOnly && h.deviceInventory != nil {
			helloInput.Capabilities = append(helloInput.Capabilities, "device_inventory_v1")
		}
		if !c.enrollmentOnly {
			helloInput.Capabilities = append(helloInput.Capabilities, "session_config_revision_v1", "file_attachments_v1", "next_message_config_v1", "bootstrap_snapshot_v1")
		}
		c.enqueueEvent(h.client.HelloAck(helloInput))
		if c.inventoryProbe || cmd.ConnectionProbe {
			return
		}
		if !c.enrollmentOnly && h.pmEnabled {
			c.enqueueEvent(h.pmSnapshot("", nil, nil))
		}
		// A provisional LAN client receives only enough information to complete
		// claim_bridge. Do not disclose sessions, goals, replay, files or backend
		// metadata before its per-device credential is persisted.
		if c.enrollmentOnly {
			return
		}
		// Runtime catalogs require app-server initialization. Refresh them after
		// the cheap hello response so reconnection latency is unaffected.
		go h.sendBackendCatalog(c)
		// Proactively push the session list, then recover any events buffered
		// while this (or the previous) client was offline — same ordering as the
		// Python bridge so the app reconciles before replayed events arrive.
		c.enqueueEvent(h.client.SessionsList(h.sessionSummaries()))
		c.enqueueEvent(h.runtimeSnapshot(c.deviceID))
		if h.events != nil {
			if snapshot, err := h.events.Snapshot(ctx, c.deviceID, 0); err == nil {
				c.enqueueEvent(h.client.ExternalEventSnapshot(snapshot))
			} else {
				log.Printf("[events] snapshot device=%s: %v", c.deviceID, err)
			}
		}
		if h.automation != nil {
			c.enqueueEvent(h.automationSnapshotEvent())
		}
		// Work reconciliation is client-cursor driven. The client responds to
		// the advertised capability with work_sync_request using the revision it
		// has durably applied. Pushing an unconditional snapshot here made every
		// reconnect O(board size) and could replace a warm cache unnecessarily.
		// Goal is durable state. Send the full latest snapshot before transient
		// replay so a dropped historical goal_update cannot leave the UI stale.
		c.enqueueEvent(h.goals.Snapshot())
		for _, entry := range h.controls.List() {
			c.enqueueEvent(controlEvent(entry, "", ""))
		}
		h.replayOffline(c)
		// Canonical media/documents use a separate per-device persistent cursor.
		// Replay any file pushes this device hasn't acked yet (parity with the
		// Python bridge, which re-emits pending file_push frames on hello).
		h.sendPendingPushes(c)

	case "request_tool_environment":
		h.handleToolEnvironment(c, cmd)
	case "prepare_tool_environment_repair":
		h.handleToolEnvironment(c, cmd)
	case "apply_tool_environment_repair":
		h.handleToolEnvironment(c, cmd)
	case "request_tool_environment_operation":
		h.handleToolEnvironment(c, cmd)
	case "cancel_tool_environment_repair":
		h.handleToolEnvironment(c, cmd)
	case "ping":
		if cmd.ClientInfo != nil {
			h.observeClientInfo(c, c.deviceID, cmd.ClientInfo)
		}
		c.enqueuePong()

	case "attachment_upload_init":
		c.uploads.initKind(cmd.SessionID, cmd.UploadRequestID, cmd.Name, cmd.MediaType, cmd.SizeBytes, cmd.UploadKind)

	case "attachment_upload_finish":
		c.uploads.finish(cmd.UploadID)

	case "attachment_upload_cancel":
		c.uploads.cancel(cmd.UploadID)

	case "tunnel_url_ack":
		// App sends tunnel URL acknowledgements (FCM de-dup handshake).
		// The Python bridge uses this to stop resend retries; Go currently
		// keeps tunnel delivery state in-process and treats this as no-op.
		return

	case "claim_bridge":
		if cmd.AuthToken == "" {
			c.enqueueEvent(h.client.Error("", "", "auth_token required for claim_bridge"))
			return
		}
		if err := h.pairing.Claim(cmd.AuthToken, cmd.DeviceID); err != nil {
			c.enqueueEvent(h.client.Error("", "", err.Error()))
			return
		}
		wasEnrollment := c.enrollmentOnly
		c.enrollmentOnly = false
		h.syncDeviceInventory()
		h.bindDeviceInventory(c, cmd.AuthToken, cmd.DeviceID, c.inventoryName, c.clientSurface, c.inventoryProbe)
		if wasEnrollment && !c.inventoryProbe {
			h.registerLatest(c)
		}
		c.enqueueEvent(h.client.ClaimAck())

	case "open_pairing_window":
		expiresAt := h.pairing.OpenEnrollment(2 * time.Minute)
		c.enqueueEvent(h.client.PairingWindowAck(expiresAt.Unix()))

	case "unclaim_bridge":
		if err := h.pairing.Unclaim(cmd.AuthToken); err != nil {
			c.enqueueEvent(h.client.Error("", "", err.Error()))
			return
		}
		c.enqueueEvent(h.client.UnclaimAck(h.pairing.IsLocked()))
		h.syncDeviceInventory()

	case "request_sessions_list":
		c.enqueueEvent(h.client.SessionsList(h.sessionSummaries()))

	case "request_backend_catalog":
		go h.sendBackendCatalog(c)

	case "request_goals_snapshot":
		c.enqueueEvent(h.goals.Snapshot())

	case "offline_replay_ack":
		h.ackOfflineReplay(c, cmd.BatchID)

	case "session_runtime_ack":
		// ACK is intentionally one-way. Echoing the updated session_runtime here
		// makes legacy clients ACK the echo again forever, rewriting the journal
		// and eventually starving WebSocket pings. The next authoritative runtime
		// change or requested snapshot carries the device-specific read state.
		_, _ = h.runtimes.Ack(c.deviceID, cmd.SessionID, cmd.Revision, cmd.Read)

	case "request_runtime_snapshot":
		c.enqueueEvent(h.runtimeSnapshot(c.deviceID))

	case "work_sync_request":
		h.sendWorkSync(c, cmd.Revision)

	case "work_sync_ack":
		h.handleWorkSyncAck(c, cmd)

	case "work_diagnostics_request":
		if h.work != nil {
			diagnostics, err := h.work.Diagnostics(context.Background())
			if err != nil {
				c.enqueueEvent(protocol.WorkError{Type: "work_error", Code: "diagnostics_failed", Message: err.Error()})
			} else {
				c.enqueueEvent(protocol.WorkDiagnostics{Type: "work_diagnostics", Diagnostics: diagnostics})
			}
		}

	case "work_project_create", "work_item_create", "work_session_import", "work_item_update", "work_item_move",
		"work_item_review_decision",
		"work_item_archive", "work_item_restore", "work_item_link_session", "work_item_unlink_session",
		"work_item_dependency_add", "work_item_dependency_remove", "work_item_comment_add",
		"work_item_comment_edit", "work_item_attachment_add", "work_item_attachment_remove",
		"work_item_start_run", "work_item_read", "work_workflow_create",
		"work_project_bootstrap_generate", "work_project_bootstrap_approve":
		h.handleWorkCommand(c, cmd)

	case "handoff_to_desktop":
		h.handleDesktopHandoff(c, cmd.SessionID)

	case "reclaim_from_desktop":
		h.handleDesktopReclaim(c, cmd.SessionID)

	case "request_session_control":
		entry := h.controls.Get(cmd.SessionID)
		c.enqueueEvent(controlEvent(entry, "", ""))

	case "request_attachment_replay":
		h.startAttachmentReplay(c, cmd.SessionID)

	case "get_all_sessions":
		go h.handleGetAllSessions(c)

	case "restart_bridge":
		h.handleRestart(c)

	case "new_session":
		// Expand "~"/"~/..." at creation time, mirroring Python's
		// os.path.expanduser(msg["cwd"] or default_cwd) in session_routes.py.
		// Storing the resolved path keeps get_git_diff / get_tasks / spawn all
		// consistent — the app sends a literal "~" as the default cwd.
		cwd := runtime.ExpandPath(cmd.Cwd)
		if existing, ok := h.registry.FindByResumeID(cmd.ResumeClaudeID); ok && existing.ID != cmd.SessionID {
			snap := existing.Snapshot()
			// Resuming an already-registered native thread is idempotent. Older
			// clients may optimistically create a fresh local row first, so close
			// that requested stub and return the canonical session plus a fresh
			// authoritative list. This prevents two bridge session IDs from ever
			// competing for one Codex thread.
			c.enqueueEvent(h.client.SessionCreated(clientproto.SessionCreatedInput{
				ID: snap.ID, Name: snap.Name, CreatedAt: snap.CreatedAt, Cwd: snap.Cwd,
				Backend: snap.Backend, Model: snap.Model, Sandbox: snap.Sandbox,
			}))
			c.enqueueEvent(h.client.SessionClosed(cmd.SessionID))
			c.enqueueEvent(h.client.SessionsList(h.sessionSummaries()))
			return
		}
		s := h.registry.Create(cmd.SessionID, cmd.Name, cwd, cmd.Backend, cmd.Model, cmd.Sandbox, cmd.ResumeClaudeID)
		h.runtimes.Ensure(cmd.SessionID, "idle", 0)
		if cmd.EffortSet {
			s.SetEffort(cmd.Effort)
		}
		s.ApplyCodexSettings(cmd.ServiceTier, cmd.CollaborationMode, cmd.Personality)
		snap := s.Snapshot()
		c.enqueueEvent(h.client.SessionCreated(clientproto.SessionCreatedInput{
			ID: snap.ID, Name: snap.Name, CreatedAt: snap.CreatedAt, Cwd: snap.Cwd,
			Backend: snap.Backend, Model: snap.Model, Sandbox: snap.Sandbox,
		}))
		go h.registry.Persist()
		h.Emit(h.client.SessionsList(h.sessionSummaries()))

	case "message":
		h.enqueueChatMessage(c, cmd)
	case "request_message_queue":
		go h.reconcileMaintenance(cmd.SessionID, false)
		h.sendMessageQueue(c, cmd)
	case "resume_session_queue":
		go h.resumeMaintenanceQueue(c, cmd)
	case "promote_queued_message":
		if queueRequestValid(cmd) {
			h.promoteQueuedMessage(c, cmd)
		}
	case "cancel_queued_message":
		if queueRequestValid(cmd) {
			h.cancelQueuedMessage(c, cmd)
		}

	case "steer_message":
		if h.messageQueue != nil {
			if _, found, err := h.messageQueue.Get(cmd.SessionID, cmd.RequestID); err == nil && found {
				h.promoteQueuedMessage(c, cmd)
				return
			}
		}
		if h.rejectMobileWrite(c, cmd.SessionID) {
			return
		}
		cacheKey := cmd.SessionID + "\x00" + cmd.RequestID
		h.steerMu.Lock()
		cached, cachedOK := h.steerResults[cacheKey]
		h.steerMu.Unlock()
		if cachedOK {
			c.enqueueEvent(cached)
			return
		}
		s, ok := h.registry.Get(cmd.SessionID)
		if !ok {
			c.enqueueEvent(protocol.NewSteerResult(cmd.SessionID, cmd.RequestID, "retained", "", "", "unknown session"))
			return
		}
		steerer, ok := h.exec.(backend.SteeringExecutor)
		if !ok {
			c.enqueueEvent(protocol.NewSteerResult(cmd.SessionID, cmd.RequestID, "unsupported", "", "", backend.ErrUnsupportedSteer.Error()))
			return
		}
		content, files, err := h.resolveUploadedVideos(cmd.SessionID, cmd.Content, cmd.Files)
		if err != nil {
			c.enqueueEvent(protocol.NewSteerResult(cmd.SessionID, cmd.RequestID, "retained", "", "", err.Error()))
			return
		}
		// Do not enter the session turn queue: steering must reach the backend
		// while the current Send call is still occupying that queue.
		go func() {
			result, steerErr := steerer.Steer(context.Background(), s, cmd.RequestID, content, cmd.Images, files)
			var event protocol.SteerResult
			if steerErr != nil {
				status := "retained"
				if errors.Is(steerErr, backend.ErrUnsupportedSteer) {
					status = "unsupported"
				}
				event = protocol.NewSteerResult(cmd.SessionID, cmd.RequestID, status, "", "", steerErr.Error())
			} else {
				event = protocol.NewSteerResult(cmd.SessionID, cmd.RequestID, "accepted", result.TurnID, result.RequestID, "")
			}
			if event.Status == "accepted" || event.Status == "unsupported" {
				h.steerMu.Lock()
				if len(h.steerResults) >= 4096 {
					for key := range h.steerResults {
						delete(h.steerResults, key)
						break
					}
				}
				h.steerResults[cacheKey] = event
				h.steerMu.Unlock()
			}
			h.Emit(event)
		}()

	case "stop":
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			releaseQueue := s.HoldQueue()
			s.MarkStopping()
			h.updateRuntime(cmd.SessionID, "stopping", "", s.QueueLen(), "", "")
			go func() {
				defer releaseQueue()
				_ = h.exec.Stop(context.Background(), s)
				s.EndTurn() // release the queue even if the backend emits no terminal event
			}()
		}

	case "clear_session":
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			releaseQueue := s.HoldQueue()
			go func() {
				defer releaseQueue()
				_ = h.exec.Clear(context.Background(), s)
				s.EndTurn() // clear cancels an in-flight turn without a done/stopped
			}()
		}

	case "close_session":
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			go func() { _ = h.exec.Close(context.Background(), s) }()
			h.closeQueuedMessages(cmd.SessionID)
			h.registry.Delete(cmd.SessionID) // also stops the session's turn worker
			h.updateRuntime(cmd.SessionID, "closed", "", 0, "closed", "")
			h.Emit(h.client.SessionClosed(cmd.SessionID))
			go h.registry.Persist()
		}

	case "request_history":
		s, ok := h.registry.Get(cmd.SessionID)
		if !ok {
			c.enqueueEvent(h.client.HistorySnapshot(cmd.SessionID, []map[string]any{}, 0, false, true, ""))
			return
		}
		go h.sendHistory(c, s, cmd)

	case "request_session_recap":
		h.handleSessionRecap(c, cmd)
	case "generate_session_recap":
		h.handleSessionRecap(c, cmd)

	case "get_resumable_sessions":
		go h.sendResumable(c, 100)

	case "rename_session":
		name := strings.TrimSpace(cmd.Name)
		if len([]rune(name)) > 200 {
			c.enqueueEvent(h.client.SessionRenameRejected(cmd.SessionID, h.cfg.InstanceID,
				cmd.MutationID, "name_too_long", "", 0))
			return
		}
		snap, changed, err := h.registry.RenameAndPersist(cmd.SessionID, name, c.deviceID,
			cmd.MutationID, cmd.ExpectedRevision)
		if err != nil {
			reason := "persist_failed"
			switch {
			case errors.Is(err, session.ErrRenameConflict):
				reason = "revision_conflict"
			case errors.Is(err, session.ErrRenameEmpty):
				reason = "name_empty"
			case errors.Is(err, session.ErrSessionMissing):
				reason = "session_not_found"
			}
			c.enqueueEvent(h.client.SessionRenameRejected(cmd.SessionID, h.cfg.InstanceID,
				cmd.MutationID, reason, snap.Name, snap.MetadataRevision))
			return
		}
		event := h.client.SessionRenamed(cmd.SessionID, snap.Name, h.cfg.InstanceID,
			cmd.MutationID, snap.MetadataRevision, snap.NameUpdatedAt, snap.NameUpdatedBy)
		if changed {
			h.Emit(event)
		} else {
			// Idempotent retries receive the same authoritative confirmation
			// without rebroadcasting a duplicate mutation to every device.
			c.enqueueEvent(event)
		}

	case "set_session_meta":
		if s, ok := h.registry.Get(cmd.SessionID); ok {
			s.SetMeta(cmd.Pinned, cmd.Hidden)
			h.Emit(h.client.SessionMetaUpdated(cmd.SessionID, cmd.Pinned, cmd.Hidden))
			go h.registry.Persist()
		}

	case "codex_goal_set":
		s, ok := h.registry.Get(cmd.SessionID)
		if !ok {
			c.enqueueEvent(h.client.Error(cmd.SessionID, "no_session", "unknown session"))
			return
		}
		gc, ok := h.exec.(backend.GoalController)
		if !ok {
			c.enqueueEvent(h.client.Error(cmd.SessionID, "goal_not_supported", backend.ErrUnsupportedGoal.Error()))
			return
		}
		go func() {
			if err := gc.SetGoal(context.Background(), s, cmd.Objective, cmd.GoalStatus, cmd.TokenBudget); err != nil {
				if errors.Is(err, backend.ErrThreadActiveWriter) {
					h.markDesktopWriter(s)
					return
				}
				log.Printf("[%s] goal set error: %v", s.ID, err)
			}
		}()

	case "codex_goal_get":
		s, ok := h.registry.Get(cmd.SessionID)
		if !ok {
			c.enqueueEvent(h.client.Error(cmd.SessionID, "no_session", "unknown session"))
			return
		}
		gc, ok := h.exec.(backend.GoalController)
		if !ok {
			c.enqueueEvent(h.client.Error(cmd.SessionID, "goal_not_supported", backend.ErrUnsupportedGoal.Error()))
			return
		}
		go func() {
			if err := gc.GetGoal(context.Background(), s); err != nil {
				if errors.Is(err, backend.ErrThreadActiveWriter) {
					h.markDesktopWriter(s)
					return
				}
				log.Printf("[%s] goal get error: %v", s.ID, err)
			}
		}()

	case "codex_goal_clear":
		s, ok := h.registry.Get(cmd.SessionID)
		if !ok {
			c.enqueueEvent(h.client.Error(cmd.SessionID, "no_session", "unknown session"))
			return
		}
		gc, ok := h.exec.(backend.GoalController)
		if !ok {
			c.enqueueEvent(h.client.Error(cmd.SessionID, "goal_not_supported", backend.ErrUnsupportedGoal.Error()))
			return
		}
		go func() {
			if err := gc.ClearGoal(context.Background(), s); err != nil {
				if errors.Is(err, backend.ErrThreadActiveWriter) {
					h.markDesktopWriter(s)
					return
				}
				log.Printf("[%s] goal clear error: %v", s.ID, err)
			}
		}()

	case "set_effort":
		cmd.Kind = "switch_session_config"
		cmd.EffortSet = true
		h.route(ctx, c, cmd)

	case "switch_session_config":
		s, ok := h.registry.Get(cmd.SessionID)
		if !ok {
			c.enqueueEvent(h.client.SessionConfigResult(cmd.SessionID, cmd.MutationID, false, "session_not_found", session.Snapshot{}))
			return
		}
		if cmd.ConfigScope == "next_message" {
			h.setNextMessageConfiguration(c, cmd, s)
			return
		}
		if cmd.ConfigScope != "" {
			c.enqueueEvent(h.client.SessionConfigResult(cmd.SessionID, cmd.MutationID, false, "invalid_config_scope", s.SettingsSnapshot()))
			return
		}
		h.messageQueueMu.Lock()
		defer h.messageQueueMu.Unlock()
		before, release, reserveErr := s.ReserveConfiguration(cmd.ExpectedConfigRevision)
		if reserveErr != nil {
			c.enqueueEvent(h.client.SessionConfigResult(cmd.SessionID, cmd.MutationID, false, reserveErr.Error(), before))
			return
		}
		defer release()
		if reason := sessionConfigValidationError(cmd); reason != "" {
			c.enqueueEvent(h.client.SessionConfigResult(cmd.SessionID, cmd.MutationID, false, reason, before))
			return
		}

		s.ApplyConfig(cmd.Backend, cmd.Model, cmd.Sandbox)
		if cmd.ModelSet {
			s.SetModel(cmd.Model)
		}
		if cmd.EffortSet {
			s.SetEffort(cmd.Effort)
		}
		s.ApplyCodexSettings(cmd.ServiceTier, cmd.CollaborationMode, cmd.Personality)
		if updater, supported := h.exec.(interface {
			UpdateSessionSettings(context.Context, *session.Session) error
		}); supported {
			if err := updater.UpdateSessionSettings(ctx, s); err != nil {
				s.RestoreConfig(before)
				c.enqueueEvent(h.client.SessionConfigResult(cmd.SessionID, cmd.MutationID, false, "runtime_rejected: "+err.Error(), before))
				return
			}
		}
		s.SetConfigRevision(before.ConfigRevision + 1)
		if err := h.registry.PersistDurably(); err != nil {
			s.RestoreConfig(before)
			s.SetConfigRevision(before.ConfigRevision)
			if updater, supported := h.exec.(interface {
				UpdateSessionSettings(context.Context, *session.Session) error
			}); supported {
				_ = updater.UpdateSessionSettings(ctx, s)
			}
			c.enqueueEvent(h.client.SessionConfigResult(cmd.SessionID, cmd.MutationID, false, "config_persist_failed", before))
			return
		}
		h.Emit(h.client.SessionConfigResult(cmd.SessionID, cmd.MutationID, true, "", s.Snapshot()))

	case "fork_session":
		go h.handleFork(c, cmd)

	case "get_agent_tree":
		go h.handleAgentTree(c, cmd.SessionID)

	// --- Runtime ops: usage / shell / tasks / processes / browse ----------

	case "get_usage":
		go h.sendUsage(c, cmd.SessionID)

	case "shell_create":
		shellID, errMsg := h.shells.Create(cmd.Cwd)
		if errMsg != "" {
			c.enqueueEvent(h.client.Error("", "", errMsg))
			return
		}
		c.enqueueEvent(h.client.ShellCreated(shellID))

	case "shell_input":
		// Gate behind permission approval. Run in a goroutine so the read loop
		// keeps reading — the permission_response arrives on this same loop and
		// would deadlock if we blocked here (see permission.go).
		shellID, data, dev := cmd.ShellID, cmd.Data, c.deviceID
		go func() {
			if !h.perms.Request(dev, "shell_input", "Allow shell command?",
				"Execute command in bridge shell session", truncate(data, 300), "high", "") {
				return
			}
			h.shells.Input(shellID, data)
		}()

	case "shell_close":
		h.shells.Close(cmd.ShellID)

	case "get_tasks":
		c.enqueueEvent(h.client.TasksList(h.collectTasks()))

	case "kill_task":
		c.enqueueEvent(h.client.TaskKilled(cmd.ID, h.killTask(cmd.ID)))

	case "get_processes":
		go func() { c.enqueueEvent(h.client.ProcessesList(runtime.CollectProcesses(200))) }()

	case "kill_process":
		pid, force, dev := cmd.PID, cmd.Force, c.deviceID
		go func() {
			if !h.perms.Request(dev, "kill_process", "Allow process kill?",
				"Terminate a local OS process", fmt.Sprintf("pid=%d force=%v", pid, force), "high", "") {
				c.enqueueEvent(h.client.ProcessKilled(pid, false, "permission_denied"))
				return
			}
			ok, msg := runtime.KillProcess(pid, force)
			c.enqueueEvent(h.client.ProcessKilled(pid, ok, msg))
		}()

	case "browse_dir":
		go h.sendDirListing(c, cmd)

	case "open_file":
		go h.sendFileOpened(c, cmd)

	case "scan_markdown_files":
		go h.sendMarkdownFilesListing(c, cmd)

	case "save_file":
		go h.saveFile(c, cmd)

	case "request_status":
		c.enqueueEvent(h.statusResult(cmd.SessionID))

	case "get_git_diff":
		s, ok := h.registry.Get(cmd.SessionID)
		cwd := ""
		if ok {
			// Expand "~" defensively: new sessions store a resolved cwd, but
			// sessions restored from an older persistence file may still hold a
			// literal "~" that os.Stat can't resolve (→ spurious no_cwd).
			cwd = runtime.ExpandPath(s.Cwd())
		}
		go func() {
			r := runtime.GitDiff(cwd)
			c.enqueueEvent(h.client.GitDiffResult(cmd.SessionID, r.Diff, r.Error, r.Initialized))
		}()

	case "fcm_token":
		if h.fcm != nil {
			// Trust only the authenticated connection's device identity, never
			// a device_id supplied in this command. Preferences can arrive before
			// Firebase registration, and must survive later token refreshes.
			h.fcm.RegisterDevice(c.deviceID, cmd.Token, cmd.Platform, cmd.NotificationPreferences)
		}

	case "permission_response":
		h.perms.Resolve(cmd.RequestID, cmd.Decision, c.deviceID)

	// --- WebRTC P2P signaling --------------------------------------------
	// Bridge is the answerer: webrtc_offer → webrtc_answer (+ baked ICE),
	// webrtc_ice applies the app's trickled candidates. On DataChannel open the
	// server promotes it to a full client (see webrtc.go). webrtc_answer is
	// never sent by the app and is ignored if received.

	case "webrtc_offer":
		h.handleWebRTCOffer(ctx, c, cmd.WebRTCOffer())

	case "webrtc_ice":
		h.handleWebRTCICE(c, cmd.WebRTCICE())

	case "webrtc_answer":
		// Bridge is always the answerer; clients should not send answers back.
		log.Printf("client %s: ignoring inbound webrtc_answer (server is answerer)", c.clientID)

	// --- Phase 5: instances (stub) / inbox + feed (implemented) ----------
	// list_instances is still an empty-list stub (no multi-instance supervisor
	// in Go yet). The file-push inbox and the article feed below are fully
	// implemented; the app polls all of these on connect.

	case "list_instances":
		c.enqueueEvent(h.client.InstancesList())

	case "push_file":
		go h.handlePushFile(c, cmd.Path)

	case "file_push_ack":
		h.handleFilePushAck(cmd.FileID, c.deviceID)

	case "get_inbox":
		c.enqueueEvent(h.client.InboxListItems(h.inboxItems(c.deviceID)))

	case "feed_list_request":
		if h.feed == nil {
			c.enqueueEvent(h.client.FeedList(nil))
			return
		}
		c.enqueueEvent(h.client.FeedList(h.feed.List()))

	case "feed_push":
		if h.feed == nil {
			return
		}
		id, deduped, err := h.feed.Push(cmd.Title, cmd.HTML, cmd.Source, cmd.URL, cmd.ClientDedupKey, cmd.ContentType)
		if err != nil {
			c.enqueueEvent(h.client.Error("", "", err.Error()))
			return
		}
		c.enqueueEvent(h.client.FeedAck(id))
		if deduped {
			return // already pushed; no new broadcast / push
		}
		// Broadcast the new item to all clients, and push FCM.
		for _, m := range h.feed.List() {
			if m.FeedID == id {
				h.Emit(h.client.FeedNew(m))
				break
			}
		}
		if h.fcm != nil {
			go h.fcm.NotifyFeedNew(id, cmd.Title)
		}

	case "feed_fetch":
		if h.feed == nil {
			return
		}
		if html, ct, ok := h.feed.Fetch(cmd.FeedID); ok {
			c.enqueueEvent(h.client.FeedDetail(cmd.FeedID, html, ct))
		} else {
			c.enqueueEvent(h.client.Error("", "", "Feed item not found: "+cmd.FeedID))
		}

	case "feed_mark_read":
		if h.feed == nil {
			return
		}
		if m, ok := h.feed.MarkRead(cmd.FeedID); ok {
			h.Emit(h.client.FeedUpdated(m.FeedID, m.Read, m.Deleted))
		}

	case "feed_delete":
		if h.feed == nil {
			return
		}
		if m, ok := h.feed.Delete(cmd.FeedID); ok {
			h.Emit(h.client.FeedUpdated(m.FeedID, m.Read, m.Deleted))
		}

	case "external_event_list_request":
		if h.events == nil {
			return
		}
		if snapshot, err := h.events.Snapshot(ctx, c.deviceID, 0); err == nil {
			for index := range snapshot.Items {
				h.materializeExternalEvent(&snapshot.Items[index].Event)
			}
			c.enqueueEvent(h.client.ExternalEventSnapshot(snapshot))
		} else {
			c.enqueueEvent(h.client.Error("", "", "Event inbox unavailable: "+err.Error()))
		}

	case "external_event_mark_read":
		if h.events == nil {
			return
		}
		read := true
		if item, err := h.events.Mark(ctx, cmd.EventID, c.deviceID, &read, nil); err == nil {
			c.enqueueEvent(h.client.ExternalEventState(item))
		} else {
			c.enqueueEvent(h.client.Error("", "", "Event update failed: "+err.Error()))
		}

	case "external_event_dismiss":
		if h.events == nil {
			return
		}
		dismissed := true
		if item, err := h.events.Mark(ctx, cmd.EventID, c.deviceID, nil, &dismissed); err == nil {
			c.enqueueEvent(h.client.ExternalEventState(item))
		} else {
			c.enqueueEvent(h.client.Error("", "", "Event update failed: "+err.Error()))
		}

	// --- Interactions: AskUserQuestion ------------------------------------

	case "user_input_response":
		cancelled := cmd.Cancelled != nil && *cmd.Cancelled
		if ir, ok := h.exec.(interactionResponder); ok {
			if !ir.RespondUserInput(cmd.RequestID, cmd.Answers, cancelled) {
				log.Printf("client %s: user_input_response for unknown request %q", c.clientID, cmd.RequestID)
			}
		}

	case "pending_interactions_list":
		var items []backend.UserInputPayload
		if ir, ok := h.exec.(interactionResponder); ok {
			items = ir.PendingInteractions(cmd.SessionID)
		}
		snapshot := h.client.PendingInteractionsList(items)
		snapshot.ScopeSessionID, snapshot.SnapshotAll = cmd.SessionID, cmd.SessionID == ""
		c.enqueueEvent(snapshot)

	// --- Search (FTS5) ----------------------------------------------------

	case "request_search":
		go h.sendSearch(c, cmd)

	case "request_search_health":
		go func() {
			if h.search == nil {
				return
			}
			c.enqueueEvent(h.search.Health())
		}()

	case "request_session_list":
		go func() {
			if h.search == nil {
				return
			}
			c.enqueueEvent(h.search.ListSessions(cmd.Cursor, clampLimit(cmd.Limit, 30), cmd.ProjectDir, cmd.IncludeHidden, cmd.IncludeSubagents))
		}()

	case "request_search_context":
		go func() {
			if h.search == nil {
				return
			}
			c.enqueueEvent(h.search.GetContext(cmd.SessionID, cmd.MsgUUID, cmd.Around))
		}()

	default:
		// Not yet implemented in the Go core (history/search/fork/etc.).
		log.Printf("client %s: unhandled type %q", c.clientID, cmd.Kind)
	}
}

// historyRouter is the subset of the executor that can serve history. The Mux
// implements it; if the wired executor doesn't, history is simply unavailable.
type historyRouter interface {
	ProviderFor(s *session.Session) (backend.HistoryProvider, bool)
	AllProviders() []backend.HistoryProvider
}

// interactionResponder is the subset of the executor that can answer/list paused
// AskUserQuestion interactions. The Mux implements it (delegating to the Claude
// backend); if unavailable, the commands are no-ops / empty.
type interactionResponder interface {
	RespondUserInput(id string, answers map[string]any, cancelled bool) bool
	PendingInteractions(sessionID string) []backend.UserInputPayload
}

func (h *Hub) sendHistory(c *Client, s *session.Session, cmd clientproto.Command) {
	if !c.live() {
		return
	}
	hr, ok := h.exec.(historyRouter)
	if !ok {
		return
	}
	provider, ok := hr.ProviderFor(s)
	resumeIDs := s.ResumeIDs()
	if !ok || len(resumeIDs) == 0 {
		// No history backend or no resume id yet → empty snapshot.
		c.enqueueEvent(h.client.HistorySnapshot(s.ID, []map[string]any{}, 0, false, cmd.KnownLast == "", ""))
		return
	}
	// Coalesce + cache identical history requests so a reconnect burst triggers
	// LoadHistory (JSONL parse) at most once per key within the TTL (#4/#5).
	thinkFlag := "0"
	if cmd.IncludeThinking {
		thinkFlag = "1"
	}
	key := c.deviceID + "|" + s.ID + "|" + strings.Join(resumeIDs, ",") + "|" + cmd.Mode + "|" + cmd.Before + "|" + cmd.KnownLast + "|" + itoa(cmd.Limit) + "|" + thinkFlag
	v := h.coalesce(&h.storm.histSF, h.storm.histCache, key, historyCacheTTL, func() any {
		res, err := loadLogicalSessionHistory(provider, resumeIDs, history.Opts{
			Limit: cmd.Limit, KnownLast: cmd.KnownLast, Mode: cmd.Mode, Before: cmd.Before,
			IncludeThinking: cmd.IncludeThinking,
		})
		if err != nil {
			return nil
		}
		return res
	})
	if v == nil {
		return
	}
	res := v.(*history.Result)
	if !c.live() {
		return // client replaced/gone while loading → drop (#3)
	}
	msgs := res.Messages
	if msgs == nil {
		msgs = []map[string]any{}
	}
	msgs = h.annotateDelegationHistory(s.ID, msgs)
	latestAssistantAt := int64(0)
	latestAssistantText := ""
	toolOnlyPreviews := make(map[string]struct{})
	previewProjectionChanged := false
	for _, message := range msgs {
		if role, _ := message["role"].(string); role != "assistant" {
			continue
		}
		assistantText := historyAssistantPreview(message)
		if normalizePreviewText(assistantText) == "" {
			if raw, _ := message["content"].(string); raw != "" {
				toolOnlyPreviews[truncateGraphemes(normalizePreviewText(raw), 160)] = struct{}{}
			}
			continue
		}
		var observedAt int64
		switch timestamp := message["timestamp"].(type) {
		case int64:
			observedAt = timestamp
		case int:
			observedAt = int64(timestamp)
		case float64:
			observedAt = int64(timestamp)
		}
		if observedAt >= latestAssistantAt {
			latestAssistantAt = observedAt
			latestAssistantText = assistantText
		}
	}
	// Pagination into older history must never move the canonical row backward.
	// Snapshot/delta requests without a before cursor include the newest tail and
	// are safe repair sources after a Bridge restart.
	if latestAssistantAt > 0 && cmd.Before == "" {
		if preview := truncateGraphemes(normalizePreviewText(latestAssistantText), 160); preview != "" {
			current := s.Snapshot()
			_, invalidToolProjection := toolOnlyPreviews[current.PreviewText]
			var err error
			if invalidToolProjection && current.PreviewRole == "assistant" {
				_, previewProjectionChanged, err = h.registry.RepairPreviewAndPersist(s.ID, current.PreviewText, preview, "assistant", latestAssistantAt)
			} else {
				_, previewProjectionChanged, err = h.registry.CommitPreviewAndPersist(s.ID, preview, "assistant", latestAssistantAt)
			}
			if err != nil {
				log.Printf("[session-preview] history reconciliation failed session=%s: %v", s.ID, err)
			}
		}
		observedPhase := s.State().String()
		if observedPhase == "streaming" {
			observedPhase = "running"
		}
		if view, changed := h.runtimes.ReconcileHistoryActivity(s.ID, latestAssistantAt, observedPhase); changed {
			h.broadcastRuntime(view)
		}
	}
	if res.Kind == "delta" {
		c.enqueueEvent(h.client.HistoryDelta(s.ID, cmd.KnownLast, msgs, res.SourceCount))
		if previewProjectionChanged {
			h.BroadcastSessionSummaries()
		}
		return
	}
	c.enqueueEvent(h.client.HistorySnapshot(s.ID, msgs, res.SourceCount, res.HasMoreBefore, res.KnownIDFound, res.SnapshotReason))
	if previewProjectionChanged {
		h.BroadcastSessionSummaries()
	}
}

// loadLogicalSessionHistory merges the bounded tails of archived physical
// Codex threads with the active generation, then applies the client's cursor
// once across the logical timeline. A single-generation session keeps the old
// fast path.
func loadLogicalSessionHistory(provider backend.HistoryProvider, resumeIDs []string, opts history.Opts) (*history.Result, error) {
	if len(resumeIDs) == 1 {
		return provider.LoadHistory(resumeIDs[0], opts)
	}
	all := make([]map[string]any, 0)
	total := 0
	truncated := false
	for _, resumeID := range resumeIDs {
		res, err := provider.LoadHistory(resumeID, history.Opts{
			Limit: 10_000, Mode: "snapshot", IncludeThinking: opts.IncludeThinking,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, res.Messages...)
		total += res.SourceCount
		truncated = truncated || res.HasMoreBefore
	}
	res := history.Slice(all, opts)
	res.SourceCount = total
	if truncated {
		res.HasMoreBefore = true
	}
	return res, nil
}

func (h *Hub) sendResumable(c *Client, limit int) {
	if !c.live() {
		return
	}
	if _, ok := h.exec.(historyRouter); !ok {
		c.enqueueEvent(h.client.ResumableSessions([]history.ResumableSession{}))
		return
	}
	// One provider scan per (limit) within the TTL, shared across reconnect
	// bursts and browse_dir (#6). Re-check liveness after the (slow) scan (#3).
	all := h.coalescedResumable(limit)
	if !c.live() {
		return
	}
	c.enqueueEvent(h.client.ResumableSessions(all))
}

func (h *Hub) sessionSummaries() []protocol.SessionSummary {
	sessions := h.registry.List()
	out := make([]protocol.SessionSummary, 0, len(sessions))
	runtimeIDs := make([]string, 0, len(sessions))
	for _, s := range sessions {
		runtimeIDs = append(runtimeIDs, s.Snapshot().ID)
	}
	runtimePhase := make(map[string]string, len(runtimeIDs))
	for _, view := range h.runtimes.Snapshot("", runtimeIDs) {
		runtimePhase[view.SessionID] = view.Phase
	}

	// Batch-fetch recent messages + real last-activity for preview, keyed by hub
	// session ID. Search DB uses "claude:{resumeID}" or "codex:rollout-{ts}-{uid}"
	// as keys; RecentMessagesByUID handles the mapping transparently.
	var previewByHubID map[string]*search.SessionPreview
	if h.search != nil {
		var uids []search.SessionUID
		for _, s := range sessions {
			snap := s.Snapshot()
			if snap.ResumeID != "" && snap.Backend != "" {
				uids = append(uids, search.SessionUID{
					HubID: snap.ID, Backend: snap.Backend, UID: snap.ResumeID,
				})
			}
		}
		if len(uids) > 0 {
			previewByHubID = h.search.RecentMessagesByUID(uids, 3)
		}
	}

	for _, s := range sessions {
		snap := s.SettingsSnapshot()
		active := s.Snapshot()
		var recent []protocol.RecentMessage
		// Preview text, role and timestamp are one server-authoritative projection.
		// The search DB is only a fallback for native CLI activity that is newer
		// than the persisted projection; it must never pair stale text with a fresh
		// Registry last_used value.
		lastActivity := snap.LastActivity
		previewText, previewRole, previewAt := snap.PreviewText, snap.PreviewRole, snap.PreviewUpdatedAt
		if pv := previewByHubID[snap.ID]; pv != nil {
			for _, m := range pv.Recent {
				recent = append(recent, protocol.RecentMessage{Role: m.Role, Text: m.Text})
			}
			if float64(pv.LastTS) > lastActivity {
				lastActivity = float64(pv.LastTS)
			}
			previewText, previewRole, previewAt = resolveSessionPreview(
				previewText, previewRole, previewAt, recent, pv, runtimePhase[snap.ID],
			)
		}
		if float64(previewAt)/1000 > lastActivity {
			lastActivity = float64(previewAt) / 1000
		}
		out = append(out, protocol.SessionSummary{
			ActiveModel: active.Model, ActiveEffort: active.Effort, ActiveServiceTier: active.ServiceTier, ActiveConfigRevision: active.ConfigRevision,
			ConfigRevision: snap.ConfigRevision,
			ID:             snap.ID, Name: snap.Name, IsStreaming: snap.Streaming || runtimePhaseActive(runtimePhase[snap.ID]),
			AuthorityInstanceID: h.cfg.InstanceID, MetadataRevision: snap.MetadataRevision,
			NameUpdatedAt: snap.NameUpdatedAt, NameUpdatedBy: snap.NameUpdatedBy,
			LastNameMutationID: snap.LastNameMutationID,
			CreatedAt:          snap.CreatedAt, LastActivity: lastActivity,
			Cwd: snap.Cwd, Model: snap.Model, Effort: snap.Effort, Backend: snap.Backend,
			ServiceTier: snap.ServiceTier, CollaborationMode: snap.CollaborationMode, Personality: snap.Personality,
			Sandbox: snap.Sandbox, Pinned: snap.Pinned, Hidden: snap.Hidden,
			RecentMessages: recent,
			PreviewText:    previewText, PreviewRole: previewRole, PreviewVersion: sessionPreviewPolicyVersion,
		})
	}
	return out
}

// BroadcastSessionSummaries publishes a fresh snapshot after the out-of-process
// search indexer commits new preview data. Native discovery may announce a file
// before indexing finishes; this post-index broadcast closes that ordering gap.
func (h *Hub) BroadcastSessionSummaries() {
	h.Emit(h.client.SessionsList(h.sessionSummaries()))
}

func (h *Hub) sendBackendCatalog(c *Client) {
	defs := h.cfg.Backends
	if cp, ok := h.exec.(interface {
		CatalogDefinitions(context.Context, []backend.Definition) []backend.Definition
	}); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		defs = cp.CatalogDefinitions(ctx, defs)
	}
	c.enqueueEvent(h.client.BackendRegistryUpdated(defs))
}

// enqueueEvent marshals + enqueues a reply to this specific client.
func (c *Client) enqueueEvent(event any) {
	logOutbound(event)
	c.enqueueEventUnlogged(event)
}

// enqueueEventUnlogged marshals an event for this client's negotiated wire
// version without emitting another protocol log entry. Hub fan-out logs the
// canonical event once, then uses this helper for each client-specific
// SessionKey projection.
func (c *Client) enqueueEventUnlogged(event any) {
	data, err := marshalEvent(event, c.wireAuthority())
	if err != nil {
		log.Printf("enqueueEvent marshal: %v", err)
		return
	}
	c.enqueue(data)
}
