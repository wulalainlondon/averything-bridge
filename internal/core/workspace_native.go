package core

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"everything-go/internal/messagequeue"
	"everything-go/internal/partners"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"everything-go/internal/workspace"
)

type workspaceQueueBinding struct {
	RoomID string `json:"room_id"`
	TaskID string `json:"task_id"`
}

func (h *Hub) StartWorkspaceScheduler(ctx context.Context) {
	if h.workspace == nil || !h.workspaceScheduler.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	h.workspaceCancel = cancel
	if err := h.startWorkspaceMCP(ctx); err != nil {
		cancel()
		h.workspaceScheduler.Store(false)
		return
	}
	h.workspaceWorkers.Add(1)
	h.workspaceWorkers.Add(1)
	go func() {
		defer h.workspaceWorkers.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.reconcileWorkspaceEffects(ctx)
				h.reconcileWorkspacePush(ctx)
			case <-h.workspaceEffectWake:
				h.reconcileWorkspaceEffects(ctx)
				h.reconcileWorkspacePush(ctx)
			}
		}
	}()

	go func() {
		defer h.workspaceWorkers.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				h.drainWorkspace(ctx)
			case <-h.workspaceWake:
				h.drainWorkspace(ctx)
			}
		}
	}()
	h.wakeWorkspace()
}
func (h *Hub) drainWorkspace(ctx context.Context) {
	if h.workspace == nil || h.exec == nil || h.messageQueue == nil {
		return
	}
	if pending, err := h.workspace.PendingAgents(ctx); err == nil {
		for _, p := range pending {
			if ctx.Err() != nil {
				return
			}
			if _, err = h.provisionWorkspaceAgent(ctx, p); err != nil {
				_, _ = h.workspace.AgentReady(ctx, p.Room.ID, p.Entity.ID, p.Agent.SessionID, false, "original_room_grant_or_session_unavailable")
			}
		}
	}
	if err := h.workspace.Tick(ctx); err != nil && ctx.Err() == nil {
		log.Printf("[workspace] schedule reconciliation unavailable")
	}
	tasks, err := h.workspace.Prepared(ctx, 30)
	if err != nil {
		return
	}
	for _, p := range tasks {
		if ctx.Err() != nil {
			return
		}
		h.reconcileWorkspaceTask(ctx, p)
	}
}
func (h *Hub) validateWorkspaceNative(ctx context.Context, roomID, taskID string) (workspace.PreparedTask, *session.Session, error) {
	return h.validateWorkspaceNativeMode(ctx, roomID, taskID, false)
}
func (h *Hub) validateWorkspaceActiveNative(ctx context.Context, roomID, taskID string) (workspace.PreparedTask, *session.Session, error) {
	consumed := false
	if h.workspace != nil && h.messageQueue != nil {
		sid, rid, err := h.workspace.NativeRequest(ctx, roomID, taskID)
		if err == nil {
			if _, known, e := h.messageQueue.ExactNativeAcceptance(sid, rid); e == nil && known {
				consumed = true
			}
			if proof, known, e := h.messageQueue.ProviderEvidence(sid, rid); e == nil && known && proof.Backend == "claude" {
				consumed = true
			}
		}
	}
	return h.validateWorkspaceNativeMode(ctx, roomID, taskID, consumed)
}
func (h *Hub) validateWorkspaceNativeMode(ctx context.Context, roomID, taskID string, consumed bool) (workspace.PreparedTask, *session.Session, error) {
	if h.workspace == nil {
		return workspace.PreparedTask{}, nil, errors.New("workspace_unavailable")
	}
	var p workspace.PreparedTask
	var err error
	if consumed {
		p, err = h.workspace.ValidateActiveTask(ctx, roomID, taskID)
	} else {
		p, err = h.workspace.ValidateTask(ctx, roomID, taskID)
	}
	if err != nil {
		return p, nil, err
	}
	if !consumed && p.Room.ProjectID != "" {
		if h.work == nil {
			return p, nil, errors.New("original_project_unavailable")
		}
		project, e := h.work.GetProject(ctx, p.Room.ProjectID)
		if e != nil || project.AuthorityInstanceID != h.cfg.InstanceID || project.ArchivedAt != nil {
			return p, nil, errors.New("original_project_unavailable")
		}
	}
	if !h.workspaceGrantCurrent(p.Grant) {
		return p, nil, errors.New("original_pairing_credential_revoked")
	}
	s, err := h.intakeSession(p.Task.SessionID)
	if err != nil {
		return p, nil, errors.New("original_target_unavailable")
	}
	if err = h.workspaceNativeWritePolicy(s); err != nil {
		return p, nil, errors.New("original_target_permission_changed")
	}
	thread := p.Task.NativeThreadID
	if thread == "" {
		thread = h.workspace.KnownThread(ctx, s.ID)
	}
	v := s.SettingsSnapshot()
	if thread != s.ResumeID() || v.ConfigRevision != p.Task.ConfigRevision || string(v.Backend) != p.Task.Role.Backend || v.Sandbox != p.Task.Role.Sandbox {
		return p, nil, errors.New("original_target_binding_changed")
	}
	p.Task.NativeThreadID = thread
	return p, s, nil
}

// This is a typed adapter into the EXISTING durable message queue. It records
// the actual persisted room authorization, not a fabricated Client, native
// caller, controller identity or alternate provider thread.
func (h *Hub) enqueueWorkspaceTask(ctx context.Context, p workspace.PreparedTask) error {
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	if old, found, err := h.messageQueue.Get(p.Task.SessionID, p.Task.RequestID); err != nil {
		return err
	} else if found {
		var prior queuedPayload
		if json.Unmarshal(old.Payload, &prior) != nil || prior.WorkspaceTask == nil || prior.WorkspaceTask.RoomID != p.Room.ID || prior.WorkspaceTask.TaskID != p.Entity.ID {
			return errors.New("original_queue_intent_conflict")
		}
		return nil
	}
	p, s, err := h.validateWorkspaceNative(ctx, p.Room.ID, p.Entity.ID)
	if err != nil {
		return err
	}
	if p.Task.Admission != "dispatching" {
		return errors.New("original_room_request_not_claimed")
	}
	config := session.ConfigurationFrom(s.SettingsSnapshot())
	role := &partners.RoleSnapshot{ProfileID: p.Task.AgentID, Revision: p.Task.Role.RoleRevision, Description: p.Task.Role.Description, Hash: p.Task.Role.RoleHash}
	if role.Description == "" {
		role = nil
	}
	images, files, err := h.workspaceTaskAttachments(ctx, p)
	if err != nil {
		return err
	}
	payload := queuedPayload{FormalIntake: true, Content: p.Prompt, Images: images, Files: files, PartnerRole: role, OwnerDevice: p.Grant.DeviceID, MessagePurpose: "instruction", Configuration: &config, ExpectedTarget: &dispatchTargetExpectation{ThreadID: p.Task.NativeThreadID, Revision: p.Task.ConfigRevision}, WorkspaceTask: &workspaceQueueBinding{RoomID: p.Room.ID, TaskID: p.Entity.ID}}
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err = h.registry.PersistDurably(); err != nil {
		return err
	}
	h.workspaceRequests.Store(s.ID+"\x00"+p.Task.RequestID, true)
	e, inserted, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: p.Task.RequestID, Content: truncateGraphemes(p.Task.Instruction, 4000), Payload: b, PayloadHash: workspaceHash(string(b)), FileNames: []string{}, ImageCount: len(images), API: h.workspaceTaskAdmission(ctx, p)})
	if err != nil {
		return err
	}
	if inserted {
		if !h.submitQueuedMessage(s, e.RequestID) {
			_, _, _ = h.messageQueue.Transition(s.ID, e.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Failed, "room_request_queue_full", "", "")
			return errors.New("room_request_queue_full")
		}
		h.updateRuntime(s.ID, "queued", e.RequestID, s.QueueLen(), "", "")
	}
	h.publishMessageQueue(s.ID)
	return nil
}
func (h *Hub) reconcileWorkspaceTask(ctx context.Context, p workspace.PreparedTask) {
	if err := h.projectWorkspaceTask(ctx, p.Room.ID, p.Entity.ID); err != nil {
		return
	}
	t := p.Task
	entry, found, err := h.messageQueue.Get(t.SessionID, t.RequestID)
	if err != nil {
		return
	}
	if t.Admission == "prepared" {
		if _, _, err = h.validateWorkspaceNative(ctx, p.Room.ID, p.Entity.ID); err != nil {
			_ = h.workspace.Observe(ctx, p.Room.ID, p.Entity.ID, workspace.Observation{Admission: "held", Execution: "waiting", Reason: err.Error()})
			h.workspaceInvalidation()
			return
		}
		if err = h.workspace.Claim(ctx, p.Room.ID, p.Entity.ID); err != nil {
			return
		}
		if err = h.enqueueWorkspaceTask(ctx, p); err != nil {
			// A durable queue lookup is the only acceptance source. If it
			// exists, never replace it with a failed/second intent.
			if _, exists, e := h.messageQueue.Get(t.SessionID, t.RequestID); e != nil || !exists {
				_ = h.workspace.Observe(ctx, p.Room.ID, p.Entity.ID, workspace.Observation{Admission: "unknown", Execution: "unknown", Reason: "original_queue_admission_uncertain"})
				h.workspaceInvalidation()
				return
			}
		}
		entry, found, err = h.messageQueue.Get(t.SessionID, t.RequestID)
		if err != nil {
			return
		}
	}
	if !found {
		if t.Admission == "cancel_requested" {
			_ = h.workspace.Observe(ctx, p.Room.ID, p.Entity.ID, workspace.Observation{Admission: "cancelled", Execution: "terminal", Reason: "cancelled_before_queue_admission"})
		} else {
			_ = h.workspace.Observe(ctx, p.Room.ID, p.Entity.ID, workspace.Observation{Admission: "unknown", Execution: "unknown", Reason: "original_queue_receipt_missing_no_replay"})
		}
		return
	}
	if t.Admission == "cancel_pending" && (entry.State == messagequeue.Running || entry.State == messagequeue.Queued) {
		_ = h.workspace.Observe(ctx, p.Room.ID, p.Entity.ID, workspace.Observation{Admission: "cancel_pending"})
		return
	}
	if entry.State == messagequeue.Queued || entry.State == messagequeue.Running {
		_, _, permissionErr := h.validateWorkspaceActiveNative(ctx, p.Room.ID, p.Entity.ID)
		if t.Admission == "cancel_requested" || permissionErr != nil {
			h.cancelWorkspaceNative(ctx, p, entry)
			return
		}
	}
	o := workspace.Observation{CanonicalID: apiID(h.cfg.InstanceID, "ordinary", t.SessionID+"/"+t.RequestID)}
	switch entry.State {
	case messagequeue.Queued:
		o.Admission = "queued"
		o.Execution = "waiting"
	case messagequeue.Running:
		o.Admission = "queued"
		o.Execution = "waiting"
		if native, known, e := h.messageQueue.ExactNativeAcceptance(t.SessionID, t.RequestID); e != nil {
			o.Admission = "unknown"
			o.Execution = "unknown"
			o.Reason = "native_evidence_quarantined"
		} else if known {
			if target, ok := h.registry.Get(t.SessionID); ok && h.nativeCorrelationKnown(target, native.ThreadID, t.RequestID, native.TurnID) {
				o.Admission = "running"
				o.Execution = "running"
				o.NativeThreadID = native.ThreadID
				o.NativeTurnID = native.TurnID
			}
		}
		if proof, known, e := h.messageQueue.ProviderEvidence(t.SessionID, t.RequestID); e == nil && known && proof.Backend == "claude" {
			o.Admission = "running"
			o.Execution = "running"
			o.NativeThreadID = proof.ConversationID
			o.NativeTurnID = proof.Token
		}
	case messagequeue.Completed:
		o.Admission = "final_pending"
		o.Execution = "terminal"
		o.Reason = "original_final_evidence_pending"
		metadata, _ := json.Marshal(apiMetadata{})
		_, _ = h.apiTask(ctx, taskapi.IntentRecord{Path: "ordinary", TaskID: o.CanonicalID, NativeID: t.SessionID + "/" + t.RequestID, SessionID: t.SessionID, RequestID: t.RequestID, Metadata: metadata})
		if seal, e := h.messageQueue.TaskSeal(ctx, t.SessionID, t.RequestID, ""); e == nil {
			var anchor map[string]any
			var thread, turn string
			if json.Unmarshal(seal.Anchor, &anchor) == nil {
				if execution, ok := anchor["execution"].(map[string]any); ok {
					thread, _ = execution["native_thread_id"].(string)
					turn, _ = execution["native_turn_id"].(string)
					if execution["backend"] == "claude" {
						thread, _ = execution["provider_session_id"].(string)
						turn, _ = execution["provider_token"].(string)
					}
				}
			}
			if thread != "" && turn != "" {
				o.Admission = "completed"
				o.NativeThreadID = thread
				o.NativeTurnID = turn
				o.SealID = seal.ID
				o.Text = seal.Text
				o.Hash = seal.Hash
				o.Reason = ""
			}
		}
	case messagequeue.Cancelled:
		o.Admission = "cancelled"
		o.Execution = "terminal"
		o.Reason = entry.Message
	case messagequeue.Failed:
		o.Admission = "failed"
		o.Execution = "terminal"
		o.Reason = entry.Message
		for _, terminal := range h.runtimes.WidgetTerminals(t.SessionID, 0) {
			if terminal.RequestID == t.RequestID && terminal.Status == "stopped" {
				o.Admission = "cancelled"
				o.Reason = "original_native_stop_confirmed"
			}
		}
	default:
		o.Admission = "unknown"
		o.Execution = "unknown"
		o.Reason = "original_native_request_state_unknown"
	}
	if err = h.workspace.Observe(ctx, p.Room.ID, p.Entity.ID, o); err == nil {
		_ = h.projectWorkspaceTask(ctx, p.Room.ID, p.Entity.ID)
		h.workspaceInvalidation()
	}
}
func (h *Hub) cancelWorkspaceNative(ctx context.Context, p workspace.PreparedTask, e messagequeue.Entry) {
	if p.Task.Admission == "cancel_pending" {
		return
	}
	if e.State == messagequeue.Queued {
		h.messageQueueMu.Lock()
		err := h.cancelWaitingInput(e, nil, func() bool {
			sessionID, requestID, err := h.workspace.NativeRequest(ctx, p.Room.ID, p.Entity.ID)
			return err == nil && sessionID == e.SessionID && requestID == e.RequestID
		})
		h.messageQueueMu.Unlock()
		if err == nil {
			_ = h.workspace.Observe(ctx, p.Room.ID, p.Entity.ID, workspace.Observation{Admission: "cancelled", Execution: "terminal", Reason: "original_waiting_input_cancelled"})
		} else {
			_ = h.workspace.Observe(ctx, p.Room.ID, p.Entity.ID, workspace.Observation{Admission: "unknown", Execution: "unknown", Reason: "waiting_cancel_not_confirmed"})
		}
		h.workspaceInvalidation()
		return
	}
	s, ok := h.registry.Get(e.SessionID)
	if !ok || s.ActiveQueuedID() != e.RequestID {
		_ = h.workspace.Observe(ctx, p.Room.ID, p.Entity.ID, workspace.Observation{Admission: "unknown", Execution: "unknown", Reason: "exact_active_request_not_owned_for_stop"})
		return
	}
	if err := h.workspace.Observe(ctx, p.Room.ID, p.Entity.ID, workspace.Observation{Admission: "cancel_pending", Reason: "original_native_stop_requested"}); err != nil {
		return
	}
	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = h.exec.Stop(stopCtx, s)
	// Stop acceptance is not terminal proof. The original queue/runtime result
	// is observed on the next pass; no second Stop or new run is auto-created.
	h.workspaceInvalidation()
}

// Room sessions cannot inherit account dispatch or controller privileges from
// ordinary owner conversations, even if an agent emits those tool names.
func (h *Hub) workspaceReservedSession(id string) bool {
	if strings.HasPrefix(id, "s_cw_") {
		return true
	}
	if h.workspace == nil {
		return false
	}
	_, exclusive := h.workspace.SessionRoom(context.Background(), id)
	return exclusive
}
