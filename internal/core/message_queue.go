package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode/utf8"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/partners"
	"everything-go/internal/protocol"
	"everything-go/internal/runtimejournal"
	"everything-go/internal/session"
)

type queuedPayload struct {
	WorkspaceTask       *workspaceQueueBinding     `json:"workspace_task,omitempty"`
	VoiceContext        bool                       `json:"voice_context,omitempty"`
	VoiceID             string                     `json:"voice_id,omitempty"`
	VoiceThread         string                     `json:"voice_thread,omitempty"`
	PartnerRole         *partners.RoleSnapshot     `json:"partner_role,omitempty"`
	CallbackOnComplete  *bool                      `json:"callback_on_complete,omitempty"`
	CallbackBinding     string                     `json:"callback_binding,omitempty"`
	ExternalPeer        string                     `json:"external_peer,omitempty"`
	ExternalReviewOnly  bool                       `json:"external_review_only,omitempty"`
	ExternalEventIDs    []string                   `json:"external_event_ids,omitempty"`
	VisualUserHash      string                     `json:"visual_user_hash,omitempty"`
	VisualEffectiveHash string                     `json:"visual_effective_hash,omitempty"`
	VisualPresentation  string                     `json:"visual_presentation,omitempty"`
	FormalIntake        bool                       `json:"formal_intake,omitempty"`
	ExpectedTarget      *dispatchTargetExpectation `json:"expected_target,omitempty"`
	Configuration       *session.Configuration     `json:"configuration,omitempty"`
	MessagePurpose      string                     `json:"message_purpose,omitempty"`
	OwnerDevice         string                     `json:"owner_device,omitempty"`
	Origin              *protocol.TaskOrigin       `json:"task_origin,omitempty"`
	Content             string                     `json:"content"`
	Images              []backend.ImageAttachment  `json:"images,omitempty"`
	Files               []backend.FileAttachment   `json:"files,omitempty"`
}

func (h *Hub) queueError(c *Client, cmd clientproto.Command, code, message string) {
	// A delivery/queue rejection is not a terminal event for the active AI turn.
	c.enqueueEvent(protocol.Error{Type: "error", CommandType: cmd.Kind, SessionID: cmd.SessionID, RequestID: cmd.RequestID, Code: code, Message: message})
}

func (h *Hub) enqueueChatMessage(c *Client, cmd clientproto.Command) bool {
	if cmd.TaskOrigin != nil {
		if cmd.TaskTarget == nil || cmd.TaskTarget.ThreadID == "" {
			h.queueError(c, cmd, "task_target_required", "A typed origin requires the frozen target thread and configuration")
			return false
		}
		return h.enqueueChatMessageExpected(c, cmd, &dispatchTargetExpectation{ThreadID: cmd.TaskTarget.ThreadID, Revision: cmd.TaskTarget.ConfigRevision})
	}
	return h.enqueueChatMessageExpected(c, cmd, nil)
}

type dispatchTargetExpectation struct {
	ThreadID string `json:"thread_id"`
	Revision uint64 `json:"revision"`
}

func (e *dispatchTargetExpectation) matches(s *session.Session) bool {
	if e == nil {
		return true
	}
	snap := s.SettingsSnapshot()
	return snap.ResumeID == e.ThreadID && snap.ConfigRevision == e.Revision && snap.State != session.Closed && !snap.Hidden
}

func (h *Hub) enqueueChatMessageExpected(c *Client, cmd clientproto.Command, expected *dispatchTargetExpectation) bool {
	if cmd.VoiceContext && (len(cmd.Images) == 0 || !h.voicePhotoOwner(c, cmd.SessionID, cmd.VoiceID, cmd.ThreadID)) {
		h.queueError(c, cmd, "voice_context_retired", "原通話或圖片來源已改變，草稿已保留，未改送其他對話")
		return false
	}

	if interactionControlID(cmd.RequestID) && !c.interactionControl {
		h.queueError(c, cmd, "interaction_control_reserved", "This request belongs to the original question delivery")
		return false
	}
	if h.messageQueue == nil {
		h.queueError(c, cmd, "queue_unavailable", "Message queue storage is unavailable")
		return false
	}
	if !h.controls.MobileMayWrite(cmd.SessionID) {
		h.queueError(c, cmd, "session_controlled_by_desktop", "Reclaim this conversation before sending a message")
		return false
	}
	s, ok := h.registry.Get(cmd.SessionID)
	if !ok {
		h.queueError(c, cmd, "no_session", "Unknown session")
		return false
	}
	if cmd.RequestID == "" {
		cmd.RequestID = "legacy_" + randomID()
	}
	if err := h.delegationReturnGuard(context.Background(), s, cmd.RequestID, expected); err != nil {
		h.queueError(c, cmd, "delegation_return_guard", err.Error())
		return false
	}
	if err := h.delegationExecutionGuard(context.Background(), s, cmd.RequestID, false); err != nil {
		h.queueError(c, cmd, "delegation_capacity_guard", err.Error())
		return false
	}
	if !backend.ValidVisualPresentation(cmd.VisualPresentation) {
		h.queueError(c, cmd, "visual_presentation_invalid", "Unknown visual presentation mode")
		return false
	}
	if cmd.MessagePurpose != "" && cmd.MessagePurpose != "instruction" {
		cmd.VisualPresentation = ""
	}
	content, files, err := h.resolveUploadedVideos(cmd.SessionID, cmd.Content, cmd.Files)
	if err != nil {
		h.queueError(c, cmd, "invalid_attachment", err.Error())
		return false
	}
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	config := session.ConfigurationFrom(s.SettingsSnapshot())
	if expected != nil && (s.ResumeID() != expected.ThreadID || config.Revision != expected.Revision || s.State() == session.Closed || s.Snapshot().Hidden) {
		h.queueError(c, cmd, "dispatch_target_changed", "Target thread or configuration changed; refresh before dispatching")
		return false
	}
	switch cmd.MessagePurpose {
	case "", "instruction", "control", "result_return", "question_reply":
	default:
		h.queueError(c, cmd, "message_purpose_invalid", "Unknown message purpose")
		return false
	}
	role, roleErr := h.partnerRoleForMessage(c, cmd, s)
	if roleErr != nil {
		h.queueError(c, cmd, "partner_role_rejected", roleErr.Error())
		return false
	}
	if role != nil && expected == nil {
		expected = &dispatchTargetExpectation{ThreadID: s.ResumeID(), Revision: config.Revision}
	}
	owner, admissionErr := h.admitTaskOrigin(c, cmd)
	if admissionErr != nil {
		h.queueError(c, cmd, "task_origin_rejected", admissionErr.Error())
		return false
	}
	userHash, effectiveHash := "", ""
	if cmd.VisualPresentation != "" || role != nil {
		userHash, effectiveHash = backend.PresentationHashes(backend.WithPartnerRole(backend.WithVisualPresentation(context.Background(), cmd.VisualPresentation), role), content)
	}
	intent, err := json.Marshal(queuedPayload{VoiceContext: cmd.VoiceContext, VoiceID: cmd.VoiceID, VoiceThread: cmd.ThreadID, PartnerRole: role, CallbackOnComplete: cmd.CallbackOnComplete, VisualUserHash: userHash, VisualEffectiveHash: effectiveHash, VisualPresentation: cmd.VisualPresentation, MessagePurpose: cmd.MessagePurpose, Origin: cmd.TaskOrigin, Content: content, Images: cmd.Images, Files: files, ExpectedTarget: expected})
	if err != nil {
		h.queueError(c, cmd, "invalid_message", err.Error())
		return false
	}
	intentHash := sha256.Sum256(intent)
	payload, err := json.Marshal(queuedPayload{VoiceContext: cmd.VoiceContext, VoiceID: cmd.VoiceID, VoiceThread: cmd.ThreadID, PartnerRole: role, CallbackOnComplete: cmd.CallbackOnComplete, VisualUserHash: userHash, VisualEffectiveHash: effectiveHash, VisualPresentation: cmd.VisualPresentation, MessagePurpose: cmd.MessagePurpose, OwnerDevice: owner, CallbackBinding: callbackBinding(c), Origin: cmd.TaskOrigin, Content: content, Images: cmd.Images, Files: files, Configuration: &config, ExpectedTarget: expected})
	if err != nil {
		h.queueError(c, cmd, "invalid_message", err.Error())
		return false
	}
	names := []string{}
	for _, file := range cmd.Files {
		names = append(names, file.Name)
	}
	if _, found, lookupErr := h.messageQueue.Get(cmd.SessionID, cmd.RequestID); lookupErr != nil {
		h.queueError(c, cmd, "queue_unavailable", lookupErr.Error())
		return false
	} else if !found {
		s.SetLastActivity(float64(time.Now().UnixMilli()) / 1000)
		if err := h.registry.PersistDurably(); err != nil {
			h.queueError(c, cmd, "session_persist_failed", err.Error())
			return false
		}
	}
	e, inserted, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: cmd.SessionID, RequestID: cmd.RequestID, Content: truncateGraphemes(content, 4000), ImageCount: len(cmd.Images), FileNames: names, Payload: payload, PayloadHash: hex.EncodeToString(intentHash[:])})
	if err != nil {
		if errors.Is(err, messagequeue.ErrRejected) {
			h.queueError(c, cmd, "message_rejected", "這則訊息先前已被拒絕，不會自動重送。請確認接管狀態後，以新訊息送出。")
			return false
		}
		h.queueError(c, cmd, "queue_persist_failed", err.Error())
		return false
	}
	if inserted && cmd.CallbackOnComplete != nil && *cmd.CallbackOnComplete {
		if err := h.armAdmittedCallback(context.Background(), c, s, e.RequestID); err != nil {
			h.callbackRespond(c, callbackInput{Operation: "callback_admit_" + e.RequestID}, nil, err)
		}
	}
	if inserted {
		behind := s.State() != session.Idle || s.QueueLen() > 0
		// An observed native turn is live without claiming the Session worker.
		// A queued follow-up must not replace its active request presentation.
		if views := h.runtimes.Snapshot("", []string{s.ID}); len(views) == 1 && views[0].Phase != "queued" && runtimePhaseActive(views[0].Phase) {
			behind = true
		}
		if !h.submitQueuedMessage(s, cmd.RequestID) {
			_, _, _ = h.messageQueue.Transition(cmd.SessionID, cmd.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Failed, "Queue is full or session is closed", "", "")
			h.publishMessageQueue(cmd.SessionID)
			h.queueError(c, cmd, "queue_full", "Queue is full or session is closed; this message was not started")
			return false
		}
		if !behind {
			h.updateRuntime(cmd.SessionID, "queued", cmd.RequestID, s.QueueLen(), "", "")
		}
		h.updateRuntimeQueueLength(cmd.SessionID, s.QueueLen())
	}
	// ACK is durable ownership, independent of how far execution has advanced.
	c.enqueueEvent(protocol.NewMessageAck(cmd.SessionID, e.RequestID, "queued"))
	h.publishMessageQueue(cmd.SessionID)
	if inserted && cmd.VoiceContext {
		go h.promoteQueuedMessage(c, clientproto.Command{Kind: "promote_queued_message", SessionID: cmd.SessionID, RequestID: cmd.RequestID, AutoVoicePhoto: true})
	}
	return true
}

func (h *Hub) runQueuedMessage(s *session.Session, requestID string) {
	h.messageQueueMu.Lock()
	e, changed, err := h.messageQueue.Transition(s.ID, requestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Running, "", "", "")
	if err != nil || !changed || s.State() == session.Closed {
		h.messageQueueMu.Unlock()
		s.EndTurn()
		if err != nil {
			log.Printf("[message-queue] cannot start session=%s request=%s: %v", s.ID, requestID, err)
		}
		return
	}
	h.updateRuntime(s.ID, "running", requestID, s.QueueLen(), "", "")
	h.publishMessageQueue(s.ID)
	h.messageQueueMu.Unlock()
	var payload queuedPayload
	if err = json.Unmarshal(e.Payload, &payload); err != nil {
		h.Emit(backend.NewError(s.ID, requestID, "invalid_queued_message", "Queued message could not be decoded"))
		return
	}
	if payload.WorkspaceTask != nil {
		p, target, err := h.validateWorkspaceNative(context.Background(), payload.WorkspaceTask.RoomID, payload.WorkspaceTask.TaskID)
		if err != nil || target != s || p.Task.RequestID != requestID {
			h.Emit(backend.NewError(s.ID, requestID, "room_authorization_changed", "Room authorization or original target changed before native execution"))
			return
		}
		if payload.ExpectedTarget != nil && payload.ExpectedTarget.ThreadID == "" && p.Task.NativeThreadID != "" && h.workspace.KnownThread(context.Background(), s.ID) == p.Task.NativeThreadID {
			payload.ExpectedTarget.ThreadID = p.Task.NativeThreadID
		}
	}
	if !payload.ExpectedTarget.matches(s) {
		h.Emit(backend.NewError(s.ID, requestID, "dispatch_target_changed", "Target thread or configuration changed while waiting; this instruction was not sent"))
		return
	}
	if payload.PartnerRole != nil && payload.WorkspaceTask == nil {
		if err := h.intakeWritePolicy(s); err != nil {
			h.Emit(backend.NewError(s.ID, requestID, "partner_role_scope_changed", "Role scope changed before native consumption"))
			return
		}
	}
	if payload.FormalIntake && payload.WorkspaceTask == nil {
		if err := h.intakeWritePolicy(s); err != nil {
			h.Emit(backend.NewError(s.ID, requestID, "intake_scope_changed", "Original task intake permission changed before native consumption"))
			return
		}
	}
	if payload.Configuration != nil {
		if err = s.ActivateQueuedConfiguration(*payload.Configuration); err != nil {
			h.Emit(backend.NewError(s.ID, requestID, "queued_configuration_rejected", err.Error()))
			return
		}
		active := s.Snapshot()
		result := h.client.SessionConfigResult(s.ID, "", true, "", s.SettingsSnapshot())
		result.ActiveConfiguration = &protocol.ActiveSessionConfiguration{Model: active.Model, Effort: active.Effort, ServiceTier: active.ServiceTier, Revision: active.ConfigRevision}
		result.EffectiveBoundary = "active_run_snapshot"
		h.Emit(result)
	}
	if preview := truncateGraphemes(normalizePreviewText(payload.Content), 160); preview != "" && h.delegationMessageOrigin(s.ID, requestID) == "" {
		if _, _, err := h.registry.CommitPreviewAndPersist(s.ID, preview, "user", time.Now().UnixMilli()); err != nil {
			log.Printf("[session-preview] queued start commit failed: %v", err)
		}
	}
	if !h.controls.MobileMayWrite(s.ID) {
		h.Emit(backend.NewError(s.ID, requestID, "session_controlled_by_desktop", "Reclaim the conversation before sending another message"))
		return
	}

	// Recheck after activation/events/preview writes, immediately at Send.
	if !payload.ExpectedTarget.matches(s) || (payload.ExpectedTarget != nil && payload.Configuration != nil && session.ConfigurationFrom(s.Snapshot()) != *payload.Configuration) {
		h.Emit(backend.NewError(s.ID, requestID, "dispatch_target_changed", "Target changed before Send"))
		return
	}
	if guardErr := h.delegationReturnGuard(context.Background(), s, requestID, payload.ExpectedTarget); guardErr != nil {
		h.Emit(backend.NewError(s.ID, requestID, "delegation_return_guard", guardErr.Error()))
		return
	}
	if guardErr := h.delegationExecutionGuard(context.Background(), s, requestID, true); guardErr != nil {
		h.Emit(backend.NewError(s.ID, requestID, "delegation_capacity_guard", guardErr.Error()))
		return
	}
	if !h.eventAssignmentSendAllowed(s, payload) {
		h.Emit(backend.NewError(s.ID, requestID, "event_target_scope_changed", "Event target is now managed or unavailable"))
		return
	}
	sendContext := backend.WithPartnerRole(backend.WithVisualPresentation(context.Background(), payload.VisualPresentation), payload.PartnerRole)
	if payload.WorkspaceTask != nil {
		if _, target, err := h.validateWorkspaceNative(context.Background(), payload.WorkspaceTask.RoomID, payload.WorkspaceTask.TaskID); err != nil || target != s {
			h.Emit(backend.NewError(s.ID, requestID, "room_authorization_changed", "Room authority changed at Send"))
			return
		}
	}
	if payload.ExternalReviewOnly {
		grant, err := h.dispatches.Grant(context.Background(), "peer:"+payload.ExternalPeer)
		if err != nil || !grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, s.ID) {
			h.Emit(backend.NewError(s.ID, requestID, "external_peer_revoked", "Original peer permission revoked"))
			return
		}
		sendContext = backend.WithSandboxOverride(sendContext, "read-only")
	}
	if payload.VoiceContext && !h.voicePhotoStillCurrent(s.ID, payload.VoiceID, payload.VoiceThread) {
		h.Emit(backend.NewError(s.ID, requestID, "voice_context_retired", "原通話已結束，圖片不會自動轉交新的通話"))
		return
	}
	if err = h.exec.Send(sendContext, s, requestID, payload.Content, payload.Images, payload.Files); err != nil {
		if errors.Is(err, backend.ErrNativeSubmissionBusy) {
			h.deferUnsubmittedQueuedMessage(s, requestID)
			return
		}
		if errors.Is(err, backend.ErrThreadActiveWriter) {
			h.markDesktopWriter(s)
		}
		// Backends may already have emitted their own terminal. Only synthesize one
		// while this exact request is still running.
		h.messageQueueMu.Lock()
		current, found, _ := h.messageQueue.Get(s.ID, requestID)
		h.messageQueueMu.Unlock()
		if found && current.State == messagequeue.Running {
			code := "send_failed"
			if errors.Is(err, backend.ErrThreadActiveWriter) {
				code = "session_controlled_by_desktop"
			}
			h.Emit(backend.NewError(s.ID, requestID, code, err.Error()))
		}
	}
	if err == nil && payload.VoiceContext {
		go h.notifyVoicePhoto(s, payload.VoiceID, payload.VoiceThread, requestID)
	}
}

func (h *Hub) queuedSubmissionReady(s *session.Session, requestID string) bool {
	if !h.controls.MobileMayWrite(s.ID) {
		return true // The normal start path records the existing ownership refusal.
	}
	reader, ok := h.exec.(backend.QueuedSubmissionReader)
	if !ok {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	return reader.QueuedSubmissionReady(ctx, s, requestID) == nil
}

func (h *Hub) submitQueuedMessage(s *session.Session, requestID string) bool {
	return s.SubmitNamedWhenReady(requestID,
		func() bool { return h.queuedSubmissionReady(s, requestID) },
		func() { h.runQueuedMessage(s, requestID) })
}

// Only the adapter's typed pre-input busy result permits restoring the same
// waiting input. No uncertain, accepted or old failed receipt is replayed.
func (h *Hub) deferUnsubmittedQueuedMessage(s *session.Session, requestID string) {
	h.messageQueueMu.Lock()
	_, accepted, nativeErr := h.messageQueue.ExactNativeAcceptance(s.ID, requestID)
	var changed bool
	var err error
	if nativeErr == nil && !accepted {
		_, changed, err = h.messageQueue.Transition(s.ID, requestID, []messagequeue.State{messagequeue.Running}, messagequeue.Queued, "native_submission_busy", "", "")
	}
	deferred := changed && err == nil && s.DeferActiveNamed(requestID,
		func() bool { return h.queuedSubmissionReady(s, requestID) },
		func() { h.runQueuedMessage(s, requestID) })
	if changed && !deferred {
		_, _, _ = h.messageQueue.Transition(s.ID, requestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Uncertain, "native_defer_unconfirmed", "", "")
	}
	h.messageQueueMu.Unlock()
	if !deferred {
		h.Emit(backend.NativeSubmissionUncertain{SessionID: s.ID, RequestID: requestID, Reason: "native_defer_unconfirmed"})
		return
	}
	h.updateRuntime(s.ID, "queued", requestID, s.QueueLen(), "", "")
	h.publishMessageQueue(s.ID)
}

func (h *Hub) messageQueueSnapshot(sessionID string) (protocol.MessageQueueSnapshot, error) {
	event := protocol.MessageQueueSnapshot{Type: "message_queue_snapshot", SessionID: sessionID, Items: []protocol.MessageQueueItem{}}
	snapshot, err := h.messageQueue.Snapshot(sessionID)
	if err != nil {
		return event, err
	}
	event.Revision = snapshot.Revision
	if p, ok := h.exec.(backend.MaintenanceProvider); ok {
		for _, r := range p.MaintenanceRecords() {
			if r.SessionID == sessionID {
				event.Maintenance = r
			}
		}
	}
	if p, ok := h.exec.(interface{ RuntimeDiagnostics() map[string]any }); ok {
		event.Diagnostics = p.RuntimeDiagnostics()
	}
	for _, e := range snapshot.Items {
		event.Items = append(event.Items, protocol.MessageQueueItem{PayloadHash: e.PayloadHash, RequestID: e.RequestID, State: string(e.State), Content: e.Content, Sequence: e.Sequence, ImageCount: e.ImageCount, FileNames: e.FileNames, CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt, Message: e.Message, ActiveRequestID: e.ActiveRequestID, TurnID: e.TurnID})
	}
	return event, nil
}
func (h *Hub) publishMessageQueue(sessionID string) {
	if h.messageQueue == nil {
		return
	}
	if snapshot, err := h.messageQueueSnapshot(sessionID); err == nil {
		h.broadcastOnline(snapshot)
	} else {
		log.Printf("[message-queue] snapshot failed: %v", err)
	}
}
func (h *Hub) sendMessageQueue(c *Client, cmd clientproto.Command) {
	if h.messageQueue == nil {
		h.queueError(c, cmd, "queue_unavailable", "Message queue is unavailable")
		return
	}
	snapshot, err := h.messageQueueSnapshot(cmd.SessionID)
	if err != nil {
		h.queueError(c, cmd, "queue_unavailable", err.Error())
		return
	}
	// Full text is explicitly requested, never materialized for routine queue
	// broadcasts. Attachment bytes, secrets in metadata, and configuration are
	// not exposed. The durable session/request pair is the only lookup key.
	if cmd.RequestID != "" {
		entry, found, loadErr := h.messageQueue.Get(cmd.SessionID, cmd.RequestID)
		if loadErr != nil || !found {
			h.queueError(c, cmd, "queue_not_found", "Queued message is unavailable for this session")
			return
		}
		var payload queuedPayload
		if json.Unmarshal(entry.Payload, &payload) != nil {
			h.queueError(c, cmd, "queue_unavailable", "Queued message content is unavailable")
			return
		}
		detail := protocol.MessageQueueItem{RequestID: entry.RequestID, State: string(entry.State), Content: entry.Content,
			FullContent: payload.Content, ImageCount: entry.ImageCount, FileNames: entry.FileNames, CreatedAt: entry.CreatedAt, UpdatedAt: entry.UpdatedAt}
		if len(detail.FullContent) > 1024*1024 {
			detail.FullContent = detail.FullContent[:1024*1024]
			for !utf8.ValidString(detail.FullContent) {
				detail.FullContent = detail.FullContent[:len(detail.FullContent)-1]
			}
			detail.ContentTruncated = true
		}
		snapshot.DetailRequestID, snapshot.Detail = cmd.RequestID, &detail
	}
	c.enqueueEvent(snapshot)
}
func (h *Hub) queueResult(c *Client, cmd clientproto.Command, action, status, message string, e messagequeue.Entry) {
	// Automatic photo intake projects its durable message state, not manual queue controls.
	if cmd.AutoVoicePhoto {
		return
	}
	if cmd.Kind == "steer_message" {
		if status == "pending" {
			return
		}
		legacyStatus := "retained"
		if status == "accepted" {
			legacyStatus = "accepted"
		}
		c.enqueueEvent(protocol.NewSteerResult(cmd.SessionID, cmd.RequestID, legacyStatus, e.TurnID, e.ActiveRequestID, message))
		return
	}
	c.enqueueEvent(protocol.QueueActionResult{Type: "queue_action_result", SessionID: cmd.SessionID, RequestID: cmd.RequestID, Action: action, Status: status, Message: message, ActiveRequestID: e.ActiveRequestID, TurnID: e.TurnID})
}

func (h *Hub) cancelQueuedMessage(c *Client, cmd clientproto.Command) {
	if h.messageQueue == nil {
		h.queueResult(c, cmd, "cancel", "rejected", "Message queue is unavailable", messagequeue.Entry{})
		return
	}
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	e, found, err := h.messageQueue.Get(cmd.SessionID, cmd.RequestID)
	if err != nil || !found {
		h.queueResult(c, cmd, "cancel", "rejected", "Message not found", e)
		return
	}
	if cmd.ExpectedRevision != nil {
		snap, err := h.messageQueue.Snapshot(e.SessionID)
		if err != nil || snap.Revision != *cmd.ExpectedRevision {
			h.queueResult(c, cmd, "cancel", "rejected", "Queue revision changed; refresh the original receipt", e)
			return
		}
	}
	if !h.queueInputCancellable(c, e) {
		h.queueResult(c, cmd, "cancel", "rejected", "Only a confirmed waiting input owned by this paired device can be cancelled", e)
		return
	}
	if e.State == messagequeue.Cancelled {
		h.queueResult(c, cmd, "cancel", "accepted", "", e)
		h.publishMessageQueue(cmd.SessionID)
		return
	}
	if e.State != messagequeue.Queued {
		h.queueResult(c, cmd, "cancel", "rejected", "Message has already started or is being steered", e)
		return
	}
	if err = h.cancelWaitingInput(e, cmd.ExpectedRevision, func() bool { return h.queueInputCancellable(c, e) }); err != nil {
		h.queueResult(c, cmd, "cancel", "rejected", err.Error(), e)
		return
	}
	h.publishMessageQueue(cmd.SessionID)
	h.queueResult(c, cmd, "cancel", "accepted", "", e)
}

// Caller holds messageQueueMu and has passed its own transport-specific policy.
// This common barrier cannot cancel uncertain, started, steered or consumed work.
func (h *Hub) cancelWaitingInput(e messagequeue.Entry, expectedRevision *uint64, canCommit func() bool) error {
	return h.cancelWaitingInputWithOutcome(e, expectedRevision, canCommit, "")
}
func (h *Hub) cancelWaitingInputWithOutcome(e messagequeue.Entry, expectedRevision *uint64, canCommit func() bool, outcomeKey string) error {
	if e.State != messagequeue.Queued {
		return errors.New("input is not waiting")
	}
	if expectedRevision == nil {
		snap, err := h.messageQueue.Snapshot(e.SessionID)
		if err != nil {
			return err
		}
		expectedRevision = &snap.Revision
	}
	_, accepted, err := h.messageQueue.ExactNativeAcceptance(e.SessionID, e.RequestID)
	if err != nil || accepted {
		return errors.New("native acceptance is present or unverified")
	}
	s, ok := h.registry.Get(e.SessionID)
	if !ok {
		return errors.New("session not found")
	}
	if hr, ok := h.exec.(historyRouter); ok {
		if provider, ok := hr.ProviderFor(s); ok {
			if reader, ok := provider.(interface {
				NativeTurnForRequest(*session.Session, string) (string, error)
			}); ok {
				turn, err := reader.NativeTurnForRequest(s, e.RequestID)
				if err != nil || turn != "" {
					return errors.New("provider native acceptance is present or unavailable")
				}
			}
		}
	}

	finish, err := s.ReserveWaiting(e.RequestID)
	if err != nil {
		return err
	}
	if canCommit != nil && !canCommit() {
		finish(false)
		return errors.New("input ownership changed")
	}
	_, changed, err := h.messageQueue.CancelWaitingAPI(e.SessionID, e.RequestID, e.PayloadHash, expectedRevision, outcomeKey)
	if err != nil || !changed {
		finish(false)
		return errors.New("waiting cancellation could not be saved")
	}
	finish(true)
	h.updateRuntimeQueueLength(e.SessionID, h.sessionQueueLength(e.SessionID))
	return nil
}

func (h *Hub) promoteQueuedMessage(c *Client, cmd clientproto.Command) {
	if h.messageQueue == nil {
		h.queueResult(c, cmd, "promote", "rejected", "Message queue is unavailable", messagequeue.Entry{})
		return
	}
	h.messageQueueMu.Lock()
	if h.maintenanceHolds[cmd.SessionID] != nil {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "retained", "上下文整理尚未確認，不能插入新指令", messagequeue.Entry{})
		return
	}
	e, found, err := h.messageQueue.Get(cmd.SessionID, cmd.RequestID)
	if err != nil || !found {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "rejected", "Message not found", e)
		return
	}
	if e.State == messagequeue.Steered {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "accepted", "", e)
		return
	}
	if e.State == messagequeue.Steering {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "pending", "Steering is already in progress", e)
		return
	}
	if e.State == messagequeue.Uncertain {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "uncertain", e.Message, e)
		return
	}
	if e.State != messagequeue.Queued {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "rejected", "Message is no longer waiting", e)
		return
	}
	s, ok := h.registry.Get(cmd.SessionID)
	if !ok || s.Backend() != backend.Codex || !h.controls.MobileMayWrite(cmd.SessionID) {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "rejected", "This conversation does not support steering right now", e)
		return
	}
	steerer, ok := h.exec.(backend.SteeringExecutor)
	if !ok {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "rejected", "Steering is not supported", e)
		return
	}
	var intended queuedPayload
	if json.Unmarshal(e.Payload, &intended) != nil || !intended.ExpectedTarget.matches(s) || (intended.Configuration != nil && *intended.Configuration != session.ConfigurationFrom(s.Snapshot())) {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "retained", "此訊息的設定與本輪不同，保留原本的排隊位置。", e)
		return
	}
	finish, err := s.ReserveSteeringCandidate(cmd.RequestID)
	if err != nil {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "retained", err.Error(), e)
		return
	}
	e, _, err = h.messageQueue.Transition(cmd.SessionID, cmd.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Steering, "", "", "")
	if err != nil {
		finish(false)
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "retained", "Could not save steering intent", e)
		return
	}
	h.publishMessageQueue(cmd.SessionID)
	h.messageQueueMu.Unlock()
	go func() {
		var payload queuedPayload
		decodeErr := json.Unmarshal(e.Payload, &payload)
		var result backend.SteerResult
		steerErr := decodeErr
		if decodeErr == nil {
			if e := h.delegationExecutionGuard(context.Background(), s, cmd.RequestID, false); e != nil {
				steerErr = fmt.Errorf("%w: %v", backend.ErrSteerRejected, e)
			}
		}
		if payload.VoiceContext && !h.voicePhotoStillCurrent(s.ID, payload.VoiceID, payload.VoiceThread) {
			steerErr = fmt.Errorf("%w: original voice ended", backend.ErrSteerRejected)
		}
		if decodeErr == nil && steerErr == nil {
			result, steerErr = steerer.Steer(backend.WithPartnerRole(backend.WithVisualPresentation(context.Background(), payload.VisualPresentation), payload.PartnerRole), s, cmd.RequestID, payload.Content, payload.Images, payload.Files)
			if steerErr == nil && !result.StrictACK {
				steerErr = errors.New("native steer acceptance unverified")
			}
		}
		h.messageQueueMu.Lock()
		defer h.messageQueueMu.Unlock()
		next, status, message, remove := messagequeue.Steered, "accepted", "", true
		if steerErr != nil {
			message = steerErr.Error()
			if decodeErr != nil || errors.Is(steerErr, backend.ErrNoActiveTurn) || errors.Is(steerErr, backend.ErrUnsupportedSteer) || errors.Is(steerErr, backend.ErrSteerRejected) {
				next, status, remove = messagequeue.Queued, "retained", false
			} else {
				next, status = messagequeue.Uncertain, "uncertain"
				message = "Steering result could not be confirmed; automatic resend is paused. Check the current reply before cancelling this item."
			}
		}
		updated, _, persistErr := h.messageQueue.Transition(cmd.SessionID, cmd.RequestID, []messagequeue.State{messagequeue.Steering}, next, message, result.RequestID, result.TurnID, next == messagequeue.Steered && result.StrictACK)
		if persistErr != nil {
			// The durable pre-RPC state is steering, which recovers as uncertain. Never
			// restore the callback after a failed result write.
			finish(true)
			h.queueResult(c, cmd, "promote", "uncertain", "Result could not be saved; this item will not be resent automatically", e)
			return
		}
		if next == messagequeue.Steered && payload.VoiceContext {
			go h.notifyVoicePhoto(s, payload.VoiceID, payload.VoiceThread, e.RequestID)
		}
		finish(remove)
		h.updateRuntimeQueueLength(cmd.SessionID, s.QueueLen())
		h.publishMessageQueue(cmd.SessionID)
		h.queueResult(c, cmd, "promote", status, message, updated)
	}()
}

func (h *Hub) finishQueuedMessage(view runtimejournal.View) {
	if h.messageQueue == nil {
		return
	}
	requestID := view.ActiveRequestID
	if requestID == "" {
		if s, ok := h.registry.Get(view.SessionID); ok {
			requestID = s.ActiveQueuedID()
		}
	}
	if requestID == "" {
		return
	}
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	next := messagequeue.Completed
	if view.LastTerminal != "completed" {
		next = messagequeue.Failed
	}
	from := []messagequeue.State{messagequeue.Running}
	if view.ActiveRequestID == requestID {
		// An independent startup can have marked a live run uncertain before
		// losing the port race. A terminal for this exact request confirms the
		// outcome; do not leave a duplicate pending card in the client.
		from = append(from, messagequeue.Uncertain)
	}
	_, changed, err := h.messageQueue.Transition(view.SessionID, requestID, from, next, view.LastError, "", "")
	if err != nil {
		log.Printf("[message-queue] terminal save failed: %v", err)
		return
	}
	if changed {
		h.publishMessageQueue(view.SessionID)
	}
}

func (h *Hub) resumeQueuedMessages() {
	if h.messageQueue == nil {
		return
	}
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	entries, err := h.messageQueue.Queued()
	if err != nil {
		log.Printf("[message-queue] recovery failed: %v", err)
		return
	}
	for _, e := range entries {
		s, ok := h.registry.Get(e.SessionID)
		if !ok {
			_, _, _ = h.messageQueue.Transition(e.SessionID, e.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Failed, "Session is no longer available", "", "")
			continue
		}
		id := e.RequestID
		if !h.submitQueuedMessage(s, id) {
			log.Printf("[message-queue] waiting to restore session=%s request=%s", e.SessionID, id)
		}
	}
}

func (h *Hub) closeQueuedMessages(sessionID string) {
	if h.messageQueue == nil {
		return
	}
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	snapshot, err := h.messageQueue.Snapshot(sessionID)
	if err != nil {
		return
	}
	for _, e := range snapshot.Items {
		if e.State == messagequeue.Queued {
			_, _, _ = h.messageQueue.Transition(sessionID, e.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Cancelled, "Session was closed", "", "")
		}
	}
	h.publishMessageQueue(sessionID)
}

// Keep payload-independent queue operations small and safe to retry.
func queueRequestValid(cmd clientproto.Command) bool {
	return strings.TrimSpace(cmd.SessionID) != "" && strings.TrimSpace(cmd.RequestID) != ""
}
