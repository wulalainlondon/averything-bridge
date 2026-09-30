package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"
	"unicode/utf8"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/runtimejournal"
	"everything-go/internal/session"
)

type queuedPayload struct {
	Configuration *session.Configuration    `json:"configuration,omitempty"`
	Content       string                    `json:"content"`
	Images        []backend.ImageAttachment `json:"images,omitempty"`
	Files         []backend.FileAttachment  `json:"files,omitempty"`
}

func (h *Hub) queueError(c *Client, cmd clientproto.Command, code, message string) {
	// A delivery/queue rejection is not a terminal event for the active AI turn.
	c.enqueueEvent(protocol.Error{Type: "error", CommandType: cmd.Kind, SessionID: cmd.SessionID, RequestID: cmd.RequestID, Code: code, Message: message})
}

func (h *Hub) enqueueChatMessage(c *Client, cmd clientproto.Command) {
	if h.messageQueue == nil {
		h.queueError(c, cmd, "queue_unavailable", "Message queue storage is unavailable")
		return
	}
	if !h.controls.MobileMayWrite(cmd.SessionID) {
		h.queueError(c, cmd, "session_controlled_by_desktop", "Reclaim this conversation before sending a message")
		return
	}
	s, ok := h.registry.Get(cmd.SessionID)
	if !ok {
		h.queueError(c, cmd, "no_session", "Unknown session")
		return
	}
	if cmd.RequestID == "" {
		cmd.RequestID = "legacy_" + randomID()
	}
	content, files, err := h.resolveUploadedVideos(cmd.SessionID, cmd.Content, cmd.Files)
	if err != nil {
		h.queueError(c, cmd, "invalid_attachment", err.Error())
		return
	}
	h.messageQueueMu.Lock()
	defer h.messageQueueMu.Unlock()
	config := session.ConfigurationFrom(s.SettingsSnapshot())
	intent, err := json.Marshal(queuedPayload{Content: content, Images: cmd.Images, Files: files})
	if err != nil {
		h.queueError(c, cmd, "invalid_message", err.Error())
		return
	}
	intentHash := sha256.Sum256(intent)
	payload, err := json.Marshal(queuedPayload{Content: content, Images: cmd.Images, Files: files, Configuration: &config})
	if err != nil {
		h.queueError(c, cmd, "invalid_message", err.Error())
		return
	}
	names := []string{}
	for _, file := range cmd.Files {
		names = append(names, file.Name)
	}
	if _, found, lookupErr := h.messageQueue.Get(cmd.SessionID, cmd.RequestID); lookupErr != nil {
		h.queueError(c, cmd, "queue_unavailable", lookupErr.Error())
		return
	} else if !found {
		s.SetLastActivity(float64(time.Now().UnixMilli()) / 1000)
		if err := h.registry.PersistDurably(); err != nil {
			h.queueError(c, cmd, "session_persist_failed", err.Error())
			return
		}
	}
	e, inserted, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: cmd.SessionID, RequestID: cmd.RequestID, Content: truncateGraphemes(content, 4000), ImageCount: len(cmd.Images), FileNames: names, Payload: payload, PayloadHash: hex.EncodeToString(intentHash[:])})
	if err != nil {
		if errors.Is(err, messagequeue.ErrRejected) {
			h.queueError(c, cmd, "message_rejected", "這則訊息先前已被拒絕，不會自動重送。請確認接管狀態後，以新訊息送出。")
			return
		}
		h.queueError(c, cmd, "queue_persist_failed", err.Error())
		return
	}
	if inserted {
		behind := s.State() != session.Idle || s.QueueLen() > 0
		if !s.SubmitNamed(cmd.RequestID, func() { h.runQueuedMessage(s, cmd.RequestID) }) {
			_, _, _ = h.messageQueue.Transition(cmd.SessionID, cmd.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Failed, "Queue is full or session is closed", "", "")
			h.publishMessageQueue(cmd.SessionID)
			h.queueError(c, cmd, "queue_full", "Queue is full or session is closed; this message was not started")
			return
		}
		if !behind {
			h.updateRuntime(cmd.SessionID, "queued", cmd.RequestID, s.QueueLen(), "", "")
		}
		h.updateRuntimeQueueLength(cmd.SessionID, s.QueueLen())
	}
	// ACK is durable ownership, independent of how far execution has advanced.
	c.enqueueEvent(protocol.NewMessageAck(cmd.SessionID, e.RequestID, "queued"))
	h.publishMessageQueue(cmd.SessionID)
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
	if err = h.exec.Send(context.Background(), s, requestID, payload.Content, payload.Images, payload.Files); err != nil {
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
		event.Items = append(event.Items, protocol.MessageQueueItem{RequestID: e.RequestID, State: string(e.State), Content: e.Content, Sequence: e.Sequence, ImageCount: e.ImageCount, FileNames: e.FileNames, CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt, Message: e.Message, ActiveRequestID: e.ActiveRequestID, TurnID: e.TurnID})
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
	if e.State == messagequeue.Cancelled {
		h.queueResult(c, cmd, "cancel", "accepted", "", e)
		h.publishMessageQueue(cmd.SessionID)
		return
	}
	if e.State != messagequeue.Queued && e.State != messagequeue.Uncertain {
		h.queueResult(c, cmd, "cancel", "rejected", "Message has already started or is being steered", e)
		return
	}
	finish := func(bool) {}
	if e.State == messagequeue.Queued {
		s, ok := h.registry.Get(cmd.SessionID)
		if !ok {
			h.queueResult(c, cmd, "cancel", "rejected", "Session not found", e)
			return
		}
		if finish, err = s.ReserveWaiting(cmd.RequestID); err != nil {
			h.queueResult(c, cmd, "cancel", "rejected", err.Error(), e)
			return
		}
	}
	e, _, err = h.messageQueue.Transition(cmd.SessionID, cmd.RequestID, []messagequeue.State{messagequeue.Queued, messagequeue.Uncertain}, messagequeue.Cancelled, "", "", "")
	if err != nil {
		finish(false)
		h.queueResult(c, cmd, "cancel", "rejected", "Cancellation could not be saved", e)
		return
	}
	finish(true)
	h.updateRuntimeQueueLength(cmd.SessionID, h.sessionQueueLength(cmd.SessionID))
	h.publishMessageQueue(cmd.SessionID)
	h.queueResult(c, cmd, "cancel", "accepted", "", e)
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
	if json.Unmarshal(e.Payload, &intended) != nil || (intended.Configuration != nil && *intended.Configuration != session.ConfigurationFrom(s.Snapshot())) {
		h.messageQueueMu.Unlock()
		h.queueResult(c, cmd, "promote", "retained", "此訊息的設定與本輪不同，保留原本的排隊位置。", e)
		return
	}
	finish, err := s.ReserveQueued(cmd.RequestID)
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
			result, steerErr = steerer.Steer(context.Background(), s, cmd.RequestID, payload.Content, payload.Images, payload.Files)
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
		updated, _, persistErr := h.messageQueue.Transition(cmd.SessionID, cmd.RequestID, []messagequeue.State{messagequeue.Steering}, next, message, result.RequestID, result.TurnID)
		if persistErr != nil {
			// The durable pre-RPC state is steering, which recovers as uncertain. Never
			// restore the callback after a failed result write.
			finish(true)
			h.queueResult(c, cmd, "promote", "uncertain", "Result could not be saved; this item will not be resent automatically", e)
			return
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
		if !s.SubmitNamed(id, func() { h.runQueuedMessage(s, id) }) {
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
