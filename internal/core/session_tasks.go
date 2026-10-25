package core

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/taskapi"
	"strings"

	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func (h *Hub) pairedTaskDevice(c *Client) string {
	identity := c.readIdentity.Load()
	if identity != nil && identity.deviceID == c.deviceID && h.pairing.MatchesDevice(identity.token, identity.deviceID) && !c.enrollmentOnly && !c.inventoryProbe {
		return identity.deviceID
	}
	return ""
}
func (h *Hub) admitTaskOrigin(c *Client, cmd clientproto.Command) (string, error) {
	owner := h.pairedTaskDevice(c)
	origin := cmd.TaskOrigin
	if origin == nil {
		return owner, nil
	} // additive: legacy/internal sends retain their transport
	if owner == "" || h.dispatches == nil || origin.InstanceID != h.cfg.InstanceID {
		return "", errors.New("origin requires the paired source authority")
	}
	source, ok := h.registry.Get(origin.SessionID)
	if !ok || source.ID == cmd.SessionID || !h.controllerInScope(source) || source.Snapshot().Hidden || source.State() == session.Closed || !h.controls.MobileMayWrite(source.ID) || source.ResumeID() != origin.ThreadID || source.SettingsSnapshot().ConfigRevision != origin.ConfigRevision {
		return "", errors.New("source thread/configuration/scope changed")
	}
	g, err := h.dispatches.Grant(context.Background(), source.ID)
	if err != nil || !g.Allows(h.cfg.InstanceID, h.cfg.InstanceID, cmd.SessionID) {
		return "", errors.New("source target not granted")
	}
	e, found, err := h.messageQueue.Get(source.ID, origin.RequestID)
	if err != nil || !found {
		return "", errors.New("source request unavailable")
	}
	payload := h.taskAdmission(e)
	if payload.OwnerDevice != owner || payload.MessagePurpose != "instruction" || h.nativeTaskTurn(source, origin.RequestID) == "" {
		return "", errors.New("source request ownership/native acceptance unconfirmed")
	}
	return owner, nil
}
func (h *Hub) nativeTaskTurn(s *session.Session, request string) string {
	if h.messageQueue != nil {
		if conflict, e := h.messageQueue.NativeConflict(s.ID, request); e != nil || conflict {
			return ""
		}
		if turn := h.messageQueue.NativeAcceptance(s.ID, request, s.ResumeID()); turn != "" {
			if h.nativeCorrelationKnown(s, s.ResumeID(), request, turn) {
				return turn
			}
			return ""
		}
	}
	if s.Backend() == backend.Codex {
		return ""
	}
	if hr, ok := h.exec.(historyRouter); ok {
		if provider, ok := hr.ProviderFor(s); ok {
			if evidence, ok := provider.(interface {
				NativeTurnForRequest(*session.Session, string) (string, error)
			}); ok {
				turn, err := evidence.NativeTurnForRequest(s, request)
				if err == nil {
					return turn
				}
			}
		}
	}
	return ""
}
func taskRequest(request string) bool {
	return strings.HasPrefix(request, "r_") || strings.HasPrefix(request, "scjob_") || strings.HasPrefix(request, "photo_") || strings.HasPrefix(request, "floating_")
}
func (h *Hub) projectSessionTask(s *session.Session, e messagequeue.Entry, owner string, finals map[string]map[string]any) protocol.SessionTask {
	payload := h.taskAdmission(e)
	if payload.MessagePurpose == "" && h.dispatches != nil && strings.HasPrefix(e.RequestID, "scjob_") {
		if record, found, err := h.dispatches.Get(context.Background(), strings.TrimPrefix(e.RequestID, "scjob_")); err == nil && found && record.RequestID == e.RequestID && record.SessionID == s.ID && record.InstanceID == h.cfg.InstanceID {
			payload.MessagePurpose = "instruction"
		}
	}
	task := protocol.SessionTask{MessagePurpose: payload.MessagePurpose, ReceiptFound: true, RequestID: e.RequestID, ExecutionRequestID: e.RequestID, State: string(e.State), Content: e.Content, CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt, Files: []string{}, Transport: "ordinary_queue", Origin: payload.Origin, InstanceID: h.cfg.InstanceID, InstanceName: h.cfg.InstanceName, SessionID: s.ID, SessionName: s.SettingsSnapshot().Name, ThreadID: s.ResumeID(), ConfigRevision: s.SettingsSnapshot().ConfigRevision, Error: e.Message}
	if e.State == messagequeue.Queued && payload.Content != "" {
		task.Content = payload.Content
	}
	if payload.ExpectedTarget != nil {
		task.ThreadID = payload.ExpectedTarget.ThreadID
		task.ConfigRevision = payload.ExpectedTarget.Revision
	}
	if len(task.Content) > 64000 {
		task.Content = truncateGraphemes(task.Content, 64000)
		task.ContentTruncated = true
	}
	if task.MessagePurpose == "" {
		task.MessagePurpose = "legacy_unknown"
	}
	task.NativeTurnID = h.nativeTaskTurn(s, e.RequestID)
	effective := e
	if e.State == messagequeue.Steered {
		if !h.messageQueue.StrictSteerProven(e) {
			task.Transport = "ordinary_steer"
			task.State = "legacy_unknown"
			task.NativeTurnID = ""
			task.APIAxes = map[string]string{"admission": "queued", "execution": "unknown", "final": "unknown", "qa": "not_requested", "delivery": "not_requested"}
			return task
		}
		task.Transport = "ordinary_steer"
		task.NativeTurnID = e.TurnID
		task.ExecutionRequestID = e.ActiveRequestID
		task.RelatedRequestIDs = []string{e.RequestID, e.ActiveRequestID}
		task.State = "consumed_unknown"
		active, found, err := h.messageQueue.Get(s.ID, e.ActiveRequestID)
		// The accepted steering RPC pins both the original execution and native
		// turn. An unrelated busy/terminal request never completes this supplement.
		if err != nil || !found || e.TurnID == "" || h.nativeTaskTurn(s, e.ActiveRequestID) != e.TurnID {
			return task
		}
		effective = active
	}
	for _, view := range h.runtimes.Snapshot("", []string{s.ID}) {
		if view.ActiveRequestID == task.ExecutionRequestID && task.NativeTurnID != "" {
			task.Stage = view.Stage
		}
	}
	switch effective.State {
	case messagequeue.Queued:
		task.State = "accepted_queued"
	case messagequeue.Running:
		if task.NativeTurnID != "" {
			task.State = "consumed"
		} else {
			task.State = "handoff_unknown"
		}
	case messagequeue.Steering, messagequeue.Uncertain:
		task.State = "unknown"
		if task.NativeTurnID != "" {
			task.State = "consumed_unknown"
		}
	case messagequeue.Completed:
		task.State = "completed_unanchored"
		final := finals[task.ExecutionRequestID]
		if final != nil {
			finalTurn, _ := final["source_turn_id"].(string)
			if s.Backend() == backend.Codex && (task.NativeTurnID == "" || task.NativeTurnID != finalTurn) {
				break
			}
			if e.State == messagequeue.Steered && finalTurn != e.TurnID {
				break
			}
			task.SourceMessageID, _ = final["source_message_id"].(string)
			task.Summary, _ = final["content"].(string)
			if task.Summary == "" {
				if blocks, ok := final["blocks"].([]map[string]any); ok {
					for _, block := range blocks {
						if block["type"] == "text" {
							task.Summary, _ = block["text"].(string)
						}
					}
				}
			}
			task.Summary = truncateGraphemes(task.Summary, 800)
			task.NativeTurnID = finalTurn
			if task.MessagePurpose == "legacy_unknown" {
				task.State = "legacy_unknown"
			}
			if task.SourceMessageID != "" && task.MessagePurpose == "instruction" {
				task.State = "completed"
			}
		}
	case messagequeue.Failed:
		task.State = "failed"
		task.Error = effective.Message
	case messagequeue.Cancelled:
		task.State = "cancelled"
	}
	task.APIAxes = map[string]string{"admission": "queued", "execution": "unknown", "final": "unanchored", "qa": "not_requested", "delivery": "not_requested"}
	if task.State == "accepted_queued" {
		task.APIAxes["execution"] = "not_started"
	}
	if task.NativeTurnID != "" {
		task.APIAxes["execution"] = "native_consumed"
	}
	if task.State == "completed" && task.SourceMessageID != "" {
		task.APIAxes["execution"] = "succeeded"
		task.APIAxes["final"] = "sealed"
	}
	if proof, found, err := h.messageQueue.ProviderEvidence(s.ID, e.RequestID); err == nil && found {
		task.ProviderExecution = map[string]string{"backend": "claude", "provider_session_id": proof.ConversationID, "token_kind": proof.TokenKind, "provider_token": proof.Token}
		task.APIAxes["execution"] = "native_consumed"
		if e.State == messagequeue.Running {
			task.State = "consumed"
		}
		if e.State == messagequeue.Completed && proof.Status == "succeeded" && proof.MessageID != "" && h.claudeAnchorID(s, proof) != "" {
			task.State = "completed"
			task.SourceMessageID = h.claudeAnchorID(s, proof)
			task.Summary = truncateGraphemes(proof.Text, 800)
			task.APIAxes["execution"] = "succeeded"
			task.APIAxes["final"] = "sealed"
		}
	}
	if conflict, err := h.messageQueue.NativeConflict(s.ID, e.RequestID); err != nil || conflict {
		task.State = "legacy_unknown"
		task.APIAxes["execution"] = "unknown"
		task.APIAxes["final"] = "unknown"
		task.SourceMessageID = ""
		task.NativeTurnID = ""
		task.ProviderExecution = nil
	}
	task.CanCancel = e.State == messagequeue.Queued && owner != "" && payload.OwnerDevice == owner && task.NativeTurnID == "" && task.ProviderExecution == nil && h.controls.MobileMayWrite(s.ID)
	return task
}

func (h *Hub) sessionTaskFinals(s *session.Session) (map[string]map[string]any, string) {
	finals := map[string]map[string]any{}
	hr, ok := h.exec.(historyRouter)
	if !ok {
		return finals, "unsupported"
	}
	provider, ok := hr.ProviderFor(s)
	if !ok {
		return finals, "unsupported"
	}
	// Bounded canonical history, with a truthful notice for older/legacy unanchored entries.
	result, err := loadLogicalSessionHistory(provider, s.ResumeIDs(), history.Opts{Limit: 10000, Mode: "snapshot"})
	if err != nil || result == nil {
		return finals, "unknown"
	}
	for _, m := range result.Messages {
		request, _ := m["request_id"].(string)
		if m["role"] == "assistant" && m["history_read_result_verified"] == true && request != "" {
			finals[request] = m
		}
	}
	if result.HasMoreBefore {
		return finals, "partial"
	}
	return finals, "supported"
}
func (h *Hub) sendSessionTasks(c *Client, cmd clientproto.Command) {
	event := protocol.SessionTasksSnapshot{Type: "session_tasks_snapshot", SessionID: cmd.SessionID, RequestID: cmd.RequestID, InstanceID: h.cfg.InstanceID, Status: "unknown", HistoryStatus: "unknown", ChildrenStatus: "unknown", ChildrenCoverage: "unavailable", Items: []protocol.SessionTask{}, Children: []protocol.SessionTask{}}
	proof := c.readIdentity.Load()
	owner := h.pairedTaskDevice(c)
	source, sourceFound := h.registry.Get(cmd.SessionID)
	var thread string
	var revision uint64
	if sourceFound {
		thread = source.ResumeID()
		revision = source.SettingsSnapshot().ConfigRevision
	}
	// Capture the transport's original pairing, never a later same-device pairing.
	reader := taskapi.VerifiedContext{Authority: h.cfg.InstanceID, StableScopeID: "device:" + owner, BindingKind: "paired_human", Transport: "authenticated_ws"}
	if proof != nil {
		digest := sha256.Sum256([]byte(proof.token))
		reader.NamespaceGeneration = binary.BigEndian.Uint64(digest[:8]) & ((1 << 53) - 1)
	}
	send := func() {
		current, found := h.registry.Get(cmd.SessionID)
		allowed := proof != nil && c.readIdentity.Load() == proof && h.pairedTaskDevice(c) == owner && owner != "" && found && current == source && current.ResumeID() == thread && current.SettingsSnapshot().ConfigRevision == revision && h.controllerInScope(current) && !current.Snapshot().Hidden && current.State() != session.Closed
		// History providers can block. Revalidate every disclosed target after I/O.
		if allowed {
			for _, child := range event.Children {
				if child.InstanceID != h.cfg.InstanceID || !child.CanOpenTarget {
					continue
				}
				target, exists := h.registry.Get(child.SessionID)
				if !exists || !h.controllerInScope(target) || target.Snapshot().Hidden || target.State() == session.Closed || target.ResumeID() != child.ThreadID || target.SettingsSnapshot().ConfigRevision != child.ConfigRevision {
					allowed = false
					break
				}
				if child.Transport == "human_linked" {
					link, err := h.messageQueue.LegacySourceLink(context.Background(), child.HumanSourceLink["relation_id"])
					if err != nil || !h.legacyReadAllowed(reader, link) {
						allowed = false
						break
					}
				} else if h.dispatches != nil {
					grant, err := h.dispatches.Grant(context.Background(), cmd.SessionID)
					if err != nil || !grant.Allows(h.cfg.InstanceID, child.InstanceID, child.SessionID) {
						allowed = false
						break
					}
				}
			}
		}
		if !allowed {
			event.Status = "forbidden"
			event.Items = []protocol.SessionTask{}
			event.Children = []protocol.SessionTask{}
			event.Message = "此配對或對話範圍已變更，請重新核對"
		}
		c.enqueueEvent(event)
	}
	s, ok := h.registry.Get(cmd.SessionID)
	if owner == "" || !ok || !h.controllerInScope(s) || s.Snapshot().Hidden || s.State() == session.Closed {
		event.Status = "forbidden"
		event.Message = "此配對或對話範圍無法讀取任務"
		send()
		return
	}
	if h.messageQueue == nil {
		event.Status = "unsupported"
		send()
		return
	}
	snapshot, err := h.messageQueue.Snapshot(s.ID)
	if err != nil {
		event.Message = "原請求狀態暫時無法確認；不會重送"
		send()
		return
	}
	if len(cmd.TaskRequestIDs) > 100 {
		event.Message = "Original request query exceeds limit"
		send()
		return
	}
	seen := map[string]bool{}
	for _, e := range snapshot.Items {
		seen[e.RequestID] = true
	}
	for _, id := range cmd.TaskRequestIDs {
		if !taskRequest(id) || seen[id] {
			continue
		}
		seen[id] = true
		if e, found, err := h.messageQueue.Get(s.ID, id); err == nil && found {
			snapshot.Items = append(snapshot.Items, e)
		} else {
			event.Items = append(event.Items, protocol.SessionTask{RequestID: id, MessagePurpose: "legacy_unknown", State: "unknown", Content: "尚未找到此原請求的正式受理記錄；不會重送", Files: []string{}, Transport: "ordinary_queue", InstanceID: h.cfg.InstanceID, SessionID: s.ID, ThreadID: s.ResumeID()})
		}
	}
	finals, status := h.sessionTaskFinals(s)
	event.HistoryStatus = status
	for _, entry := range snapshot.Items {
		if !taskRequest(entry.RequestID) {
			continue
		}
		full, found, err := h.messageQueue.Get(s.ID, entry.RequestID)
		if err != nil || !found {
			event.Message = "部分原請求狀態無法讀取"
			send()
			return
		}
		event.Items = append(event.Items, h.projectSessionTask(s, full, owner, finals))
	}
	childFinalCache := map[string]map[string]map[string]any{}
	childFinals := func(target *session.Session, entry messagequeue.Entry) map[string]map[string]any {
		need := entry.State == messagequeue.Completed
		if entry.State == messagequeue.Steered {
			if active, found, err := h.messageQueue.Get(target.ID, entry.ActiveRequestID); err == nil && found && active.State == messagequeue.Completed {
				need = true
			}
		}
		if !need {
			return nil
		}
		if cached, ok := childFinalCache[target.ID]; ok {
			return cached
		}
		result, _ := h.sessionTaskFinals(target)
		childFinalCache[target.ID] = result
		return result
	}
	// A parent relation can be displayed without enabling a controller grant.
	// Target details remain gated by the current grant and registered scope.
	if h.dispatches != nil {
		event.ChildrenStatus = "supported"
		event.ChildrenCoverage = "trusted_relations_only"
		grant, err := h.dispatches.Grant(context.Background(), s.ID)
		if err != nil {
			event.Message = "下派範圍狀態無法確認"
			send()
			return
		}
		records, err := h.dispatches.List(context.Background(), s.ID)
		if err != nil {
			event.Message = "下派記錄無法確認"
			send()
			return
		}
		for _, r := range records {
			if !taskSourceThread(s, r.ParentThreadID) {
				continue
			}
			task := protocol.SessionTask{MessagePurpose: "instruction", ReceiptFound: true, ExecutionRequestID: r.ExecutionRequestID, RequestID: r.RequestID, State: "unknown", Content: truncateGraphemes(r.Content, 500), CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, Files: []string{}, Transport: "controller_" + r.Mode, InstanceID: r.InstanceID, SessionID: r.SessionID, ThreadID: r.ThreadID, ConfigRevision: r.ConfigRevision, Origin: &protocol.TaskOrigin{InstanceID: h.cfg.InstanceID, SessionID: s.ID, ThreadID: r.ParentThreadID, RequestID: r.OriginRequestID}}
			if !grant.Allows(h.cfg.InstanceID, r.InstanceID, r.SessionID) {
				task.State = "forbidden"
				task.Error = "目標目前未授權"
			} else if r.InstanceID == h.cfg.InstanceID {
				if target, ok := h.registry.Get(r.SessionID); ok && h.controllerInScope(target) && !target.Snapshot().Hidden && target.ResumeID() == r.ThreadID {
					if e, found, err := h.messageQueue.Get(target.ID, r.RequestID); err == nil && found {
						task = h.projectSessionTask(target, e, "", childFinals(target, e))
						task.CanOpenTarget = target.SettingsSnapshot().ConfigRevision == r.ConfigRevision && target.State() != session.Closed
						task.Transport = "controller_" + r.Mode
						task.Origin = &protocol.TaskOrigin{InstanceID: h.cfg.InstanceID, SessionID: s.ID, ThreadID: r.ParentThreadID, RequestID: r.OriginRequestID}
					}
				}
			} else {
				task.State = r.State
				task.NativeTurnID = r.ExecutionTurnID
				task.Error = r.Error
				if r.State == "running" && r.ExecutionTurnID == "" {
					task.State = "handoff_unknown"
				}
				if r.State == "queued" {
					task.State = "accepted_queued"
				}
			}
			event.Children = append(event.Children, task)
		}
		ordinary, err := h.messageQueue.OriginEntries(h.cfg.InstanceID, s.ID)
		if err != nil {
			event.Message = "普通下派記錄無法確認"
			send()
			return
		}
		for _, e := range ordinary {
			payload := h.taskAdmission(e)
			if payload.Origin == nil || !taskSourceThread(s, payload.Origin.ThreadID) {
				continue
			}
			if !grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, e.SessionID) {
				thread := ""
				var revision uint64
				if payload.ExpectedTarget != nil {
					thread = payload.ExpectedTarget.ThreadID
					revision = payload.ExpectedTarget.Revision
				}
				event.Children = append(event.Children, protocol.SessionTask{ReceiptFound: true, RequestID: e.RequestID, State: "forbidden", Content: e.Content, CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt, Files: []string{}, Transport: "ordinary_queue", Origin: payload.Origin, InstanceID: h.cfg.InstanceID, SessionID: e.SessionID, ThreadID: thread, ConfigRevision: revision, Error: "目標目前未授權；來源關係已保留"})
				continue
			}
			target, ok := h.registry.Get(e.SessionID)
			if !ok || !h.controllerInScope(target) || target.Snapshot().Hidden {
				thread := ""
				var revision uint64
				if payload.ExpectedTarget != nil {
					thread = payload.ExpectedTarget.ThreadID
					revision = payload.ExpectedTarget.Revision
				}
				event.Children = append(event.Children, protocol.SessionTask{ReceiptFound: true, RequestID: e.RequestID, State: "unknown", Content: e.Content, CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt, Files: []string{}, Transport: "ordinary_queue", Origin: payload.Origin, InstanceID: h.cfg.InstanceID, SessionID: e.SessionID, ThreadID: thread, ConfigRevision: revision, Error: "目標目前無法查看；來源關係已保留"})
				continue
			}
			child := h.projectSessionTask(target, e, "", childFinals(target, e))
			child.CanOpenTarget = payload.ExpectedTarget != nil && payload.ExpectedTarget.matches(target)
			event.Children = append(event.Children, child)
		}
	}
	if h.dispatches == nil {
		event.ChildrenStatus = "unsupported"
	}
	for _, legacy := range h.legacySessionChildren(context.Background(), s.ID, reader) {
		duplicate := false
		for _, child := range event.Children {
			if child.SessionID == legacy.SessionID && child.RequestID == legacy.RequestID {
				duplicate = true
				break
			}
		}
		if !duplicate {
			event.Children = append(event.Children, legacy)
		}
	}
	event.Status = "supported"
	send()
}

func (h *Hub) queueNativeAcceptance(sessionID, request string) string {
	if s, ok := h.registry.Get(sessionID); ok {
		return h.nativeTaskTurn(s, request)
	}
	return ""
}

func (h *Hub) taskAdmission(e messagequeue.Entry) queuedPayload {
	var payload queuedPayload
	if data, found, err := h.messageQueue.TaskAdmission(e.SessionID, e.RequestID); err == nil && found {
		_ = json.Unmarshal(data, &payload)
	} else {
		_ = json.Unmarshal(e.Payload, &payload)
	}
	return payload
}

func taskSourceThread(s *session.Session, thread string) bool {
	if thread == "" {
		return false
	}
	for _, id := range s.ResumeIDs() {
		if id == thread {
			return true
		}
	}
	return false
}
