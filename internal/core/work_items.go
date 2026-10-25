package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/workitems"
)

func (h *Hub) sendWorkSnapshot(c *Client, authoritativeReset bool) {
	if h.work == nil {
		return
	}
	snapshot, err := h.work.SnapshotForDevice(context.Background(), c.deviceID)
	if err != nil {
		c.enqueueEvent(protocol.WorkError{Type: "work_error", Code: "snapshot_failed", Message: err.Error()})
		return
	}
	h.materializeWorkSnapshot(&snapshot)
	c.enqueueEvent(protocol.NewWorkSnapshot(snapshot, authoritativeReset))
	_ = h.work.AckSync(context.Background(), c.deviceID, snapshot.Revision, 0)
}

func (h *Hub) sendWorkSync(c *Client, since uint64) {
	if h.work == nil {
		c.enqueueEvent(protocol.WorkError{Type: "work_error", Code: "unsupported", Message: "native work items are unavailable"})
		return
	}
	changes, latest, compacted, err := h.work.ChangesSince(context.Background(), since, 256)
	if err != nil {
		c.enqueueEvent(protocol.WorkError{Type: "work_error", Code: "sync_failed", Message: err.Error()})
		return
	}
	if compacted || since == 0 {
		h.sendWorkSnapshot(c, true)
		return
	}
	h.materializeWorkChanges(changes)
	event := protocol.NewWorkDeltaBatch(since, latest, changes)
	c.enqueueEvent(event)
	_ = h.work.AckSync(context.Background(), c.deviceID, event.ToRevision, 0)
}

func (h *Hub) handleWorkSyncAck(c *Client, cmd clientproto.Command) {
	if h.work == nil {
		return
	}
	delivered := cmd.DeliveredRevision
	acked := cmd.AckedRevision
	if delivered == 0 {
		delivered = cmd.Revision
	}
	if acked == 0 {
		acked = cmd.Revision
	}
	if err := h.work.AckSync(context.Background(), c.deviceID, delivered, acked); err != nil {
		c.enqueueEvent(protocol.WorkError{Type: "work_error", Code: "ack_failed", Message: err.Error()})
	}
}

func (h *Hub) handleWorkCommand(c *Client, cmd clientproto.Command) {
	if h.work == nil {
		c.enqueueEvent(protocol.WorkError{Type: "work_error", MutationID: cmd.MutationID,
			Code: "unsupported", Message: "native work items are unavailable"})
		return
	}
	ctx := context.Background()
	if cmd.WorkItemID != "" && cmd.Kind != "work_item_read" {
		managed, err := h.work.IsCollaborationItem(ctx, cmd.WorkItemID)
		if err != nil || managed {
			h.sendWorkError(c, cmd.MutationID, errors.New("protocol_upgrade_required"))
			return
		}
	}
	actorType := workitems.ActorUser
	switch c.clientSurface {
	case "android", "ios":
		actorType = workitems.ActorMobile
	case "desktop", "web":
		actorType = workitems.ActorDesktop
	}
	actor := workitems.Actor{Type: actorType, DeviceID: c.deviceID}
	if cmd.Kind != "work_item_read" {
		if strings.TrimSpace(c.deviceID) == "" || strings.TrimSpace(cmd.MutationID) == "" {
			h.sendWorkError(c, cmd.MutationID, errors.New("device id and mutation id are required"))
			return
		}
		h.workMutationMu.Lock()
		defer h.workMutationMu.Unlock()
		cached, ok, err := h.work.MutationResponse(ctx, c.deviceID, cmd.MutationID)
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		if ok {
			c.enqueue(cached)
			return
		}
	}
	switch cmd.Kind {
	case "work_project_bootstrap_generate":
		result, err := h.generateProjectBootstrap(ctx, cmd)
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		ack := protocol.WorkMutationAck{Type: "work_mutation_ack", MutationID: cmd.MutationID,
			EntityVersion: result.Draft.Version, Revision: result.FirstRevision, Bootstrap: &result.Draft}
		if !result.Changed {
			ack.Revision = bootstrapAckRevision(ctx, h.work)
			h.completeWorkMutationWithoutBroadcast(c, cmd.MutationID, ack)
		} else {
			h.completeWorkMutationRange(c, cmd.MutationID, ack, result.FirstRevision)
		}

	case "work_project_bootstrap_approve":
		result, err := h.approveProjectBootstrap(ctx, cmd, actor)
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		ack := bootstrapMutationAck(cmd.MutationID, result)
		if result.AlreadyApplied {
			ack.Revision = bootstrapAckRevision(ctx, h.work)
			h.completeWorkMutationWithoutBroadcast(c, cmd.MutationID, ack)
		} else {
			h.completeWorkMutationRange(c, cmd.MutationID, ack, result.FirstRevision)
		}

	case "work_project_create":
		project, err := h.work.CreateProject(ctx, workitems.CreateProjectInput{
			ID: cmd.ProjectID, Name: cmd.Name, WorkspacePath: cmd.WorkspacePath, Context: cmd.ProjectContext,
		})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		snapshot, _ := h.work.SnapshotForDevice(ctx, c.deviceID)
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: project.Version, Revision: snapshot.Revision, Project: &project})

	case "work_item_create":
		item, err := h.work.CreateItem(ctx, workitems.CreateItemInput{
			ID: cmd.WorkItemID, ProjectID: cmd.ProjectID, Title: cmd.Title,
			Description: cmd.Description, Outcome: cmd.Outcome, NextStep: cmd.NextStep,
			AcceptanceCriteria: cmd.AcceptanceCriteria, Priority: workitems.Priority(cmd.Priority),
			SortKey: cmd.SortKey, Assignee: cmd.Assignee, DueAt: cmd.DueAt, Labels: cmd.Labels,
			AutomationMode: cmd.AutomationMode, WorkflowID: cmd.WorkflowID, WorkflowNodeID: cmd.WorkflowNodeID, Actor: actor,
		})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_session_import":
		s, ok := h.registry.Get(cmd.SessionID)
		if !ok {
			h.sendWorkError(c, cmd.MutationID, errors.New("cannot import an unknown Bridge session"))
			return
		}
		snapshot := s.Snapshot()
		// Session identity and workspace come from the Bridge registry, never
		// from client-provided metadata. The user still confirms all semantic
		// fields before this command is sent.
		result, err := h.work.ImportSessionDraft(ctx, workitems.ImportSessionDraftInput{
			ProjectID: cmd.ProjectID, ProjectName: cmd.Name, WorkspacePath: snapshot.Cwd,
			WorkItemID: cmd.WorkItemID, SessionID: snapshot.ID, ThreadIDSnapshot: snapshot.ResumeID,
			Title: cmd.Title, Description: cmd.Description, Outcome: cmd.Outcome,
			NextStep: cmd.NextStep, AcceptanceCriteria: cmd.AcceptanceCriteria,
			Priority: workitems.Priority(cmd.Priority), SortKey: cmd.SortKey, Actor: actor,
		})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		ack := protocol.WorkMutationAck{Type: "work_mutation_ack", MutationID: cmd.MutationID,
			EntityVersion: result.Item.Version, Revision: result.Item.ActivityRevision,
			Project: &result.Project, Item: &result.Item, Link: &result.Link}
		if result.AlreadyLinked {
			h.completeWorkMutationWithoutBroadcast(c, cmd.MutationID, ack)
		} else {
			h.completeWorkMutationRange(c, cmd.MutationID, ack, result.FirstRevision)
		}

	case "work_item_update":
		if len(cmd.Fields) == 0 {
			h.sendWorkError(c, cmd.MutationID, errors.New("work item update requires fields"))
			return
		}
		input := workitems.UpdateItemInput{ID: cmd.WorkItemID, ExpectedVersion: cmd.ExpectedVersion, Actor: actor}
		for _, field := range cmd.Fields {
			switch field {
			case "title":
				input.Title = &cmd.Title
			case "description":
				input.Description = &cmd.Description
			case "outcome":
				input.Outcome = &cmd.Outcome
			case "next_step":
				input.NextStep = &cmd.NextStep
			case "acceptance_criteria":
				input.AcceptanceCriteria = &cmd.AcceptanceCriteria
			case "priority":
				priority := workitems.Priority(cmd.Priority)
				input.Priority = &priority
			case "blocked_reason_code":
				input.BlockedReasonCode = &cmd.BlockedReasonCode
			case "blocked_note":
				input.BlockedNote = &cmd.BlockedNote
			case "assignee":
				input.Assignee = &cmd.Assignee
			case "due_at":
				due := cmd.DueAt
				input.DueAt = &due
			case "labels":
				input.Labels = &cmd.Labels
			case "automation_mode":
				input.AutomationMode = &cmd.AutomationMode
			case "workflow_id":
				input.WorkflowID = &cmd.WorkflowID
			case "workflow_node_id":
				input.WorkflowNodeID = &cmd.WorkflowNodeID
			default:
				h.sendWorkError(c, cmd.MutationID, errors.New("unsupported work item field: "+field))
				return
			}
		}
		item, err := h.work.UpdateItem(ctx, input)
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_workflow_create":
		workflow, err := h.work.CreateWorkflow(ctx, workitems.CreateWorkflowInput{ID: cmd.WorkflowID,
			ProjectID: cmd.ProjectID, Name: cmd.WorkflowName, Description: cmd.WorkflowDescription,
			Definition: cmd.WorkflowDefinition})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		snapshot, _ := h.work.SnapshotForDevice(ctx, c.deviceID)
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: workflow.Version, Revision: snapshot.Revision, Workflow: &workflow})

	case "work_item_move":
		item, err := h.work.MoveItem(ctx, workitems.MoveItemInput{ID: cmd.WorkItemID,
			ExpectedVersion: cmd.ExpectedVersion, Lifecycle: workitems.Lifecycle(cmd.Lifecycle),
			SortKey: cmd.SortKey, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_item_review_decision":
		result, err := h.work.DecideReview(ctx, workitems.ReviewDecisionInput{
			WorkItemID: cmd.WorkItemID, ExpectedVersion: cmd.ExpectedVersion, Decision: cmd.Decision,
			Feedback: cmd.Body, CommentID: cmd.CommentID, RunID: cmd.RunID, RequestID: cmd.RequestID, Actor: actor,
		})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		ack := protocol.WorkMutationAck{Type: "work_mutation_ack", MutationID: cmd.MutationID,
			EntityVersion: result.Item.Version, Revision: result.Item.ActivityRevision, Item: &result.Item,
			Comment: result.Comment, Run: result.Run}
		h.completeWorkMutation(c, cmd.MutationID, ack)
		if result.Run != nil {
			h.WakeWorkScheduler()
		}

	case "work_item_archive", "work_item_restore":
		item, err := h.work.ArchiveItem(ctx, workitems.ArchiveItemInput{ID: cmd.WorkItemID,
			ExpectedVersion: cmd.ExpectedVersion, Restore: cmd.Kind == "work_item_restore", Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_item_link_session":
		if _, ok := h.registry.Get(cmd.SessionID); !ok {
			h.sendWorkError(c, cmd.MutationID, errors.New("cannot link an unknown Bridge session"))
			return
		}
		link, item, err := h.work.LinkSession(ctx, workitems.LinkSessionInput{
			WorkItemID: cmd.WorkItemID, SessionID: cmd.SessionID, ThreadIDSnapshot: cmd.ThreadID,
			Role: cmd.Role, ExpectedVersion: cmd.ExpectedVersion, Actor: actor,
		})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item, Link: &link})

	case "work_item_unlink_session":
		link, item, err := h.work.UnlinkSession(ctx, workitems.UnlinkSessionInput{LinkID: cmd.WorkLinkID,
			ExpectedVersion: cmd.ExpectedVersion, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item, Link: &link})

	case "work_item_dependency_add":
		item, err := h.work.AddDependency(ctx, workitems.AddDependencyInput{WorkItemID: cmd.WorkItemID,
			DependsOnID: cmd.DependsOnID, ExpectedVersion: cmd.ExpectedVersion, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_item_dependency_remove":
		item, err := h.work.RemoveDependency(ctx, workitems.AddDependencyInput{WorkItemID: cmd.WorkItemID,
			DependsOnID: cmd.DependsOnID, ExpectedVersion: cmd.ExpectedVersion, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision, Item: &item})

	case "work_item_comment_add":
		comment, item, err := h.work.AddComment(ctx, workitems.AddCommentInput{ID: cmd.CommentID,
			WorkItemID: cmd.WorkItemID, ExpectedVersion: cmd.ExpectedVersion, Body: cmd.Body, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision,
			Item: &item, Comment: &comment})

	case "work_item_comment_edit":
		comment, item, err := h.work.EditComment(ctx, workitems.EditCommentInput{CommentID: cmd.CommentID,
			ExpectedVersion: cmd.ExpectedVersion, Body: cmd.Body, Actor: actor})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision,
			Item: &item, Comment: &comment})

	case "work_item_attachment_add":
		record, ok := h.attachments.Get(cmd.AttachmentID)
		if !ok {
			h.sendWorkError(c, cmd.MutationID, errors.New("canonical attachment not found"))
			return
		}
		found, newlyPinned := h.attachments.Pin(record.AttachmentID, cmd.WorkItemID)
		if !found {
			h.sendWorkError(c, cmd.MutationID, errors.New("canonical attachment could not be pinned"))
			return
		}
		displayName := strings.TrimSpace(cmd.Name)
		if displayName == "" {
			displayName = record.DisplayName
		}
		ref, item, err := h.work.AddAttachment(ctx, workitems.AddAttachmentInput{
			ID: cmd.WorkAttachmentID, WorkItemID: cmd.WorkItemID, AttachmentID: record.AttachmentID,
			DisplayName: displayName, SortKey: cmd.SortKey, ExpectedVersion: cmd.ExpectedVersion, Actor: actor,
		})
		if err != nil {
			if newlyPinned {
				h.attachments.Unpin(record.AttachmentID, cmd.WorkItemID)
			}
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		ref = h.materializeWorkAttachment(ref)
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision,
			Item: &item, Attachment: &ref})

	case "work_item_attachment_remove":
		ref, item, err := h.work.RemoveAttachment(ctx, workitems.RemoveAttachmentInput{
			AttachmentRefID: cmd.WorkAttachmentID, ExpectedVersion: cmd.ExpectedVersion, Actor: actor,
		})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.attachments.Unpin(ref.AttachmentID, ref.WorkItemID)
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision,
			Item: &item, Attachment: &ref})

	case "work_item_start_run":
		if h.rejectMobileWrite(c, cmd.SessionID) {
			return
		}
		if _, ok := h.registry.Get(cmd.SessionID); !ok {
			h.sendWorkError(c, cmd.MutationID, errors.New("cannot start a run in an unknown Bridge session"))
			return
		}
		run, item, err := h.work.StartRun(ctx, workitems.StartRunInput{
			ID: cmd.RunID, WorkItemID: cmd.WorkItemID, SessionID: cmd.SessionID,
			RequestID: cmd.RequestID, Kind: cmd.RunKind, Instruction: cmd.Content,
			ExpectedVersion: cmd.ExpectedVersion, Actor: actor,
		})
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		pack, err := h.work.BuildContextPack(ctx, item.ID, 24_000)
		if err != nil {
			_, _ = h.work.AdvanceRun(ctx, cmd.SessionID, cmd.RequestID, "failed", "context compilation failed: "+err.Error())
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		for i := range pack.Attachments {
			pack.Attachments[i] = h.materializeWorkAttachment(pack.Attachments[i])
		}
		pack.Prompt, pack.Truncated = workitems.RenderContextPrompt(pack, 24_000, pack.Truncated)
		if err := h.work.MarkRunSubmitted(ctx, run.ID); err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		h.completeWorkMutation(c, cmd.MutationID, protocol.WorkMutationAck{Type: "work_mutation_ack",
			MutationID: cmd.MutationID, EntityVersion: item.Version, Revision: item.ActivityRevision,
			Item: &item, Run: &run})
		// Reuse the ordinary Session actor and mobile/desktop control lease. The
		// durable run exists before submission, so runtime projection is ordered.
		h.route(ctx, c, clientproto.Command{Kind: "message", SessionID: cmd.SessionID,
			RequestID: cmd.RequestID, Content: h.workRunContent(pack, cmd.Content, h.cfg.Port)})

	case "work_item_read":
		view, err := h.work.MarkRead(ctx, c.deviceID, cmd.WorkItemID, cmd.Revision)
		if err != nil {
			h.sendWorkError(c, cmd.MutationID, err)
			return
		}
		c.enqueueEvent(protocol.WorkReadAck{Type: "work_read_ack", Item: view})
	}
}

func (h *Hub) workRunContent(pack workitems.ContextPack, instruction string, port int) string {
	api := ""
	if pack.Item.ID != "" {
		api = fmt.Sprintf("\n## Bridge Work API\nRead current context: GET http://127.0.0.1:%d/api/work/v1/items/%s/context\nRecord progress with POST http://127.0.0.1:%d/api/work/v1/items/%s/events using the current expected_version. Allowed actions: comment, progress, update_next_step, blocked, clear_blocked, request_review. The API cannot mark work done.\n", port, pack.Item.ID, port, pack.Item.ID)
	}
	if h.automation != nil {
		api += fmt.Sprintf("If and only if the trusted operator policy explicitly asks for a provider action draft, propose one with POST http://127.0.0.1:%d/api/automation/v1/proposals using JSON fields: id, connector_account_id, work_item_id, run_id, action_type, target_id, payload, display_preview. Allowed initial actions are facebook.page_post.publish, facebook.comment.reply, instagram.comment.reply, and threads.post.publish; payload is {\"message\":\"...\"}. Proposal creation does not execute it; a human must approve the exact payload hash.\n", port)
	}
	return pack.Prompt + api + "\n[User instruction]\n" + strings.TrimSpace(instruction)
}

func (h *Hub) completeWorkMutation(c *Client, mutationID string, event protocol.WorkMutationAck) {
	h.completeWorkMutationRange(c, mutationID, event, event.Revision)
}

func (h *Hub) completeWorkMutationRange(c *Client, mutationID string, event protocol.WorkMutationAck, firstRevision uint64) {
	if !h.rememberAndSendWorkMutation(c, mutationID, event) {
		return
	}
	if firstRevision == 0 {
		firstRevision = event.Revision
	}
	h.broadcastWorkRange(firstRevision)
	h.WakeAutomationScheduler()
	go func() {
		if _, err := h.work.CompactChanges(context.Background()); err != nil {
			log.Printf("[work] compact changes: %v", err)
		}
	}()
}

func (h *Hub) completeWorkMutationWithoutBroadcast(c *Client, mutationID string, event protocol.WorkMutationAck) {
	h.rememberAndSendWorkMutation(c, mutationID, event)
}

func (h *Hub) rememberAndSendWorkMutation(c *Client, mutationID string, event protocol.WorkMutationAck) bool {
	if event.Attachment != nil {
		materialized := h.materializeWorkAttachment(*event.Attachment)
		event.Attachment = &materialized
	}
	raw, err := json.Marshal(event)
	if err != nil {
		h.sendWorkError(c, mutationID, err)
		return false
	}
	if err := h.work.RememberMutation(context.Background(), c.deviceID, mutationID, raw); err != nil {
		h.sendWorkError(c, mutationID, err)
		return false
	}
	c.enqueue(raw)
	return true
}

func (h *Hub) sendWorkError(c *Client, mutationID string, err error) {
	var conflict *workitems.ConflictError
	if errors.As(err, &conflict) {
		c.enqueueEvent(protocol.WorkConflict{Type: "work_conflict", MutationID: mutationID,
			Reason: "version_conflict", Current: conflict.Current})
		return
	}
	code := "invalid_request"
	switch {
	case errors.Is(err, workitems.ErrNotFound):
		code = "not_found"
	case errors.Is(err, workitems.ErrHumanRequired):
		code = "human_acceptance_required"
	case errors.Is(err, workitems.ErrReviewDecisionRequired):
		code = "review_decision_required"
	case errors.Is(err, workitems.ErrDependencyCycle):
		code = "dependency_cycle"
	case errors.Is(err, workitems.ErrCrossProject):
		code = "cross_project_relation"
	case errors.Is(err, workitems.ErrSessionLinked):
		code = "session_already_linked"
	case errors.Is(err, workitems.ErrInvalidTransition):
		code = "invalid_transition"
	}
	c.enqueueEvent(protocol.WorkError{Type: "work_error", MutationID: mutationID, Code: code, Message: err.Error()})
}

func (h *Hub) broadcastWorkRevision(revision uint64) {
	h.broadcastWorkRange(revision)
}

func (h *Hub) broadcastWorkRange(firstRevision uint64) {
	if h.work == nil || firstRevision == 0 {
		return
	}
	changes, latest, compacted, err := h.work.ChangesSince(context.Background(), firstRevision-1, 16)
	if err != nil || compacted || len(changes) == 0 {
		if err != nil {
			log.Printf("[work] broadcast delta revision=%d: %v", firstRevision, err)
		}
		return
	}
	h.materializeWorkChanges(changes)
	event := protocol.NewWorkDeltaBatch(firstRevision-1, latest, changes)
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for client := range h.clients {
		if !client.enrollmentOnly && strings.TrimSpace(client.deviceID) != "" {
			clients = append(clients, client)
		}
	}
	h.mu.RUnlock()
	for _, client := range clients {
		client.enqueueEvent(event)
		_ = h.work.AckSync(context.Background(), client.deviceID, event.ToRevision, 0)
	}
}

func (h *Hub) materializeWorkSnapshot(snapshot *workitems.DeviceSnapshot) {
	for i := range snapshot.Attachments {
		snapshot.Attachments[i] = h.materializeWorkAttachment(snapshot.Attachments[i])
	}
}

func (h *Hub) materializeWorkChanges(changes []workitems.Change) {
	for i := range changes {
		var payload workitems.ChangePayload
		if json.Unmarshal(changes[i].Payload, &payload) != nil || payload.Attachment == nil {
			continue
		}
		materialized := h.materializeWorkAttachment(*payload.Attachment)
		payload.Attachment = &materialized
		if body, err := json.Marshal(payload); err == nil {
			changes[i].Payload = body
		}
	}
}

func (h *Hub) materializeWorkAttachment(ref workitems.AttachmentRef) workitems.AttachmentRef {
	ref.Status = "missing"
	record, ok := h.attachments.Get(ref.AttachmentID)
	if !ok {
		return ref
	}
	ref.Kind = record.Kind
	ref.MIMEType = record.MIMEType
	if ref.DisplayName == "" {
		ref.DisplayName = record.DisplayName
	}
	if info, err := os.Stat(record.Path); err != nil || info.IsDir() {
		return ref
	}
	ref.Status = "available"
	ref.URL = h.mediaScan.LocalURL(record.Path)
	return ref
}
