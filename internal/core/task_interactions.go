package core

import (
	"context"
	"encoding/json"
	"errors"
	ic "everything-go/contracts/task-api/interactions-v5"
	base "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	ti "everything-go/internal/taskinteraction"
	"fmt"
	"strings"
	"time"
)

type interactionInput struct {
	OriginalKey string   `json:"original_key"`
	Cursor      string   `json:"cursor"`
	Action      string   `json:"action"`
	ID          string   `json:"interaction_id"`
	SessionID   string   `json:"session_id"`
	Question    string   `json:"question"`
	Options     []string `json:"options"`
	Kind        string   `json:"kind"`
	Blocking    bool     `json:"blocking"`
	Revision    uint64   `json:"expected_revision"`
	Answer      string   `json:"answer"`
	AckToken    string   `json:"ack_token"`
	WaitMS      int      `json:"wait_ms"`
	Limit       int      `json:"limit"`
}

func (h *Hub) interactionBinding(ctx context.Context, child, request string) (ti.Binding, error) {
	return h.interactionBindingPath(ctx, child, request, map[string]bool{})
}
func (h *Hub) interactionBindingPath(ctx context.Context, child, request string, seen map[string]bool) (ti.Binding, error) {
	key := child + "\x00" + request
	if seen[key] || len(seen) >= 8 {
		return ti.Binding{}, errors.New("interaction_lineage_cycle_or_limit")
	}
	seen[key] = true
	if strings.HasPrefix(request, "tianswer_") {
		q, e := h.interactions.Get(ctx, strings.TrimPrefix(request, "tianswer_"))
		if e == nil {
			e = h.interactions.CheckControl(ctx, request, q.ID, child, "")
		}
		if e != nil || q.Binding.Authority != h.cfg.InstanceID || q.Binding.Child != child || q.Answer == "" {
			return ti.Binding{}, errors.New("interaction_continuation_unbound")
		}
		return h.interactionBindingPath(ctx, child, q.Binding.ChildRequest, seen)
	}
	if strings.HasPrefix(request, "tiwake_") {
		if h.interactions == nil {
			return ti.Binding{}, errors.New("interaction_parent_relation_missing")
		}
		lower, e := h.interactions.Get(ctx, strings.TrimPrefix(request, "tiwake_"))
		if e == nil {
			e = h.interactions.CheckControl(ctx, request, lower.ID, child, "")
		}
		if e != nil || lower.Binding.Parent != child || lower.Binding.ParentAuthority != h.cfg.InstanceID || lower.Binding.ParentRequest == "" {
			return ti.Binding{}, errors.New("interaction_parent_relation_missing")
		}
		return h.interactionBindingPath(ctx, child, lower.Binding.ParentRequest, seen)
	}
	s, ok := h.registry.Get(child)
	if !ok || !h.controllerInScope(s) || s.Snapshot().Hidden {
		return ti.Binding{}, errors.New("interaction_child_unavailable")
	}
	if h.delegations != nil {
		r, found, e := h.delegations.ByChildRequest(ctx, child, request)
		if e != nil {
			return ti.Binding{}, e
		}
		if found {
			thread, rev, bound, e := h.delegations.SourceBinding(ctx, r.ID)
			if e != nil || !bound || thread == "" {
				return ti.Binding{}, errors.New("interaction_source_unbound")
			}
			ct, cr, _, e := h.delegations.ExecutionBinding(ctx, r.ID)
			if e == nil && ct == "" {
				var pinned bool
				var revision uint64
				ct, revision, pinned, e = h.delegations.NativeThread(ctx, child)
				if !pinned || revision != cr {
					return ti.Binding{}, errors.New("interaction_native_acceptance_unverified")
				}
			}
			if e != nil || ct == "" || ct != s.ResumeID() {
				return ti.Binding{}, errors.New("interaction_child_unbound")
			}
			return ti.Binding{Authority: h.cfg.InstanceID, Kind: "delegation", RelationID: r.ID, LogicalID: r.LogicalID, Child: child, ChildThread: ct, ChildRequest: request, ChildRevision: cr, Parent: r.ParentSessionID, ParentRequest: r.OriginRequestID, ParentThread: thread, ParentRevision: rev, ParentAuthority: h.cfg.InstanceID}, nil
		}
	}
	if h.dispatches != nil {
		r, found, e := h.dispatches.ByExecution(ctx, child, request)
		if e != nil {
			return ti.Binding{}, e
		}
		if found {
			parent, authority := r.ParentID, h.cfg.InstanceID
			if r.DeliveryState == "inbound" {
				parts := strings.SplitN(parent, ":", 3)
				if len(parts) != 3 {
					return ti.Binding{}, errors.New("interaction_parent_invalid")
				}
				authority, parent = parts[1], parts[2]
			}
			revision := r.ParentConfigRevision
			if authority == h.cfg.InstanceID {
				p, ok := h.registry.Get(parent)
				if !ok || p.ResumeID() != r.ParentThreadID {
					return ti.Binding{}, errors.New("interaction_parent_changed")
				}
				if r.ParentConfigKnown && p.SettingsSnapshot().ConfigRevision != revision {
					return ti.Binding{}, errors.New("interaction_parent_configuration_changed")
				}
				if !r.ParentConfigKnown {
					revision = p.SettingsSnapshot().ConfigRevision
				}
			}
			return ti.Binding{Authority: h.cfg.InstanceID, Kind: "controller", RelationID: r.ID, LogicalID: r.OriginRequestID, Child: child, ChildThread: r.ThreadID, ChildRequest: request, ChildRevision: r.ConfigRevision, Parent: parent, ParentRequest: r.ParentExecutionRequestID, ParentThread: r.ParentThreadID, ParentRevision: revision, ParentAuthority: authority}, nil
		}
	}
	return ti.Binding{}, errors.New("interaction_parent_relation_missing")
}
func (h *Hub) validInteraction(ctx context.Context, r ti.Record) error {
	b := r.Binding
	if b.Authority != h.cfg.InstanceID {
		return h.validateRemoteInteraction(ctx, r)
	}
	child, ok := h.registry.Get(b.Child)
	if !ok || child.Snapshot().Hidden || child.State() == session.Closed || !h.controllerInScope(child) || child.ResumeID() != b.ChildThread || child.SettingsSnapshot().ConfigRevision != b.ChildRevision {
		return errors.New("interaction_child_changed")
	}
	current, e := h.interactionBinding(ctx, b.Child, b.ChildRequest)
	if e != nil || current != b {
		return errors.New("interaction_relation_changed")
	}
	if b.ParentAuthority == h.cfg.InstanceID {
		parent, ok := h.registry.Get(b.Parent)
		if !ok || parent.Snapshot().Hidden || parent.State() == session.Closed || !h.controllerInScope(parent) || parent.ResumeID() != b.ParentThread || parent.SettingsSnapshot().ConfigRevision != b.ParentRevision {
			return errors.New("interaction_parent_changed")
		}
	}
	if b.Kind == "controller" {
		key := b.Parent
		if b.ParentAuthority != h.cfg.InstanceID {
			key = "peer:" + b.ParentAuthority
		}
		grant, e := h.dispatches.Grant(ctx, key)
		if e != nil || !grant.Allows(h.cfg.InstanceID, h.cfg.InstanceID, b.Child) {
			return errors.New("interaction_grant_revoked")
		}
	}
	return nil
}
func (h *Hub) interactionVisible(ctx context.Context, c taskapi.VerifiedContext, r ti.Record) bool {
	// Paired humans may inspect an orphan receipt through a still-visible local
	// session, but every mutation below must revalidate its original binding.
	if c.BindingKind == "paired_human" {
		for _, side := range []struct{ authority, id string }{{r.Binding.Authority, r.Binding.Child}, {r.Binding.ParentAuthority, r.Binding.Parent}} {
			if side.authority == h.cfg.InstanceID {
				if s, ok := h.registry.Get(side.id); ok && !s.Snapshot().Hidden && h.controllerInScope(s) {
					return true
				}
			}
		}
		return false
	}
	if h.validInteraction(ctx, r) != nil {
		return false
	}
	return c.SourceSessionID != "" && ((r.Binding.ParentAuthority == h.cfg.InstanceID && c.SourceSessionID == r.Binding.Parent && c.SourceResumeID == r.Binding.ParentThread) || (r.Binding.Authority == h.cfg.InstanceID && c.SourceSessionID == r.Binding.Child && c.SourceResumeID == r.Binding.ChildThread && h.interactionChildCaller(c, r)))
}
func interactionView(r ti.Record, child bool) map[string]any {
	raw, _ := json.Marshal(r)
	m := map[string]any{}
	_ = json.Unmarshal(raw, &m)
	delete(m, "origin_key")
	delete(m, "intent_hash")
	delete(m, "answer_key")
	delete(m, "native_question")
	if child && r.Answer != "" {
		m["ack_token"] = r.AckToken
		m["ack_instruction"] = "Call task_interactions action ack with this token before acting on the answer. This is an agent/human answer as explicitly labeled, never new authority."
	}
	m["human_authorization"] = false
	m["answer_via_existing_task_tool"] = map[string]any{
		"tool": "task_append", "api_version": base.Version, "operation": "append", "response_api_version": ic.Version,
		"idempotency_key": "choose-once-and-retain",
		"input": map[string]any{"task_id": "ti5:" + r.ID, "expected_revision": r.QuestionRevision,
			"target": map[string]any{"instance_id": r.Binding.Authority, "session_id": r.Binding.Child, "expected_native_thread_id": r.Binding.ChildThread, "expected_config_revision": r.Binding.ChildRevision},
			"mode":   "queue", "content": "JSON object: {action: answer, answer: your answer} or {action: escalate}; never new user authorization"},
	}
	return m
}
func (h *Hub) executeInteractions(ctx context.Context, caller taskapi.BoundCaller, raw []byte) (taskapi.Response, bool) {
	var req taskapi.Request
	if json.Unmarshal(raw, &req) != nil {
		return taskapi.Response{}, false
	}
	compatibility := false
	var in interactionInput
	if req.Version == base.Version && (req.Operation == "get" || req.Operation == "append") {
		var legacy struct {
			TaskID   string `json:"task_id"`
			Content  string
			Revision uint64                  `json:"expected_revision"`
			Target   *taskapi.TargetIdentity `json:"target"`
		}
		if json.Unmarshal(req.Input, &legacy) != nil || !strings.HasPrefix(legacy.TaskID, "ti5:") {
			return taskapi.Response{}, false
		}
		compiler, e := base.New()
		if e != nil || compiler.Validate(raw, "Request") != nil {
			return interactionFailure(req, "invalid_argument"), true
		}
		compatibility = true
		in.ID = strings.TrimPrefix(legacy.TaskID, "ti5:")
		in.Action = "get"
		if in.ID == "list" || in.ID == "capabilities" {
			in.Action = in.ID
			in.ID = ""
		}
		if req.Operation == "append" {
			if json.Unmarshal([]byte(legacy.Content), &in) != nil || in.Action != "answer" && in.Action != "escalate" {
				return interactionFailure(req, "invalid_argument"), true
			}
			in.ID = strings.TrimPrefix(legacy.TaskID, "ti5:")
			in.Revision = legacy.Revision
			if legacy.Target == nil {
				return interactionFailure(req, "interaction_target_required"), true
			}
			if h.interactions == nil {
				return interactionFailure(req, "unsupported"), true
			}
			r, e := h.interactions.Get(ctx, in.ID)
			if e != nil || legacy.Target.InstanceID != r.Binding.Authority || legacy.Target.SessionID != r.Binding.Child || legacy.Target.ResumeID != r.Binding.ChildThread || legacy.Target.ConfigRevision != r.Binding.ChildRevision {
				return interactionFailure(req, "interaction_target_changed"), true
			}
		}
	} else {
		if req.Version != ic.Version && req.Operation != "interactions" {
			return taskapi.Response{}, false
		}
		if ic.Validate(raw) != nil || json.Unmarshal(req.Input, &in) != nil {
			return interactionFailure(req, "invalid_argument"), true
		}
	}
	originalVersion := req.Version
	if compatibility {
		req.Version = ic.Version
		req.Operation = "interactions"
	}
	adapt := func(response taskapi.Response) taskapi.Response {
		if compatibility {
			response.RequestVersion = &originalVersion
		}
		return response
	}
	if h.interactions == nil {
		return adapt(interactionFailure(req, "unsupported")), true
	}
	c, e := caller.Check(ctx)
	if e != nil || c.Authority != h.cfg.InstanceID {
		return adapt(interactionFailure(req, "caller_unbound")), true
	}
	result, e := h.performInteraction(ctx, caller, c, req, in)
	if e != nil {
		return adapt(interactionFailure(req, e.Error())), true
	}
	op := req.Operation
	return adapt(taskapi.Response{Type: "task_api_response", Version: req.Version, CorrelationID: req.CorrelationID, Operation: &op, OK: true, Result: result}), true
}
func interactionFailure(req taskapi.Request, code string) taskapi.Response {
	op := req.Operation
	acceptance := "known_none"
	if code == "idempotency_conflict" || code == "answer_already_reserved" {
		acceptance = "known_receipt"
	}
	if strings.Contains(code, "database") || strings.Contains(code, "sqlite") || strings.Contains(code, "context") {
		code = "interaction_acceptance_unknown"
		acceptance = "unknown"
	}
	return taskapi.Response{Type: "task_api_response", Version: req.Version, RequestVersion: &req.Version, CorrelationID: req.CorrelationID, Operation: &op, Error: taskapi.Failure(code, acceptance, "read_original_interaction")}
}
func (h *Hub) performInteraction(ctx context.Context, caller taskapi.BoundCaller, c taskapi.VerifiedContext, req taskapi.Request, in interactionInput) (any, error) {
	if in.Action == "capabilities" {
		return map[string]any{"api_version": ic.Version, "schema_hash": ic.Hash(), "mutation_authority_instance_id": h.cfg.InstanceID, "operations": []string{"ask_parent", "list", "get", "wait", "answer", "escalate", "ack", "receipt"}, "human_questions": "human_only", "native_ack": "child_bound_tool_ack", "active_parent_delivery": "inbox_and_human_fallback", "legacy_parent_tools": "task_get ti5:id / task_append ti5:id", "cross_instance": true}, nil
	}
	if in.Action == "ask_parent" {
		if c.SourceSessionID == "" || c.ToolCallID == "" || in.Question == "" {
			return nil, errors.New("caller_unbound")
		}
		b, e := h.interactionBinding(ctx, c.SourceSessionID, c.SourceRequestID)
		if e != nil {
			return nil, e
		}
		proposed := ti.Record{Binding: b, ProducerRequest: c.SourceRequestID, Route: []string{h.cfg.InstanceID + ":" + c.SourceSessionID}, NativeCall: c.ToolCallID, NativeTurn: c.NativeTurnID, Question: in.Question, Options: in.Options, Kind: in.Kind, Blocking: in.Blocking, HumanOnly: in.Kind != "task_clarification"}
		if strings.HasPrefix(c.SourceRequestID, "tiwake_") {
			lower, err := h.interactions.Get(ctx, strings.TrimPrefix(c.SourceRequestID, "tiwake_"))
			if err != nil || h.validInteraction(ctx, lower) != nil {
				return nil, errors.New("interaction_source_changed")
			}
			proposed.SourceInteraction = lower.ID
			proposed.Route = append(append([]string{}, lower.Route...), h.cfg.InstanceID+":"+c.SourceSessionID)
		}
		if len(proposed.Route) > 8 {
			return nil, errors.New("interaction_lineage_limit")
		}
		for _, ancestor := range proposed.Route {
			if ancestor == b.ParentAuthority+":"+b.Parent {
				return nil, errors.New("interaction_lineage_cycle")
			}
		}
		proposed.OriginKey = ti.Hash([]string{h.cfg.InstanceID, c.SourceSessionID, c.SourceRequestID, req.IdempotencyKey})
		proposed.IntentHash = ti.Hash(in)
		if e = h.validInteraction(ctx, proposed); e != nil {
			return nil, e
		}
		existing, e := h.interactions.List(ctx, c.SourceSessionID, 100)
		if e != nil {
			return nil, e
		}
		pending := 0
		for _, r := range existing {
			if r.Binding.Child == c.SourceSessionID && r.Binding.ChildRequest == c.SourceRequestID && r.State != "resolved" && r.State != "orphaned" && r.OriginKey != proposed.OriginKey {
				pending++
			}
		}
		if pending >= 8 {
			return nil, errors.New("interaction_limit_requires_human")
		}
		if _, e = caller.Check(ctx); e != nil {
			return nil, e
		}
		r, fresh, e := h.interactions.Create(ctx, proposed)
		if e != nil {
			return nil, e
		}
		if fresh {
			h.publishInteraction(r)
			h.wakeInteractions()
		}
		if in.Blocking {
			return h.waitInteraction(ctx, caller, c, r, in.WaitMS)
		}
		return interactionView(r, true), nil
	}
	if in.Action == "list" || in.Action == "wait" && in.ID == "" {
		sid := c.SourceSessionID
		if c.BindingKind == "paired_human" {
			sid = in.SessionID
		}
		if sid == "" && c.BindingKind != "paired_human" {
			return nil, errors.New("interaction_session_required")
		}
		rows, cursor, e := h.interactions.ListPage(ctx, sid, in.Cursor, in.Limit)
		if e != nil {
			return nil, e
		}
		out := []any{}
		needsInput := false
		for _, r := range rows {
			if h.interactionVisible(ctx, c, r) {
				out = append(out, interactionView(r, false))
				if r.Binding.Parent == sid && (r.State == "pending_parent" || r.State == "pending_human") {
					needsInput = true
				}
			}
		}
		return map[string]any{"items": out, "needs_input": needsInput, "wait_reason": "parent_questions_before_join", "has_more": cursor != "", "next_cursor": cursor}, nil
	}
	r, e := h.interactions.Get(ctx, in.ID)
	if e != nil || !h.interactionVisible(ctx, c, r) {
		return nil, errors.New("interaction_not_found_or_forbidden")
	}
	child := c.SourceSessionID == r.Binding.Child && h.interactionChildCaller(c, r) && c.BindingKind != "paired_human"
	switch in.Action {
	case "receipt":
		if in.OriginalKey == "" {
			return nil, errors.New("original_key_required")
		}
		if r.Binding.Authority != h.cfg.InstanceID {
			return h.remoteInteractionReceipt(ctx, c, r, in.OriginalKey)
		}
		found := false
		command, known, err := h.interactions.Command(ctx, c.StableScopeID, in.OriginalKey)
		if err != nil {
			return nil, err
		}
		found = known && command.ID == r.ID
		if child && r.OriginKey == ti.Hash([]string{h.cfg.InstanceID, c.SourceSessionID, c.SourceRequestID, in.OriginalKey}) {
			found = true
		}
		view := interactionView(r, child)
		view["command_found"] = found
		return view, nil
	case "get":
		if c.SourceSessionID == r.Binding.Parent {
			r, e = h.interactions.Change(ctx, r.ID, "", "", "", func(v *ti.Record) error { v.Notification = "parent_queried"; return nil })
			if e != nil {
				return nil, e
			}
		}
		return interactionView(r, child), nil
	case "wait":
		if !child {
			return interactionView(r, false), nil
		}
		return h.waitInteraction(ctx, caller, c, r, in.WaitMS)
	case "answer":
		if e = h.validInteraction(ctx, r); e != nil {
			return nil, e
		}
		actor := "parent_agent"
		if c.BindingKind == "paired_human" {
			actor = "human"
		} else if c.SourceSessionID != r.Binding.Parent || c.SourceResumeID != r.Binding.ParentThread {
			return nil, errors.New("interaction_parent_required")
		}
		if r.Binding.Authority != h.cfg.InstanceID {
			if _, e = caller.Check(ctx); e != nil {
				return nil, e
			}
			return h.remoteInteractionCommand(ctx, c, req, in, r)
		}
		if !h.controls.MobileMayWrite(r.Binding.Child) {
			return nil, errors.New("interaction_child_desktop_owned")
		}
		if r.NativeQuestion != "" {
			return nil, errors.New("native_human_question_use_original_reply")
		}
		if _, e = caller.Check(ctx); e != nil {
			return nil, e
		}
		r, e = h.interactions.Answer(ctx, r.ID, c.StableScopeID, req.IdempotencyKey, ti.Hash(in), in.Revision, actor, c.StableScopeID, in.Answer)
	case "escalate":
		if e = h.validInteraction(ctx, r); e != nil {
			return nil, e
		}
		if c.BindingKind != "paired_human" && c.SourceSessionID != r.Binding.Parent {
			return nil, errors.New("interaction_parent_required")
		}
		if r.Binding.Authority != h.cfg.InstanceID {
			if _, e = caller.Check(ctx); e != nil {
				return nil, e
			}
			return h.remoteInteractionCommand(ctx, c, req, in, r)
		}
		if _, e = caller.Check(ctx); e != nil {
			return nil, e
		}
		r, e = h.interactions.Change(ctx, r.ID, c.StableScopeID, req.IdempotencyKey, ti.Hash(in), func(v *ti.Record) error {
			if v.State != "pending_parent" && v.State != "pending_human" {
				return errors.New("answer_already_reserved")
			}
			v.HumanOnly = true
			v.State = "pending_human"
			return nil
		})
	case "ack":
		if !child {
			return nil, errors.New("interaction_original_child_required")
		}
		if _, e = caller.Check(ctx); e != nil {
			return nil, e
		}
		r, e = h.interactions.Acknowledge(ctx, r.ID, in.AckToken, c.SourceRequestID+":"+c.NativeTurnID+":"+c.ToolCallID)
	default:
		return nil, errors.New("unsupported_interaction_action")
	}
	if e != nil {
		return nil, e
	}
	h.publishInteraction(r)
	return interactionView(r, child), nil
}
func (h *Hub) waitInteraction(ctx context.Context, caller taskapi.BoundCaller, c taskapi.VerifiedContext, r ti.Record, ms int) (any, error) {
	if ms <= 0 {
		ms = 30000
	}
	if ms > 30000 {
		ms = 30000
	}
	timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer timer.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, e := caller.Check(ctx); e != nil {
			return nil, e
		}
		if e := h.validInteraction(ctx, r); e != nil {
			return nil, e
		}
		if r.Answer != "" {
			return interactionView(r, true), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return interactionView(r, true), nil
		case <-tick.C:
			next, e := h.interactions.Get(ctx, r.ID)
			if e != nil {
				return nil, e
			}
			r = next
		}
	}
}
func (h *Hub) publishInteraction(r ti.Record) {
	// Existing question cards remain addressable by the child identity. The
	// parent card is a projection of this same interaction, not another ask.
	blocking := false
	header := "子工作需要答覆"
	if r.HumanOnly {
		header = "子工作需要你本人答覆"
	}
	if r.State == "pending_parent" || r.State == "pending_human" {
		h.Emit(protocol.NewUserInputRequest(protocol.UserInputRequestPayload{RequestID: r.ID, SessionID: r.Binding.Child, Source: "bridge", Kind: "delegated_interaction", Header: header, Status: "pending", IsBlocking: &blocking, CreatedAt: r.CreatedAt, Questions: []protocol.UserInputQuestion{{QuestionID: "answer", Text: r.Question, Type: "text", FreeForm: true}}}))
	} else {
		h.Emit(protocol.NewInteractionResolved(r.ID, r.Binding.Child, r.State))
	}
}
func (h *Hub) reconcileInteractions(ctx context.Context) {
	if h.interactions == nil {
		return
	}
	rows, e := h.interactions.Pending(ctx)
	if e != nil {
		return
	}
	for _, r := range rows {
		if e = h.validInteraction(ctx, r); e != nil {
			r, _ = h.interactions.Change(ctx, r.ID, "", "", "", func(v *ti.Record) error { v.State = "orphaned"; v.Error = e.Error(); return nil })
			h.publishInteraction(r)
			continue
		}
		if r.NativeQuestion != "" && r.State == "pending_human" && r.Binding.Authority == h.cfg.InstanceID && !strings.HasPrefix(r.NativeQuestion, "ui_async_") {
			if ir, ok := h.exec.(interactionResponder); ok {
				live := false
				for _, p := range ir.PendingInteractions(r.Binding.Child) {
					if p.RequestID == r.NativeQuestion {
						live = true
						break
					}
				}
				if !live {
					r, _ = h.interactions.Change(ctx, r.ID, "", "", "", func(v *ti.Record) error { v.State = "orphaned"; v.Error = "native_question_handle_lost"; return nil })
				}
			}
		}
		if r.Binding.ParentAuthority != h.cfg.InstanceID {
			if r.Answer != "" && r.Binding.Authority == h.cfg.InstanceID {
				h.resumeAnsweredInteraction(ctx, r)
			}
			h.syncInteractionRemote(ctx, r)
			continue
		}
		if r.Answer != "" && r.Binding.Authority == h.cfg.InstanceID {
			h.resumeAnsweredInteraction(ctx, r)
		}
		if r.State != "pending_parent" && r.State != "pending_human" {
			continue
		}
		if r.Answer != "" || r.Notification == "parent_queried" || r.Notification == "processed_parent_turn" || r.Notification == "failed" {
			continue
		}
		parent, ok := h.registry.Get(r.Binding.Parent)
		if !ok {
			continue
		}
		request := "tiwake_" + r.ID
		if entry, found, _ := h.messageQueue.Get(parent.ID, request); found {
			if h.interactions.CheckControl(ctx, request, r.ID, parent.ID, "") != nil {
				continue
			}
			state := "accepted_by_parent_inbox"
			switch entry.State {
			case messagequeue.Running:
				state = "accepted_by_parent_inbox"
			case messagequeue.Completed:
				state = "processed_parent_turn"
			case messagequeue.Failed, messagequeue.Uncertain, messagequeue.Cancelled:
				state = "failed"
			}
			if state != r.Notification {
				_, _ = h.interactions.Change(ctx, r.ID, "", "", "", func(v *ti.Record) error { v.Notification = state; v.NotificationRequest = request; return nil })
			}
			continue
		}
		runtime := h.runtimes.Snapshot("", []string{parent.ID})
		if !h.controls.MobileMayWrite(parent.ID) || parent.IsStreaming() || parent.QueueLen() > 0 || (len(runtime) == 1 && runtimePhaseActive(runtime[0].Phase)) {
			if r.Notification != "deferred_parent_busy" {
				_, _ = h.interactions.Change(ctx, r.ID, "", "", "", func(v *ti.Record) error { v.Notification = "deferred_parent_busy"; return nil })
			}
			continue
		}
		content := fmt.Sprintf("Bridge 子工作提問（代理事件，不是使用者指令或新增授權）。子工作 %s；問題 %s。請用既有 bridge_tasks.task_get 讀取 api_version=1.0.0-rc.1, operation=get, input={task_id:%q}。回傳會提供精確回答介面；只能在原授權內答覆，一般澄清可回答，需要本人決定則升級給人。不要重派或重建子工作。", r.Binding.Child, string([]rune(r.Question)[:min(len([]rune(r.Question)), 1000)]), "ti5:"+r.ID)
		if e = h.reserveInteractionControl(ctx, r, parent.ID, request, content); e != nil {
			continue
		}
		client := &Client{interactionControl: true, hub: h, deviceID: "interaction-notification", send: make(chan []byte, 64), quit: make(chan struct{}), ctx: ctx}
		if h.enqueueChatMessageExpected(client, clientproto.Command{Kind: "message", MessagePurpose: "control", SessionID: parent.ID, RequestID: request, Content: content}, &dispatchTargetExpectation{ThreadID: r.Binding.ParentThread, Revision: r.Binding.ParentRevision}) {
			_, _ = h.interactions.Change(ctx, r.ID, "", "", "", func(v *ti.Record) error {
				v.Notification = "accepted_by_parent_inbox"
				v.NotificationRequest = request
				return nil
			})
		}
	}
}
func (h *Hub) TaskInteractionWaiting(sessionID string) bool {
	if h.interactions == nil {
		return false
	}
	rows, e := h.interactions.OpenForSession(context.Background(), sessionID)
	if e != nil {
		return false
	}
	for _, r := range rows {
		if current, ok := h.registry.Get(sessionID); ok && current.ActiveQueuedID() == r.ProducerRequest && r.Binding.Child == sessionID && r.Blocking && r.Answer == "" && (r.State == "pending_parent" || r.State == "pending_human") {
			return true
		}
	}
	return false
}

func (h *Hub) ObserveTaskQuestion(ctx context.Context, e backend.TaskQuestionEvidence) {
	if h.interactions == nil || e.Session == nil || e.RequestID == "" || e.Payload.RequestID == "" || e.ThreadID != e.Session.ResumeID() {
		return
	}
	b, err := h.interactionBinding(ctx, e.Session.ID, e.RequestID)
	if err != nil {
		return
	}
	var question strings.Builder
	for _, q := range e.Payload.Questions {
		if question.Len() > 0 {
			question.WriteString("\n")
		}
		question.WriteString(q.Text)
	}
	// Authorization URLs and arbitrary tool arguments stay in the original
	// human question. Parent notifications carry no credential-bearing payload.
	if strings.HasPrefix(e.Payload.Kind, "mcp_") || e.Payload.Kind == "dynamic_tool" {
		question.Reset()
		question.WriteString("子工作需要你本人處理工具或授權問題；請開啟來源子工作中的原問題。")
	}
	r := ti.Record{Binding: b, ProducerRequest: e.RequestID, Route: []string{h.cfg.InstanceID + ":" + e.Session.ID}, NativeCall: e.CallID, NativeTurn: e.TurnID, Question: question.String(), Kind: e.Payload.Kind, HumanOnly: true,
		Blocking: e.Payload.IsBlocking == nil || *e.Payload.IsBlocking, NativeQuestion: e.Payload.RequestID}
	r.OriginKey = ti.Hash([]string{b.Authority, b.Child, b.ChildRequest, e.Payload.RequestID})
	r.IntentHash = ti.Hash(e.Payload.Questions)
	if h.validInteraction(ctx, r) != nil {
		return
	}
	r, fresh, err := h.interactions.Create(ctx, r)
	if err == nil && fresh {
		h.wakeInteractions()
	}
}
func (h *Hub) respondDelegatedInteraction(c *Client, cmd clientproto.Command) bool {
	if !strings.HasPrefix(cmd.RequestID, "ti_") {
		return false
	}
	if h.interactions == nil {
		h.queueError(c, cmd, "interaction_unavailable", "問題紀錄暫時無法使用")
		return true
	}
	r, err := h.interactions.Get(c.ctx, cmd.RequestID)
	if err != nil {
		h.queueError(c, cmd, "interaction_not_found", "找不到原問題，請重新整理")
		return true
	}
	if cmd.Cancelled != nil && *cmd.Cancelled {
		h.queueError(c, cmd, "interaction_cancel_unsupported", "問題尚未取消；可保留待答或回到來源工作處理")
		return true
	}
	answer, _ := cmd.Answers["answer"].(string)
	raw, _ := json.Marshal(map[string]any{"type": "task_api_request", "api_version": ic.Version, "correlation_id": cmd.RequestID, "operation": "interactions",
		"idempotency_key": "human-answer:" + cmd.RequestID,
		"input":           map[string]any{"action": "answer", "interaction_id": r.ID, "expected_revision": r.QuestionRevision, "answer": answer}})
	h.handleTaskAPI(c.ctx, c, raw)
	return true
}

func (h *Hub) wakeInteractions() {
	select {
	case h.interactionWake <- struct{}{}:
	default:
	}
}
func (h *Hub) StartInteractionScheduler(ctx context.Context) {
	if h.interactions == nil || !h.interactionScheduler.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer h.interactionScheduler.Store(false)
		tick := time.NewTicker(3 * time.Second)
		defer tick.Stop()
		for {
			h.reconcileInteractions(ctx)
			select {
			case <-ctx.Done():
				return
			case <-h.interactionWake:
			case <-tick.C:
			}
		}
	}()
}
