package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	ti "everything-go/internal/taskinteraction"
	"fmt"
	"strings"
)

// Only the original producer or this server's one durable answer continuation
// may consume the answer. An unrelated new turn in the same session is excluded.
func (h *Hub) interactionChildCaller(c taskapi.VerifiedContext, r ti.Record) bool {
	if c.SourceRequestID == r.ProducerRequest {
		return true
	}
	if c.SourceRequestID != "tianswer_"+r.ID || r.Answer == "" || h.messageQueue == nil {
		return false
	}
	if h.interactions.CheckControl(context.Background(), c.SourceRequestID, r.ID, r.Binding.Child, "") != nil {
		return false
	}
	e, ok, err := h.messageQueue.Get(r.Binding.Child, c.SourceRequestID)
	return err == nil && ok && e.State == messagequeue.Running
}
func (h *Hub) resumeAnsweredInteraction(ctx context.Context, r ti.Record) {
	if r.State != "answer_reserved" && r.State != "awaiting_consumption" || r.NativeQuestion != "" || h.messageQueue == nil {
		return
	}
	child, ok := h.registry.Get(r.Binding.Child)
	if !ok || child.IsStreaming() || child.ActiveQueuedID() != "" || child.QueueLen() > 0 || !h.controls.MobileMayWrite(child.ID) {
		return
	}
	runtime := h.runtimes.Snapshot("", []string{child.ID})
	if len(runtime) == 1 && runtimePhaseActive(runtime[0].Phase) {
		return
	}
	original, found, err := h.messageQueue.Get(child.ID, r.ProducerRequest)
	// Unknown/failed submission is not permission to retry. Only a proven
	// completed producer may receive a separate answer continuation.
	if err != nil || !found || original.State != messagequeue.Completed {
		return
	}
	id := "tianswer_" + r.ID
	if entry, exists, _ := h.messageQueue.Get(child.ID, id); exists {
		if entry.State == messagequeue.Failed || entry.State == messagequeue.Uncertain || entry.State == messagequeue.Cancelled || entry.State == messagequeue.Completed {
			_, _ = h.interactions.Change(ctx, r.ID, "", "", "", func(q *ti.Record) error {
				if q.State != "resolved" {
					q.Error = "answer_continuation_not_acknowledged; inspect original receipt, do not replay"
				}
				return nil
			})
		}
		return
	}
	if h.validInteraction(ctx, r) != nil {
		return
	}
	content := fmt.Sprintf("Bridge 子工作問題已收到答覆（代理事件，不是新的使用者授權）。沿用本對話處理問題 %s；先以 task_interactions get 讀取 interaction_id=%s，核對 answer_actor 與 human_authorization=false，再用 ack_token 呼叫 ack 確認接收。可透過既有 task_get api_version=1.0.0-rc.1 input={task_id:%q} 讀取。只接續原問題所需工作，不重跑原任務；若沒有安全的 ACK 工具，回報能力缺口，不冒稱已確認。", r.ID, r.ID, "ti5:"+r.ID)
	if h.reserveInteractionControl(ctx, r, child.ID, id, content) != nil {
		return
	}
	client := &Client{interactionControl: true, hub: h, deviceID: "interaction-answer", send: make(chan []byte, 64), quit: make(chan struct{}), ctx: ctx}
	if h.enqueueChatMessageExpected(client, clientproto.Command{Kind: "message", MessagePurpose: "control", SessionID: child.ID, RequestID: id, Content: content}, &dispatchTargetExpectation{ThreadID: r.Binding.ChildThread, Revision: r.Binding.ChildRevision}) {
		_, _ = h.interactions.Change(ctx, r.ID, "", "", "", func(q *ti.Record) error {
			if q.State == "answer_reserved" {
				q.State = "awaiting_consumption"
			}
			return nil
		})
	}
}

// Native resolved events prove the original reply path wrote/observed a reply,
// not that a model used it. Keep that evidence distinct from an explicit ACK.
func (h *Hub) observeInteractionResolution(e protocol.InteractionResolved) {
	if h.interactions == nil || strings.HasPrefix(e.RequestID, "ti_") {
		return
	}
	ctx := context.Background()
	rows, err := h.interactions.OpenForSession(ctx, e.SessionID)
	if err != nil {
		return
	}
	for _, q := range rows {
		if q.NativeQuestion != e.RequestID || q.Binding.Authority != h.cfg.InstanceID {
			continue
		}
		_, _ = h.interactions.Change(ctx, q.ID, "", "", "", func(r *ti.Record) error {
			if r.State != "pending_human" {
				return nil
			}
			switch e.Status {
			case "resolved":
				r.State = "native_reply_written"
				r.Actor = "human"
			case "expired":
				r.State = "orphaned"
				r.Error = "native_question_expired"
			case "cancelled":
				r.State = "cancelled"
			}
			return nil
		})
	}
	h.wakeInteractions()
}
func interactionPayload(r ti.Record) protocol.UserInputRequestPayload {
	blocking := false
	header := "子工作需要答覆"
	if r.HumanOnly {
		header = "子工作需要你本人答覆"
	}
	return protocol.UserInputRequestPayload{RequestID: r.ID, SessionID: r.Binding.Child, Source: "bridge", Kind: "delegated_interaction", Header: header, Status: "pending", IsBlocking: &blocking, CreatedAt: r.CreatedAt, Questions: []protocol.UserInputQuestion{{QuestionID: "answer", Text: r.Question, Type: "text", FreeForm: true}}}
}
func (h *Hub) appendTaskInteractionSnapshot(items []backend.UserInputPayload, sessionID string) []backend.UserInputPayload {
	if h.interactions == nil {
		return items
	}
	rows, err := h.interactions.OpenForSession(context.Background(), sessionID)
	if err != nil {
		return items
	}
	for _, r := range rows {
		if r.Binding.Authority != h.cfg.InstanceID || r.NativeQuestion != "" || sessionID != "" && r.Binding.Child != sessionID || (r.State != "pending_parent" && r.State != "pending_human") {
			continue
		}
		if h.validInteraction(context.Background(), r) == nil {
			items = append(items, interactionPayload(r))
		}
	}
	return items
}

// Status/round reads expose questions before a parent keeps waiting for final.
// Read receipts never imply the parent model has consumed these questions.
func (h *Hub) parentInteractionInbox(parent *session.Session, child string) []any {
	out := []any{}
	if h.interactions == nil || parent == nil {
		return out
	}
	rows, e := h.interactions.OpenForSession(context.Background(), parent.ID)
	if e != nil {
		return out
	}
	for _, q := range rows {
		if q.Binding.ParentAuthority != h.cfg.InstanceID || q.Binding.Parent != parent.ID || q.Binding.ParentThread != parent.ResumeID() || child != "" && q.Binding.Child != child || q.State != "pending_parent" && q.State != "pending_human" || h.validInteraction(context.Background(), q) != nil {
			continue
		}
		out = append(out, interactionView(q, false))
	}
	return out
}

// Claude's original owned process acknowledges the input UUID. This proof has
// no fabricated Codex turn ID and cannot be supplied by a public API request.
func (h *Hub) pinInteractionProviderThread(p backend.NativeProviderEvidence) {
	if h.delegations == nil || p.Backend != backend.Claude || p.Status != "" || p.TokenKind != "message_uuid" || p.Token == "" || p.ConversationID == "" {
		return
	}
	child, ok := h.registry.Get(p.SessionID)
	if !ok || child.Backend() != backend.Claude || child.ActiveQueuedID() != p.RequestID || child.ResumeID() != p.ConversationID {
		return
	}
	r, found, e := h.delegations.ByChildRequest(context.Background(), p.SessionID, p.RequestID)
	if e != nil || !found {
		return
	}
	_, revision, _, e := h.delegations.ExecutionBinding(context.Background(), r.ID)
	if e != nil || child.SettingsSnapshot().ConfigRevision != revision {
		return
	}
	_ = h.delegations.PinNativeThread(context.Background(), p.SessionID, p.ConversationID, revision)
}
