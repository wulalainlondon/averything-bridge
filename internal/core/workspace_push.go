package core

import (
	"context"
	"errors"
	"everything-go/internal/workspace"
	"time"
)

func (h *Hub) SetWorkspacePush(sender workspace.PushSender) error {
	if h.workspaceScheduler.Load() {
		return errors.New("workspace_push_immutable_after_start")
	}
	h.workspacePush = sender
	return nil
}
func (h *Hub) reconcileWorkspacePush(ctx context.Context) {
	if h.workspacePush == nil || h.workspace == nil {
		return
	}
	ids, err := h.workspace.PushPending(ctx)
	if err != nil {
		return
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		p, err := h.workspace.ClaimPush(ctx, id)
		if err != nil {
			_ = h.workspace.DiscardPush(ctx, id)
			continue
		}
		if err = h.workspace.ValidatePush(ctx, id); err != nil {
			_ = h.workspace.FinishPush(ctx, p, workspace.PushReceipt{State: "revoked", Reason: "push_authorization_revoked"})
			continue
		}
		sendCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		receipt := h.workspacePush.Send(sendCtx, p.Token, p.Platform, h.cfg.InstanceID, p.RoomID, p.NotificationID)
		cancel()
		_ = h.workspace.FinishPush(ctx, p, receipt)
	}
}
