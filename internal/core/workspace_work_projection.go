package core

import (
	"context"
	"errors"

	"everything-go/internal/workitems"
	"everything-go/internal/workspace"
)

func (h *Hub) projectWorkspaceTask(ctx context.Context, roomID, taskID string) error {
	r, t, err := h.workspace.WorkProjectionState(ctx, roomID, taskID)
	if err != nil {
		return err
	}
	if r.ProjectID == "" {
		return nil
	}
	if h.work == nil {
		return errors.New("original_work_service_unavailable")
	}
	link, err := h.work.ProjectRoomTask(ctx, workitems.RoomTaskProjection{RoomID: roomID, TaskID: taskID, ProjectID: r.ProjectID, SessionID: t.SessionID, RequestID: t.RequestID, NativeThreadID: t.NativeThreadID, ConfigRevision: t.ConfigRevision, Execution: t.Axes.Execution, Final: t.Axes.Final, Admission: t.Admission, Human: t.Axes.Human})
	if err != nil {
		return err
	}
	if t.WorkItemID == link.WorkItemID && t.RunID == link.RunID {
		return nil
	}
	return h.workspace.Observe(ctx, roomID, taskID, workspace.Observation{WorkItemID: link.WorkItemID, RunID: link.RunID})
}
