package core

import (
	"context"
	"time"

	"everything-go/internal/protocol"
)

func workspaceEventIdentity(event any) (string, string) {
	switch e := event.(type) {
	case protocol.TextChunk:
		return e.SessionID, e.RequestID
	case protocol.ThinkingChunk:
		return e.SessionID, e.RequestID
	case protocol.ToolStart:
		return e.SessionID, e.RequestID
	case protocol.ToolResult:
		return e.SessionID, e.RequestID
	case protocol.ToolEnd:
		return e.SessionID, e.RequestID
	case protocol.Done:
		return e.SessionID, e.RequestID
	case protocol.Stopped:
		return e.SessionID, e.RequestID
	case protocol.Error:
		return e.SessionID, e.RequestID
	}
	return "", ""
}
func (h *Hub) workspacePrivateNativeEvent(event any) bool {
	sid, rid := workspaceEventIdentity(event)
	return sid != "" && h.workspaceManagedRequest(sid, rid)
}
func (h *Hub) observeWorkspaceResearch(event any) {
	if h.workspace == nil {
		return
	}
	var sid, rid, tool, name, query, output string
	result := false
	switch e := event.(type) {
	case protocol.ToolStart:
		sid, rid, tool, name, query = e.SessionID, e.RequestID, e.ToolUseID, e.Name, e.Command
	case protocol.ToolResult:
		sid, rid, tool, output = e.SessionID, e.RequestID, e.ToolUseID, e.Output
		result = true
	default:
		return
	}
	if !h.workspaceManagedRequest(sid, rid) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, taskID, err := h.workspace.NativeRequestByProvider(ctx, sid, rid)
	if err != nil {
		return
	}
	g, err := h.workspace.TaskGrant(ctx, taskID)
	if err != nil || !h.workspaceGrantCurrent(g) {
		return
	}
	if h.workspace.RecordResearchTool(ctx, sid, rid, tool, name, query, output, result) == nil {
		h.workspaceInvalidation()
	}
}
