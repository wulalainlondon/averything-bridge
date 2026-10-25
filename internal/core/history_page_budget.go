package core

import (
	"encoding/json"
	"fmt"

	"everything-go/internal/clientproto"
	"everything-go/internal/history"
)

const (
	// The first page should not download megabytes of collapsed tool output.
	ordinaryHistoryPageBytes = 512 << 10
	// A larger individual row still has to make progress through pagination.
	// Keep ample space below Android's16Mi UTF16 ceiling for the wire envelope.
	historyRowMaxBytes  = 12 << 20
	historyWireMaxBytes = 15 << 20
)

// boundOrdinaryHistoryFrame moves only a contiguous page boundary. Source
// bodies and IDs remain untouched, and the original cursor retrieves every
// earlier row. A single larger row is delivered alone below the hard ceiling.
func boundOrdinaryHistoryFrame(input *history.Result, cmd clientproto.Command) (*history.Result, error) {
	result := *input
	kept, size := 0, 0
	forward := cmd.Mode == "beginning" || cmd.Mode == "continuation"
	minimum := 1
	if cmd.Mode == "continuation" {
		minimum = 2
	} // anchor plus forward progress
	for index := range input.Messages {
		i := len(input.Messages) - index - 1
		if forward {
			i = index
		}
		raw, err := json.Marshal(input.Messages[i])
		if err != nil {
			return nil, err
		}
		rowBytes := len(raw) + 1024 // reserve space for projected IDs/row metadata
		if kept >= minimum && size+rowBytes > ordinaryHistoryPageBytes {
			break
		}
		if size+rowBytes > historyRowMaxBytes {
			return nil, fmt.Errorf("history row or continuation anchor exceeds the safe page size; original content retained")
		}
		size += rowBytes
		kept++
	}
	if kept < len(input.Messages) {
		if forward {
			result.Messages = input.Messages[:kept]
			result.HasMoreAfter = true
		} else {
			result.Messages = input.Messages[len(input.Messages)-kept:]
			result.HasMoreBefore = true
		}
		// A shortened delta must expose a page boundary instead of silently
		// claiming that the unseen middle is fully synchronized.
		if result.Kind == "delta" {
			result.Kind = "snapshot"
			result.SnapshotReason = "byte_budget"
		}
	}
	return &result, nil
}

func (h *Hub) historyReadError(c *Client, sessionID string, cmd clientproto.Command, code, message string) {
	event := h.client.Error(sessionID, code, message)
	event.CommandType = "request_history"
	if cmd.HistoryContinuity {
		event.RequestID = cmd.HistoryRequestID
	}
	c.enqueueEvent(event)
}

// Check the actual projected envelope as well as the pre-projection row budget.
// A bad history page must never take down unrelated chat, control or voice.
func (h *Hub) enqueueHistoryFrame(c *Client, sessionID string, cmd clientproto.Command, event any) {
	data, err := marshalEvent(event, c.wireAuthority())
	if err != nil || len(data) > historyWireMaxBytes {
		h.historyReadError(c, sessionID, cmd, "history_frame_too_large", "history page exceeds the safe wire size; original content retained")
		return
	}
	if !h.workspaceNativeFrameAllowed(c, data) {
		return
	}
	logOutbound(event)
	c.enqueue(data)
}
