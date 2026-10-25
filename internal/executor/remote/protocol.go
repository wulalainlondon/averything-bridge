package remote

import (
	"encoding/json"

	"everything-go/internal/backend"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
)

const (
	frameRemoteHello              = "remote_hello"
	frameRemoteHelloAck           = "remote_hello_ack"
	frameTurnStart                = "turn_start"
	frameTurnStop                 = "turn_stop"
	frameSessionClear             = "session_clear"
	frameSessionClose             = "session_close"
	frameHistoryRequest           = "history_request"
	frameResumableSessionsRequest = "resumable_sessions_request"
	frameUsageRequest             = "usage_request"
	frameUserInputRequest         = "user_input_request"
	frameUserInputResponse        = "user_input_response"
	frameInteractionResolved      = "interaction_resolved"
)

type remoteFrame struct {
	Type         string                      `json:"type"`
	SessionID    string                      `json:"session_id,omitempty"`
	RequestID    string                      `json:"request_id,omitempty"`
	Delta        string                      `json:"delta,omitempty"`
	Content      string                      `json:"content,omitempty"`
	ToolID       string                      `json:"tool_id,omitempty"`
	ToolUseID    string                      `json:"tool_use_id,omitempty"`
	Name         string                      `json:"name,omitempty"`
	Command      string                      `json:"command,omitempty"`
	Output       string                      `json:"output,omitempty"`
	Code         string                      `json:"code,omitempty"`
	Message      string                      `json:"message,omitempty"`
	ResumeID     string                      `json:"resume_id,omitempty"`
	Capabilities map[string]bool             `json:"capabilities,omitempty"`
	RPCID        string                      `json:"rpc_id,omitempty"`
	Header       string                      `json:"header,omitempty"`
	Agent        string                      `json:"requesting_agent,omitempty"`
	Kind         string                      `json:"kind,omitempty"`
	Questions    []backend.UserInputQuestion `json:"questions,omitempty"`
	CreatedAt    int64                       `json:"created_at,omitempty"`
	Status       string                      `json:"status,omitempty"`
}

func parseRemoteFrame(data []byte) (remoteFrame, bool) {
	var f remoteFrame
	if json.Unmarshal(data, &f) != nil {
		return remoteFrame{}, false
	}
	return f, true
}

type remoteHelloFrame struct {
	Type    string `json:"type"`
	Version int    `json:"version"`
}

func remoteHello() remoteHelloFrame {
	return remoteHelloFrame{Type: frameRemoteHello, Version: 1}
}

type turnStartFrame struct {
	Type      string                    `json:"type"`
	SessionID string                    `json:"session_id"`
	RequestID string                    `json:"request_id"`
	Content   string                    `json:"content"`
	Model     string                    `json:"model"`
	Images    []backend.ImageAttachment `json:"images"`
	Files     []backend.FileAttachment  `json:"files"`
}

func turnStart(sessionID, reqID, content, model string, images []backend.ImageAttachment, files []backend.FileAttachment) turnStartFrame {
	return turnStartFrame{
		Type: frameTurnStart, SessionID: sessionID, RequestID: reqID,
		Content: content, Model: model, Images: images, Files: files,
	}
}

type turnStopFrame struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
}

func turnStop(sessionID, reqID string) turnStopFrame {
	return turnStopFrame{Type: frameTurnStop, SessionID: sessionID, RequestID: reqID}
}

type sessionFrame struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
}

func sessionClear(sessionID string) sessionFrame {
	return sessionFrame{Type: frameSessionClear, SessionID: sessionID}
}

func sessionClose(sessionID string) sessionFrame {
	return sessionFrame{Type: frameSessionClose, SessionID: sessionID}
}

type rpcFrame struct {
	Type     string `json:"type"`
	RPCID    string `json:"rpc_id"`
	ResumeID string `json:"resume_id,omitempty"`
	Opts     any    `json:"opts,omitempty"`
	Limit    int    `json:"limit,omitempty"`
}

func historyRequest(rpcID, resumeID string, opts history.Opts) rpcFrame {
	return rpcFrame{Type: frameHistoryRequest, RPCID: rpcID, ResumeID: resumeID, Opts: opts}
}

func resumableSessionsRequest(rpcID string, limit int) rpcFrame {
	return rpcFrame{Type: frameResumableSessionsRequest, RPCID: rpcID, Limit: limit}
}

func usageRequest(rpcID string) rpcFrame {
	return rpcFrame{Type: frameUsageRequest, RPCID: rpcID}
}

type historyResultFrame struct {
	Kind           string           `json:"kind"`
	Messages       []map[string]any `json:"messages"`
	SourceCount    int              `json:"source_count"`
	KnownIDFound   bool             `json:"known_id_found"`
	SnapshotReason string           `json:"snapshot_reason"`
	HasMoreBefore  bool             `json:"has_more_before"`
	Error          string           `json:"error"`
}

type resumableSessionsResultFrame struct {
	Sessions []history.ResumableSession `json:"sessions"`
	Error    string                     `json:"error"`
}

type usageResultFrame struct {
	Report *protocol.UsageReport `json:"report"`
	Error  string                `json:"error"`
}

type userInputResponseFrame struct {
	Type      string         `json:"type"`
	RequestID string         `json:"request_id"`
	SessionID string         `json:"session_id"`
	Answers   map[string]any `json:"answers"`
	Cancelled bool           `json:"cancelled"`
}

func userInputResponse(payload backend.UserInputPayload, answers map[string]any, cancelled bool) userInputResponseFrame {
	return userInputResponseFrame{
		Type: frameUserInputResponse, RequestID: payload.RequestID,
		SessionID: payload.SessionID, Answers: answers, Cancelled: cancelled,
	}
}

func usageReportFromWire(rep *protocol.UsageReport) *backend.UsageReport {
	if rep == nil {
		return nil
	}
	out := backend.NewUsageReport(
		usageWindowFromWire(rep.FiveHour),
		usageWindowFromWire(rep.SevenDay),
		usageWindowFromWire(rep.SevenDaySonnet),
	)
	out.BackendID, out.Source, out.CollectedAt = rep.BackendID, rep.Source, rep.CollectedAt
	return &out
}

func usageWindowFromWire(w *protocol.UsageWindow) *backend.UsageWindow {
	if w == nil {
		return nil
	}
	return &backend.UsageWindow{Utilization: w.Utilization, ResetsAt: w.ResetsAt, Unit: w.Unit, Label: w.Label, DurationMinutes: w.DurationMinutes}
}
