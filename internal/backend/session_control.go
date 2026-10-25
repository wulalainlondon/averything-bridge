package backend

import (
	"context"
	"everything-go/internal/session"
)

// Caller is derived from the app-server tool invocation, never from model arguments.
type SessionControlCaller struct {
	Parent                                 *session.Session
	RequestID, TurnID, ToolCallID, VoiceID string
}
type SessionControlRequest struct {
	Action                 string `json:"action"`
	InstanceID             string `json:"instance_id,omitempty"`
	SessionID              string `json:"session_id,omitempty"`
	ExpectedThreadID       string `json:"expected_thread_id,omitempty"`
	ExpectedConfigRevision uint64 `json:"expected_config_revision,omitempty"`
	Content                string `json:"content,omitempty"`
	Mode                   string `json:"mode,omitempty"`
	DispatchID             string `json:"dispatch_id,omitempty"`
	Offset                 int    `json:"offset,omitempty"`
	Limit                  int    `json:"limit,omitempty"`
	Query                  string `json:"query,omitempty"`
}
type SessionControlProvider interface {
	ControlSession(context.Context, SessionControlCaller, SessionControlRequest) (any, error)
}
type VoiceDelegationProvider interface {
	DelegateVoiceSession(SessionControlCaller, DelegationSpec) (DelegationReceipt, error)
}

// Task callbacks are confined to the actual live voice caller's own source.
type TaskCallbackControl struct {
	Action        string `json:"action"`
	TaskRequestID string `json:"task_request_id,omitempty"`
	Enabled       *bool  `json:"enabled,omitempty"`
}
type TaskCallbackProvider interface {
	ControlTaskCallback(context.Context, SessionControlCaller, TaskCallbackControl) (any, error)
}
