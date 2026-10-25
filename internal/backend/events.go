package backend

import "everything-go/internal/protocol"

// CompletedAnswer is internal, exact-request terminal model text. Never broadcast as a user message.
type CompletedAnswer struct{ SessionID, RequestID, Text string }

// Event type aliases keep the current app-v1 wire structs as the concrete
// runtime representation while giving backend adapters a protocol-neutral
// package to depend on. The clientproto layer remains responsible for future
// wire-version translation.
type (
	TextChunk             = protocol.TextChunk
	ThinkingChunk         = protocol.ThinkingChunk
	ToolStart             = protocol.ToolStart
	ToolResult            = protocol.ToolResult
	ToolEnd               = protocol.ToolEnd
	TodoItem              = protocol.TodoItem
	TodoUpdate            = protocol.TodoUpdate
	Goal                  = protocol.Goal
	GoalUpdate            = protocol.GoalUpdate
	GoalCleared           = protocol.GoalCleared
	Done                  = protocol.Done
	Stopped               = protocol.Stopped
	Error                 = protocol.Error
	SessionUUID           = protocol.SessionUUID
	SessionWarning        = protocol.SessionWarning
	SessionCommandStarted = protocol.SessionCommandStarted
	SessionCommandDone    = protocol.SessionCommandDone
	SessionCommandFailed  = protocol.SessionCommandFailed
	UserInputOption       = protocol.UserInputOption
	UserInputQuestion     = protocol.UserInputQuestion
	UserInputPayload      = protocol.UserInputRequestPayload
	UserInputRequest      = protocol.UserInputRequestEvent
	InteractionResolved   = protocol.InteractionResolved
	TurnProgress          = protocol.TurnProgress
	SessionInitInfo       = protocol.SessionInitInfo
	MCPServerStatus       = protocol.MCPServerStatus
)

// UsageWindow is one quota window reported by a backend. Utilization is a
// normalized 0..1 fraction unless the backend explicitly documents another
// unit; ResetsAt is an optional ISO timestamp.
type UsageWindow struct {
	Utilization     *float64
	ResetsAt        *string
	Unit            string
	Label           string
	DurationMinutes int
}

// UsageReport is the backend-neutral usage payload. Client protocol adapters
// choose the outbound event name and wire tags.
type UsageReport struct {
	BackendID      string
	Source         string
	CollectedAt    int64
	FiveHour       *UsageWindow
	SevenDay       *UsageWindow
	SevenDaySonnet *UsageWindow
}

const (
	ErrBackendUnavailable = "backend_unavailable"
	ErrModelNotFound      = "model_not_found"
	ErrAuth               = "auth_error"
	ErrRateLimited        = "rate_limited"
	ErrTool               = "tool_error"
	ErrProcessDied        = "process_died"
	ErrTimeout            = "timeout"
	ErrTurn               = "turn_error"
	ErrSend               = "send_error"
	ErrPanic              = "executor_panic"
	ErrOllama             = "ollama_error"
	ErrRemote             = "remote_error"
	ErrRemoteReplaced     = "remote_replaced"
	ErrRemoteSendFailed   = "remote_send_failed"
	ErrRemoteDisconnected = "remote_disconnected"
)

const (
	MaxToolResultOutputBytes = 256 * 1024
	ToolResultTruncatedMark  = "\n...(tool output truncated)"
)

func TruncateToolOutput(output string) string {
	if len(output) <= MaxToolResultOutputBytes {
		return output
	}
	keep := MaxToolResultOutputBytes - len(ToolResultTruncatedMark)
	if keep < 0 {
		keep = 0
	}
	return output[:keep] + ToolResultTruncatedMark
}

func NewTextChunk(sessionID, reqID, content string) TextChunk {
	return protocol.NewTextChunk(sessionID, reqID, content)
}

func NewThinkingChunk(sessionID, reqID, content string) ThinkingChunk {
	return protocol.NewThinkingChunk(sessionID, reqID, content)
}

func NewToolStart(sessionID, reqID, toolUseID, name, command string) ToolStart {
	return protocol.NewToolStart(sessionID, reqID, toolUseID, name, command)
}

func NewToolResult(sessionID, reqID, toolUseID, output string) ToolResult {
	return protocol.NewToolResult(sessionID, reqID, toolUseID, TruncateToolOutput(output))
}

func NewToolEnd(sessionID, reqID, toolUseID string) ToolEnd {
	return protocol.NewToolEnd(sessionID, reqID, toolUseID)
}

func NewTodoUpdate(sessionID, reqID string, todos []TodoItem) TodoUpdate {
	return protocol.NewTodoUpdate(sessionID, reqID, todos)
}

func NewGoalUpdate(sessionID string, goal Goal) GoalUpdate {
	return protocol.NewGoalUpdate(sessionID, goal)
}

func NewGoalCleared(sessionID string) GoalCleared {
	return protocol.NewGoalCleared(sessionID)
}

func NewDone(sessionID, reqID string) Done {
	return protocol.NewDone(sessionID, reqID)
}

func NewStopped(sessionID, reqID string) Stopped {
	return protocol.NewStopped(sessionID, reqID)
}

func NewError(sessionID, reqID, code, message string) Error {
	return Error{Type: "error", SessionID: sessionID, RequestID: reqID, Code: code, Message: message}
}

func NewSessionUUID(sessionID, resumeID string) SessionUUID {
	return protocol.NewSessionUUID(sessionID, resumeID)
}

func NewSessionInitInfo(sessionID, model, permissionMode string, tools, slashCommands []string, mcpServers []MCPServerStatus) SessionInitInfo {
	return protocol.NewSessionInitInfo(sessionID, model, permissionMode, tools, slashCommands, mcpServers)
}

func NewSessionRuntimeMetadata(sessionID string, model, effort *string) protocol.SessionMetaUpdated {
	return protocol.NewSessionRuntimeMetadata(sessionID, model, effort)
}

func NewSessionWarning(sessionID, message string) SessionWarning {
	return protocol.NewSessionWarning(sessionID, message)
}

func NewSessionCommandStarted(sessionID, requestID string, queueLength int) SessionCommandStarted {
	return protocol.NewSessionCommandStarted(sessionID, requestID, queueLength)
}

func NewSessionCommandDone(sessionID, requestID string, queueLength int) SessionCommandDone {
	return protocol.NewSessionCommandDone(sessionID, requestID, queueLength)
}

func NewSessionCommandFailed(sessionID, requestID, message string, queueLength int) SessionCommandFailed {
	return protocol.NewSessionCommandFailed(sessionID, requestID, message, queueLength)
}

func NewUserInputRequest(payload UserInputPayload) UserInputRequest {
	return protocol.NewUserInputRequest(payload)
}

func NewInteractionResolved(requestID, sessionID, status string) InteractionResolved {
	return protocol.NewInteractionResolved(requestID, sessionID, status)
}

func NewTurnProgress(sessionID, requestID, stage, message string) TurnProgress {
	return TurnProgress{SessionID: sessionID, RequestID: requestID, Stage: stage, Message: message}
}

func NewUsageReport(fiveHour, sevenDay, sevenDaySonnet *UsageWindow) UsageReport {
	return UsageReport{FiveHour: fiveHour, SevenDay: sevenDay, SevenDaySonnet: sevenDaySonnet}
}
