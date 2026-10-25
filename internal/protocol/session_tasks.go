package protocol

// TaskOrigin is optional ordinary-dispatch intent; admission verifies registry,
// source thread/revision/request and controller grant, never a caller supplied by a model.
type TaskTarget struct {
	ThreadID       string `json:"thread_id"`
	ConfigRevision uint64 `json:"config_revision"`
}
type TaskOrigin struct {
	InstanceID     string `json:"instance_id"`
	SessionID      string `json:"session_id"`
	ThreadID       string `json:"thread_id"`
	ConfigRevision uint64 `json:"config_revision"`
	RequestID      string `json:"request_id"`
}
type SessionTask struct {
	HumanSourceLink    map[string]string `json:"human_source_link,omitempty"`
	APIAxes            map[string]string `json:"api_axes,omitempty"`
	ProviderExecution  map[string]string `json:"provider_execution,omitempty"`
	MessagePurpose     string            `json:"message_purpose,omitempty"`
	ReceiptFound       bool              `json:"receipt_found"`
	ExecutionRequestID string            `json:"execution_request_id,omitempty"`
	RelatedRequestIDs  []string          `json:"related_request_ids,omitempty"`
	CanOpenTarget      bool              `json:"can_open_target"`
	SessionName        string            `json:"session_name,omitempty"`
	InstanceName       string            `json:"instance_name,omitempty"`
	ConfigRevision     uint64            `json:"config_revision"`
	Stage              string            `json:"stage,omitempty"`
	ContentTruncated   bool              `json:"content_truncated,omitempty"`
	RequestID          string            `json:"request_id"`
	State              string            `json:"state"`
	Content            string            `json:"content"`
	CreatedAt          int64             `json:"created_at"`
	UpdatedAt          int64             `json:"updated_at"`
	NativeTurnID       string            `json:"native_turn_id,omitempty"`
	SourceMessageID    string            `json:"source_message_id,omitempty"`
	Summary            string            `json:"summary,omitempty"`
	Files              []string          `json:"files"`
	CanCancel          bool              `json:"can_cancel"`
	Transport          string            `json:"transport"`
	Origin             *TaskOrigin       `json:"origin,omitempty"`
	InstanceID         string            `json:"instance_id"`
	SessionID          string            `json:"session_id"`
	ThreadID           string            `json:"thread_id"`
	Error              string            `json:"error,omitempty"`
}
type SessionTasksSnapshot struct {
	ChildrenCoverage string        `json:"children_coverage,omitempty"`
	ChildrenStatus   string        `json:"children_status"`
	Type             string        `json:"type"`
	SessionID        string        `json:"session_id"`
	RequestID        string        `json:"request_id"`
	InstanceID       string        `json:"instance_id"`
	Status           string        `json:"status"`
	Message          string        `json:"message,omitempty"`
	Items            []SessionTask `json:"items"`
	Children         []SessionTask `json:"children"`
	HistoryStatus    string        `json:"history_status"`
}
