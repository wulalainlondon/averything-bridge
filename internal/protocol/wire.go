// Package protocol defines the external WebSocket wire contract shared with the
// React/Capacitor app. Field names and shapes MUST stay byte-compatible with
// app/src/schemas/bridge.ts (the Zod discriminated union is the source of truth).
//
// Go only needs the envelope ({type, session_id}) to route; the typed outbound
// builders below mirror the exact JSON the Python bridge emits so the same app
// renders identically against either backend.
package protocol

import (
	"encoding/json"
	"everything-go/internal/coordination"

	"everything-go/internal/eventinbox"
	"everything-go/internal/identity"
	"everything-go/internal/toolenv"
	"everything-go/internal/workitems"
)

// Inbound is a flat superset of every client→bridge frame we currently handle.
// All fields are optional; only `Type` is always present. New command fields can
// be appended without breaking older ones.
type Inbound struct {
	ToolEnvironment *toolenv.Request                   `json:"tool_environment,omitempty"`
	PM              *coordination.Command              `json:"pm,omitempty"`
	Collaboration   *coordination.CollaborationCommand `json:"collaboration,omitempty"`
	Type            string                             `json:"type"`
	SessionID       string                             `json:"session_id"`
	RequestID       string                             `json:"request_id"`

	// hello / pairing
	DeviceID        string      `json:"device_id"`
	DeviceName      string      `json:"device_name"`
	ClientSurface   string      `json:"client_surface"`
	AuthToken       string      `json:"auth_token"`
	ReplayAck       bool        `json:"replay_ack"`
	ProtocolVersion int         `json:"protocol_version"`
	ClientInfo      *ClientInfo `json:"client_info,omitempty"`
	ConnectionProbe bool        `json:"connection_probe,omitempty"`
	BatchID         string      `json:"batch_id"`
	Revision        uint64      `json:"revision"`
	Read            bool        `json:"read"`

	// new_session
	Name           string `json:"name"`
	Cwd            string `json:"cwd"`
	Backend        string `json:"backend"`
	Model          string `json:"model"`
	ModelSet       bool   `json:"model_set"`
	Sandbox        string `json:"sandbox"`
	ResumeClaudeID string `json:"resume_claude_id"`

	// message
	Content string `json:"content"`

	// binary video upload control frames
	UploadRequestID string `json:"upload_request_id"`
	UploadID        string `json:"upload_id"`
	UploadKind      string `json:"upload_kind,omitempty"`
	MediaType       string `json:"media_type"`
	SizeBytes       int64  `json:"size_bytes"`

	// request_history
	Limit     int    `json:"limit"`
	KnownLast string `json:"known_last_source_message_id"`
	Mode      string `json:"mode"`
	Before    string `json:"before_source_message_id"`
	// Client opt-in for thinking blocks in history messages. Legacy clients
	// never send it and keep receiving the Python-era text/tool_call-only shape.
	IncludeThinking bool `json:"include_thinking"`

	// rename / meta / effort
	Effort                 *string `json:"effort"`
	ExpectedConfigRevision *uint64 `json:"expected_config_revision,omitempty"`
	ConfigScope            string  `json:"config_scope,omitempty"`
	ServiceTier            *string `json:"service_tier"`
	CollaborationMode      *string `json:"collaboration_mode"`
	Personality            *string `json:"personality"`
	Pinned                 *bool   `json:"pinned"`
	Hidden                 *bool   `json:"hidden"`

	// Codex goal commands.
	Objective   string `json:"objective"`
	Status      string `json:"status"`
	TokenBudget *int   `json:"token_budget"`

	// runtime ops: shell / tasks / processes
	ShellID string `json:"shell_id"`
	Data    string `json:"data"`
	ID      string `json:"id"`    // kill_task target
	PID     int    `json:"pid"`   // kill_process target
	Force   bool   `json:"force"` // kill_process SIGKILL

	// file ops: browse_dir / open_file / fcm
	Path                    string                   `json:"path"`
	Paths                   []string                 `json:"paths"`
	ClientHash              string                   `json:"client_hash"`
	ExpectedModified        *int64                   `json:"expected_modified"`
	Token                   string                   `json:"token"`
	Platform                string                   `json:"platform"`
	NotificationPreferences *NotificationPreferences `json:"notification_preferences,omitempty"`

	// search
	Query            string         `json:"query"`
	Offset           int            `json:"offset"`
	Filters          *SearchFilters `json:"filters"`
	Cursor           string         `json:"cursor"`
	ProjectDir       string         `json:"project_dir"`
	IncludeHidden    bool           `json:"include_hidden"`
	IncludeSubagents bool           `json:"include_subagents"`
	MsgUUID          string         `json:"msg_uuid"`
	Around           int            `json:"around"`

	// WebRTC signaling: webrtc_offer carries SDP; webrtc_ice carries a trickled
	// candidate. SDPMLineIndex is a pointer because 0 is a valid index distinct
	// from "absent" (the app sends null when unknown).
	SDP           string `json:"sdp"`
	Candidate     string `json:"candidate"`
	SDPMid        string `json:"sdpMid"`
	SDPMLineIndex *int   `json:"sdpMLineIndex"`

	// user_input_response: the app's answer to an AskUserQuestion interaction.
	// Answers is keyed by question_id. Cancelled is a pointer to distinguish
	// "not sent" from an explicit false.
	Answers   map[string]any `json:"answers"`
	Cancelled *bool          `json:"cancelled"`

	// message attachments: images (base64, no data-URL prefix) and files. The
	// app puts them on the `message` frame alongside content.
	Images []InboundImage `json:"images"`
	Files  []InboundFile  `json:"files"`

	// permission_response: the user's decision on a gated op.
	Decision string `json:"decision"`

	// file push: push_file carries `path` (reuses Path above); file_push_ack
	// carries file_id.
	FileID string `json:"file_id"`

	// fork_session: optional truncation point; empty = copy the full transcript.
	// Name (above) optionally overrides the "<parent> (fork)" default.
	ForkAfterMessageID string `json:"fork_after_message_id"`

	// feed: feed_push carries the article; fetch/mark_read/delete carry feed_id.
	FeedID         string `json:"feed_id"`
	EventID        string `json:"event_id"`
	Title          string `json:"title"`
	HTML           string `json:"html"`
	Source         string `json:"source"`
	URL            string `json:"url"`
	ClientDedupKey string `json:"client_dedup_key"`
	ContentType    string `json:"content_type"`

	// Native WorkItem commands. Revision is reused as the sync/read cursor;
	// command-specific fields stay flat for backward-compatible decoding.
	ProjectID             string                       `json:"project_id"`
	WorkItemID            string                       `json:"work_item_id"`
	MutationID            string                       `json:"mutation_id"`
	ExpectedVersion       uint64                       `json:"expected_version"`
	ExpectedRevision      *uint64                      `json:"expected_revision"`
	Lifecycle             string                       `json:"lifecycle"`
	Priority              string                       `json:"priority"`
	SortKey               int64                        `json:"sort_key"`
	Description           string                       `json:"description"`
	Outcome               string                       `json:"outcome"`
	NextStep              string                       `json:"next_step"`
	AcceptanceCriteria    string                       `json:"acceptance_criteria"`
	WorkspacePath         string                       `json:"workspace_path"`
	ThreadID              string                       `json:"thread_id"`
	Role                  string                       `json:"role"`
	DependsOnID           string                       `json:"depends_on_id"`
	WorkLinkID            string                       `json:"work_link_id"`
	CommentID             string                       `json:"comment_id"`
	WorkAttachmentID      string                       `json:"work_attachment_id"`
	AttachmentID          string                       `json:"attachment_id"`
	RunID                 string                       `json:"run_id"`
	RunKind               string                       `json:"run_kind"`
	Body                  string                       `json:"body"`
	BlockedReasonCode     string                       `json:"blocked_reason_code"`
	BlockedNote           string                       `json:"blocked_note"`
	Assignee              string                       `json:"assignee"`
	DueAt                 *int64                       `json:"due_at"`
	Labels                []string                     `json:"labels"`
	AutomationMode        string                       `json:"automation_mode"`
	WorkflowID            string                       `json:"workflow_id"`
	WorkflowNodeID        string                       `json:"workflow_node_id"`
	WorkflowName          string                       `json:"workflow_name"`
	WorkflowDescription   string                       `json:"workflow_description"`
	WorkflowDefinition    workitems.WorkflowDefinition `json:"workflow_definition"`
	ProjectContext        string                       `json:"project_context"`
	DeliveredRevision     uint64                       `json:"delivered_revision"`
	AckedRevision         uint64                       `json:"acked_revision"`
	Fields                []string                     `json:"fields"`
	BootstrapID           string                       `json:"bootstrap_id"`
	CurrentState          string                       `json:"current_state"`
	SessionIDs            []string                     `json:"session_ids"`
	SelectedSuggestionIDs []string                     `json:"selected_suggestion_ids"`
}

// ClientInfo is optional telemetry on authenticated hello/ping frames. Build is
// a string because iOS and Android use different native build formats.
type ClientInfo struct {
	AppID    string `json:"app_id"`
	Platform string `json:"platform"`
	Version  string `json:"version"`
	Build    string `json:"build"`
	Channel  string `json:"channel"`
}

// InboundImage is one attached image on a message (app strips the data-URL
// prefix, so Data is raw base64).
type InboundImage struct {
	Data      string `json:"data"`
	MediaType string `json:"media_type"`
}

// InboundFile is one attached file on a message.
type InboundFile struct {
	Name         string `json:"name"`
	Content      string `json:"content"`
	MediaType    string `json:"media_type"`
	RemotePath   string `json:"remote_path"`
	AttachmentID string `json:"attachment_id,omitempty"`
	SizeBytes    int64  `json:"size_bytes"`
}

// SearchFilters mirrors the nested `filters` object on request_search.
type SearchFilters struct {
	ProjectDir       string `json:"project_dir"`
	Since            string `json:"since"`
	Role             string `json:"role"`
	ExcludeSubagents bool   `json:"exclude_subagents"`
	Source           string `json:"source"`
	MaxPerSession    int    `json:"max_per_session"`
}

// ParseInbound extracts the typed envelope from a raw frame.
func ParseInbound(raw []byte) (Inbound, error) {
	var in Inbound
	err := json.Unmarshal(raw, &in)
	if err == nil {
		in.SessionID = identity.WireSessionID(in.SessionID)
		for index, sessionID := range in.SessionIDs {
			in.SessionIDs[index] = identity.WireSessionID(sessionID)
		}
	}
	return in, err
}

// --- Outbound builders (bridge→client) -------------------------------------
// Each returns a struct whose json tags match bridge.ts exactly. The hub
// marshals the returned value and writes it to the socket.

type HelloAck struct {
	Type            string              `json:"type"`
	ProtocolVersion int                 `json:"protocol_version"`
	ClientID        string              `json:"client_id"`
	DeviceID        string              `json:"device_id"`
	DeviceName      string              `json:"device_name,omitempty"`
	InstanceID      string              `json:"instance_id,omitempty"`
	Gen             string              `json:"gen,omitempty"`
	IsLocked        bool                `json:"is_locked"`
	LockedToMe      bool                `json:"locked_to_me"`
	PairingOpen     bool                `json:"pairing_open,omitempty"`
	InstanceName    string              `json:"instance_name,omitempty"`
	RootDir         string              `json:"root_dir"`
	DataDir         string              `json:"data_dir"`
	LanIP           string              `json:"lan_ip,omitempty"`
	TunnelURL       string              `json:"tunnel_url,omitempty"`
	Backends        []BackendDefinition `json:"backend_registry,omitempty"`
	Capabilities    []string            `json:"capabilities,omitempty"`
}

type WorkSnapshot struct {
	Type               string `json:"type"`
	AuthoritativeReset bool   `json:"authoritative_reset,omitempty"`
	workitems.DeviceSnapshot
}

func NewWorkSnapshot(snapshot workitems.DeviceSnapshot, authoritativeReset ...bool) WorkSnapshot {
	if snapshot.Projects == nil {
		snapshot.Projects = []workitems.Project{}
	}
	if snapshot.Items == nil {
		snapshot.Items = []workitems.ItemView{}
	}
	if snapshot.SessionLinks == nil {
		snapshot.SessionLinks = []workitems.SessionLink{}
	}
	if snapshot.Dependencies == nil {
		snapshot.Dependencies = []workitems.Dependency{}
	}
	if snapshot.Comments == nil {
		snapshot.Comments = []workitems.Comment{}
	}
	if snapshot.Runs == nil {
		snapshot.Runs = []workitems.Run{}
	}
	if snapshot.Attachments == nil {
		snapshot.Attachments = []workitems.AttachmentRef{}
	}
	if snapshot.Activities == nil {
		snapshot.Activities = []workitems.Activity{}
	}
	if snapshot.Workflows == nil {
		snapshot.Workflows = []workitems.Workflow{}
	}
	if snapshot.Bootstraps == nil {
		snapshot.Bootstraps = []workitems.ProjectBootstrapDraft{}
	}
	reset := len(authoritativeReset) > 0 && authoritativeReset[0]
	return WorkSnapshot{Type: "work_snapshot", AuthoritativeReset: reset, DeviceSnapshot: snapshot}
}

type WorkDeltaBatch struct {
	Type         string             `json:"type"`
	FromRevision uint64             `json:"from_revision"`
	ToRevision   uint64             `json:"to_revision"`
	Latest       uint64             `json:"latest_revision"`
	Changes      []workitems.Change `json:"changes"`
}

func NewWorkDeltaBatch(from, latest uint64, changes []workitems.Change) WorkDeltaBatch {
	if changes == nil {
		changes = []workitems.Change{}
	}
	to := from
	if len(changes) > 0 {
		to = changes[len(changes)-1].Revision
	}
	return WorkDeltaBatch{Type: "work_delta_batch", FromRevision: from,
		ToRevision: to, Latest: latest, Changes: changes}
}

type WorkMutationAck struct {
	Type          string                           `json:"type"`
	MutationID    string                           `json:"mutation_id"`
	EntityVersion uint64                           `json:"entity_version"`
	Revision      uint64                           `json:"revision"`
	Project       *workitems.Project               `json:"project,omitempty"`
	Item          *workitems.WorkItem              `json:"item,omitempty"`
	Link          *workitems.SessionLink           `json:"link,omitempty"`
	Comment       *workitems.Comment               `json:"comment,omitempty"`
	Run           *workitems.Run                   `json:"run,omitempty"`
	Attachment    *workitems.AttachmentRef         `json:"attachment,omitempty"`
	Workflow      *workitems.Workflow              `json:"workflow,omitempty"`
	Bootstrap     *workitems.ProjectBootstrapDraft `json:"bootstrap,omitempty"`
	Items         []workitems.WorkItem             `json:"items,omitempty"`
	Links         []workitems.SessionLink          `json:"links,omitempty"`
}

type WorkConflict struct {
	Type       string             `json:"type"`
	MutationID string             `json:"mutation_id"`
	Reason     string             `json:"reason"`
	Current    workitems.WorkItem `json:"current"`
}

type WorkReadAck struct {
	Type string             `json:"type"`
	Item workitems.ItemView `json:"item"`
}

type WorkError struct {
	Type       string `json:"type"`
	MutationID string `json:"mutation_id,omitempty"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

type WorkDiagnostics struct {
	Type string `json:"type"`
	workitems.Diagnostics
}

type SessionControlState struct {
	Type           string `json:"type"`
	SessionID      string `json:"session_id"`
	ThreadID       string `json:"thread_id,omitempty"`
	Owner          string `json:"owner"`
	State          string `json:"state"`
	DesktopCommand string `json:"desktop_command,omitempty"`
	Message        string `json:"message,omitempty"`
	UpdatedAt      int64  `json:"updated_at,omitempty"`
}

// SessionRuntime is the server-authoritative lifecycle snapshot for one
// logical session. It is compact and durable; message content is reconciled
// separately from native history.
type SessionRuntime struct {
	Type               string `json:"type"`
	SessionID          string `json:"session_id"`
	Revision           uint64 `json:"revision"`
	Phase              string `json:"phase"`
	Stage              string `json:"stage,omitempty"`
	StageMessage       string `json:"stage_message,omitempty"`
	StageStartedAt     int64  `json:"stage_started_at,omitempty"`
	ActiveStartedAt    int64  `json:"active_started_at,omitempty"`
	ActiveRequestID    string `json:"active_request_id,omitempty"`
	QueueLength        int    `json:"queue_length"`
	LastTerminalStatus string `json:"last_terminal_status,omitempty"`
	LastError          string `json:"last_error,omitempty"`
	UpdatedAt          int64  `json:"updated_at"`
	CompletedAt        int64  `json:"completed_at,omitempty"`
	Unread             int    `json:"unread"`
	DeliveryPending    bool   `json:"delivery_pending"`
	HistoryReconcile   bool   `json:"history_reconcile"`
}

type SessionRuntimeSnapshot struct {
	Type  string           `json:"type"`
	Items []SessionRuntime `json:"items"`
}

// TurnProgress is an internal executor-to-Hub signal. Hub converts it into a
// revisioned SessionRuntime event and does not forward this transient value to
// clients directly, so reconnect always observes the same canonical stage.
type TurnProgress struct {
	SessionID string
	RequestID string
	Stage     string
	Message   string
}

func NewSessionControlState(sessionID, threadID, owner, state, command, message string, updatedAt int64) SessionControlState {
	return SessionControlState{Type: "session_control", SessionID: sessionID, ThreadID: threadID, Owner: owner,
		State: state, DesktopCommand: command, Message: message, UpdatedAt: updatedAt}
}

// BackendDefinition is the bridge-advertised backend/model registry consumed by
// the app's new-session UI. It is intentionally wire-level and static per
// bridge process; runtime status remains in sessions/tasks/usage events.
type BackendDefinition struct {
	ID                 string                        `json:"id"`
	Label              string                        `json:"label"`
	DefaultModel       string                        `json:"default_model,omitempty"`
	Models             []ModelDefinition             `json:"models,omitempty"`
	CollaborationModes []CollaborationModeDefinition `json:"collaboration_modes,omitempty"`
	Capabilities       BackendCapabilities           `json:"capabilities"`
}

type ModelDefinition struct {
	ID                        string                  `json:"id"`
	Label                     string                  `json:"label,omitempty"`
	Description               string                  `json:"description,omitempty"`
	SupportedReasoningEfforts []string                `json:"supported_reasoning_efforts,omitempty"`
	DefaultReasoningEffort    string                  `json:"default_reasoning_effort,omitempty"`
	InputModalities           []string                `json:"input_modalities,omitempty"`
	SupportsPersonality       bool                    `json:"supports_personality,omitempty"`
	ServiceTiers              []ServiceTierDefinition `json:"service_tiers,omitempty"`
	DefaultServiceTier        string                  `json:"default_service_tier,omitempty"`
	IsDefault                 bool                    `json:"is_default,omitempty"`
}

type ServiceTierDefinition struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

type CollaborationModeDefinition struct {
	Name            string `json:"name"`
	Mode            string `json:"mode,omitempty"`
	Model           string `json:"model,omitempty"`
	ReasoningEffort string `json:"reasoning_effort,omitempty"`
}

type BackendCapabilities struct {
	History      bool `json:"history,omitempty"`
	Usage        bool `json:"usage,omitempty"`
	Interactions bool `json:"interactions,omitempty"`
	Sandbox      bool `json:"sandbox,omitempty"`
	Images       bool `json:"images,omitempty"`
	Files        bool `json:"files,omitempty"`
	Remote       bool `json:"remote,omitempty"`
	Steering     bool `json:"steering,omitempty"`
}

// SteerResult is a direct acknowledgement for a steer_message command. A
// rejected command is never enqueued by the server; the client keeps its local
// queued item and can send it as the next ordinary turn.
type SteerResult struct {
	Type            string `json:"type"`
	SessionID       string `json:"session_id"`
	RequestID       string `json:"request_id"`
	Status          string `json:"status"`
	TurnID          string `json:"turn_id,omitempty"`
	ActiveRequestID string `json:"active_request_id,omitempty"`
	Message         string `json:"message,omitempty"`
}

func NewSteerResult(sessionID, requestID, status, turnID, activeRequestID, message string) SteerResult {
	return SteerResult{
		Type: "steer_result", SessionID: sessionID, RequestID: requestID,
		Status: status, TurnID: turnID, ActiveRequestID: activeRequestID, Message: message,
	}
}

type BackendRegistryUpdated struct {
	Type     string              `json:"type"`
	Backends []BackendDefinition `json:"backend_registry"`
}

func NewBackendRegistryUpdated(backends []BackendDefinition) BackendRegistryUpdated {
	if backends == nil {
		backends = []BackendDefinition{}
	}
	return BackendRegistryUpdated{Type: "backend_registry_updated", Backends: backends}
}

// --- Pairing acks -----------------------------------------------------------

type ClaimAck struct {
	Type       string `json:"type"`
	IsLocked   bool   `json:"is_locked"`
	LockedToMe bool   `json:"locked_to_me"`
}

func NewClaimAck() ClaimAck {
	return ClaimAck{Type: "claim_ack", IsLocked: true, LockedToMe: true}
}

type UnclaimAck struct {
	Type     string `json:"type"`
	IsLocked bool   `json:"is_locked"`
}

func NewUnclaimAck(isLocked bool) UnclaimAck {
	return UnclaimAck{Type: "unclaim_ack", IsLocked: isLocked}
}

type PairingWindowAck struct {
	Type      string `json:"type"`
	ExpiresAt int64  `json:"expires_at"`
}

func NewPairingWindowAck(expiresAt int64) PairingWindowAck {
	return PairingWindowAck{Type: "pairing_window_ack", ExpiresAt: expiresAt}
}

type Pong struct {
	Type string `json:"type"`
}

func NewPong() Pong { return Pong{Type: "pong"} }

type MessageAck struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
	Status    string `json:"status"`
}

func NewMessageAck(sessionID, requestID, status string) MessageAck {
	return MessageAck{Type: "message_ack", SessionID: sessionID, RequestID: requestID, Status: status}
}

// RecentMessage is one entry in SessionSummary.RecentMessages.
type RecentMessage struct {
	Role string `json:"role"`
	Text string `json:"text"`
}

// SessionSummary mirrors SessionSummarySchema.
type SessionSummary struct {
	ActiveModel          string          `json:"active_model,omitempty"`
	ActiveEffort         string          `json:"active_effort,omitempty"`
	ActiveServiceTier    string          `json:"active_service_tier,omitempty"`
	ActiveConfigRevision uint64          `json:"active_config_revision"`
	ConfigRevision       uint64          `json:"config_revision"`
	ID                   string          `json:"id"`
	Name                 string          `json:"name"`
	AuthorityInstanceID  string          `json:"authority_instance_id,omitempty"`
	MetadataRevision     uint64          `json:"metadata_revision"`
	NameUpdatedAt        int64           `json:"name_updated_at,omitempty"`
	NameUpdatedBy        string          `json:"name_updated_by,omitempty"`
	LastNameMutationID   string          `json:"last_name_mutation_id,omitempty"`
	IsStreaming          bool            `json:"is_streaming"`
	CreatedAt            float64         `json:"created_at"`
	LastActivity         float64         `json:"last_activity,omitempty"`
	Cwd                  string          `json:"cwd,omitempty"`
	Model                string          `json:"model,omitempty"`
	Effort               string          `json:"effort,omitempty"`
	ServiceTier          string          `json:"service_tier,omitempty"`
	CollaborationMode    string          `json:"collaboration_mode,omitempty"`
	Personality          string          `json:"personality,omitempty"`
	Backend              string          `json:"backend,omitempty"`
	Sandbox              string          `json:"sandbox,omitempty"`
	Pinned               bool            `json:"pinned"`
	Hidden               bool            `json:"hidden"`
	RecentMessages       []RecentMessage `json:"recent_messages,omitempty"`
	PreviewText          string          `json:"preview_text,omitempty"`
	PreviewRole          string          `json:"preview_role,omitempty"`
	PreviewVersion       int             `json:"preview_version,omitempty"`
}

// CodexLiveDiff carries the latest aggregated turn diff. Replacing rather than
// appending makes it reconnect/idempotency safe.
type CodexLiveDiff struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	Diff      string `json:"diff"`
}

func NewCodexLiveDiff(sessionID, requestID, diff string) CodexLiveDiff {
	return CodexLiveDiff{Type: "codex_live_diff", SessionID: sessionID, RequestID: requestID, Diff: diff}
}

type SessionsList struct {
	Type     string           `json:"type"`
	Sessions []SessionSummary `json:"sessions"`
}

func NewSessionsList(sessions []SessionSummary) SessionsList {
	if sessions == nil {
		sessions = []SessionSummary{}
	}
	return SessionsList{Type: "sessions_list", Sessions: sessions}
}

// SessionsListAppend is one batch of the full session snapshot delivered in
// response to get_all_sessions. Mirrors SessionsListAppendSchema and Python's
// send_all_sessions (batch_size 50; offset/total/done let the app assemble it).
type SessionsListAppend struct {
	Type     string           `json:"type"`
	Sessions []SessionSummary `json:"sessions"`
	Offset   int              `json:"offset"`
	Total    int              `json:"total"`
	Done     bool             `json:"done"`
}

func NewSessionsListAppend(sessions []SessionSummary, offset, total int, done bool) SessionsListAppend {
	if sessions == nil {
		sessions = []SessionSummary{}
	}
	return SessionsListAppend{Type: "sessions_list_append", Sessions: sessions, Offset: offset, Total: total, Done: done}
}

// RestartAck confirms a restart_bridge request was accepted. The app doesn't
// consume it (it just waits for the socket to drop and reconnects), but we emit
// it for wire parity with Python.
type RestartAck struct {
	Type string `json:"type"`
}

func NewRestartAck() RestartAck { return RestartAck{Type: "restart_ack"} }

type SessionCreated struct {
	Type      string  `json:"type"`
	SessionID string  `json:"session_id"`
	Name      string  `json:"name"`
	CreatedAt float64 `json:"created_at"`
	Cwd       string  `json:"cwd"`
	Backend   string  `json:"backend,omitempty"`
	Model     string  `json:"model,omitempty"`
	Sandbox   string  `json:"sandbox,omitempty"`
}

type SessionClosed struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
}

func NewSessionClosed(sessionID string) SessionClosed {
	return SessionClosed{Type: "session_closed", SessionID: sessionID}
}

type SessionWarning struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Message   string `json:"message"`
}

func NewSessionWarning(sessionID, msg string) SessionWarning {
	return SessionWarning{Type: "session_warning", SessionID: sessionID, Message: msg}
}

type SessionCommandStarted struct {
	Type        string `json:"type"`
	SessionID   string `json:"session_id"`
	RequestID   string `json:"request_id"`
	QueueLength int    `json:"queue_length"`
}

func NewSessionCommandStarted(sessionID, requestID string, queueLength int) SessionCommandStarted {
	return SessionCommandStarted{
		Type: "session_command_started", SessionID: sessionID,
		RequestID: requestID, QueueLength: queueLength,
	}
}

type SessionCommandDone struct {
	Type        string `json:"type"`
	SessionID   string `json:"session_id"`
	RequestID   string `json:"request_id"`
	QueueLength int    `json:"queue_length"`
}

func NewSessionCommandDone(sessionID, requestID string, queueLength int) SessionCommandDone {
	return SessionCommandDone{
		Type: "session_command_done", SessionID: sessionID,
		RequestID: requestID, QueueLength: queueLength,
	}
}

type SessionCommandFailed struct {
	Type        string `json:"type"`
	SessionID   string `json:"session_id"`
	RequestID   string `json:"request_id"`
	Message     string `json:"message"`
	QueueLength int    `json:"queue_length"`
}

func NewSessionCommandFailed(sessionID, requestID, message string, queueLength int) SessionCommandFailed {
	return SessionCommandFailed{
		Type: "session_command_failed", SessionID: sessionID,
		RequestID: requestID, Message: message, QueueLength: queueLength,
	}
}

// --- Streaming / turn events (session-scoped) ------------------------------

type TextChunk struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	Content   string `json:"content"`
}

func NewTextChunk(sessionID, reqID, content string) TextChunk {
	return TextChunk{Type: "text_chunk", SessionID: sessionID, RequestID: reqID, Content: content}
}

type ThinkingChunk struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	Content   string `json:"content"`
}

func NewThinkingChunk(sessionID, reqID, content string) ThinkingChunk {
	return ThinkingChunk{Type: "thinking_chunk", SessionID: sessionID, RequestID: reqID, Content: content}
}

type ToolStart struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	ToolUseID string `json:"tool_use_id"`
	Name      string `json:"name"`
	Command   string `json:"command"`
}

func NewToolStart(sessionID, reqID, toolUseID, name, command string) ToolStart {
	return ToolStart{Type: "tool_start", SessionID: sessionID, RequestID: reqID,
		ToolUseID: toolUseID, Name: name, Command: command}
}

type ToolResult struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	ToolUseID string `json:"tool_use_id"`
	Output    string `json:"output"`
}

func NewToolResult(sessionID, reqID, toolUseID, output string) ToolResult {
	return ToolResult{Type: "tool_result", SessionID: sessionID, RequestID: reqID,
		ToolUseID: toolUseID, Output: output}
}

type ToolEnd struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
	ToolUseID string `json:"tool_use_id"`
}

func NewToolEnd(sessionID, reqID, toolUseID string) ToolEnd {
	return ToolEnd{Type: "tool_end", SessionID: sessionID, RequestID: reqID, ToolUseID: toolUseID}
}

// TodoItem is one normalized task in the cross-backend todo panel. Mirrors the
// app's TodoItemSchema: id may be empty, content is required, status is one of
// pending|in_progress|completed, activeForm is nullable (→ JSON null).
type TodoItem struct {
	ID         string  `json:"id"`
	Content    string  `json:"content"`
	Status     string  `json:"status"`
	ActiveForm *string `json:"activeForm"`
}

// TodoUpdate is the normalized snapshot of the agent's task list, folding
// Claude TodoWrite/Task*, Codex update_plan, and Gemini write_todos into one
// full-replace event. Mirrors bridge's _evt_todo_update.
type TodoUpdate struct {
	Type      string     `json:"type"`
	SessionID string     `json:"session_id"`
	RequestID string     `json:"request_id,omitempty"`
	Todos     []TodoItem `json:"todos"`
}

func NewTodoUpdate(sessionID, reqID string, todos []TodoItem) TodoUpdate {
	if todos == nil {
		todos = []TodoItem{}
	}
	return TodoUpdate{Type: "todo_update", SessionID: sessionID, RequestID: reqID, Todos: todos}
}

type Goal struct {
	ThreadID        string `json:"threadId"`
	Objective       string `json:"objective"`
	Status          string `json:"status"`
	TokenBudget     *int   `json:"tokenBudget"`
	TokensUsed      int    `json:"tokensUsed"`
	TimeUsedSeconds int    `json:"timeUsedSeconds"`
	CreatedAt       int64  `json:"createdAt"`
	UpdatedAt       int64  `json:"updatedAt"`
}

type GoalUpdate struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Goal      Goal   `json:"goal"`
}

func NewGoalUpdate(sessionID string, goal Goal) GoalUpdate {
	return GoalUpdate{Type: "goal_update", SessionID: sessionID, Goal: goal}
}

type GoalCleared struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
}

// GoalSnapshotItem is the latest durable Goal state for one bridge session.
// Goal is nil when the latest operation cleared the goal. Revision is bridge-
// monotonic and lets clients reject stale live events after a reconnect.
type GoalSnapshotItem struct {
	SessionID string `json:"session_id"`
	Goal      *Goal  `json:"goal"`
	Revision  uint64 `json:"revision"`
}

type GoalsSnapshot struct {
	Type     string             `json:"type"`
	Revision uint64             `json:"revision"`
	Items    []GoalSnapshotItem `json:"items"`
}

func NewGoalsSnapshot(revision uint64, items []GoalSnapshotItem) GoalsSnapshot {
	if items == nil {
		items = []GoalSnapshotItem{}
	}
	return GoalsSnapshot{Type: "goals_snapshot", Revision: revision, Items: items}
}

// OfflineReplayBatch is application-acknowledged replay. Events remain in the
// bridge journal until the client applies the whole batch and replies with
// offline_replay_ack{batch_id}. Keeping the original events nested preserves
// wire compatibility and the normal event router semantics.
type OfflineReplayBatch struct {
	Type      string `json:"type"`
	BatchID   string `json:"batch_id"`
	Events    []any  `json:"events"`
	Remaining int    `json:"remaining"`
}

func NewOfflineReplayBatch(batchID string, events []any, remaining int) OfflineReplayBatch {
	if events == nil {
		events = []any{}
	}
	return OfflineReplayBatch{Type: "offline_replay_batch", BatchID: batchID, Events: events, Remaining: remaining}
}

func NewGoalCleared(sessionID string) GoalCleared {
	return GoalCleared{Type: "goal_cleared", SessionID: sessionID}
}

type Done struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
}

func NewDone(sessionID, reqID string) Done {
	return Done{Type: "done", SessionID: sessionID, RequestID: reqID}
}

type Stopped struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id,omitempty"`
}

func NewStopped(sessionID, reqID string) Stopped {
	return Stopped{Type: "stopped", SessionID: sessionID, RequestID: reqID}
}

type Error struct {
	CommandType string `json:"command_type,omitempty"`
	Type        string `json:"type"`
	SessionID   string `json:"session_id,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
	Code        string `json:"code,omitempty"`
	Message     string `json:"message"`
}

func NewError(sessionID, code, msg string) Error {
	return Error{Type: "error", SessionID: sessionID, Code: code, Message: msg}
}

type SessionUUID struct {
	Type       string `json:"type"`
	SessionID  string `json:"session_id"`
	ClaudeUUID string `json:"claude_uuid"`
}

func NewSessionUUID(sessionID, claudeUUID string) SessionUUID {
	return SessionUUID{Type: "session_uuid", SessionID: sessionID, ClaudeUUID: claudeUUID}
}

// MCPServerStatus is one entry of the CLI's system/init mcp_servers report.
type MCPServerStatus struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// SessionInitInfo surfaces the CLI's system/init capability report — available
// tools, slash commands, MCP servers, and the session's permission mode — so
// clients can build slash autocompletion and show the session's real surface.
type SessionInitInfo struct {
	Type           string            `json:"type"`
	SessionID      string            `json:"session_id"`
	Model          string            `json:"model,omitempty"`
	PermissionMode string            `json:"permission_mode,omitempty"`
	Tools          []string          `json:"tools"`
	SlashCommands  []string          `json:"slash_commands"`
	MCPServers     []MCPServerStatus `json:"mcp_servers"`
}

func NewSessionInitInfo(sessionID, model, permissionMode string, tools, slashCommands []string, mcpServers []MCPServerStatus) SessionInitInfo {
	if tools == nil {
		tools = []string{}
	}
	if slashCommands == nil {
		slashCommands = []string{}
	}
	if mcpServers == nil {
		mcpServers = []MCPServerStatus{}
	}
	return SessionInitInfo{
		Type: "session_init_info", SessionID: sessionID,
		Model: model, PermissionMode: permissionMode,
		Tools: tools, SlashCommands: slashCommands, MCPServers: mcpServers,
	}
}

// --- History ----------------------------------------------------------------

type HistorySnapshot struct {
	Type           string           `json:"type"`
	SessionID      string           `json:"session_id"`
	Messages       []map[string]any `json:"messages"`
	SourceCount    int              `json:"source_count"`
	HasMoreBefore  bool             `json:"has_more_before"`
	KnownIDFound   bool             `json:"known_id_found"`
	SnapshotReason string           `json:"snapshot_reason,omitempty"`
}

type HistoryDelta struct {
	Type                 string           `json:"type"`
	SessionID            string           `json:"session_id"`
	AfterSourceMessageID string           `json:"after_source_message_id"`
	Messages             []map[string]any `json:"messages"`
	SourceCount          int              `json:"source_count"`
}

type ResumableSessions struct {
	Type     string `json:"type"`
	Sessions any    `json:"sessions"`
}

func NewResumableSessions(sessions any) ResumableSessions {
	return ResumableSessions{Type: "resumable_sessions", Sessions: sessions}
}

// --- Session mgmt tail ------------------------------------------------------

type SessionRenamed struct {
	Type                string `json:"type"`
	SessionID           string `json:"session_id"`
	Name                string `json:"name"`
	AuthorityInstanceID string `json:"authority_instance_id,omitempty"`
	MutationID          string `json:"mutation_id,omitempty"`
	Revision            uint64 `json:"revision"`
	UpdatedAt           int64  `json:"updated_at,omitempty"`
	UpdatedBy           string `json:"updated_by,omitempty"`
}

func NewSessionRenamed(sessionID, name, authorityInstanceID, mutationID string, revision uint64, updatedAt int64, updatedBy string) SessionRenamed {
	return SessionRenamed{Type: "session_renamed", SessionID: sessionID, Name: name,
		AuthorityInstanceID: authorityInstanceID, MutationID: mutationID, Revision: revision,
		UpdatedAt: updatedAt, UpdatedBy: updatedBy}
}

type SessionRenameRejected struct {
	Type                string `json:"type"`
	SessionID           string `json:"session_id"`
	AuthorityInstanceID string `json:"authority_instance_id,omitempty"`
	MutationID          string `json:"mutation_id,omitempty"`
	Reason              string `json:"reason"`
	CurrentName         string `json:"current_name,omitempty"`
	CurrentRevision     uint64 `json:"current_revision"`
}

func NewSessionRenameRejected(sessionID, authorityInstanceID, mutationID, reason, currentName string, currentRevision uint64) SessionRenameRejected {
	return SessionRenameRejected{Type: "session_rename_rejected", SessionID: sessionID,
		AuthorityInstanceID: authorityInstanceID, MutationID: mutationID, Reason: reason,
		CurrentName: currentName, CurrentRevision: currentRevision}
}

// SessionForked announces a new session created by forking another's transcript.
type SessionForked struct {
	Type            string  `json:"type"`
	SessionID       string  `json:"session_id"`
	ParentSessionID string  `json:"parent_session_id"`
	Name            string  `json:"name"`
	CreatedAt       float64 `json:"created_at"`
}

func NewSessionForked(sessionID, parentID, name string, createdAt float64) SessionForked {
	return SessionForked{
		Type: "session_forked", SessionID: sessionID,
		ParentSessionID: parentID, Name: name, CreatedAt: createdAt,
	}
}

// ForkError reports why a fork_session request could not be completed
// (parent_busy / no_history_file / fork_point_not_found / copy_failed: …).
type ForkError struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
	Reason    string `json:"reason"`
}

func NewForkError(sessionID, reason string) ForkError {
	return ForkError{Type: "fork_error", SessionID: sessionID, Reason: reason}
}

// --- Agent tree (subagent hierarchy, read-only) -----------------------------

// AgentToolCall is one tool invocation recorded in an agent's transcript.
type AgentToolCall struct {
	Name string `json:"name"`
	TS   *int64 `json:"ts"`
}

// AgentNode is one node in the subagent tree. Mirrors AgentNodeWire in
// app/src/schemas/bridge.ts — nullable fields are pointers so they serialise to
// JSON null; ToolCalls/Children must always be arrays (never null).
type AgentNode struct {
	AgentID       string          `json:"agent_id"`
	AgentType     string          `json:"agent_type"`
	Description   string          `json:"description"`
	PromptID      *string         `json:"prompt_id"`
	ParentAgentID *string         `json:"parent_agent_id"`
	StartTS       *int64          `json:"start_ts"`
	EndTS         *int64          `json:"end_ts"`
	DurationMS    *int64          `json:"duration_ms"`
	ToolCalls     []AgentToolCall `json:"tool_calls"`
	OutputPreview string          `json:"output_preview"`
	Children      []*AgentNode    `json:"children"`
}

// AgentTree is the agent_tree event payload (response to get_agent_tree).
type AgentTree struct {
	Type        string       `json:"type"`
	SessionID   string       `json:"session_id"`
	ResumeID    string       `json:"resume_id"`
	TotalAgents int          `json:"total_agents"`
	Tree        []*AgentNode `json:"tree"`
}

func NewAgentTree(sessionID, resumeID string, total int, tree []*AgentNode) AgentTree {
	if tree == nil {
		tree = []*AgentNode{}
	}
	return AgentTree{
		Type: "agent_tree", SessionID: sessionID,
		ResumeID: resumeID, TotalAgents: total, Tree: tree,
	}
}

type SessionMetaUpdated struct {
	Type      string  `json:"type"`
	SessionID string  `json:"session_id"`
	Pinned    *bool   `json:"pinned,omitempty"`
	Hidden    *bool   `json:"hidden,omitempty"`
	Model     *string `json:"model,omitempty"`
	Effort    *string `json:"effort,omitempty"`
}

// SessionConfigResult is the authoritative response to switch_session_config.
// Every value is returned (including empty values that clear an override), so
// clients never have to infer whether an optimistic selection actually stuck.
type SessionConfigResult struct {
	ActiveConfiguration *ActiveSessionConfiguration `json:"active_configuration,omitempty"`
	EffectiveBoundary   string                      `json:"effective_boundary,omitempty"`
	ConfigRevision      uint64                      `json:"config_revision"`
	Type                string                      `json:"type"`
	SessionID           string                      `json:"session_id"`
	MutationID          string                      `json:"mutation_id,omitempty"`
	Accepted            bool                        `json:"accepted"`
	Reason              string                      `json:"reason,omitempty"`
	Backend             string                      `json:"backend"`
	Model               string                      `json:"model"`
	Effort              string                      `json:"effort"`
	Sandbox             string                      `json:"sandbox"`
	ServiceTier         string                      `json:"service_tier"`
	CollaborationMode   string                      `json:"collaboration_mode"`
	Personality         string                      `json:"personality"`
}

type ActiveSessionConfiguration struct {
	Model       string `json:"model"`
	Effort      string `json:"effort"`
	ServiceTier string `json:"service_tier"`
	Revision    uint64 `json:"revision"`
}

func NewSessionConfigResult(sessionID, mutationID string, accepted bool, reason string, backend, model, effort, sandbox, serviceTier, collaborationMode, personality string) SessionConfigResult {
	return SessionConfigResult{
		Type: "session_config_result", SessionID: sessionID, MutationID: mutationID,
		Accepted: accepted, Reason: reason, Backend: backend, Model: model,
		Effort: effort, Sandbox: sandbox, ServiceTier: serviceTier,
		CollaborationMode: collaborationMode, Personality: personality,
	}
}

func NewSessionRuntimeMetadata(sessionID string, model, effort *string) SessionMetaUpdated {
	return SessionMetaUpdated{Type: "session_meta_updated", SessionID: sessionID, Model: model, Effort: effort}
}

// --- Runtime ops: shell -----------------------------------------------------

type ShellCreated struct {
	Type    string `json:"type"`
	ShellID string `json:"shell_id"`
}

func NewShellCreated(shellID string) ShellCreated {
	return ShellCreated{Type: "shell_created", ShellID: shellID}
}

type ShellOutput struct {
	Type    string `json:"type"`
	ShellID string `json:"shell_id"`
	Data    string `json:"data"`
}

func NewShellOutput(shellID, data string) ShellOutput {
	return ShellOutput{Type: "shell_output", ShellID: shellID, Data: data}
}

type ShellClosed struct {
	Type    string `json:"type"`
	ShellID string `json:"shell_id"`
}

func NewShellClosed(shellID string) ShellClosed {
	return ShellClosed{Type: "shell_closed", ShellID: shellID}
}

// --- Runtime ops: tasks / processes -----------------------------------------

// Task mirrors the get_tasks row (sessions + shells).
type Task struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	PID         *int   `json:"pid"`
	IsStreaming bool   `json:"is_streaming"`
	Cwd         string `json:"cwd"`
}

type TasksList struct {
	Type  string `json:"type"`
	Tasks []Task `json:"tasks"`
}

func NewTasksList(tasks []Task) TasksList {
	if tasks == nil {
		tasks = []Task{}
	}
	return TasksList{Type: "tasks_list", Tasks: tasks}
}

type TaskKilled struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Success bool   `json:"success"`
}

func NewTaskKilled(id string, success bool) TaskKilled {
	return TaskKilled{Type: "task_killed", ID: id, Success: success}
}

// Process mirrors a get_processes row.
type Process struct {
	PID        int     `json:"pid"`
	CPUPercent float64 `json:"cpu_percent"`
	MemRSSKB   int     `json:"mem_rss_kb"`
	User       string  `json:"user"`
	Command    string  `json:"command"`
	Args       string  `json:"args"`
}

type ProcessesList struct {
	Type      string    `json:"type"`
	Processes []Process `json:"processes"`
}

func NewProcessesList(procs []Process) ProcessesList {
	if procs == nil {
		procs = []Process{}
	}
	return ProcessesList{Type: "processes_list", Processes: procs}
}

type ProcessKilled struct {
	Type    string `json:"type"`
	PID     int    `json:"pid"`
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

func NewProcessKilled(pid int, success bool, message string) ProcessKilled {
	return ProcessKilled{Type: "process_killed", PID: pid, Success: success, Message: message}
}

// --- File ops: browse_dir ---------------------------------------------------

// DirEntry mirrors a dir_listing entry.
type DirEntry struct {
	Name     string `json:"name"`
	IsDir    bool   `json:"is_dir"`
	Size     int64  `json:"size"`
	Modified int64  `json:"modified"`
}

// DirSession mirrors a dir_listing session row (active or resumable).
type DirSession struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	ClaudeUUID string `json:"claude_uuid"`
	LastUsed   int64  `json:"last_used"`
	Backend    string `json:"backend"`
	IsActive   bool   `json:"is_active"`
}

type DirListing struct {
	Type      string       `json:"type"`
	Path      string       `json:"path"`
	Entries   []DirEntry   `json:"entries"`
	Sessions  []DirSession `json:"sessions"`
	Hash      string       `json:"hash"`
	Unchanged bool         `json:"unchanged"`
}

func NewDirListing(path string, entries []DirEntry, sessions []DirSession, hash string, unchanged bool) DirListing {
	if entries == nil {
		entries = []DirEntry{}
	}
	if sessions == nil {
		sessions = []DirSession{}
	}
	return DirListing{Type: "dir_listing", Path: path, Entries: entries, Sessions: sessions, Hash: hash, Unchanged: unchanged}
}

type FileOpened struct {
	Type     string `json:"type"`
	Path     string `json:"path"`
	Name     string `json:"name"`
	Content  string `json:"content,omitempty"`
	URL      string `json:"url,omitempty"`
	Size     int64  `json:"size"`
	Modified int64  `json:"modified,omitempty"`
	MimeType string `json:"mime_type"`
	Error    string `json:"error,omitempty"`
}

func NewFileOpened(path, name, content, mediaURL string, size int64, mimeType, errorMessage string) FileOpened {
	return NewFileOpenedWithModified(path, name, content, mediaURL, size, 0, mimeType, errorMessage)
}

func NewFileOpenedWithModified(path, name, content, mediaURL string, size, modified int64, mimeType, errorMessage string) FileOpened {
	return FileOpened{
		Type:     "file_opened",
		Path:     path,
		Name:     name,
		Content:  content,
		URL:      mediaURL,
		Size:     size,
		Modified: modified,
		MimeType: mimeType,
		Error:    errorMessage,
	}
}

type MarkdownFileEntry struct {
	Path         string `json:"path"`
	Root         string `json:"root"`
	Name         string `json:"name"`
	RelativePath string `json:"relative_path"`
	Size         int64  `json:"size"`
	Modified     int64  `json:"modified"`
}

type MarkdownScanError struct {
	Path  string `json:"path"`
	Error string `json:"error"`
}

type MarkdownFilesListing struct {
	Type   string              `json:"type"`
	Roots  []string            `json:"roots"`
	Files  []MarkdownFileEntry `json:"files"`
	Errors []MarkdownScanError `json:"errors"`
}

func NewMarkdownFilesListing(roots []string, files []MarkdownFileEntry, errors []MarkdownScanError) MarkdownFilesListing {
	if roots == nil {
		roots = []string{}
	}
	if files == nil {
		files = []MarkdownFileEntry{}
	}
	if errors == nil {
		errors = []MarkdownScanError{}
	}
	return MarkdownFilesListing{Type: "markdown_files_listing", Roots: roots, Files: files, Errors: errors}
}

type FileSaved struct {
	Type     string `json:"type"`
	Path     string `json:"path"`
	Name     string `json:"name"`
	Content  string `json:"content,omitempty"`
	Size     int64  `json:"size"`
	Modified int64  `json:"modified,omitempty"`
	MimeType string `json:"mime_type"`
	Error    string `json:"error,omitempty"`
}

func NewFileSaved(path, name, content string, size, modified int64, mimeType, errorMessage string) FileSaved {
	return FileSaved{
		Type: "file_saved", Path: path, Name: name, Content: content,
		Size: size, Modified: modified, MimeType: mimeType, Error: errorMessage,
	}
}

// --- Usage ------------------------------------------------------------------

// UsageWindow mirrors one quota window. Utilization is a 0..1 fraction (or a
// raw token count for the Codex token fallback); both fields are nullable.
type UsageWindow struct {
	Utilization     *float64 `json:"utilization"`
	ResetsAt        *string  `json:"resets_at"`
	Unit            string   `json:"unit,omitempty"`
	Label           string   `json:"label,omitempty"`
	DurationMinutes int      `json:"duration_minutes,omitempty"`
}

type UsageReport struct {
	Type           string       `json:"type"`
	BackendID      string       `json:"backend_id,omitempty"`
	Source         string       `json:"source,omitempty"`
	CollectedAt    int64        `json:"collected_at,omitempty"`
	FiveHour       *UsageWindow `json:"five_hour"`
	SevenDay       *UsageWindow `json:"seven_day"`
	SevenDaySonnet *UsageWindow `json:"seven_day_sonnet"`
}

func NewUsageReport(fiveHour, sevenDay, sevenDaySonnet *UsageWindow) UsageReport {
	return UsageReport{Type: "usage_report", FiveHour: fiveHour, SevenDay: sevenDay, SevenDaySonnet: sevenDaySonnet}
}

// --- Status -----------------------------------------------------------------

type StatusResult struct {
	Type      string         `json:"type"`
	SessionID string         `json:"session_id"`
	Status    map[string]any `json:"status"`
}

// --- Git diff ---------------------------------------------------------------

type GitDiffResult struct {
	Type        string  `json:"type"`
	SessionID   string  `json:"session_id"`
	Diff        string  `json:"diff"`
	Error       *string `json:"error"`
	Initialized bool    `json:"initialized"`
}

func NewGitDiffResult(sessionID, diff, errCode string, initialized bool) GitDiffResult {
	var ep *string
	if errCode != "" {
		ep = &errCode
	}
	return GitDiffResult{
		Type: "git_diff_result", SessionID: sessionID, Diff: diff,
		Error: ep, Initialized: initialized,
	}
}

// --- WebRTC signaling (bridge→client) --------------------------------------
// The bridge is always the answerer: it bakes its ICE candidates into the
// answer SDP (Pion, like aiortc, doesn't trickle outbound), so it emits
// webrtc_answer + webrtc_ready only — never webrtc_ice. Shapes mirror
// WebRtcAnswerSchema / WebRtcReadySchema in bridge.ts.

type WebRTCAnswer struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

func NewWebRTCAnswer(sdp string) WebRTCAnswer {
	return WebRTCAnswer{Type: "webrtc_answer", SDP: sdp}
}

type WebRTCReady struct {
	Type string `json:"type"`
}

func NewWebRTCReady() WebRTCReady {
	return WebRTCReady{Type: "webrtc_ready"}
}

// --- Phase 5 read stubs: instances / inbox / feed --------------------------
// The app emits list_instances / get_inbox / feed_list_request on connect. The
// Go bridge implements neither the multi-instance supervisor, the file-push
// inbox, nor the article feed, so it answers with valid empty lists — the same
// shape Python returns on a master bridge with nothing configured. This gives
// the app well-formed data (no schema rejection) instead of silence. Arrays are
// always [] never null, per the app's z.array schemas.

type InstancesList struct {
	Type      string `json:"type"`
	Instances []any  `json:"instances"`
}

func NewInstancesList() InstancesList {
	return InstancesList{Type: "instances_list", Instances: []any{}}
}

// InboxItem is one file-push record as the app consumes it (inbox_list items and
// the file_push broadcast share these fields). PushedAt is omitted on a file_push
// broadcast (zero value) and present on inbox_list, mirroring the Python bridge.
type InboxItem struct {
	FileID   string  `json:"file_id"`
	Filename string  `json:"filename"`
	URL      string  `json:"url,omitempty"`
	Data     string  `json:"data,omitempty"`
	Size     int64   `json:"size"`
	MimeType string  `json:"mime_type"`
	PushedAt float64 `json:"pushed_at,omitempty"`
}

type InboxList struct {
	Type  string      `json:"type"`
	Items []InboxItem `json:"items"`
}

func NewInboxList() InboxList {
	return InboxList{Type: "inbox_list", Items: []InboxItem{}}
}

// NewInboxListItems builds inbox_list from pending items (get_inbox reply).
func NewInboxListItems(items []InboxItem) InboxList {
	if items == nil {
		items = []InboxItem{}
	}
	return InboxList{Type: "inbox_list", Items: items}
}

// FilePush is the broadcast announcing a newly pushed file (or a pending item
// replayed on hello). Carries the inline base64 body (Data) or a Storage URL.
type FilePush struct {
	Type     string `json:"type"`
	FileID   string `json:"file_id"`
	Filename string `json:"filename"`
	URL      string `json:"url,omitempty"`
	Data     string `json:"data,omitempty"`
	Size     int64  `json:"size"`
	MimeType string `json:"mime_type"`
}

func NewFilePush(fileID, filename, url, data string, size int64, mimeType string) FilePush {
	return FilePush{
		Type: "file_push", FileID: fileID, Filename: filename,
		URL: url, Data: data, Size: size, MimeType: mimeType,
	}
}

// PushAck is the direct acknowledgement to the sender that a push_file landed.
type PushAck struct {
	Type     string `json:"type"`
	FileID   string `json:"file_id"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
}

func NewPushAck(fileID, filename string, size int64) PushAck {
	return PushAck{Type: "push_ack", FileID: fileID, Filename: filename, Size: size}
}

type FeedList struct {
	Type  string `json:"type"`
	Items any    `json:"items"`
}

// NewFeedList wraps the feed metadata slice (pass a non-nil slice; the router
// passes feed.Store.List() or an empty slice).
func NewFeedList(items any) FeedList {
	if items == nil {
		items = []any{}
	}
	return FeedList{Type: "feed_list", Items: items}
}

// --- feed write events (bridge↔client), mirrors feed_ops.py -----------------

type FeedAck struct {
	Type   string `json:"type"`
	FeedID string `json:"feed_id"`
}

func NewFeedAck(feedID string) FeedAck { return FeedAck{Type: "feed_ack", FeedID: feedID} }

type FeedNew struct {
	Type string `json:"type"`
	Item any    `json:"item"`
}

func NewFeedNew(item any) FeedNew { return FeedNew{Type: "feed_new", Item: item} }

type FeedDetail struct {
	Type        string `json:"type"`
	FeedID      string `json:"feed_id"`
	HTML        string `json:"html"`
	ContentType string `json:"content_type"`
}

func NewFeedDetail(feedID, html, contentType string) FeedDetail {
	return FeedDetail{Type: "feed_detail", FeedID: feedID, HTML: html, ContentType: contentType}
}

type FeedUpdated struct {
	Type    string `json:"type"`
	FeedID  string `json:"feed_id"`
	Read    bool   `json:"read"`
	Deleted bool   `json:"deleted"`
}

func NewFeedUpdated(feedID string, read, deleted bool) FeedUpdated {
	return FeedUpdated{Type: "feed_updated", FeedID: feedID, Read: read, Deleted: deleted}
}

// --- Source-neutral external event inbox ----------------------------------

type ExternalEventSnapshot struct {
	Type     string            `json:"type"`
	Revision uint64            `json:"revision"`
	Items    []eventinbox.View `json:"items"`
}

func NewExternalEventSnapshot(snapshot eventinbox.Snapshot) ExternalEventSnapshot {
	if snapshot.Items == nil {
		snapshot.Items = []eventinbox.View{}
	}
	return ExternalEventSnapshot{Type: "external_event_snapshot", Revision: snapshot.Revision, Items: snapshot.Items}
}

type ExternalEventUpsert struct {
	Type string          `json:"type"`
	Item eventinbox.View `json:"item"`
}

func NewExternalEventUpsert(item eventinbox.View) ExternalEventUpsert {
	return ExternalEventUpsert{Type: "external_event_upsert", Item: item}
}

type ExternalEventState struct {
	Type      string `json:"type"`
	EventID   string `json:"event_id"`
	Read      bool   `json:"read"`
	Dismissed bool   `json:"dismissed"`
}

func NewExternalEventState(item eventinbox.View) ExternalEventState {
	return ExternalEventState{Type: "external_event_state", EventID: item.ID, Read: item.Read, Dismissed: item.Dismissed}
}

// --- Interactions: AskUserQuestion (bridge↔app) ----------------------------
// Mirrors bridge/interactions.py. A paused Claude turn surfaces an
// AskUserQuestion tool_use as user_input_request; the app answers with
// user_input_response {request_id, answers}; the bridge writes the answer back
// into the CLI's stdin as a tool_result and emits interaction_resolved. Field
// shapes match UserInputRequestPayloadSchema / InteractionResolvedSchema in
// bridge.ts. The list payload carries NO `type`; the event embeds it.

type UserInputOption struct {
	ID          string `json:"id"`
	Label       string `json:"label"`
	Description string `json:"description"`
	Recommended bool   `json:"recommended"`
}

type UserInputQuestion struct {
	QuestionID  string            `json:"question_id"`
	Text        string            `json:"text"`
	Header      string            `json:"header"`
	Type        string            `json:"type"`
	Options     []UserInputOption `json:"options"`
	MultiSelect bool              `json:"multi_select"`
	FreeForm    bool              `json:"free_form"`
	Secret      bool              `json:"secret,omitempty"`
}

// UserInputRequestPayload is the interaction body shared by the
// user_input_request event and the pending_interactions_list entries.
type UserInputRequestPayload struct {
	RequestID       string              `json:"request_id"`
	SessionID       string              `json:"session_id"`
	Source          string              `json:"source"`
	Kind            string              `json:"kind"`
	Header          string              `json:"header"`
	ToolUseID       string              `json:"tool_use_id,omitempty"`
	RequestingAgent string              `json:"requesting_agent,omitempty"`
	Questions       []UserInputQuestion `json:"questions"`
	CreatedAt       int64               `json:"created_at"`
	ExpiresAt       int64               `json:"expires_at,omitempty"`
	IsBlocking      *bool               `json:"is_blocking,omitempty"`
	Status          string              `json:"status"`
}

// UserInputRequestEvent is the payload plus a type discriminant (embedding
// flattens the payload fields into the same JSON object).
type UserInputRequestEvent struct {
	Type string `json:"type"`
	UserInputRequestPayload
}

func NewUserInputRequest(p UserInputRequestPayload) UserInputRequestEvent {
	return UserInputRequestEvent{Type: "user_input_request", UserInputRequestPayload: p}
}

type PendingInteractionsList struct {
	ScopeSessionID string                    `json:"scope_session_id,omitempty"`
	SnapshotAll    bool                      `json:"snapshot_all,omitempty"`
	Type           string                    `json:"type"`
	Interactions   []UserInputRequestPayload `json:"interactions"`
}

func NewPendingInteractionsList(items []UserInputRequestPayload) PendingInteractionsList {
	if items == nil {
		items = []UserInputRequestPayload{}
	}
	return PendingInteractionsList{Type: "pending_interactions_list", Interactions: items}
}

type InteractionResolved struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
	SessionID string `json:"session_id,omitempty"`
	Status    string `json:"status,omitempty"`
}

func NewInteractionResolved(requestID, sessionID, status string) InteractionResolved {
	return InteractionResolved{Type: "interaction_resolved", RequestID: requestID, SessionID: sessionID, Status: status}
}

// --- Media scan events (bridge→client) ----------------------------------------
// Emitted after a turn completes when the assistant text contains file paths
// that exist on disk. Mirrors scan_for_media in the Python bridge.

// Media announces an image or video file found in the assistant's output.
// media_type is "image" | "video".
type Media struct {
	Type         string `json:"type"` // "media"
	SessionID    string `json:"session_id"`
	RequestID    string `json:"request_id,omitempty"` // mirrors the done event's request_id so app attaches to the right message
	MediaType    string `json:"media_type"`           // "image" | "video"
	Path         string `json:"path"`
	URL          string `json:"url"`
	AttachmentID string `json:"attachment_id,omitempty"`
	MIMEType     string `json:"mime_type,omitempty"`
	DisplayName  string `json:"display_name,omitempty"`
	CreatedAt    int64  `json:"created_at,omitempty"`
	Sequence     uint64 `json:"attachment_sequence,omitempty"`
	Status       string `json:"status,omitempty"` // available | missing
}

// Document announces a document file (pdf/html) found in the assistant's output.
// doc_type is "pdf" | "html".
type Document struct {
	Type         string `json:"type"` // "document"
	SessionID    string `json:"session_id"`
	RequestID    string `json:"request_id,omitempty"` // mirrors the done event's request_id
	Path         string `json:"path"`
	URL          string `json:"url"`
	Title        string `json:"title"`
	DocType      string `json:"doc_type"` // "pdf" | "html"
	AttachmentID string `json:"attachment_id,omitempty"`
	MIMEType     string `json:"mime_type,omitempty"`
	CreatedAt    int64  `json:"created_at,omitempty"`
	Sequence     uint64 `json:"attachment_sequence,omitempty"`
	Status       string `json:"status,omitempty"` // available | missing
}

// --- permission approval (bridge↔client), mirrors permission_manager.py ------

type PermissionRequest struct {
	Type           string `json:"type"`
	RequestID      string `json:"request_id"`
	SessionID      string `json:"session_id,omitempty"`
	Action         string `json:"action"`
	Title          string `json:"title"`
	Justification  string `json:"justification"`
	CommandPreview string `json:"command_preview,omitempty"`
	RiskLevel      string `json:"risk_level,omitempty"`
	ExpiresAt      int64  `json:"expires_at,omitempty"`
}

func NewPermissionRequest(requestID, sessionID, action, title, justification, preview, risk string, expiresAt int64) PermissionRequest {
	return PermissionRequest{
		Type: "permission_request", RequestID: requestID, SessionID: sessionID,
		Action: action, Title: title, Justification: justification,
		CommandPreview: preview, RiskLevel: risk, ExpiresAt: expiresAt,
	}
}

type PermissionResult struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
	SessionID string `json:"session_id,omitempty"`
	Action    string `json:"action,omitempty"`
	Decision  string `json:"decision"`
	Message   string `json:"message,omitempty"`
}

func NewPermissionResult(requestID, sessionID, action, decision, message string) PermissionResult {
	return PermissionResult{
		Type: "permission_result", RequestID: requestID, SessionID: sessionID,
		Action: action, Decision: decision, Message: message,
	}
}

// MessageQueueSnapshot is authoritative for one Session. Revision survives
// reconnect/restart; attachment bodies stay on the server.
type MessageQueueItem struct {
	FullContent      string   `json:"full_content,omitempty"`
	ContentTruncated bool     `json:"content_truncated,omitempty"`
	RequestID        string   `json:"request_id"`
	State            string   `json:"state"`
	Content          string   `json:"content"`
	Sequence         int64    `json:"sequence"`
	ImageCount       int      `json:"image_count"`
	FileNames        []string `json:"file_names"`
	CreatedAt        int64    `json:"created_at"`
	UpdatedAt        int64    `json:"updated_at"`
	Message          string   `json:"message,omitempty"`
	ActiveRequestID  string   `json:"active_request_id,omitempty"`
	TurnID           string   `json:"turn_id,omitempty"`
}
type MessageQueueSnapshot struct {
	DetailRequestID string             `json:"detail_request_id,omitempty"`
	Detail          *MessageQueueItem  `json:"detail,omitempty"`
	Type            string             `json:"type"`
	SessionID       string             `json:"session_id"`
	Revision        uint64             `json:"revision"`
	Items           []MessageQueueItem `json:"items"`
	Maintenance     any                `json:"maintenance,omitempty"`
	Diagnostics     any                `json:"diagnostics,omitempty"`
}
type QueueActionResult struct {
	Type            string `json:"type"`
	SessionID       string `json:"session_id"`
	RequestID       string `json:"request_id"`
	Action          string `json:"action"`
	Status          string `json:"status"`
	Message         string `json:"message,omitempty"`
	ActiveRequestID string `json:"active_request_id,omitempty"`
	TurnID          string `json:"turn_id,omitempty"`
}
