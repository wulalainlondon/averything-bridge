// Package workspace owns the shared conversation container. Native execution,
// queue admission and final seals remain owned by Bridge's existing gateway.
package workspace

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"everything-go/internal/coordination"

	"github.com/rivo/uniseg"
	"golang.org/x/text/unicode/norm"
)

const Capability = "collaboration_workspace_v1"
const MaxTextBytes = 65536
const MaxAttachmentBytes = 8 * 1024 * 1024
const MaxContextReferences = 128
const MaxPromptBytes = 192 * 1024
const MaxResultBytes = 16 * 1024 * 1024

// Actor is supplied by an authenticated transport, never decoded from a command.
// Owner credentials are verified by core again immediately before native Send.
type Actor struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	Owner          bool   `json:"-"`
	DeviceID       string `json:"-"`
	CredentialHash string `json:"-"`
}

type Workspace struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Owner    string `json:"owner"`
	Revision uint64 `json:"revision"`
}
type Room struct {
	ID                 string `json:"id"`
	WorkspaceID        string `json:"workspace_id"`
	Name               string `json:"name"`
	Description        string `json:"description"`
	Owner              string `json:"owner"`
	Revision           uint64 `json:"revision"`
	ACLRevision        uint64 `json:"acl_revision"`
	Archived           bool   `json:"archived"`
	ExpiresAt          int64  `json:"expires_at,omitempty"`
	CreatedAt          int64  `json:"created_at"`
	ProjectID          string `json:"project_id,omitempty"`
	Pinned             bool   `json:"pinned,omitempty"`
	PreferenceRevision uint64 `json:"preference_revision,omitempty"`
}
type Member struct {
	RoomID      string `json:"room_id"`
	PrincipalID string `json:"principal_id"`
	Name        string `json:"name"`
	Role        string `json:"role"`
	Revision    uint64 `json:"revision"`
	Active      bool   `json:"active"`
}
type Invite struct {
	ID          string `json:"id"`
	RoomID      string `json:"room_id"`
	Role        string `json:"role"`
	ExpiresAt   int64  `json:"expires_at"`
	UsedBy      string `json:"used_by,omitempty"`
	Revoked     bool   `json:"revoked"`
	PrincipalID string `json:"principal_id,omitempty"`
}
type Reference struct {
	Kind     string `json:"kind"`
	ID       string `json:"id"`
	Revision uint64 `json:"revision"`
	Hash     string `json:"hash"`
}
type Message struct {
	DispatchAgentID string      `json:"dispatch_agent_id,omitempty"`
	Body            string      `json:"body"`
	ThreadID        string      `json:"thread_id"`
	ReplyID         string      `json:"reply_id,omitempty"`
	Quote           *Reference  `json:"quote,omitempty"`
	Attachments     []Reference `json:"attachments"`
	Mentions        []string    `json:"mentions"`
	Deleted         bool        `json:"deleted"`
	TaskID          string      `json:"task_id,omitempty"`
}
type MessageVersion struct {
	MessageID    string  `json:"message_id"`
	Revision     uint64  `json:"revision"`
	Author       string  `json:"author"`
	OriginalHash string  `json:"original_hash"`
	Message      Message `json:"message"`
}
type Document struct {
	Title           string `json:"title"`
	Body            string `json:"body"`
	VersionID       string `json:"version_id"`
	AcceptedVersion string `json:"accepted_version,omitempty"`
}
type DocumentVersion struct {
	DocumentID string      `json:"document_id"`
	Title      string      `json:"title"`
	Body       string      `json:"body"`
	Base       uint64      `json:"base_revision"`
	Submission string      `json:"submission_id,omitempty"`
	References []Reference `json:"references"`
}
type DocumentProposal struct {
	DocumentID string `json:"document_id"`
	Base       uint64 `json:"base_revision"`
	Title      string `json:"title"`
	Body       string `json:"body"`
	State      string `json:"state"`
	TaskID     string `json:"task_id,omitempty"`
}
type Agent struct {
	ContextACLRevision uint64 `json:"context_acl_revision"`
	Ready              bool   `json:"ready"`
	SetupReason        string `json:"setup_reason,omitempty"`
	Name               string `json:"name"`
	Description        string `json:"description"`
	SessionID          string `json:"session_id"`
	ThreadID           string `json:"native_thread_id"`
	Config             uint64 `json:"config_revision"`
	RoleRevision       uint64 `json:"role_revision"`
	SourceRoleRevision uint64 `json:"source_role_revision,omitempty"`
	RoleHash           string `json:"role_hash"`
	ProfileID          string `json:"profile_id,omitempty"`
	Backend            string `json:"backend"`
	Model              string `json:"model"`
	Sandbox            string `json:"sandbox"`
	RoomExclusive      bool   `json:"room_exclusive"`
	Disabled           bool   `json:"disabled"`
}
type Manifest struct {
	ACLRevision uint64      `json:"acl_revision"`
	AgentID     string      `json:"agent_id"`
	AgentRev    uint64      `json:"agent_revision"`
	RoleHash    string      `json:"role_hash"`
	References  []Reference `json:"references"`
	Hash        string      `json:"hash"`
}
type Axes struct {
	Execution string `json:"execution"`
	Final     string `json:"final"`
	QA        string `json:"qa"`
	Delivery  string `json:"delivery"`
	Human     string `json:"human_acceptance"`
}
type Task struct {
	FinalBytes         int                          `json:"final_bytes,omitempty"`
	FinalTruncated     bool                         `json:"final_truncated"`
	ResultAttachmentID string                       `json:"result_attachment_id,omitempty"`
	Title              string                       `json:"title"`
	Instruction        string                       `json:"instruction"`
	ThreadID           string                       `json:"thread_id"`
	AgentID            string                       `json:"agent_id"`
	Manifest           Manifest                     `json:"manifest"`
	RequestID          string                       `json:"request_id"`
	SessionID          string                       `json:"session_id"`
	NativeThreadID     string                       `json:"native_thread_id"`
	ConfigRevision     uint64                       `json:"config_revision"`
	Role               Agent                        `json:"role"`
	CanonicalID        string                       `json:"canonical_task_id,omitempty"`
	WorkItemID         string                       `json:"work_item_id,omitempty"`
	RunID              string                       `json:"run_id,omitempty"`
	WorkflowID         string                       `json:"workflow_id,omitempty"`
	Step               string                       `json:"step,omitempty"`
	Draft              int                          `json:"draft_version"`
	OwnerTaskID        string                       `json:"owner_task_id,omitempty"`
	OwnerRoundID       string                       `json:"owner_round_id,omitempty"`
	Axes               Axes                         `json:"axes"`
	Admission          string                       `json:"admission"`
	Reason             string                       `json:"reason,omitempty"`
	FinalText          string                       `json:"final_text,omitempty"`
	FinalHash          string                       `json:"final_hash,omitempty"`
	SealID             string                       `json:"seal_id,omitempty"`
	NativeTurnID       string                       `json:"native_turn_id,omitempty"`
	CreatedBy          string                       `json:"created_by"`
	OwnerDevice        string                       `json:"-"`
	OwnerTokenHash     string                       `json:"-"`
	Deadline           int64                        `json:"deadline"`
	Contract           coordination.TaskContract    `json:"contract"`
	Context            coordination.ContextManifest `json:"context"`
}

// Grant is stored separately from public task JSON. It is a record of the
// original human authorization, not an API for a model to acquire permission.
type Grant struct {
	PrincipalID    string `json:"principal_id"`
	DeviceID       string `json:"device_id"`
	CredentialHash string `json:"credential_hash"`
	Owner          bool   `json:"owner"`
	ACLRevision    uint64 `json:"acl_revision"`
}
type Workflow struct {
	Title       string      `json:"title"`
	Instruction string      `json:"instruction"`
	ThreadID    string      `json:"thread_id"`
	Researcher  string      `json:"researcher"`
	Writer      string      `json:"writer"`
	Reviewer    string      `json:"reviewer"`
	References  []Reference `json:"references"`
	State       string      `json:"state"`
	Draft       int         `json:"draft_version"`
	ActiveTask  string      `json:"active_task,omitempty"`
	LatestDraft string      `json:"latest_draft,omitempty"`
	MaxDrafts   int         `json:"max_drafts"`
	Calls       int         `json:"calls"`
	Deadline    int64       `json:"deadline"`
	DocumentID  string      `json:"document_id,omitempty"`
	Reason      string      `json:"reason,omitempty"`
}
type Review struct {
	OwnerRoundID string `json:"owner_round_id"`
	QARoundID    string `json:"qa_round_id"`
	OwnerHash    string `json:"owner_hash"`
	QAHash       string `json:"qa_hash"`
	Verdict      string `json:"verdict"`
	Findings     string `json:"findings"`
}
type Knowledge struct {
	Text       string      `json:"text"`
	State      string      `json:"state"`
	References []Reference `json:"references"`
	AcceptedBy string      `json:"accepted_by,omitempty"`
	Supersedes string      `json:"supersedes,omitempty"`
}
type MediaComment struct {
	AttachmentID string `json:"attachment_id"`
	FileHash     string `json:"file_hash"`
	Milliseconds int64  `json:"milliseconds"`
	Text         string `json:"text"`
}
type Attachment struct {
	Name      string `json:"name"`
	MIME      string `json:"mime"`
	Hash      string `json:"hash"`
	Size      int64  `json:"size"`
	CreatedBy string `json:"created_by"`
}
type Schedule struct {
	PendingDSTSkips    []string    `json:"pending_dst_skips,omitempty"`
	DefinitionRevision uint64      `json:"definition_revision"`
	Title              string      `json:"title"`
	AgentID            string      `json:"agent_id"`
	Instruction        string      `json:"instruction"`
	References         []Reference `json:"references"`
	Timezone           string      `json:"timezone"`
	Hour               int         `json:"hour"`
	Minute             int         `json:"minute"`
	Weekdays           []int       `json:"weekdays"`
	NextUTC            int64       `json:"next_utc"`
	Enabled            bool        `json:"enabled"`
	LateMinutes        int         `json:"late_minutes"`
}
type Occurrence struct {
	ScheduleID       string `json:"schedule_id"`
	ScheduleRevision uint64 `json:"schedule_revision"`
	WallTime         string `json:"wall_time"`
	DueAt            int64  `json:"due_at"`
	State            string `json:"state"`
	TaskID           string `json:"task_id,omitempty"`
	Reason           string `json:"reason,omitempty"`
}
type Effect struct {
	AdapterRevision string          `json:"adapter_revision,omitempty"`
	Adapter         string          `json:"adapter"`
	Action          string          `json:"action"`
	Account         string          `json:"account"`
	Destination     string          `json:"destination"`
	Title           string          `json:"title"`
	Content         string          `json:"content"`
	Parameters      json.RawMessage `json:"parameters,omitempty"`
	Attachments     []Reference     `json:"attachments"`
	ContentHash     string          `json:"content_hash"`
	State           string          `json:"state"`
	ApprovedBy      string          `json:"approved_by,omitempty"`
	ApprovedRev     uint64          `json:"approved_revision,omitempty"`
	ExpiresAt       int64           `json:"expires_at"`
	ProviderID      string          `json:"provider_id,omitempty"`
	ProviderState   string          `json:"provider_state,omitempty"`
	Reason          string          `json:"reason,omitempty"`
}
type Notification struct {
	PrincipalID string `json:"principal_id"`
	Kind        string `json:"kind"`
	EntityID    string `json:"entity_id"`
	Read        bool   `json:"read"`
}

// Entity metadata and its immutable versions are independently addressable.
// Body contains only that room's data, never provider secrets or file locators.
type Entity struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`
	RoomID    string          `json:"room_id"`
	Revision  uint64          `json:"revision"`
	Sequence  int64           `json:"sequence"`
	CreatedBy string          `json:"created_by"`
	CreatedAt int64           `json:"created_at"`
	UpdatedAt int64           `json:"updated_at"`
	Hash      string          `json:"hash"`
	Body      json.RawMessage `json:"body"`
}
type Event struct {
	Sequence    int64  `json:"sequence"`
	RoomID      string `json:"room_id"`
	Kind        string `json:"kind"`
	EntityID    string `json:"entity_id"`
	Revision    uint64 `json:"revision"`
	ACLRevision uint64 `json:"acl_revision"`
	Actor       string `json:"actor"`
	At          int64  `json:"at"`
}
type Page struct {
	Items          []Entity `json:"items"`
	Cursor         string   `json:"next_cursor,omitempty"`
	HasMore        bool     `json:"has_more"`
	ReadMessageIDs []string `json:"read_message_ids,omitempty"`
}
type Snapshot struct {
	Room           Room            `json:"room"`
	Role           string          `json:"role"`
	Members        []Member        `json:"members"`
	Data           map[string]Page `json:"data"`
	Sequence       int64           `json:"sequence"`
	UnreadMessages int64           `json:"unread_messages"`
}

type Command struct {
	// Supplied only by the core adapter after actual session/config/grant
	// validation. A wire client cannot mark its own binding as verified.
	VerifiedProjectID       string          `json:"-"`
	VerifiedAgent           *Agent          `json:"-"`
	VerifiedAdapterRevision string          `json:"-"`
	Action                  string          `json:"action"`
	Authority               string          `json:"authority"`
	WorkspaceID             string          `json:"workspace_id,omitempty"`
	RoomID                  string          `json:"room_id,omitempty"`
	EntityID                string          `json:"entity_id,omitempty"`
	MutationID              string          `json:"mutation_id,omitempty"`
	OriginalMutation        string          `json:"original_mutation,omitempty"`
	ExpectedRevision        *uint64         `json:"expected_revision,omitempty"`
	ExpectedACL             *uint64         `json:"expected_acl_revision,omitempty"`
	Kind                    string          `json:"kind,omitempty"`
	ThreadID                string          `json:"thread_id,omitempty"`
	Cursor                  string          `json:"cursor,omitempty"`
	Query                   string          `json:"query,omitempty"`
	Limit                   int             `json:"limit,omitempty"`
	Offset                  int64           `json:"offset,omitempty"`
	Data                    json.RawMessage `json:"data,omitempty"`
}
type Result struct {
	ToolAccounts []ToolAccount `json:"tool_accounts,omitempty"`
	ToolData     any           `json:"tool_data,omitempty"`
	Action       string        `json:"action"`
	MutationID   string        `json:"mutation_id,omitempty"`
	Entity       *Entity       `json:"entity,omitempty"`
	Room         *Room         `json:"room,omitempty"`
	Workspace    *Workspace    `json:"workspace,omitempty"`
	Workspaces   []Workspace   `json:"workspaces,omitempty"`
	Rooms        []Room        `json:"rooms,omitempty"`
	Snapshot     *Snapshot     `json:"snapshot,omitempty"`
	Page         *Page         `json:"page,omitempty"`
	Events       []Event       `json:"events,omitempty"`
	Invite       *Invite       `json:"invite,omitempty"`
	InviteCode   string        `json:"invite_code,omitempty"`
	PrincipalID  string        `json:"principal_id,omitempty"`
	Role         string        `json:"role,omitempty"`
	Bytes        []byte        `json:"bytes,omitempty"`
	NextOffset   int64         `json:"next_offset,omitempty"`
	HasMore      bool          `json:"has_more"`
}

func decode(data json.RawMessage, target any) error {
	if len(data) == 0 || len(data) > MaxAttachmentBytes*2 {
		return errors.New("invalid_payload")
	}
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return errors.New("invalid_payload")
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		return errors.New("invalid_payload")
	}
	return nil
}
func digest(b []byte) string { v := sha256.Sum256(b); return hex.EncodeToString(v[:]) }
func text(v string, min, max int) (string, error) {
	v = norm.NFC.String(strings.TrimSpace(v))
	if !utf8.ValidString(v) || len(v) > MaxTextBytes || uniseg.GraphemeClusterCount(v) < min || uniseg.GraphemeClusterCount(v) > max {
		return "", errors.New("text_limit")
	}
	return v, nil
}
func validRole(role string) bool {
	return role == "owner" || role == "admin" || role == "editor" || role == "reviewer" || role == "viewer"
}
func permits(role, right string) bool {
	if role == "owner" {
		return true
	}
	switch right {
	case "read":
		return validRole(role)
	case "manage":
		return role == "admin"
	case "edit", "dispatch", "schedule":
		return role == "admin" || role == "editor"
	case "comment", "review":
		return role == "admin" || role == "editor" || role == "reviewer"
	case "effect":
		return false // account effects additionally require the original paired owner
	}
	return false
}
