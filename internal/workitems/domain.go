// Package workitems owns Bridge's human work model. A WorkItem describes an
// outcome; sessions and Codex threads are execution details linked to it.
package workitems

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type Lifecycle string

const (
	LifecycleInbox     Lifecycle = "inbox"
	LifecycleReady     Lifecycle = "ready"
	LifecycleActive    Lifecycle = "active"
	LifecycleReview    Lifecycle = "review"
	LifecycleDone      Lifecycle = "done"
	LifecycleCancelled Lifecycle = "cancelled"
)

type ActorType string

const (
	ActorUser    ActorType = "user"
	ActorAgent   ActorType = "agent"
	ActorDesktop ActorType = "desktop"
	ActorMobile  ActorType = "mobile"
	ActorSystem  ActorType = "system"
)

type Priority string

const (
	PriorityNone   Priority = "none"
	PriorityLow    Priority = "low"
	PriorityMedium Priority = "medium"
	PriorityHigh   Priority = "high"
	PriorityUrgent Priority = "urgent"
)

var (
	ErrNotFound               = errors.New("work item not found")
	ErrConflict               = errors.New("work item version conflict")
	ErrInvalidTransition      = errors.New("invalid work item lifecycle transition")
	ErrHumanRequired          = errors.New("human acceptance is required")
	ErrReviewDecisionRequired = errors.New("a structured human review decision is required")
	ErrDependencyCycle        = errors.New("work item dependency cycle")
	ErrCrossProject           = errors.New("work item relation crosses projects")
	ErrSessionLinked          = errors.New("session is already linked to an active work item")
)

type Actor struct {
	Type     ActorType `json:"type"`
	DeviceID string    `json:"device_id,omitempty"`
}

type Project struct {
	ID                  string `json:"id"`
	AuthorityInstanceID string `json:"authority_instance_id"`
	Name                string `json:"name"`
	WorkspacePath       string `json:"workspace_path,omitempty"`
	Context             string `json:"context,omitempty"`
	Version             uint64 `json:"version"`
	CreatedAt           int64  `json:"created_at"`
	UpdatedAt           int64  `json:"updated_at"`
	ArchivedAt          *int64 `json:"archived_at,omitempty"`
}

type WorkItem struct {
	ID                 string    `json:"id"`
	ProjectID          string    `json:"project_id"`
	Title              string    `json:"title"`
	Description        string    `json:"description,omitempty"`
	Outcome            string    `json:"outcome,omitempty"`
	NextStep           string    `json:"next_step,omitempty"`
	AcceptanceCriteria string    `json:"acceptance_criteria,omitempty"`
	Lifecycle          Lifecycle `json:"lifecycle"`
	Priority           Priority  `json:"priority"`
	SortKey            int64     `json:"sort_key"`
	Version            uint64    `json:"version"`
	ActivityRevision   uint64    `json:"activity_revision"`
	BlockedReasonCode  string    `json:"blocked_reason_code,omitempty"`
	BlockedNote        string    `json:"blocked_note,omitempty"`
	Assignee           string    `json:"assignee,omitempty"`
	DueAt              *int64    `json:"due_at,omitempty"`
	Labels             []string  `json:"labels"`
	AutomationMode     string    `json:"automation_mode"`
	WorkflowID         string    `json:"workflow_id,omitempty"`
	WorkflowNodeID     string    `json:"workflow_node_id,omitempty"`
	CreatedAt          int64     `json:"created_at"`
	UpdatedAt          int64     `json:"updated_at"`
	AcceptedAt         *int64    `json:"accepted_at,omitempty"`
	CancelledAt        *int64    `json:"cancelled_at,omitempty"`
	ArchivedAt         *int64    `json:"archived_at,omitempty"`
}

type SessionLink struct {
	ID               string `json:"id"`
	WorkItemID       string `json:"work_item_id"`
	SessionID        string `json:"session_id"`
	ThreadIDSnapshot string `json:"thread_id_snapshot,omitempty"`
	Role             string `json:"role"`
	LinkedAt         int64  `json:"linked_at"`
	UnlinkedAt       *int64 `json:"unlinked_at,omitempty"`
}

type Dependency struct {
	WorkItemID string `json:"work_item_id"`
	DependsOn  string `json:"depends_on_id"`
	CreatedAt  int64  `json:"created_at"`
}

type Comment struct {
	ID             string    `json:"id"`
	WorkItemID     string    `json:"work_item_id"`
	AuthorType     ActorType `json:"author_type"`
	AuthorDeviceID string    `json:"author_device_id,omitempty"`
	Body           string    `json:"body"`
	CreatedAt      int64     `json:"created_at"`
	EditedAt       *int64    `json:"edited_at,omitempty"`
	DeletedAt      *int64    `json:"deleted_at,omitempty"`
}

type Run struct {
	ID             string `json:"id"`
	WorkItemID     string `json:"work_item_id"`
	SessionLinkID  string `json:"session_link_id,omitempty"`
	RequestID      string `json:"request_id"`
	Kind           string `json:"kind"`
	Status         string `json:"status"`
	StartedAt      int64  `json:"started_at"`
	FinishedAt     *int64 `json:"finished_at,omitempty"`
	TerminalReason string `json:"terminal_reason,omitempty"`
	SessionID      string `json:"session_id,omitempty"`
	Instruction    string `json:"instruction,omitempty"`
	AvailableAt    int64  `json:"available_at"`
	Attempt        int    `json:"attempt"`
	MaxAttempts    int    `json:"max_attempts"`
	ClaimedAt      *int64 `json:"claimed_at,omitempty"`
	QueueReason    string `json:"queue_reason,omitempty"`
}

type WorkflowNode struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Description string `json:"description,omitempty"`
}

type WorkflowEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type WorkflowDefinition struct {
	Nodes []WorkflowNode `json:"nodes"`
	Edges []WorkflowEdge `json:"edges"`
}

type Workflow struct {
	ID          string             `json:"id"`
	ProjectID   string             `json:"project_id"`
	Name        string             `json:"name"`
	Description string             `json:"description,omitempty"`
	Version     uint64             `json:"version"`
	Definition  WorkflowDefinition `json:"definition"`
	CreatedAt   int64              `json:"created_at"`
	UpdatedAt   int64              `json:"updated_at"`
	ArchivedAt  *int64             `json:"archived_at,omitempty"`
}

// BootstrapSource is a bounded, read-only evidence reference captured from a
// server-authorized workspace. Excerpts are intentionally small and redacted.
type BootstrapSource struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	Label       string `json:"label"`
	Path        string `json:"path,omitempty"`
	Excerpt     string `json:"excerpt,omitempty"`
	Fingerprint string `json:"fingerprint"`
	ModifiedAt  int64  `json:"modified_at,omitempty"`
}

type BootstrapSuggestion struct {
	ID                 string   `json:"id"`
	WorkItemID         string   `json:"work_item_id"`
	SessionID          string   `json:"session_id,omitempty"`
	Title              string   `json:"title"`
	Description        string   `json:"description,omitempty"`
	Outcome            string   `json:"outcome,omitempty"`
	NextStep           string   `json:"next_step,omitempty"`
	AcceptanceCriteria string   `json:"acceptance_criteria,omitempty"`
	EvidenceRefs       []string `json:"evidence_refs"`
}

// ProjectBootstrapDraft is server-authoritative review data. It never becomes
// Project context or WorkItems until an explicit human approval mutation.
type ProjectBootstrapDraft struct {
	ID                  string                `json:"id"`
	AuthorityInstanceID string                `json:"authority_instance_id"`
	ProjectID           string                `json:"project_id,omitempty"`
	ProjectVersion      uint64                `json:"project_version,omitempty"`
	ProjectName         string                `json:"project_name"`
	WorkspacePath       string                `json:"workspace_path"`
	Status              string                `json:"status"`
	Fingerprint         string                `json:"fingerprint"`
	Objective           string                `json:"objective"`
	CurrentState        string                `json:"current_state"`
	NextStep            string                `json:"next_step"`
	AcceptanceCriteria  string                `json:"acceptance_criteria"`
	Constraints         []string              `json:"constraints"`
	Decisions           []string              `json:"decisions"`
	OpenQuestions       []string              `json:"open_questions"`
	Suggestions         []BootstrapSuggestion `json:"suggestions"`
	Sources             []BootstrapSource     `json:"sources"`
	SessionIDs          []string              `json:"session_ids"`
	Version             uint64                `json:"version"`
	CreatedAt           int64                 `json:"created_at"`
	UpdatedAt           int64                 `json:"updated_at"`
	AppliedAt           *int64                `json:"applied_at,omitempty"`
}

// AttachmentRef stores only the durable relationship to the canonical
// attachment journal. URL, MIME and availability are delivery projections
// populated by core and are never written to the Work database.
type AttachmentRef struct {
	ID           string `json:"id"`
	WorkItemID   string `json:"work_item_id"`
	AttachmentID string `json:"attachment_id"`
	DisplayName  string `json:"display_name,omitempty"`
	SortKey      int64  `json:"sort_key"`
	CreatedAt    int64  `json:"created_at"`
	RemovedAt    *int64 `json:"removed_at,omitempty"`
	Kind         string `json:"kind,omitempty"`
	MIMEType     string `json:"mime_type,omitempty"`
	URL          string `json:"url,omitempty"`
	Status       string `json:"status,omitempty"`
}

type Activity struct {
	Revision   uint64    `json:"revision"`
	WorkItemID string    `json:"work_item_id"`
	Kind       string    `json:"kind"`
	Actor      ActorType `json:"actor"`
	Payload    string    `json:"payload"`
	CreatedAt  int64     `json:"created_at"`
}

type ReviewDecisionRecord struct {
	Decision  string `json:"decision"`
	Feedback  string `json:"feedback,omitempty"`
	RunID     string `json:"run_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	CreatedAt int64  `json:"created_at"`
}

type Change struct {
	Revision  uint64          `json:"revision"`
	Entity    string          `json:"entity"`
	EntityID  string          `json:"entity_id"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt int64           `json:"created_at"`
}

// ChangePayload is the native delta envelope. A single transaction can update
// an item and one related entity without forcing clients to race a second sync.
type ChangePayload struct {
	Item           *WorkItem              `json:"item,omitempty"`
	Link           *SessionLink           `json:"link,omitempty"`
	Dependency     *Dependency            `json:"dependency,omitempty"`
	Comment        *Comment               `json:"comment,omitempty"`
	Run            *Run                   `json:"run,omitempty"`
	Attachment     *AttachmentRef         `json:"attachment,omitempty"`
	Activity       *Activity              `json:"activity,omitempty"`
	Workflow       *Workflow              `json:"workflow,omitempty"`
	Bootstrap      *ProjectBootstrapDraft `json:"bootstrap,omitempty"`
	ReviewDecision *ReviewDecisionRecord  `json:"review_decision,omitempty"`
}

type Snapshot struct {
	Revision     uint64                  `json:"revision"`
	Projects     []Project               `json:"projects"`
	Items        []WorkItem              `json:"items"`
	SessionLinks []SessionLink           `json:"session_links"`
	Dependencies []Dependency            `json:"dependencies"`
	Comments     []Comment               `json:"comments"`
	Runs         []Run                   `json:"runs"`
	Attachments  []AttachmentRef         `json:"attachments"`
	Activities   []Activity              `json:"activities"`
	Workflows    []Workflow              `json:"workflows"`
	Bootstraps   []ProjectBootstrapDraft `json:"bootstraps"`
}

type ItemView struct {
	WorkItem
	Unread int `json:"unread"`
}

type DeviceSnapshot struct {
	Revision     uint64                  `json:"revision"`
	Projects     []Project               `json:"projects"`
	Items        []ItemView              `json:"items"`
	SessionLinks []SessionLink           `json:"session_links"`
	Dependencies []Dependency            `json:"dependencies"`
	Comments     []Comment               `json:"comments"`
	Runs         []Run                   `json:"runs"`
	Attachments  []AttachmentRef         `json:"attachments"`
	Activities   []Activity              `json:"activities"`
	Workflows    []Workflow              `json:"workflows"`
	Bootstraps   []ProjectBootstrapDraft `json:"bootstraps"`
}

// ContextPack is the durable WorkItem projection supplied to an agent before a
// run. It deliberately contains stable identities and canonical attachment
// references; transport-specific media URLs are materialized by core.
type ContextPack struct {
	Version      int             `json:"version"`
	GeneratedAt  int64           `json:"generated_at"`
	Project      Project         `json:"project"`
	Item         WorkItem        `json:"item"`
	Dependencies []WorkItem      `json:"dependencies"`
	Comments     []Comment       `json:"comments"`
	Runs         []Run           `json:"runs"`
	Attachments  []AttachmentRef `json:"attachments"`
	SessionLinks []SessionLink   `json:"session_links"`
	Prompt       string          `json:"prompt"`
	Truncated    bool            `json:"truncated"`
}

type ConflictError struct {
	Expected uint64
	Current  WorkItem
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%v: expected %d, current %d", ErrConflict, e.Expected, e.Current.Version)
}

func (e *ConflictError) Unwrap() error { return ErrConflict }

func validLifecycle(v Lifecycle) bool {
	switch v {
	case LifecycleInbox, LifecycleReady, LifecycleActive, LifecycleReview, LifecycleDone, LifecycleCancelled:
		return true
	default:
		return false
	}
}

func validPriority(v Priority) bool {
	switch v {
	case PriorityNone, PriorityLow, PriorityMedium, PriorityHigh, PriorityUrgent:
		return true
	default:
		return false
	}
}

func validateTransition(from, to Lifecycle, actor Actor) error {
	if !validLifecycle(to) {
		return fmt.Errorf("%w: unknown target %q", ErrInvalidTransition, to)
	}
	if from == to {
		return nil
	}
	allowed := map[Lifecycle]map[Lifecycle]bool{
		LifecycleInbox:     {LifecycleReady: true, LifecycleCancelled: true},
		LifecycleReady:     {LifecycleInbox: true, LifecycleActive: true, LifecycleCancelled: true},
		LifecycleActive:    {LifecycleReady: true, LifecycleReview: true, LifecycleCancelled: true},
		LifecycleReview:    {LifecycleActive: true, LifecycleDone: true, LifecycleCancelled: true},
		LifecycleDone:      {LifecycleActive: true},
		LifecycleCancelled: {LifecycleInbox: true},
	}
	if !allowed[from][to] {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	if to == LifecycleDone && actor.Type != ActorUser && actor.Type != ActorDesktop && actor.Type != ActorMobile {
		return ErrHumanRequired
	}
	return nil
}

func normalizeTitle(title string) (string, error) {
	title = strings.TrimSpace(title)
	if title == "" {
		return "", errors.New("work item title is required")
	}
	if len([]rune(title)) > 240 {
		return "", errors.New("work item title exceeds 240 characters")
	}
	return title, nil
}
