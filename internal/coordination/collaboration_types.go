package coordination

import "encoding/json"

const CollaborationCapability = "human_ai_collaboration_v2"

type Criterion struct {
	ID        string `json:"id"`
	Text      string `json:"text"`
	Mandatory bool   `json:"mandatory"`
}

// SourceRef is a reference, never an instruction or a caller-selected identity.
type SourceRef struct {
	Kind    string `json:"kind" enum:"artifact,evidence,report,submission,decision,knowledge,contract"`
	ID      string `json:"id"`
	Version string `json:"version,omitempty"`
	Label   string `json:"label,omitempty"`
}

type TaskContract struct {
	ID        string      `json:"id"`
	ProjectID string      `json:"project_id"`
	TaskID    string      `json:"task_id"`
	Revision  uint64      `json:"revision"`
	Outcome   string      `json:"outcome"`
	NonGoals  []string    `json:"non_goals"`
	Criteria  []Criterion `json:"criteria"`
	Inputs    []SourceRef `json:"inputs"`
	Sources   []SourceRef `json:"sources"`
	Sandbox   string      `json:"sandbox"`
	Backend   string      `json:"backend"`
	ProfileID string      `json:"profile_id"`
	Hash      string      `json:"hash"`
	CreatedAt int64       `json:"created_at"`
}

type Grant struct {
	ID            string `json:"id"`
	ProjectID     string `json:"project_id"`
	TaskID        string `json:"task_id"`
	ContractID    string `json:"contract_id"`
	ContractHash  string `json:"contract_hash"`
	ApprovedBy    string `json:"approved_by"`
	Mode          string `json:"mode"`
	MaxRevisions  int    `json:"max_revisions"`
	UsedRevisions int    `json:"used_revisions"`
	ExpiresAt     int64  `json:"expires_at"`
	RevokedAt     int64  `json:"revoked_at,omitempty"`
	CreatedAt     int64  `json:"created_at"`
}

type TaskCoordination struct {
	ID                   string `json:"id"`
	ProjectID            string `json:"project_id"`
	Revision             uint64 `json:"revision"`
	ContractID           string `json:"contract_id"`
	GrantID              string `json:"grant_id,omitempty"`
	ManifestID           string `json:"manifest_id,omitempty"`
	Readiness            string `json:"readiness"`
	Phase                string `json:"phase"`
	ActiveRunID          string `json:"active_run_id,omitempty"`
	ActiveEpoch          uint64 `json:"active_epoch,omitempty"`
	ActiveRequestID      string `json:"active_request_id,omitempty"`
	SubmissionID         string `json:"submission_id,omitempty"`
	AcceptedSubmissionID string `json:"accepted_submission_id,omitempty"`
	AcceptedContractID   string `json:"accepted_contract_id,omitempty"`
	HandoffID            string `json:"handoff_id,omitempty"`
	AssistanceID         string `json:"assistance_id,omitempty"`
	NeedsContext         bool   `json:"needs_context"`
	NoProgressRounds     int    `json:"no_progress_rounds"`
}

type ContextManifest struct {
	ID          string      `json:"id"`
	ProjectID   string      `json:"project_id"`
	TaskID      string      `json:"task_id"`
	ContractID  string      `json:"contract_id"`
	DecisionIDs []string    `json:"decision_ids"`
	Inputs      []SourceRef `json:"inputs"`
	Required    string      `json:"required"`
	Hash        string      `json:"hash"`
	CreatedAt   int64       `json:"created_at"`
}

type ReportInput struct {
	Kind          string      `json:"kind" enum:"received,progress,question,answer_ack,blocked,change_requested,checkpoint"`
	ContractID    string      `json:"contract_id"`
	ManifestID    string      `json:"manifest_id"`
	Text          string      `json:"text"`
	Readiness     string      `json:"readiness,omitempty" enum:"confirmed,needs_clarification"`
	Blocking      bool        `json:"blocking,omitempty"`
	DecisionOwner string      `json:"decision_owner,omitempty"`
	ReasonCode    string      `json:"reason_code,omitempty"`
	QuestionID    string      `json:"question_id,omitempty"`
	AnswerID      string      `json:"answer_id,omitempty"`
	Adopted       bool        `json:"adopted,omitempty"`
	Sources       []SourceRef `json:"sources,omitempty"`
}

type WorkerReport struct {
	ID        string      `json:"id"`
	ProjectID string      `json:"project_id"`
	TaskID    string      `json:"task_id"`
	SessionID string      `json:"session_id"`
	RunID     string      `json:"run_id"`
	Epoch     uint64      `json:"epoch"`
	Actor     string      `json:"actor"`
	Input     ReportInput `json:"input"`
	Stale     bool        `json:"stale"`
	CreatedAt int64       `json:"created_at"`
}

type Actionable struct {
	ID            string `json:"id"`
	ProjectID     string `json:"project_id"`
	TaskID        string `json:"task_id"`
	ReportID      string `json:"report_id,omitempty"`
	SubmissionID  string `json:"submission_id,omitempty"`
	Kind          string `json:"kind"`
	Text          string `json:"text"`
	Assignee      string `json:"assignee"`
	State         string `json:"state"`
	WaitingReason string `json:"waiting_reason,omitempty"`
	Blocking      bool   `json:"blocking"`
	Revision      uint64 `json:"revision"`
	AnswerID      string `json:"answer_id,omitempty"`
	Answer        string `json:"answer,omitempty"`
	DueAt         int64  `json:"due_at,omitempty"`
	CreatedAt     int64  `json:"created_at"`
	UpdatedAt     int64  `json:"updated_at"`
}

type DispositionInput struct {
	ActionableID string      `json:"actionable_id"`
	Action       string      `json:"action"`
	Text         string      `json:"text"`
	Sources      []SourceRef `json:"sources,omitempty"`
}

type Disposition struct {
	ID           string      `json:"id"`
	ProjectID    string      `json:"project_id"`
	TaskID       string      `json:"task_id"`
	ActionableID string      `json:"actionable_id"`
	Action       string      `json:"action"`
	Text         string      `json:"text"`
	Actor        string      `json:"actor"`
	Sources      []SourceRef `json:"sources"`
	CreatedAt    int64       `json:"created_at"`
}

type Coverage struct {
	CriterionID string   `json:"criterion_id"`
	Claim       string   `json:"claim" enum:"passed,failed,unknown,not_run"`
	EvidenceIDs []string `json:"evidence_ids"`
	Reason      string   `json:"reason,omitempty"`
}

type Artifact struct {
	ID        string `json:"id"`
	ProjectID string `json:"project_id"`
	TaskID    string `json:"task_id"`
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
	// Locator is server-only. Never include this in client/tool projections.
	Locator    string `json:"locator,omitempty"`
	Content    string `json:"content,omitempty"`
	SourcePath string `json:"source_path,omitempty"`
	SourceRoot string `json:"source_root,omitempty"`
	CreatedAt  int64  `json:"created_at"`
}

type Evidence struct {
	ID         string `json:"id"`
	ProjectID  string `json:"project_id"`
	TaskID     string `json:"task_id"`
	ArtifactID string `json:"artifact_id,omitempty"`
	Text       string `json:"text"`
	Trust      string `json:"trust"`
	Observer   string `json:"observer"`
	CreatedAt  int64  `json:"created_at"`
}

type SubmissionInput struct {
	ContractID  string     `json:"contract_id"`
	ManifestID  string     `json:"manifest_id"`
	Summary     string     `json:"summary"`
	ArtifactIDs []string   `json:"artifact_ids"`
	Coverage    []Coverage `json:"coverage"`
	Limitations []string   `json:"limitations"`
}

type Submission struct {
	ID        string          `json:"id"`
	ProjectID string          `json:"project_id"`
	TaskID    string          `json:"task_id"`
	RunID     string          `json:"run_id"`
	Epoch     uint64          `json:"epoch"`
	Version   uint64          `json:"version"`
	Input     SubmissionInput `json:"input"`
	Status    string          `json:"status"`
	CreatedAt int64           `json:"created_at"`
}

type Decision struct {
	ID          string      `json:"id"`
	ProjectID   string      `json:"project_id"`
	TaskID      string      `json:"task_id,omitempty"`
	Text        string      `json:"text"`
	Supersedes  string      `json:"supersedes,omitempty"`
	Sources     []SourceRef `json:"sources"`
	ConfirmedBy string      `json:"confirmed_by"`
	CreatedAt   int64       `json:"created_at"`
}

type Handoff struct {
	ID           string `json:"id"`
	ProjectID    string `json:"project_id"`
	TaskID       string `json:"task_id"`
	Epoch        uint64 `json:"epoch"`
	FromEvent    uint64 `json:"from_event"`
	ThroughEvent uint64 `json:"through_event"`
	Summary      string `json:"summary"`
	Complete     bool   `json:"complete"`
	Status       string `json:"status"`
	ConfirmedBy  string `json:"confirmed_by,omitempty"`
	CreatedAt    int64  `json:"created_at"`
}

type Assistance struct {
	ID                 string      `json:"id"`
	ProjectID          string      `json:"project_id"`
	RequesterTaskID    string      `json:"requester_task_id"`
	RequesterEpoch     uint64      `json:"requester_epoch"`
	TargetTaskID       string      `json:"target_task_id,omitempty"`
	Mode               string      `json:"mode"`
	Text               string      `json:"text"`
	Inputs             []SourceRef `json:"inputs"`
	Blocking           bool        `json:"blocking"`
	State              string      `json:"state"`
	ResultSubmissionID string      `json:"result_submission_id,omitempty"`
	Revision           uint64      `json:"revision"`
	Deadline           int64       `json:"deadline"`
	CreatedAt          int64       `json:"created_at"`
}

type KnowledgeRule struct {
	ID           string      `json:"id"`
	ProjectID    string      `json:"project_id"`
	TaskID       string      `json:"task_id,omitempty"`
	Text         string      `json:"text"`
	FailureClass string      `json:"failure_class"`
	Sources      []SourceRef `json:"sources"`
	Evaluation   string      `json:"evaluation,omitempty"`
	State        string      `json:"state"`
	Supersedes   string      `json:"supersedes,omitempty"`
	ApprovedBy   string      `json:"approved_by,omitempty"`
	Revision     uint64      `json:"revision"`
	CreatedAt    int64       `json:"created_at"`
}

type CollaborationState struct {
	Tasks        map[string]TaskCoordination `json:"task_states"`
	Contracts    map[string]TaskContract     `json:"contracts"`
	Grants       map[string]Grant            `json:"grants"`
	Manifests    map[string]ContextManifest  `json:"manifests"`
	Reports      map[string]WorkerReport     `json:"reports"`
	Actionables  map[string]Actionable       `json:"actionables"`
	Dispositions map[string]Disposition      `json:"dispositions"`
	Artifacts    map[string]Artifact         `json:"artifacts"`
	Evidence     map[string]Evidence         `json:"evidence"`
	Submissions  map[string]Submission       `json:"submissions"`
	Decisions    map[string]Decision         `json:"decisions"`
	Handoffs     map[string]Handoff          `json:"handoffs"`
	Assistance   map[string]Assistance       `json:"assistance"`
	Knowledge    map[string]KnowledgeRule    `json:"knowledge"`
	Receipts     map[string]json.RawMessage  `json:"receipts,omitempty"`
}

type CollaborationCommand struct {
	Command
	AuthorityInstanceID    string            `json:"authority_instance_id,omitempty"`
	EntityID               string            `json:"entity_id,omitempty"`
	ExpectedEntityRevision uint64            `json:"expected_entity_revision,omitempty"`
	ExpectedTaskRevision   uint64            `json:"expected_task_revision,omitempty"`
	ContractID             string            `json:"contract_id,omitempty"`
	NonGoals               []string          `json:"non_goals,omitempty"`
	Criteria               []Criterion       `json:"criteria,omitempty"`
	Inputs                 []SourceRef       `json:"inputs,omitempty"`
	Sources                []SourceRef       `json:"sources,omitempty"`
	Report                 *ReportInput      `json:"report,omitempty"`
	Disposition            *DispositionInput `json:"disposition,omitempty"`
	Submission             *SubmissionInput  `json:"submission,omitempty"`
	Mode                   string            `json:"mode,omitempty"`
	Blocking               bool              `json:"blocking,omitempty"`
	Complete               bool              `json:"complete,omitempty"`
	Supersedes             string            `json:"supersedes,omitempty"`
	Waiver                 string            `json:"waiver,omitempty"`
	Evaluation             string            `json:"evaluation,omitempty"`
	Deadline               int64             `json:"deadline,omitempty"`
	MaxRevisions           int               `json:"max_revisions,omitempty"`
}

type CollaborationResult struct {
	Result
	EntityID       string `json:"entity_id,omitempty"`
	EntityRevision uint64 `json:"entity_revision,omitempty"`
	ActionableID   string `json:"actionable_id,omitempty"`
}

func (s *State) NormalizeCollaboration() {
	if s.Collaboration == nil {
		s.Collaboration = &CollaborationState{}
	}
	v := s.Collaboration
	if v.Tasks == nil {
		v.Tasks = map[string]TaskCoordination{}
	}
	if v.Contracts == nil {
		v.Contracts = map[string]TaskContract{}
	}
	if v.Grants == nil {
		v.Grants = map[string]Grant{}
	}
	if v.Manifests == nil {
		v.Manifests = map[string]ContextManifest{}
	}
	if v.Reports == nil {
		v.Reports = map[string]WorkerReport{}
	}
	if v.Actionables == nil {
		v.Actionables = map[string]Actionable{}
	}
	if v.Dispositions == nil {
		v.Dispositions = map[string]Disposition{}
	}
	if v.Artifacts == nil {
		v.Artifacts = map[string]Artifact{}
	}
	if v.Evidence == nil {
		v.Evidence = map[string]Evidence{}
	}
	if v.Submissions == nil {
		v.Submissions = map[string]Submission{}
	}
	if v.Decisions == nil {
		v.Decisions = map[string]Decision{}
	}
	if v.Handoffs == nil {
		v.Handoffs = map[string]Handoff{}
	}
	if v.Assistance == nil {
		v.Assistance = map[string]Assistance{}
	}
	if v.Knowledge == nil {
		v.Knowledge = map[string]KnowledgeRule{}
	}
	if v.Receipts == nil {
		v.Receipts = map[string]json.RawMessage{}
	}
}
