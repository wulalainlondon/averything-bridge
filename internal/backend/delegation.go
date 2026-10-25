package backend

import "everything-go/internal/session"

// DelegationSpec is a one-turn, independent conversation request. The Bridge
// supplies the parent identity from the active tool call, never from model
// arguments.
type DelegationSpec struct {
	Name, Cwd, Instruction, Model, Effort, Sandbox string
}

type DelegationReceipt struct {
	ID             string `json:"delegation_id"`
	ChildSessionID string `json:"child_session_id"`
	ChildRequestID string `json:"child_request_id"`
}

type DelegationResultPage struct {
	ID         string   `json:"delegation_id"`
	Status     string   `json:"status"`
	Text       string   `json:"text"`
	Artifacts  []string `json:"artifacts"`
	NextOffset int      `json:"next_offset"`
	HasMore    bool     `json:"has_more"`
}

type DelegationProvider interface {
	DelegateSession(parent *session.Session, parentRequestID, toolCallID string, spec DelegationSpec) (DelegationReceipt, error)
	ReadDelegationResult(parent *session.Session, delegationID string, offset, limit int) (DelegationResultPage, error)
}

// DelegationQueueProvider is optional for adapters. All identities remain
// bound by the native tool transport, never supplied by the model.
type DelegationQueueProvider interface {
	CancelWaitingDelegation(parent *session.Session, id string) (map[string]any, error)
	DelegationCapacity(parent *session.Session) (map[string]int, error)
}

// Continuation arguments contain expectations, never a caller identity, sandbox
// or replacement session. The native transport binds the original parent.
type DelegationContinuation struct {
	DelegationID           string `json:"delegation_id"`
	ExpectedPreviousID     string `json:"expected_previous_round_id"`
	InstanceID             string `json:"instance_id"`
	ExpectedThreadID       string `json:"expected_thread_id"`
	ExpectedConfigRevision uint64 `json:"expected_config_revision"`
	Content                string `json:"content"`
	Mode                   string `json:"mode"`
	ActiveRequestID        string `json:"active_request_id,omitempty"`
}
type DelegationContinuityProvider interface {
	ContinueDelegation(*session.Session, string, string, DelegationContinuation) (any, error)
	DelegationRounds(*session.Session, string, uint64) (any, error)
	BindDelegationQA(*session.Session, string, string, string, string) (any, error)
	RecordDelegationReview(*session.Session, string, string, string, string) (any, error)
}

// Context preservation is an execution constraint, not worker permissions.
type DelegationContextProvider interface {
	PreserveDelegationContext(*session.Session) (bool, error)
}
