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
