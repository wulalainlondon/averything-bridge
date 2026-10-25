// Package taskapi is a business-service boundary, not a store or transport.
// Production wiring to the canonical owner stores is deliberately separate.
package taskapi

import (
	"context"
	"encoding/json"
)

type Request struct {
	Type           string          `json:"type"`
	Version        string          `json:"api_version"`
	CorrelationID  string          `json:"correlation_id"`
	Operation      string          `json:"operation"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Input          json.RawMessage `json:"input"`
}
type APIError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	Retryable  bool   `json:"retryable"`
	Acceptance string `json:"acceptance"`
	ReceiptID  string `json:"receipt_id,omitempty"`
	Action     string `json:"recommended_action"`
	Capability string `json:"capability,omitempty"`
}

func (e *APIError) Error() string { return e.Code }
func Failure(code, acceptance, action string) *APIError {
	return &APIError{Code: code, Message: code, Acceptance: acceptance, Action: action}
}

type Response struct {
	Type           string    `json:"type"`
	Version        string    `json:"api_version"`
	CorrelationID  string    `json:"correlation_id"`
	Operation      *string   `json:"operation"`
	OK             bool      `json:"ok"`
	Result         any       `json:"result,omitempty"`
	Error          *APIError `json:"error,omitempty"`
	RequestVersion *string   `json:"request_api_version,omitempty"`
}

// Task responses may contain private source/result text and artifact bytes.
// Protocol diagnostics retain the frame type without logging that payload.
func (Response) PrivatePayload() {}

func (r Response) MarshalJSON() ([]byte, error) {
	type wire Response
	raw, err := json.Marshal(wire(r))
	if err != nil {
		return nil, err
	}
	if r.OK {
		return raw, nil
	}
	var object map[string]any
	if err = json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	// Unknown/malformed version is represented by null, not silently omitted.
	if r.RequestVersion == nil {
		object["request_api_version"] = nil
	}
	return json.Marshal(object)
}

// VerifiedContext has no wire representation. Only a trusted server verifier
// derives it from actual transport/provider invocation and original authority.
type VerifiedContext struct {
	NativeTurnID, ToolCallID, ProcessGeneration            string
	Authority                                              string
	StableScopeID                                          string
	NamespaceGeneration                                    uint64
	InvocationGeneration                                   string
	SourceSessionID                                        string
	SourceRequestID                                        string
	SourceResumeID                                         string
	SourceConfigRevision                                   uint64
	SourceConfigKnown                                      bool
	PMProjectID, PMTaskID, PMRunID, ContractID, ManifestID string
	PMEpoch, GrantRevision                                 uint64
	Transport                                              string
	BindingKind                                            string
	InternalDelivery                                       bool
}
type Invocation struct{ Provider, ThreadID, TurnID, CallID, ProcessGeneration string }
type CallerVerifier interface {
	Verify(context.Context, Invocation) (VerifiedContext, error)
}
type BoundCaller struct {
	context    VerifiedContext
	verifier   CallerVerifier
	invocation Invocation
}

func BindCaller(ctx context.Context, verifier CallerVerifier, invocation Invocation) (BoundCaller, error) {
	if verifier == nil {
		return BoundCaller{}, Failure("caller_unbound", "known_none", "read_capabilities")
	}
	binding, err := verifier.Verify(ctx, invocation)
	if err != nil {
		return BoundCaller{}, err
	}
	if binding.Authority == "" || binding.StableScopeID == "" || binding.InvocationGeneration == "" {
		return BoundCaller{}, Failure("caller_unbound", "known_none", "read_capabilities")
	}
	return BoundCaller{binding, verifier, invocation}, nil
}

// Check is called afresh for every operation/page. A reconnect may change
// invocation generation, but cannot select another idempotency namespace.
func (c BoundCaller) Check(ctx context.Context) (VerifiedContext, error) {
	if c.verifier == nil {
		return VerifiedContext{}, Failure("caller_unbound", "known_none", "read_capabilities")
	}
	current, err := c.verifier.Verify(ctx, c.invocation)
	if err != nil {
		return VerifiedContext{}, err
	}
	if current != c.context {
		return VerifiedContext{}, Failure("caller_unbound", "known_none", "refresh_identity")
	}
	return current, nil
}

type Locator struct {
	Authority string `json:"authority_instance_id"`
	Path      string `json:"path"`
	Operation string `json:"operation"`
	Key       string `json:"idempotency_key"`
	TaskID    string `json:"task_id,omitempty"`
}
type Namespace struct {
	Authority, StableScopeID string
	Generation               uint64
	Path, Operation, TaskID  string
}
type AuthorizedCommand struct {
	Revalidate func(context.Context) error
	Caller     VerifiedContext
	Request    Request
	Namespace  Namespace
	Locator    Locator
	IntentHash string
}

// Authorizer must invoke existing policy: grants, PM approval/lease/run/epoch,
// thread/revision/authority and child scope. It resolves path using canonical
// records, never by an LLM-selected path or runtime scan.
type Authorizer interface {
	Authorize(context.Context, VerifiedContext, Request) (Locator, error)
}

// Gateway is implemented by original store owners. Mutate must atomically claim
// namespace+intent, preserve the same effect receipt and enqueue its original
// outbox. No implementation or fallback store lives in this package.
// Receipt lookup Read uses only AuthorizedCommand.Namespace/Locator, never
// re-routes from raw input. Namespace contains the current server-bound scope.
type Gateway interface {
	Mutate(context.Context, AuthorizedCommand) (any, error)
	Read(context.Context, AuthorizedCommand) (any, error)
}
