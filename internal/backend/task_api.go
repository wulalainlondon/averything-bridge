package backend

import (
	"context"
	"encoding/json"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

// TaskInvocationState is supplied by a server-owned native/process registry.
// No JSON/tool argument or observed runtime directory can construct authority.
type TaskInvocationState struct {
	ProviderVersion                 string
	Namespace                       string
	SchemaHash                      string
	Registered, Loaded, ActiveLease bool
	Invocation                      taskapi.Invocation
	Context                         taskapi.VerifiedContext
}

// Original executor/tool transport implements this after owner-reviewed wiring.
// It must validate actual native call or MCP process generation, grant/run/epoch
// and lease. B4 builds provide only this port and isolated fixtures.
type TaskInvocationAuthority interface {
	LookupTaskInvocation(context.Context, taskapi.Invocation) (TaskInvocationState, error)
}
type TaskWorkerProfile struct{ Backend, Model, Effort string }
type TaskWorkerPlan struct {
	Profile                 TaskWorkerProfile
	ProviderVersion         string
	Parameters              map[string]any
	Scope                   taskapi.ChildScope
	NativeConsumptionProven bool
}

type TaskCaller struct {
	Session                                                                 *session.Session
	RequestID, ThreadID, TurnID, CallID, ProcessGeneration, ProviderVersion string
	Validate                                                                func() bool
}
type TaskAPIProvider interface {
	TaskTools(*session.Session) ([]map[string]any, error)
	ExecuteTask(context.Context, TaskCaller, []byte) taskapi.Response
	TaskWorkerScope(*session.Session) (*taskapi.ChildScope, error)
}

// An executor must explicitly implement per-command enforcement for existing
// native threads. Session sandbox equality alone does not enforce task roots,
// tool/network policy or recursive delegation bounds.
type ExistingTaskScopeEnforcer interface {
	ValidateExistingTaskScope(*session.Session, taskapi.ChildScope, string) error
}
type TaskCapabilitySource interface {
	TaskAPICapabilities() []taskapi.ProviderCapability
}

// Original response owner only, optional default-off read evidence sink.
type NativeTaskResponseCapture interface {
	CaptureTaskResponse(context.Context, TaskCaller, json.RawMessage, taskapi.Response) error
}

// TaskQuestionObserver receives provider-observed, source-bound questions.
// It is an internal port, never a client-supplied authority envelope.
type TaskQuestionEvidence struct {
	Session                             *session.Session
	RequestID, ThreadID, TurnID, CallID string
	Payload                             UserInputPayload
}
type TaskQuestionObserver interface {
	ObserveTaskQuestion(context.Context, TaskQuestionEvidence)
}
