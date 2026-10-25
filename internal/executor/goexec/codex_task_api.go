package goexec

import (
	"context"
	taskcontract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/taskapi"
)

// CodexTaskAPIVerifier is not wired to existing tools/guards by this batch.
// A missing or unloaded authority is explicitly unsupported.
type CodexTaskAPIVerifier struct {
	Authority backend.TaskInvocationAuthority
}

func (v CodexTaskAPIVerifier) Verify(ctx context.Context, inv taskapi.Invocation) (taskapi.VerifiedContext, error) {
	if v.Authority == nil {
		return taskapi.VerifiedContext{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	state, err := v.Authority.LookupTaskInvocation(ctx, inv)
	if err != nil {
		return taskapi.VerifiedContext{}, err
	}
	if !taskapi.SupportedNativeProviderVersion("codex", state.ProviderVersion) || state.Namespace != "bridge_tasks" || state.SchemaHash != taskcontract.Hash() || !state.Registered || !state.Loaded {
		return taskapi.VerifiedContext{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	if !state.ActiveLease || state.Invocation != inv || inv.Provider != "codex" || inv.ThreadID == "" || inv.TurnID == "" || inv.CallID == "" || state.Context.BindingKind != "native_tool" || state.Context.SourceRequestID == "" || state.Context.SourceSessionID == "" {
		return taskapi.VerifiedContext{}, taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
	}
	return state.Context, nil
}
func CodexTaskAPIInterfaceCapability() taskapi.ProviderCapability {
	return taskapi.UnloadedCapability("codex", "0.160.0", "B4 interface only; no registered/loaded native invocation authority")
}
func PrepareCodexTaskWorker(profile backend.TaskWorkerProfile, parent, scope taskapi.ChildScope, enforcement taskapi.ScopeEnforcement, providerVersions ...string) (backend.TaskWorkerPlan, error) {
	version := "0.160.0" // Preserve the existing explicit legacy interface plan.
	if len(providerVersions) > 1 {
		return backend.TaskWorkerPlan{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	if len(providerVersions) == 1 {
		version = providerVersions[0]
	}
	if !taskapi.SupportedNativeProviderVersion("codex", version) {
		return backend.TaskWorkerPlan{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	if profile.Backend != "codex" || profile.Model == "" || profile.Effort == "" {
		return backend.TaskWorkerPlan{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	if err := taskapi.ValidateChildScope(parent, scope, enforcement); err != nil {
		return backend.TaskWorkerPlan{}, err
	}
	// Scope enforcement alone is not model/effort entitlement or tool loading.
	// This is a parameter plan, with zero spawn/RPC and no consumption assertion.
	return backend.TaskWorkerPlan{Profile: profile, ProviderVersion: version, Scope: scope, Parameters: map[string]any{"model": profile.Model, "effort": profile.Effort}}, nil
}
