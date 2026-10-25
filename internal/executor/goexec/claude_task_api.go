package goexec

import (
	"context"
	taskcontract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/taskapi"
)

// Version-specific public-interface tags are distinct; a remote tag is not
// local evidence and this interface does not enable rolling CLI flags.
type ClaudeTaskAPIVerifier struct {
	Authority backend.TaskInvocationAuthority
	Version   string
}

func (v ClaudeTaskAPIVerifier) Verify(ctx context.Context, inv taskapi.Invocation) (taskapi.VerifiedContext, error) {
	if v.Authority == nil || !taskapi.SupportedNativeProviderVersion("claude", v.Version) {
		return taskapi.VerifiedContext{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	state, err := v.Authority.LookupTaskInvocation(ctx, inv)
	if err != nil {
		return taskapi.VerifiedContext{}, err
	}
	if state.ProviderVersion != v.Version || state.Namespace != "bridge_tasks" || state.SchemaHash != taskcontract.Hash() || !state.Registered || !state.Loaded {
		return taskapi.VerifiedContext{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	if !state.ActiveLease || state.Invocation != inv || inv.Provider != "claude" || inv.ProcessGeneration == "" || inv.CallID == "" || inv.TurnID != "" || state.Context.BindingKind != "mcp_process_binding" || state.Context.InvocationGeneration != inv.ProcessGeneration || state.Context.SourceRequestID == "" || state.Context.SourceSessionID == "" {
		return taskapi.VerifiedContext{}, taskapi.Failure("caller_unbound", "known_none", "refresh_identity")
	}
	return state.Context, nil
}
func ClaudeTaskAPIInterfaceCapability(version string) taskapi.ProviderCapability {
	return taskapi.UnloadedCapability("claude", version, "B4 interface only; authenticated per-process MCP not registered/loaded")
}
func PrepareClaudeTaskWorker(profile backend.TaskWorkerProfile, version string, parent, scope taskapi.ChildScope, enforcement taskapi.ScopeEnforcement) (backend.TaskWorkerPlan, error) {
	if profile.Backend != "claude" || profile.Model == "" || profile.Effort == "" || !taskapi.SupportedNativeProviderVersion("claude", version) {
		return backend.TaskWorkerPlan{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	if err := taskapi.ValidateChildScope(parent, scope, enforcement); err != nil {
		return backend.TaskWorkerPlan{}, err
	}
	if enforcement.Mode != "gateway_only_tools" || scope.Sandbox != "read-only" {
		return backend.TaskWorkerPlan{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
	}
	for _, op := range scope.Operations {
		if op != "read" {
			return backend.TaskWorkerPlan{}, taskapi.Failure("unsupported", "known_none", "read_capabilities")
		}
	}
	// Credential creation/IPC/actual spawn await original owner integration.
	// No token, argv, global settings or unverified version-specific flags here.
	return backend.TaskWorkerPlan{Profile: profile, ProviderVersion: version, Scope: scope, Parameters: map[string]any{"builtin_tools": []string{}, "mcp_scope": "gateway_only", "ambient_settings": false, "hooks": false, "plugins": false}}, nil
}
