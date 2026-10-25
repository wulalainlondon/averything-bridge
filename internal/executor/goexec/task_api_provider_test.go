package goexec

import (
	"context"
	taskcontract "everything-go/contracts/task-api/v1"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"strings"
	"testing"
)

// Synthetic invocation state only; no live tools/native/provider evidence.
type fixtureTaskAuthority struct{ state backend.TaskInvocationState }

func (f *fixtureTaskAuthority) LookupTaskInvocation(context.Context, taskapi.Invocation) (backend.TaskInvocationState, error) {
	return f.state, nil
}
func TestTaskAPIProviderUnloadedUnsupported(t *testing.T) {
	for _, v := range []taskapi.CallerVerifier{CodexTaskAPIVerifier{}, ClaudeTaskAPIVerifier{Version: "2.1.280"}, ClaudeBackgroundTaskAPIVerifier{Version: "2.1.291"}} {
		if _, err := v.Verify(context.Background(), taskapi.Invocation{}); err == nil {
			t.Fatal("unloaded tools became callable")
		}
	}
}
func TestTaskAPICodexBoundFixtureAndSpoof(t *testing.T) {
	for _, version := range []string{"0.160.0", "0.162.1"} {
		t.Run(version, func(t *testing.T) {
			inv := taskapi.Invocation{Provider: "codex", ThreadID: "thread", TurnID: "turn", CallID: "call"}
			binding := taskapi.VerifiedContext{Authority: "authority", StableScopeID: "scope", InvocationGeneration: "owned", SourceSessionID: "source", SourceRequestID: "request", BindingKind: "native_tool"}
			a := &fixtureTaskAuthority{backend.TaskInvocationState{ProviderVersion: version, Namespace: "bridge_tasks", SchemaHash: taskcontract.Hash(), Registered: true, Loaded: true, ActiveLease: true, Invocation: inv, Context: binding}}
			v := CodexTaskAPIVerifier{a}
			if _, err := taskapi.BindCaller(context.Background(), v, inv); err != nil {
				t.Fatal(err)
			}
			bad := inv
			bad.TurnID = "forged"
			if _, err := v.Verify(context.Background(), bad); err == nil {
				t.Fatal("spoof turn")
			}
			a.state.Loaded = false
			if _, err := v.Verify(context.Background(), inv); err == nil {
				t.Fatal("namespace alone accepted")
			}
			a.state.Loaded = true
			a.state.ActiveLease = false
			if _, err := v.Verify(context.Background(), inv); err == nil {
				t.Fatal("expired lease")
			}
		})
	}
}
func TestTaskAPIClaudeBoundProcessTagsAndSpoof(t *testing.T) {
	for _, version := range []string{"2.1.280", "2.1.291", "2.1.296"} {
		inv := taskapi.Invocation{Provider: "claude", CallID: "server-mcp-call", ProcessGeneration: "process1"}
		binding := taskapi.VerifiedContext{Authority: "authority", StableScopeID: "scope", InvocationGeneration: "process1", SourceSessionID: "source", SourceRequestID: "request", BindingKind: "mcp_process_binding"}
		a := &fixtureTaskAuthority{backend.TaskInvocationState{ProviderVersion: version, Namespace: "bridge_tasks", SchemaHash: taskcontract.Hash(), Registered: true, Loaded: true, ActiveLease: true, Invocation: inv, Context: binding}}
		v := ClaudeTaskAPIVerifier{a, version}
		if _, err := taskapi.BindCaller(context.Background(), v, inv); err != nil {
			t.Fatal(err)
		}
		bad := inv
		bad.ProcessGeneration = "other-process"
		if _, err := v.Verify(context.Background(), bad); err == nil {
			t.Fatal("cross process spoof")
		}
		bad = inv
		bad.TurnID = "invented-codex-turn"
		if _, err := v.Verify(context.Background(), bad); err == nil {
			t.Fatal("Claude native turn invented")
		}
		a.state.ProviderVersion = "unknown"
		if _, err := v.Verify(context.Background(), inv); err == nil {
			t.Fatal("rolling version assumed")
		}
	}
}
func TestTaskAPIWorkerPlansNoSpawnOrConsumption(t *testing.T) {
	root := t.TempDir()
	scope := taskapi.ChildScope{Roots: []string{root}, Operations: []string{"read"}, Sandbox: "read-only", Network: "deny"}
	e := taskapi.ScopeEnforcement{Mode: "gateway_only_tools", Roots: true, Tools: true, Network: true, Delegation: true}
	p, err := PrepareClaudeTaskWorker(backend.TaskWorkerProfile{Backend: "claude", Model: "fixture-model", Effort: "high"}, "2.1.280", scope, scope, e)
	if err != nil || p.NativeConsumptionProven {
		t.Fatal(p, err)
	}
	if _, err := PrepareCodexTaskWorker(backend.TaskWorkerProfile{Backend: "codex", Model: "gpt-6.1-sol", Effort: "high"}, scope, scope, taskapi.ScopeEnforcement{}); err == nil {
		t.Fatal("unrepresentable scope prepared")
	}
}

func TestClaudeTaskPolicyNormalizationKeepsCallerAndNarrowsVerifiedWorker(t *testing.T) {
	args := claudeReadOnlyWorkerArgs(session.Snapshot{Model: "sonnet", Sandbox: "read-only"}, "http://127.0.0.1/fixture")
	tools := []string{"mcp__bridge_tasks__task_capabilities", "mcp__bridge_tasks__task_read_input"}
	worker := normalizeClaudeTaskArgs(args, true, tools)
	count := func(values []string, key string) int {
		n := 0
		for _, value := range values {
			if value == key {
				n++
			}
		}
		return n
	}
	for _, key := range []string{"--tools", "--allowedTools", "--settings", "--setting-sources", "--strict-mcp-config"} {
		if count(worker, key) != 1 {
			t.Fatal("ambiguous worker policy", key)
		}
	}
	for i, value := range worker {
		if value == "--tools" && worker[i+1] != "" {
			t.Fatal("worker builtin scope widened")
		}
		if value == "--allowedTools" && worker[i+1] != strings.Join(tools, ",") {
			t.Fatal("worker MCP scope widened")
		}
	}
	caller := normalizeClaudeTaskArgs(claudeSpawnArgs(session.Snapshot{Sandbox: "read-only"}, "http://127.0.0.1/fixture"), false, []string{"mcp__bridge_tasks__task_create_dispatch"})
	for i, value := range caller {
		if value == "--allowedTools" && (!strings.Contains(caller[i+1], "Read") || !strings.Contains(caller[i+1], "mcp__bridge_tasks__task_create_dispatch")) {
			t.Fatal("authenticated task tool not exposed under original caller policy")
		}
	}
	if !validTaskWorkerTools(tools) || validTaskWorkerTools([]string{"Read", tools[1]}) || validTaskWorkerTools([]string{"mcp__bridge_tasks__task_create_dispatch", tools[1]}) || validTaskWorkerTools([]string{"ToolSearch"}) {
		t.Fatal("worker init did not enforce exact gateway-only read catalog")
	}
}

func TestClaudeMCPObjectRootPreservesFullRequestSchema(t *testing.T) {
	defs, err := taskcontract.ToolInputs()
	if err != nil {
		t.Fatal(err)
	}
	for name, original := range defs {
		schema, err := mcpTaskInputSchema(original)
		if err != nil {
			t.Fatal(name, err)
		}
		if schema["type"] != "object" || schema["properties"] == nil || schema["required"] == nil || schema["$defs"] == nil {
			t.Fatal("MCP discovery lost request root", name)
		}
	}
}
