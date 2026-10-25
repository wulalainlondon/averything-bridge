package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
	"time"
)

func (c *Codex) SetTaskAPIProvider(p backend.TaskAPIProvider) { c.taskProvider = p }
func (c *Codex) applyTaskAPIThreadTools(s *session.Session, params map[string]any) error {
	if c.taskProvider == nil {
		return nil
	}
	scope, scopeErr := c.taskProvider.TaskWorkerScope(s)
	if scopeErr != nil {
		return scopeErr
	}
	tools, err := c.taskProvider.TaskTools(s)
	if err != nil || len(tools) == 0 {
		if scope != nil {
			return errors.New("unsupported: bounded task tools unavailable")
		}
		return nil
	}
	existing, _ := params["dynamicTools"].([]map[string]any)
	params["dynamicTools"] = append(existing, map[string]any{"type": "namespace", "name": "bridge_tasks", "description": "Authenticated scoped Bridge tasks; acceptance is not native execution or final delivery.", "tools": tools})
	if scope != nil {
		// Bounded workers receive only the scoped gateway namespace. Legacy
		// controller/delegation/read tools retain their original parent policy.
		params["dynamicTools"] = []map[string]any{{"type": "namespace", "name": "bridge_tasks", "description": "Bounded task input/context only.", "tools": tools}}
		if _, resume := params["threadId"]; resume {
			// 0.162.1 restores the SAME thread's original dynamic catalog;
			// ThreadResumeParams has no dynamicTools field. Never claim an
			// ignored parameter re-registered or narrowed an arbitrary thread.
			delete(params, "dynamicTools")
		}
		version := c.taskDaemonVersion()
		if !taskapi.SupportedNativeProviderVersion("codex", version) {
			return errors.New("unsupported: exact bounded Codex daemon version is unverified")
		}
		raw, e := c.rpcCall("config/read", map[string]any{"includeLayers": false}, 15*time.Second)
		if e != nil {
			return errors.New("unsupported: bounded task MCP config inspection")
		}
		var loaded struct {
			Config struct {
				MCP map[string]json.RawMessage `json:"mcp_servers"`
			} `json:"config"`
		}
		if json.Unmarshal(raw, &loaded) != nil {
			return errors.New("unsupported: bounded task MCP config")
		}
		names := []string{}
		for name := range loaded.Config.MCP {
			names = append(names, name)
		}
		params["config"] = codexPMConfig(names)
		params["approvalPolicy"] = "never"
		params["sandbox"] = "read-only"
		params["baseInstructions"] = "Bounded read-only worker. You may only use bridge_tasks scoped input/context tools. No shell, network, recursive delegation or ambient tools."
	}
	return nil
}
func (c *Codex) handleTaskAPIServerRequest(id any, method string, raw json.RawMessage) bool {
	if method != "item/tool/call" || c.taskProvider == nil {
		return false
	}
	var call struct {
		ThreadID, TurnID, CallID, Namespace, Tool string
		Arguments                                 json.RawMessage
	}
	if json.Unmarshal(raw, &call) != nil || call.Namespace != "bridge_tasks" {
		return false
	}
	s := c.sessionForCodexParams(codexParams(raw))
	if s == nil {
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"success": false, "contentItems": []map[string]any{{"type": "inputText", "text": "caller_unbound"}}}})
		return true
	}
	go func() {
		var envelope struct{ Operation string }
		if json.Unmarshal(call.Arguments, &envelope) != nil || call.Tool != "task_"+envelope.Operation {
			_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"success": false, "contentItems": []map[string]any{{"type": "inputText", "text": "unsupported_tool_schema_mismatch"}}}})
			return
		}
		tools, e := c.taskProvider.TaskTools(s)
		registered := false
		if e == nil {
			for _, tool := range tools {
				if tool["name"] == call.Tool {
					registered = true
				}
			}
		}
		if !registered {
			_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"success": false, "contentItems": []map[string]any{{"type": "inputText", "text": "unsupported_tool_not_registered"}}}})
			return
		}

		st := c.state(s.ID)
		st.mu.Lock()
		request := st.reqID
		owned := st.turnActive && !st.turnStartPending && st.currentTurnID == call.TurnID && st.threadID == call.ThreadID
		st.mu.Unlock()
		if !owned || request == "" {
			_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"success": false, "contentItems": []map[string]any{{"type": "inputText", "text": "caller_unbound"}}}})
			return
		}
		caller := backend.TaskCaller{Session: s, RequestID: request, ThreadID: call.ThreadID, TurnID: call.TurnID, CallID: call.CallID, ProviderVersion: c.taskDaemonVersion(), Validate: func() bool {
			st.mu.Lock()
			defer st.mu.Unlock()
			return st.turnActive && !st.turnStartPending && st.reqID == request && st.currentTurnID == call.TurnID && st.threadID == call.ThreadID
		}}
		response := c.taskProvider.ExecuteTask(context.Background(), caller, call.Arguments)
		if sink, ok := c.taskProvider.(backend.NativeTaskResponseCapture); ok {
			if err := sink.CaptureTaskResponse(context.Background(), caller, call.Arguments, response); err != nil {
				_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"success": false, "contentItems": []map[string]any{{"type": "inputText", "text": "owned_capture_failed"}}}})
				return
			}
		}

		encoded, _ := json.Marshal(response)
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"success": response.OK, "contentItems": []map[string]any{{"type": "inputText", "text": string(encoded)}}}})
	}()
	return true
}

func (c *Codex) TaskAPICapabilities() []taskapi.ProviderCapability {
	version := c.taskDaemonVersion()
	if version == "" {
		version = "unknown"
	}
	capability := taskapi.UnloadedCapability("codex", version, "Original daemon version and provider tool registration must be verified; account/quota are unknown.")
	if c.taskProvider != nil && taskapi.SupportedNativeProviderVersion("codex", version) {
		capability.Binding = "native_tool"
		capability.Lifecycle = "registered"
		capability.NativeEvidence = "exact_turn"
		capability.Operations = []string{"capabilities", "create_dispatch", "list", "get", "read_result", "snapshot", "events", "read_input"}
		if version == "0.162.1" {
			capability.Operations = append(capability.Operations, "append")
		}
		capability.Models = []any{map[string]any{"model": "gpt-6.1-sol", "efforts": []string{"high"}}}
		capability.Enforcement = taskapi.ScopeEnforcement{Mode: "gateway_only_tools", Roots: true, Tools: true, Network: true, Delegation: true}
		capability.Reason = "Adapter registered for qualified daemon " + version + "; per-thread RPC and invocation are distinct later evidence. Only original-owner retained bounded append is qualified. Arbitrary existing-thread narrowing, active cancel, steer and BG are unsupported. Account/quota remain unknown."
	}
	return []taskapi.ProviderCapability{capability}
}

func (c *Codex) taskDaemonVersion() string {
	diagnostics := c.RuntimeDiagnostics()
	native, _ := diagnostics["codex"].(map[string]any)
	if native == nil {
		return "unknown"
	}
	version, _ := native["running_version"].(string)
	if version == "" {
		return "unknown"
	}
	return version
}
