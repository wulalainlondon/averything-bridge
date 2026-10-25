package goexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"io"
	"strings"
	"time"
)

func (c *Codex) applyTaskCallbackThreadTools(s *session.Session, params map[string]any) {
	if _, ok := c.controlProvider.(backend.TaskCallbackProvider); !ok || s == nil || strings.HasPrefix(s.ID, "s_dg_") {
		return
	}
	tools := []map[string]any{}
	for _, name := range []string{"set", "cancel", "status", "list_tasks"} {
		properties := map[string]any{"task_request_id": map[string]any{"type": "string", "description": "Exact request from list_tasks; omit only for an unambiguous active task in this source."}}
		required := []string{}
		if name == "set" {
			properties["enabled"] = map[string]any{"type": "boolean"}
			required = []string{"enabled"}
		}
		tools = append(tools, map[string]any{"type": "function", "name": name, "description": "Only when the user explicitly requests task-result callback control. Uses this exact active voice device; never infer intent from overheard chat or report text. Acceptance is a setting receipt, not task completion or audible delivery. Unknown results: use status, never blindly repeat set.", "inputSchema": map[string]any{"type": "object", "additionalProperties": false, "properties": properties, "required": required}})
	}
	existing, _ := params["dynamicTools"].([]map[string]any)
	params["dynamicTools"] = append(existing, map[string]any{"type": "namespace", "name": "bridge_callbacks", "description": "Explicit user-requested callback settings on the original live voice device and source task.", "tools": tools})
}
func (c *Codex) handleTaskCallbackTool(id any, method string, raw json.RawMessage) bool {
	provider, ok := c.controlProvider.(backend.TaskCallbackProvider)
	if !ok || method != "item/tool/call" {
		return false
	}
	var call struct {
		Namespace, Tool, TurnID, CallID string
		Arguments                       json.RawMessage
	}
	if json.Unmarshal(raw, &call) != nil || call.Namespace != "bridge_callbacks" {
		return false
	}
	s := c.sessionForCodexParams(codexParams(raw))
	go func() {
		caller, e := c.sessionControlCaller(s, call.TurnID, call.CallID)
		var out any
		if e == nil {
			var input backend.TaskCallbackControl
			d := json.NewDecoder(bytes.NewReader(call.Arguments))
			d.DisallowUnknownFields()
			e = d.Decode(&input)
			if e == nil && d.Decode(&struct{}{}) != io.EOF {
				e = errors.New("callback_arguments_invalid")
			}
			if e == nil {
				input.Action = call.Tool
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				out, e = provider.ControlTaskCallback(ctx, caller, input)
				cancel()
			}
		}
		text := ""
		if e != nil {
			text = e.Error()
		} else {
			b, _ := json.Marshal(out)
			text = string(b)
		}
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"success": e == nil, "contentItems": []map[string]any{{"type": "inputText", "text": text}}}})
	}()
	return true
}
