package goexec

import (
	"encoding/json"
	"strings"
)

type codexToolCall struct {
	ID      string
	Name    string
	Command string
	Output  string
}

func (t codexToolCall) historyBlock() map[string]any {
	return map[string]any{
		"type":        "tool_call",
		"tool_use_id": t.ID,
		"name":        t.Name,
		"command":     t.Command,
		"output":      t.Output,
	}
}

type codexToolPayload struct {
	Type      string          `json:"type"`
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
	Input     json.RawMessage `json:"input"`
	CallID    string          `json:"call_id"`
	CallID2   string          `json:"callId"`
	ID        string          `json:"id"`
	Output    string          `json:"output"`
}

func normalizeCodexResponseTool(payload json.RawMessage, output string) (codexToolCall, bool) {
	var p codexToolPayload
	if json.Unmarshal(payload, &p) != nil {
		return codexToolCall{}, false
	}
	switch p.Type {
	case "function_call":
		if p.Name == "update_plan" {
			return codexToolCall{}, false
		}
		name, command := normalizeCodexToolNameCommand(p.Name, firstRaw(p.Arguments, p.Input))
		return codexToolCall{ID: firstNonEmpty(p.CallID, p.CallID2, p.ID, "codex_tool"), Name: name, Command: command, Output: output}, true
	case "custom_tool_call":
		if p.Name == "update_plan" {
			return codexToolCall{}, false
		}
		name, command := normalizeCodexToolNameCommand(p.Name, firstRaw(p.Input, p.Arguments))
		return codexToolCall{ID: firstNonEmpty(p.CallID, p.CallID2, p.ID, "codex_tool"), Name: name, Command: command, Output: output}, true
	default:
		return codexToolCall{}, false
	}
}

func codexResponseToolOutput(payload json.RawMessage) (string, string, bool) {
	var p codexToolPayload
	if json.Unmarshal(payload, &p) != nil {
		return "", "", false
	}
	if p.Type != "function_call_output" && p.Type != "custom_tool_call_output" {
		return "", "", false
	}
	id := firstNonEmpty(p.CallID, p.CallID2, p.ID)
	if id == "" {
		return "", "", false
	}
	return id, p.Output, true
}

func normalizeCodexLiveTool(params json.RawMessage) (codexToolCall, bool) {
	var p struct {
		ItemID    string          `json:"itemId"`
		CallID    string          `json:"callId"`
		ToolCall  string          `json:"toolCallId"`
		ToolUseID string          `json:"toolUseId"`
		ID        string          `json:"id"`
		Name      string          `json:"name"`
		Type      string          `json:"type"`
		Command   json.RawMessage `json:"command"`
		Input     json.RawMessage `json:"input"`
		Arguments json.RawMessage `json:"arguments"`
		Item      struct {
			ID        string          `json:"id"`
			Name      string          `json:"name"`
			Type      string          `json:"type"`
			Command   json.RawMessage `json:"command"`
			Input     json.RawMessage `json:"input"`
			Arguments json.RawMessage `json:"arguments"`
		} `json:"item"`
	}
	if json.Unmarshal(params, &p) != nil {
		return codexToolCall{}, false
	}
	if p.Type == "function_call" || p.Type == "custom_tool_call" {
		return normalizeCodexResponseTool(params, "")
	}
	if p.Item.Type == "function_call" || p.Item.Type == "custom_tool_call" {
		raw, _ := json.Marshal(p.Item)
		return normalizeCodexResponseTool(raw, "")
	}
	itemType := firstNonEmpty(p.Type, p.Item.Type)
	if !isCodexLiveToolType(itemType) {
		return codexToolCall{}, false
	}
	rawName := firstNonEmpty(p.Name, p.Item.Name, p.Type, p.Item.Type, "codex_tool")
	if rawName == "update_plan" {
		return codexToolCall{}, false
	}
	commandRaw := firstRaw(p.Command, p.Input, p.Arguments, p.Item.Command, p.Item.Input, p.Item.Arguments)
	name, command := normalizeCodexToolNameCommand(rawName, commandRaw)
	return codexToolCall{
		ID:      firstNonEmpty(p.ItemID, p.CallID, p.ToolCall, p.ToolUseID, p.ID, p.Item.ID, "codex_item"),
		Name:    name,
		Command: command,
	}, true
}

// isCodexLiveToolType is intentionally an allow-list. App-server emits
// item/started and item/completed for conversational lifecycle items such as
// userMessage, agentMessage, reasoning and plan as well as real tool calls.
// Treating every unknown item as a tool makes an ordinary first response flash
// "running tool: userMessage/agentMessage" in the authoritative runtime UI.
func isCodexLiveToolType(itemType string) bool {
	switch itemType {
	case "commandExecution", "command_execution", "command",
		"fileChange", "file_change", "applyPatch",
		"mcpToolCall", "mcp_tool_call",
		"dynamicToolCall", "dynamic_tool_call",
		"webSearch", "web_search",
		"imageGeneration", "image_generation",
		"imageView", "image_view":
		return true
	default:
		return false
	}
}

func normalizeCodexToolNameCommand(rawName string, raw json.RawMessage) (string, string) {
	normalized := strings.ReplaceAll(rawName, "-", "_")
	switch normalized {
	case "exec_command", "commandExecution", "command_execution":
		if args, ok := codexRawObject(raw); ok {
			return "Bash", codexAnyString(codexFirstAny(args, "cmd", "command"))
		}
		return "Bash", rawToString(raw)
	case "write_stdin":
		return "Stdin", rawToString(raw)
	case "view_image":
		if args, ok := codexRawObject(raw); ok {
			if path := codexAnyString(args["path"]); path != "" {
				return "ViewImage", path
			}
		}
		return "ViewImage", rawToString(raw)
	case "apply_patch", "fileChange", "file_change":
		return "ApplyPatch", rawToString(raw)
	default:
		return firstNonEmpty(rawName, "codex_tool"), rawToString(raw)
	}
}

func codexRawObject(raw json.RawMessage) (map[string]any, bool) {
	var args map[string]any
	if json.Unmarshal(raw, &args) == nil && args != nil {
		return args, true
	}
	var encoded string
	if json.Unmarshal(raw, &encoded) == nil {
		if json.Unmarshal([]byte(encoded), &args) == nil && args != nil {
			return args, true
		}
	}
	return nil, false
}

func firstRaw(values ...json.RawMessage) json.RawMessage {
	for _, v := range values {
		if len(v) > 0 && string(v) != "null" {
			return v
		}
	}
	return nil
}
