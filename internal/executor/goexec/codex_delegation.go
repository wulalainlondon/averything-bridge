package goexec

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func (c *Codex) SetDelegationProvider(p backend.DelegationProvider) { c.delegationProvider = p }

// applyDelegationThreadTools leaves every ordinary Codex tool intact. PM
// policy, when present, replaces this namespace with its own bounded tools.
func (c *Codex) applyDelegationThreadTools(s *session.Session, params map[string]any) {
	if c.delegationProvider == nil || s == nil || strings.HasPrefix(s.ID, "s_dg_") {
		return
	}
	params["dynamicTools"] = []map[string]any{{
		"type": "namespace", "name": "bridge_sessions",
		"description": "Delegate a user-authorized, independent one-turn task to a new Bridge conversation. Bridge returns its final answer to this conversation after completion.",
		"tools": []map[string]any{{
			"type": "function", "name": "delegate_session",
			"description": "Create a fresh independent Codex session for a bounded task. Use only when the user asks you to delegate or independently check work. The child sees only the instruction you supply, not this chat history.",
			"inputSchema": map[string]any{
				"type": "object", "additionalProperties": false,
				"properties": map[string]any{
					"name":        map[string]any{"type": "string", "description": "Short child conversation title"},
					"cwd":         map[string]any{"type": "string", "description": "Existing directory inside this session's workspace; defaults to the current cwd"},
					"instruction": map[string]any{"type": "string", "description": "Complete standalone task instructions and explicitly permitted inputs"},
					"model":       map[string]any{"type": "string", "description": "Optional model; default inherits parent"},
					"effort":      map[string]any{"type": "string", "enum": []string{"low", "medium", "high", "xhigh", "max", "ultra"}, "description": "Optional reasoning effort; default inherits parent"},
					"sandbox":     map[string]any{"type": "string", "enum": []string{"read-only", "workspace-write", "danger-full-access"}, "description": "Optional sandbox, never broader than parent"},
				},
				"required": []string{"name", "instruction"},
			},
		}, {
			"type": "function", "name": "read_result",
			"description": "Read a sealed delegated result belonging to this parent conversation, in bounded pages. Use when the automatic return was truncated or you need to revisit it.",
			"inputSchema": map[string]any{
				"type": "object", "additionalProperties": false,
				"properties": map[string]any{
					"delegation_id": map[string]any{"type": "string"},
					"offset":        map[string]any{"type": "integer", "minimum": 0},
					"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": 16000},
				},
				"required": []string{"delegation_id"},
			},
		}},
	}}
}

func (c *Codex) handleDelegationServerRequest(id any, method string, raw json.RawMessage) bool {
	if method != "item/tool/call" || c.delegationProvider == nil {
		return false
	}
	var call struct {
		ThreadID  string          `json:"threadId"`
		Namespace string          `json:"namespace"`
		Tool      string          `json:"tool"`
		CallID    string          `json:"callId"`
		TurnID    string          `json:"turnId"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if json.Unmarshal(raw, &call) != nil || call.Namespace != "bridge_sessions" {
		return false
	}
	s := c.sessionForCodexParams(codexParams(raw))
	if s == nil {
		_ = c.rpc.write(map[string]any{"id": id, "error": map[string]any{"code": -32000, "message": "delegation_session_not_found"}})
		return true
	}
	go func() {
		var err error
		var result any
		st := c.state(s.ID)
		st.mu.Lock()
		requestID, turnID, active := st.reqID, st.currentTurnID, st.turnActive
		st.mu.Unlock()
		if call.CallID == "" || !active || requestID == "" || turnID == "" || call.TurnID != turnID || strings.HasPrefix(s.ID, "s_dg_") {
			err = errors.New("delegation_tool_forbidden")
		} else {
			switch call.Tool {
			case "delegate_session":
				var spec backend.DelegationSpec
				decoder := json.NewDecoder(bytes.NewReader(call.Arguments))
				decoder.DisallowUnknownFields()
				if err = decoder.Decode(&spec); err == nil {
					result, err = c.delegationProvider.DelegateSession(s, requestID, call.CallID, spec)
				}
			case "read_result":
				var args struct {
					DelegationID string `json:"delegation_id"`
					Offset       int    `json:"offset"`
					Limit        int    `json:"limit"`
				}
				decoder := json.NewDecoder(bytes.NewReader(call.Arguments))
				decoder.DisallowUnknownFields()
				if err = decoder.Decode(&args); err == nil {
					result, err = c.delegationProvider.ReadDelegationResult(s, args.DelegationID, args.Offset, args.Limit)
				}
			default:
				err = errors.New("delegation_tool_forbidden")
			}
		}
		output := ""
		if err != nil {
			output = err.Error()
		} else {
			encoded, _ := json.Marshal(result)
			output = string(encoded)
		}
		c.tools.Start(s.ID, requestID, call.CallID, "bridge_sessions__"+call.Tool, string(call.Arguments))
		c.tools.Result(s.ID, requestID, call.CallID, output)
		c.tools.End(s.ID, requestID, call.CallID)
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"success": err == nil,
			"contentItems": []map[string]any{{"type": "inputText", "text": output}}}})
	}()
	return true
}
