package goexec

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"

	delegationcontract "everything-go/contracts/delegation/v1"
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
		"description": "Manage this original parent’s authorized delegated tasks. Create a session for a new logical task; continue corrections/rechecks on the original owner/QA sessions. Each round returns its own final to this parent.",
		"tools": []map[string]any{{
			"type": "function", "name": "delegate_session",
			"description": "Accept a fresh independent Codex task; excess work waits in a durable FIFO. An acceptance receipt does not prove execution started. Query get_capacity for this parent's separate execution, waiting and delivery limits. Use only for a new logical task the user authorized you to delegate or independently check. For an existing logical task use continue_delegation or its original TaskAPI append; reuse the bound independent QA. The child sees only the instruction you supply, not this chat history.",
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
	if _, ok := c.delegationProvider.(backend.DelegationQueueProvider); ok {
		namespaces := params["dynamicTools"].([]map[string]any)
		entries := namespaces[0]["tools"].([]map[string]any)
		namespaces[0]["tools"] = append(entries,
			map[string]any{"type": "function", "name": "get_capacity", "description": "Read this parent's configured execution, waiting and reserved-delivery bounds and counts. No work is started.", "inputSchema": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{}}},
			map[string]any{"type": "function", "name": "cancel_waiting_delegation", "description": "Cancel this parent's exact delegation only while it is durably waiting for execution capacity. Already-started or uncertain work is never reported stopped.", "inputSchema": map[string]any{"type": "object", "additionalProperties": false, "properties": map[string]any{"delegation_id": map[string]any{"type": "string", "minLength": 1}}, "required": []string{"delegation_id"}}})
	}
	if _, ok := c.delegationProvider.(backend.DelegationContinuityProvider); ok {
		namespaces := params["dynamicTools"].([]map[string]any)
		entries := namespaces[0]["tools"].([]map[string]any)
		namespaces[0]["tools"] = append(entries, delegationcontract.Tools()...)
	}
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
		requestID, turnID, active := st.reqID, st.currentTurnID, st.turnActive && !st.turnStartPending
		st.mu.Unlock()
		owned := active && requestID != "" && turnID != "" && call.TurnID == turnID
		var voiceCaller backend.SessionControlCaller
		if !owned {
			voiceCaller, err = c.sessionControlCaller(s, call.TurnID, call.CallID)
			if err == nil && voiceCaller.VoiceID != "" {
				requestID = voiceCaller.RequestID
			} else {
				err = errors.New("delegation_tool_forbidden")
			}
		}
		if call.CallID == "" || (!owned && err != nil) || strings.HasPrefix(s.ID, "s_dg_") {
			err = errors.New("delegation_tool_forbidden")
		} else {
			switch call.Tool {
			case "delegate_session":
				var spec backend.DelegationSpec
				decoder := json.NewDecoder(bytes.NewReader(call.Arguments))
				decoder.DisallowUnknownFields()
				if err = decoder.Decode(&spec); err == nil {
					if owned {
						result, err = c.delegationProvider.DelegateSession(s, requestID, call.CallID, spec)
					} else if provider, ok := c.delegationProvider.(backend.VoiceDelegationProvider); ok {
						result, err = provider.DelegateVoiceSession(voiceCaller, spec)
					} else {
						err = errors.New("delegation_voice_unsupported")
					}
				}
			case "cancel_waiting_delegation", "get_capacity":
				provider, ok := c.delegationProvider.(backend.DelegationQueueProvider)
				if !ok {
					err = errors.New("delegation_queue_unsupported")
					break
				}
				var args struct {
					ID string `json:"delegation_id"`
				}
				decoder := json.NewDecoder(bytes.NewReader(call.Arguments))
				decoder.DisallowUnknownFields()
				if err = decoder.Decode(&args); err == nil {
					if call.Tool == "get_capacity" {
						result, err = provider.DelegationCapacity(s)
					} else {
						result, err = provider.CancelWaitingDelegation(s, args.ID)
					}
				}
			case "continue_delegation", "get_delegation_rounds", "bind_delegation_qa", "record_delegation_review":
				provider, ok := c.delegationProvider.(backend.DelegationContinuityProvider)
				if !ok || !owned {
					err = errors.New("delegation_continuation_parent_not_owned")
					break
				}
				if err = delegationcontract.Validate(call.Tool, call.Arguments); err != nil {
					break
				}
				decoder := json.NewDecoder(bytes.NewReader(call.Arguments))
				decoder.DisallowUnknownFields()
				switch call.Tool {
				case "continue_delegation":
					var in backend.DelegationContinuation
					if err = decoder.Decode(&in); err == nil {
						result, err = provider.ContinueDelegation(s, requestID, call.CallID, in)
					}
				case "get_delegation_rounds":
					var in struct {
						ID    string `json:"delegation_id"`
						After uint64 `json:"after_version"`
					}
					if err = decoder.Decode(&in); err == nil {
						result, err = provider.DelegationRounds(s, in.ID, in.After)
					}
				case "bind_delegation_qa":
					var in struct {
						Owner string `json:"owner_delegation_id"`
						QA    string `json:"qa_delegation_id"`
					}
					if err = decoder.Decode(&in); err == nil {
						result, err = provider.BindDelegationQA(s, requestID, call.CallID, in.Owner, in.QA)
					}
				case "record_delegation_review":
					var in struct {
						Owner string `json:"owner_round_id"`
						QA    string `json:"qa_round_id"`
					}
					if err = decoder.Decode(&in); err == nil {
						result, err = provider.RecordDelegationReview(s, requestID, call.CallID, in.Owner, in.QA)
					}
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

func (c *Codex) delegationContextPinned(s *session.Session) (bool, error) {
	if provider, ok := c.delegationProvider.(backend.DelegationContextProvider); ok {
		return provider.PreserveDelegationContext(s)
	}
	return false, nil
}
