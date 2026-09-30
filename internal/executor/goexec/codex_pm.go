package goexec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func (c *Codex) SetPMProvider(p backend.PMProvider) { c.pmProvider = p }

func (c *Codex) applyPMTurnPolicy(sessionID string, params map[string]any) error {
	if c.pmProvider == nil {
		if strings.HasPrefix(sessionID, "pm_") {
			return errors.New("pm_provider_missing")
		}
		return nil
	}
	p, err := c.pmProvider.PMConfiguration(sessionID)
	if err != nil || p == nil {
		return err
	}
	if p.Worker {
		if p.Sandbox != "read-only" && p.Sandbox != "workspace-write" {
			return errors.New("collaboration_worker_sandbox_unsupported")
		}
		params["sandboxPolicy"] = codexTurnSandboxPolicy(session.Snapshot{Cwd: p.RuntimeDir, Sandbox: p.Sandbox}, "")
		params["approvalPolicy"] = "never"
		delete(params, "collaborationMode")
		return nil
	}
	params["sandboxPolicy"] = map[string]any{"type": "readOnly"}
	params["approvalPolicy"] = "never"
	params["cwd"] = p.RuntimeDir
	params["model"] = "gpt-5.6-sol"
	delete(params, "collaborationMode")
	return nil
}

// ConnectExistingDaemon attaches without starting, restarting, upgrading or
// reconfiguring Codex. Useful for bounded live verification and shared hosts.
func (c *Codex) ConnectExistingDaemon() error {
	c.startMu.Lock()
	defer c.startMu.Unlock()
	if c.serverRunningLocked() {
		return nil
	}
	c.appServerMode = "daemon"
	c.remoteReconnect = false
	return c.startRemoteServerLocked(filepath.Dir(c.sessionsRoot))
}

// Each PM thread gets its own overrides. Never mutate the shared daemon's
// configuration: regular worker conversations retain their normal tools.
func codexPMConfig(mcpNames []string) map[string]any {
	features := map[string]any{}
	for _, name := range []string{"shell_tool", "unified_exec", "multi_agent", "multi_agent_v2", "hooks", "plugins", "remote_plugin", "apps", "browser_use", "browser_use_external", "browser_use_full_cdp_access", "computer_use", "in_app_browser", "in_app_local_automation", "image_generation", "goals", "memories", "context_management", "skill_search", "skill_mcp_dependency_install", "sleep_tool", "tool_suggest", "workspace_dependencies", "view_image"} {
		features[name] = false
	}
	// Exclusion removes BOTH code-mode declarations and executor bindings,
	// including apply_patch, which has no supported standalone disable flag.
	features["code_mode"] = map[string]any{"enabled": true, "excluded_tool_namespaces": []string{"functions"}}
	features["code_mode_host"] = true
	mcp := map[string]any{}
	for _, name := range mcpNames {
		mcp[name] = map[string]any{"enabled": false}
	}
	return map[string]any{"features": features, "agents": map[string]any{"enabled": false}, "mcp_servers": mcp, "web_search": "disabled", "tools": map[string]any{"view_image": false}, "project_doc_max_bytes": 0}
}

func (c *Codex) applyPMThreadPolicy(s *session.Session, params map[string]any) error {
	if c.pmProvider == nil {
		if strings.HasPrefix(s.ID, "pm_") {
			return errors.New("pm_provider_missing")
		}
		return nil
	}
	p, err := c.pmProvider.PMConfiguration(s.ID)
	if err != nil || p == nil {
		return err
	}
	if err := os.MkdirAll(p.RuntimeDir, 0700); err != nil {
		return err
	}
	raw, err := c.rpcCall("config/read", map[string]any{"includeLayers": false}, 15*time.Second)
	if err != nil {
		return fmt.Errorf("pm_config_inspection: %w", err)
	}
	var loaded struct {
		Config struct {
			MCP map[string]json.RawMessage `json:"mcp_servers"`
		} `json:"config"`
	}
	if err = json.Unmarshal(raw, &loaded); err != nil {
		return err
	}
	names := []string{}
	for name := range loaded.Config.MCP {
		names = append(names, name)
	}
	params["config"] = codexPMConfig(names)
	params["cwd"] = p.RuntimeDir
	params["model"] = "gpt-5.6-sol"
	params["sandbox"] = "read-only"
	params["approvalPolicy"] = "never"
	params["baseInstructions"] = p.Instructions
	params["developerInstructions"] = p.Instructions
	namespace, description := "bridge_pm", "Project-scoped PM coordination. No implementation or human approval capability."
	if p.Worker {
		if p.Sandbox != "read-only" && p.Sandbox != "workspace-write" {
			return errors.New("collaboration_worker_sandbox_unsupported")
		}
		config := codexPMConfig(names)
		features := config["features"].(map[string]any)
		features["shell_tool"], features["unified_exec"], features["view_image"] = true, true, true
		features["code_mode"] = map[string]any{"enabled": true}
		config["project_doc_max_bytes"] = 32000
		config["tools"] = map[string]any{"view_image": true}
		params["config"] = config
		params["sandbox"] = p.Sandbox
		params["model"] = s.Snapshot().Model
		if params["model"] == "" {
			params["model"] = c.defaultModel()
		}
		delete(params, "baseInstructions")
		namespace, description = "bridge_work", "Run-scoped task context, reports, questions and immutable delivery. No human approval capability."
	}
	dynamic := []map[string]any{}
	for _, spec := range p.Tools {
		dynamic = append(dynamic, map[string]any{"type": "function", "name": spec["name"], "description": spec["description"], "inputSchema": spec["inputSchema"]})
	}
	params["dynamicTools"] = []map[string]any{{"type": "namespace", "name": namespace, "description": description, "tools": dynamic}}
	return nil
}

func (c *Codex) handlePMServerRequest(id any, method string, raw json.RawMessage) bool {
	s := c.sessionForCodexParams(codexParams(raw))
	if s == nil {
		return false
	}
	if c.pmProvider == nil {
		return false
	}
	p, err := c.pmProvider.PMConfiguration(s.ID)
	if p == nil && err == nil {
		return false
	}
	reply := func(result any) { _ = c.rpc.write(map[string]any{"id": id, "result": result}) }
	switch method {
	case "item/permissions/requestApproval":
		reply(map[string]any{"permissions": map[string]any{}, "scope": "turn"})
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval", "applyPatchApproval", "execCommandApproval":
		reply(map[string]any{"decision": "decline"})
	case "mcpServer/elicitation/request":
		reply(map[string]any{"action": "cancel", "content": nil})
	case "item/tool/requestUserInput":
		reply(map[string]any{"answers": map[string]any{}})
	case "item/tool/call":
		go func() {
			var call struct {
				Namespace string          `json:"namespace"`
				Tool      string          `json:"tool"`
				Arguments json.RawMessage `json:"arguments"`
				CallID    string          `json:"callId"`
				TurnID    string          `json:"turnId"`
			}
			callErr := json.Unmarshal(raw, &call)
			allowed := false
			namespace := "bridge_pm"
			if p != nil && p.Worker {
				namespace = "bridge_work"
			}
			if p != nil && err == nil && call.Namespace == namespace {
				for _, spec := range p.Tools {
					if spec["name"] == call.Tool {
						allowed = true
					}
				}
			}
			if p != nil && p.Worker {
				st := c.state(s.ID)
				st.mu.Lock()
				reqID, turnID := st.reqID, st.currentTurnID
				st.mu.Unlock()
				if p.RequestID == "" || p.RequestID != reqID || call.TurnID == "" || (turnID != "" && call.TurnID != turnID) {
					allowed = false
				}
			}
			var result any
			if !allowed || callErr != nil {
				callErr = errors.New("pm_tool_forbidden")
			} else {
				result, callErr = callPMMCP(p, call.Tool, call.Arguments)
			}
			output := ""
			if callErr != nil {
				output = callErr.Error()
			} else {
				b, _ := json.Marshal(result)
				output = string(b)
			}
			st := c.state(s.ID)
			st.mu.Lock()
			reqID := st.reqID
			st.mu.Unlock()
			c.tools.Start(s.ID, reqID, call.CallID, namespace+"__"+call.Tool, string(call.Arguments))
			c.tools.Result(s.ID, reqID, call.CallID, output)
			c.tools.End(s.ID, reqID, call.CallID)
			reply(map[string]any{"success": callErr == nil, "contentItems": []map[string]any{{"type": "inputText", "text": output}}})
		}()
	default:
		return false
	}
	return true
}

func callPMMCP(p *backend.PMConfiguration, name string, args json.RawMessage) (any, error) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": name, "arguments": args}})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, p.MCPURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+p.Token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(req)
	if err != nil {
		return nil, errors.New("pm_control_unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pm_control_http_%d", response.StatusCode)
	}
	var envelope struct {
		Result any `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 1024*1024)).Decode(&envelope); err != nil {
		return nil, err
	}
	if envelope.Error != nil {
		return nil, errors.New(envelope.Error.Message)
	}
	return envelope.Result, nil
}
