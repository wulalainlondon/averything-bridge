package goexec

import (
	"encoding/json"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

// --allowedTools is only an approval allowlist, NOT a tool removal mechanism.
// --tools "" removes every built-in, including Bash/Edit/Agent. The only MCP
// server comes from trusted Bridge config; inherited hooks/skills are disabled.
func claudePMSpawnArgs(s session.Snapshot, p backend.PMConfiguration) []string {
	cfg, _ := json.Marshal(map[string]any{"mcpServers": map[string]any{"bridge_pm": map[string]any{"type": "http", "url": p.MCPURL, "headers": map[string]string{"Authorization": "Bearer ${BRIDGE_PM_SESSION_TOKEN}"}}}})
	args := []string{"--print", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--include-partial-messages",
		"--tools", "", "--strict-mcp-config", "--mcp-config", string(cfg), "--disable-slash-commands",
		"--setting-sources", "", "--settings", `{"disableAllHooks":true,"autoMemoryEnabled":false,"enabledPlugins":{}}`,
		"--permission-mode", "dontAsk", "--allowedTools", "mcp__bridge_pm__project_get_context,mcp__bridge_pm__project_propose_task,mcp__bridge_pm__project_delegate_task,mcp__bridge_pm__session_read_updates",
		"--system-prompt", p.Instructions}
	args = append(args, "--max-turns", "12")
	if p.Room {
		for i := 0; i+1 < len(args); i++ {
			if args[i] == "--tools" {
				args[i+1] = "WebSearch,WebFetch"
			}
			if args[i] == "--allowedTools" {
				args[i+1] = "WebSearch,WebFetch,mcp__bridge_pm__room_get_context,mcp__bridge_pm__room_read_source"
			}
		}
	}
	if s.Model != "" {
		args = append(args, "--model", s.Model)
	}
	if s.ResumeID != "" {
		args = append(args, "--resume", s.ResumeID)
	}
	if s.Effort != "" && s.Effort != "auto" {
		args = append(args, "--effort", s.Effort)
	}
	return args
}

func validPMTools(tools []string) bool {
	for _, tool := range tools {
		switch strings.TrimSpace(tool) {
		case "mcp__bridge_pm__project_get_context", "mcp__bridge_pm__project_propose_task", "mcp__bridge_pm__project_delegate_task", "mcp__bridge_pm__session_read_updates", "ToolSearch", "EndConversation":
		default:
			return false
		}
	}
	return true
}
func validPMPolicyTools(p *backend.PMConfiguration, tools []string) bool {
	if p == nil || !p.Room {
		return validPMTools(tools)
	}
	for _, tool := range tools {
		switch tool {
		case "WebSearch", "WebFetch", "mcp__bridge_pm__room_get_context", "mcp__bridge_pm__room_read_source", "ToolSearch", "EndConversation":
		default:
			return false
		}
	}
	return true
}

func claudeReadOnlyWorkerArgs(s session.Snapshot, mcpURL string) []string {
	args := claudeSpawnArgs(s, mcpURL)
	// Narrow the ordinary read-only preset rather than sending duplicate --tools
	// options (consumers and CLI versions must see one unambiguous policy).
	narrowed := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		if args[i] == "--tools" && i+1 < len(args) {
			i++
			continue
		}
		narrowed = append(narrowed, args[i])
	}
	args = narrowed
	args = append(args, "--tools", "Read,Glob,Grep", "--strict-mcp-config", "--disable-slash-commands", "--setting-sources", "", "--settings", `{"disableAllHooks":true,"autoMemoryEnabled":false,"enabledPlugins":{}}`)
	return args
}

func validReadOnlyWorkerTools(tools []string) bool {
	for _, tool := range tools {
		switch tool {
		case "Read", "Glob", "Grep", "mcp__ask_user__ask_question", "ToolSearch", "EndConversation":
		default:
			return false
		}
	}
	return true
}
