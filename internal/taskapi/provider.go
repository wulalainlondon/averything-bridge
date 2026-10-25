package taskapi

import (
	"encoding/json"
	"errors"
	"regexp"

	taskcontract "everything-go/contracts/task-api/v1"
)

type Conversation struct {
	Backend      string
	ResumeID     string
	EvidenceKind string
}
type TargetIdentity struct {
	InstanceID     string `json:"instance_id"`
	SessionID      string `json:"session_id"`
	ResumeID       string `json:"expected_native_thread_id"`
	ConfigRevision uint64 `json:"expected_config_revision"`
}

var nativeUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// ClaudeConversation maps only the published init/result session_id. A native
// message UUID, process generation or tool_use ID is never a ResumeID.
func ClaudeConversation(raw []byte) (Conversation, error) {
	value, err := taskcontract.Parse(raw)
	if err != nil {
		return Conversation{}, err
	}
	event, ok := value.(map[string]any)
	if !ok {
		return Conversation{}, errors.New("invalid provider event")
	}
	kind, _ := event["type"].(string)
	subtype, _ := event["subtype"].(string)
	if kind != "result" && (kind != "system" || subtype != "init") {
		return Conversation{}, Failure("unsupported", "known_none", "read_capabilities")
	}
	id, _ := event["session_id"].(string)
	if !nativeUUID.MatchString(id) {
		return Conversation{}, Failure("unsupported", "known_none", "read_capabilities")
	}
	return Conversation{"claude", id, "claude_" + kind}, nil
}

// CodexConversation maps the public thread/start/resume response thread.id.
func CodexConversation(raw []byte) (Conversation, error) {
	value, err := taskcontract.Parse(raw)
	if err != nil {
		return Conversation{}, err
	}
	event, ok := value.(map[string]any)
	if !ok {
		return Conversation{}, errors.New("invalid provider reply")
	}
	thread, ok := event["thread"].(map[string]any)
	if !ok {
		return Conversation{}, Failure("unsupported", "known_none", "read_capabilities")
	}
	id, _ := thread["id"].(string)
	if !nativeUUID.MatchString(id) {
		return Conversation{}, Failure("unsupported", "known_none", "read_capabilities")
	}
	return Conversation{"codex", id, "codex_thread_reply"}, nil
}
func (c Conversation) Target(instance, session string, revision uint64) (TargetIdentity, error) {
	if instance == "" || session == "" || !nativeUUID.MatchString(c.ResumeID) || !((c.Backend == "codex" && c.EvidenceKind == "codex_thread_reply") || (c.Backend == "claude" && (c.EvidenceKind == "claude_result" || c.EvidenceKind == "claude_system"))) {
		return TargetIdentity{}, Failure("unsupported", "known_none", "read_capabilities")
	}
	return TargetIdentity{instance, session, c.ResumeID, revision}, nil
}

type ScopeEnforcement struct {
	Mode       string `json:"mode"`
	Roots      bool   `json:"filesystem_roots"`
	Tools      bool   `json:"tool_allowlist"`
	Network    bool   `json:"task_network_policy"`
	Delegation bool   `json:"delegation_limit"`
}
type ProviderCapability struct {
	Backend        string           `json:"backend"`
	Version        string           `json:"provider_version"`
	Binding        string           `json:"binding_kind"`
	Lifecycle      string           `json:"lifecycle"`
	Operations     []string         `json:"operations"`
	NativeEvidence string           `json:"native_evidence"`
	Models         []any            `json:"models"`
	Account        string           `json:"account_ready"`
	Quota          string           `json:"quota"`
	Voice          string           `json:"voice"`
	Background     string           `json:"background"`
	Interrupt      string           `json:"interrupt"`
	Enforcement    ScopeEnforcement `json:"scope_enforcement"`
	Reason         string           `json:"reason"`
}

// Interface builds do not prove registered/loaded/invoked tools or account access.
func UnloadedCapability(backend, version, reason string) ProviderCapability {
	return ProviderCapability{backend, version, "unsupported", "unsupported", []string{}, "unsupported", []any{}, "unknown", "unknown", "unknown", "unsupported", "unsupported", ScopeEnforcement{Mode: "unsupported"}, reason}
}
func (c ProviderCapability) JSON() ([]byte, error) { return json.Marshal(c) }
