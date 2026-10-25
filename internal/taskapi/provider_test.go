package taskapi

import (
	"os"
	"path/filepath"
	"testing"

	taskcontract "everything-go/contracts/task-api/v1"
)

func TestTypedClaudeResumeIDAndCodexNegative(t *testing.T) {
	id := "12345678-1234-1234-1234-123456789abc"
	for _, raw := range []string{`{"type":"system","subtype":"init","session_id":"` + id + `"}`, `{"type":"result","session_id":"` + id + `"}`} {
		c, err := ClaudeConversation([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		target, err := c.Target("instance", "s_fixture12345678", 0)
		if err != nil || target.ResumeID != id || target.ConfigRevision != 0 {
			t.Fatal(target, err)
		}
	}
	for _, raw := range []string{`{"type":"assistant","uuid":"` + id + `"}`, `{"type":"result","session_id":"s_bridge12345678"}`, `{"type":"result","tool_use_id":"` + id + `"}`, `{"type":"system","subtype":"init","session_id":"process-generation"}`, `{"thread":{"id":"` + id + `"}}`} {
		if _, err := ClaudeConversation([]byte(raw)); err == nil {
			t.Fatal("wrong identity accepted", raw)
		}
	}
	if _, err := CodexConversation([]byte(`{"type":"result","session_id":"` + id + `"}`)); err == nil {
		t.Fatal("Claude result used as Codex thread")
	}
	c, err := CodexConversation([]byte(`{"thread":{"id":"` + id + `"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Target("instance", "s_fixture12345678", 0); err != nil {
		t.Fatal(err)
	}
}
func TestProviderVersionTagsUnloaded(t *testing.T) {
	contract, _ := taskcontract.New()
	for _, tag := range [][2]string{{"codex", "0.160.0"}, {"claude", "2.1.280"}, {"claude", "2.1.291"}, {"claude_background", "2.1.280"}} {
		c := UnloadedCapability(tag[0], tag[1], "interface only; no actual registration/load/invocation proof")
		raw, _ := c.JSON()
		if err := contract.Validate(raw, "ProviderCapability"); err != nil {
			t.Fatal(err)
		}
		if c.Version != tag[1] || len(c.Operations) != 0 || c.Lifecycle != "unsupported" {
			t.Fatal(c)
		}
	}
}
func TestScopeSubsetAndEnforcementBeforeSpawn(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	child := filepath.Join(root, "child")
	os.Mkdir(child, 0700)
	escape := filepath.Join(child, "escape")
	if err := os.Symlink(outside, escape); err != nil {
		t.Fatal(err)
	}
	parent := ChildScope{[]string{root}, []string{"read", "test"}, "read-only", "deny", 0}
	valid := ChildScope{[]string{child}, []string{"read"}, "read-only", "deny", 0}
	enforced := ScopeEnforcement{"gateway_only_tools", true, true, true, true}
	if err := ValidateChildScope(parent, valid, enforced); err != nil {
		t.Fatal(err)
	}
	if err := ValidateChildScope(parent, valid, ScopeEnforcement{}); err == nil {
		t.Fatal("prompt-only scope accepted")
	}
	for _, s := range []ChildScope{{[]string{escape}, []string{"read"}, "read-only", "deny", 0}, {[]string{child}, []string{"write"}, "workspace-write", "deny", 0}, {[]string{child}, []string{"read"}, "read-only", "inherit_authorized", 0}, {[]string{child}, []string{"read"}, "read-only", "deny", 1}} {
		if err := ValidateChildScope(parent, s, enforced); err == nil {
			t.Fatal("scope escalation", s)
		}
	}
}
