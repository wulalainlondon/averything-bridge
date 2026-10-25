package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/voicecontrol"
	"os"
	"path/filepath"
	"testing"
)

func voiceControlOriginFixture(t *testing.T) (*Codex, *session.Session) {
	t.Helper()
	c := NewCodex(&capSink{}, "codex")
	s := session.NewRegistry().Create("line-original", "LINE", t.TempDir(), backend.Codex, "", "read-only", "original-thread")
	st := c.state(s.ID)
	st.threadID = s.ResumeID()
	st.turnActive = true
	st.currentTurnID = "original-turn"
	st.reqID = "original-request"
	return c, s
}
func voiceControlCommandEvent(t *testing.T, c *Codex, s *session.Session, command, source, tool, thread string) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"threadId": thread, "turnId": "original-turn", "item": map[string]any{"id": tool, "type": "commandExecution", "source": source, "commandActions": []map[string]string{{"type": "unknown", "command": command}}}})
	c.observeVoiceControlNative(s, "item/started", b)
}
func TestVoiceControlCLIOriginRequiresExactNativeCommandPayloadAndCurrentCaller(t *testing.T) {
	c, s := voiceControlOriginFixture(t)
	home, _ := os.UserHomeDir()
	command := filepath.Join(home, ".local/bin/bridge-voice") + " --id vtr_original_readonly --method list_sessions --params '{}'"
	q, e := voicecontrol.ParseInvocation([]string{"--id", "vtr_original_readonly", "--method", "list_sessions", "--params", "{}"})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.ResolveVoiceControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("environment without native proof accepted")
	}
	voiceControlCommandEvent(t, c, s, command, "unifiedExecStartup", "native-exec-original", s.ResumeID())
	caller, e := c.ResolveVoiceControlCaller(s.ResumeID(), q.ID, q.Fingerprint())
	if e != nil || caller.RequestID != "original-request" || caller.ToolCallID != "native-exec-original" {
		t.Fatal(caller, e)
	}
	if _, e = c.ResolveVoiceControlCaller("other-thread", q.ID, q.Fingerprint()); e == nil {
		t.Fatal("foreign thread accepted")
	}
	if _, e = c.ResolveVoiceControlCaller(s.ResumeID(), q.ID, "changed"); e == nil {
		t.Fatal("payload substitution accepted")
	}
	c.state(s.ID).currentTurnID = "new-turn"
	if _, e = c.ResolveVoiceControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("retired caller accepted")
	}
}
func TestVoiceControlCLIRejectsQuotedMentionsShellChainsAndDelegatedCaller(t *testing.T) {
	home, _ := os.UserHomeDir()
	base := filepath.Join(home, ".local/bin/bridge-voice") + " --id vtr_rejected_intent --method list_sessions"
	for _, command := range []string{"echo '" + base + "'", base + "; true", "/tmp/bridge-voice --id vtr_rejected_intent --method list_sessions", base + " | cat"} {
		c, s := voiceControlOriginFixture(t)
		voiceControlCommandEvent(t, c, s, command, "agent", "actual-call", s.ResumeID())
		q, _ := voicecontrol.ParseInvocation([]string{"--id", "vtr_rejected_intent", "--method", "list_sessions"})
		if _, e := c.ResolveVoiceControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
			t.Fatal("nonliteral invocation accepted")
		}
	}
	c, s := voiceControlOriginFixture(t)
	s.ID = "s_dg_child"
	voiceControlCommandEvent(t, c, s, base, "agent", "actual-call", s.ResumeID())
	q, _ := voicecontrol.ParseInvocation([]string{"--id", "vtr_rejected_intent", "--method", "list_sessions"})
	if _, e := c.ResolveVoiceControlCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("delegation acquired voice transfer")
	}
}
