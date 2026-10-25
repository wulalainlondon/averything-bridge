package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/linecontrol"
	"everything-go/internal/session"
	"os"
	"path/filepath"
	"testing"
)

func lineOriginFixture(t *testing.T) (*Codex, *session.Session) {
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
func lineCommandEvent(t *testing.T, c *Codex, s *session.Session, command, source, tool, thread string) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"threadId": thread, "turnId": "original-turn", "item": map[string]any{"id": tool, "type": "commandExecution", "source": source, "commandActions": []map[string]string{{"type": "unknown", "command": command}}}})
	c.observeLineNative(s, "item/started", b)
}
func TestLineCLIOriginRequiresExactNativeCommandPayloadAndCurrentCaller(t *testing.T) {
	c, s := lineOriginFixture(t)
	home, _ := os.UserHomeDir()
	command := filepath.Join(home, ".local/bin/bridge-line") + " --id lop_original_readonly --method accounts --params '{}'"
	q, e := linecontrol.ParseInvocation([]string{"--id", "lop_original_readonly", "--method", "accounts", "--params", "{}"})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.ResolveLineCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("environment without native proof accepted")
	}
	lineCommandEvent(t, c, s, command, "unifiedExecStartup", "native-exec-original", s.ResumeID())
	caller, e := c.ResolveLineCaller(s.ResumeID(), q.ID, q.Fingerprint())
	if e != nil || caller.RequestID != "original-request" || caller.ToolCallID != "native-exec-original" {
		t.Fatal(caller, e)
	}
	if _, e = c.ResolveLineCaller("other-thread", q.ID, q.Fingerprint()); e == nil {
		t.Fatal("foreign thread accepted")
	}
	if _, e = c.ResolveLineCaller(s.ResumeID(), q.ID, "changed"); e == nil {
		t.Fatal("payload substitution accepted")
	}
	c.state(s.ID).currentTurnID = "new-turn"
	if _, e = c.ResolveLineCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("retired caller accepted")
	}
}
func TestLineCLIRejectsQuotedMentionsShellChainsAndDelegatedCaller(t *testing.T) {
	home, _ := os.UserHomeDir()
	base := filepath.Join(home, ".local/bin/bridge-line") + " --id lop_rejected_intent --method accounts"
	for _, command := range []string{"echo '" + base + "'", base + "; true", "/tmp/bridge-line --id lop_rejected_intent --method accounts", base + " | cat"} {
		c, s := lineOriginFixture(t)
		lineCommandEvent(t, c, s, command, "agent", "actual-call", s.ResumeID())
		q, _ := linecontrol.ParseInvocation([]string{"--id", "lop_rejected_intent", "--method", "accounts"})
		if _, e := c.ResolveLineCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
			t.Fatal("nonliteral invocation accepted")
		}
	}
	c, s := lineOriginFixture(t)
	s.ID = "s_dg_child"
	lineCommandEvent(t, c, s, base, "agent", "actual-call", s.ResumeID())
	q, _ := linecontrol.ParseInvocation([]string{"--id", "lop_rejected_intent", "--method", "accounts"})
	if _, e := c.ResolveLineCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("delegation acquired LINE")
	}
}

func TestLINEVoiceOriginUsesOriginalGrantAndExpiresWithCall(t *testing.T) {
	c, s := lineOriginFixture(t)
	st := c.state(s.ID)
	st.turnActive = false
	st.observedTurnID = "original-turn"
	st.observedRequestID = "voice-request"
	c.voiceCalls = map[string]*codexVoiceCall{s.ResumeID(): {voiceID: "voice-exact", sessionID: s.ID, threadID: s.ResumeID()}}
	home, _ := os.UserHomeDir()
	command := filepath.Join(home, ".local/bin/bridge-line") + " --id lop_voice_accounts --method accounts --params '{}'"
	q, e := linecontrol.ParseInvocation([]string{"--id", "lop_voice_accounts", "--method", "accounts", "--params", "{}"})
	if e != nil {
		t.Fatal(e)
	}
	lineCommandEvent(t, c, s, command, "unifiedExecStartup", "voice-tool", s.ResumeID())
	caller, e := c.ResolveLineCaller(s.ResumeID(), q.ID, q.Fingerprint())
	if e != nil || caller.Parent != s || caller.VoiceID != "voice-exact" {
		t.Fatal(caller, e)
	}
	c.voiceCalls[s.ResumeID()].voiceID = "new-voice"
	if _, e = c.ResolveLineCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("old voice operation reused on new call")
	}
	delete(c.voiceCalls, s.ResumeID())
	if _, e = c.ResolveLineCaller(s.ResumeID(), q.ID, q.Fingerprint()); e == nil {
		t.Fatal("ended voice remained authorized")
	}
}
