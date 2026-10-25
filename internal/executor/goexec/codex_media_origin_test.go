package goexec

import (
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"testing"
)

func mediaNativeItem(c *Codex, s *session.Session, turn string, item any) {
	b, _ := json.Marshal(map[string]any{"threadId": s.ResumeID(), "turnId": turn, "item": item})
	c.observeMediaNative(s, "item/started", b)
}
func mediaHandoff(c *Codex, s *session.Session, voice, generation, input, id string) {
	b, _ := json.Marshal(map[string]any{"item": map[string]any{"type": "handoff_request", "handoff_id": id, "input_transcript": input}})
	call := &codexVoiceCall{sessionID: s.ID, threadID: s.ResumeID(), voiceID: voice, mediaGeneration: generation, closed: make(chan struct{}), failure: make(chan error, 1)}
	c.voiceMu.Lock()
	if c.voiceCalls == nil {
		c.voiceCalls = map[string]*codexVoiceCall{}
	}
	c.voiceCalls[s.ResumeID()] = call
	c.voiceMu.Unlock()
	c.observeMediaHandoff(call, b)
}
func mediaUser(c *Codex, s *session.Session, turn, input string) {
	mediaNativeItem(c, s, turn, map[string]any{"type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "<realtime_delegation><input>" + input + "</input></realtime_delegation>"}}})
}
func mediaCommand(c *Codex, s *session.Session, turn, id string) {
	c.mediaOrigins.launcher = "/fixture/bridge-media"
	cmd := "/fixture/bridge-media --id " + id + " --method status"
	mediaNativeItem(c, s, turn, map[string]any{"type": "commandExecution", "source": "agent", "command": cmd, "commandActions": []any{map[string]string{"type": "unknown", "command": cmd}}})
}
func TestMediaOriginRequiresProviderHandoffAndActualCommand(t *testing.T) {
	c := NewCodex(&caplessSink{}, "codex")
	s := session.NewRegistry().Create("f", "f", t.TempDir(), backend.Codex, "", "", "native")
	mediaUser(c, s, "turn-1", "play")
	mediaCommand(c, s, "turn-1", "vop_without_handoff")
	if _, e := c.ResolveMediaOrigin("native", "vop_without_handoff"); e == nil {
		t.Fatal("text became authority")
	}
	mediaHandoff(c, s, "voice-A", "generation-A", "play", "h1")
	mediaUser(c, s, "turn-A", "play")
	if _, e := c.ResolveMediaOrigin("native", "vop_not_executed"); e == nil {
		t.Fatal("unexecuted command admitted")
	}
	mediaCommand(c, s, "turn-A", "vop_actual_command")
	o, e := c.ResolveMediaOrigin("native", "vop_actual_command")
	if e != nil || o.VoiceID != "voice-A" || o.Generation != "generation-A" || o.TurnID != "turn-A" {
		t.Fatal(o, e)
	}
	if _, e = c.ResolveMediaOrigin("foreign", "vop_actual_command"); e == nil {
		t.Fatal("foreign thread admitted")
	}
}
func TestMediaOriginOldTurnCannotRebindToNewVoice(t *testing.T) {
	c := NewCodex(&caplessSink{}, "codex")
	s := session.NewRegistry().Create("f", "f", t.TempDir(), backend.Codex, "", "", "native")
	mediaHandoff(c, s, "voice-A", "generation-A", "old", "h-A")
	mediaUser(c, s, "turn-A", "old")
	mediaHandoff(c, s, "voice-B", "generation-B", "new", "h-B")
	mediaUser(c, s, "turn-B", "new")
	// The delayed command still carries the original A origin, even after B exists.
	mediaCommand(c, s, "turn-A", "vop_delayed_old_turn")
	o, e := c.ResolveMediaOrigin("native", "vop_delayed_old_turn")
	if e != nil || o.VoiceID != "voice-A" {
		t.Fatal(o, e)
	}
	mediaCommand(c, s, "turn-B", "vop_fresh_new_turn")
	o, e = c.ResolveMediaOrigin("native", "vop_fresh_new_turn")
	if e != nil || o.VoiceID != "voice-B" {
		t.Fatal(o, e)
	}
	// A duplicate input in an unrelated new turn cannot consume A again.
	mediaUser(c, s, "late-turn-A", "old")
	mediaCommand(c, s, "late-turn-A", "vop_delayed_start")
	o, e = c.ResolveMediaOrigin("native", "vop_delayed_start")
	if e == nil {
		t.Fatal("reused consumed handoff", o)
	}
}
func TestMediaOriginRepeatedSpeechIsAmbiguous(t *testing.T) {
	c := NewCodex(&caplessSink{}, "codex")
	s := session.NewRegistry().Create("f", "f", t.TempDir(), backend.Codex, "", "", "native")
	mediaHandoff(c, s, "A", "gen-A", "same", "hA")
	mediaHandoff(c, s, "B", "gen-B", "same", "hB")
	mediaUser(c, s, "turn", "same")
	mediaCommand(c, s, "turn", "vop_ambiguous_speech")
	if _, e := c.ResolveMediaOrigin("native", "vop_ambiguous_speech"); e == nil {
		t.Fatal("selected newest voice")
	}
}

func TestMediaOriginRawHandoffCanArriveAfterNativeExecutionStarts(t *testing.T) {
	c := NewCodex(&caplessSink{}, "codex")
	s := session.NewRegistry().Create("f", "f", t.TempDir(), backend.Codex, "", "", "native")
	call := &codexVoiceCall{sessionID: s.ID, threadID: s.ResumeID(), voiceID: "A", mediaGeneration: "gen-A"}
	c.voiceMu.Lock()
	c.voiceCalls = map[string]*codexVoiceCall{s.ResumeID(): call}
	c.voiceMu.Unlock()
	mediaUser(c, s, "turn", "late")
	mediaCommand(c, s, "turn", "vop_wait_for_handoff")
	if _, e := c.ResolveMediaOrigin("native", "vop_wait_for_handoff"); e == nil {
		t.Fatal("admitted before source")
	}
	mediaHandoff(c, s, "A", "gen-A", "late", "handoff-late")
	o, e := c.ResolveMediaOrigin("native", "vop_wait_for_handoff")
	if e != nil || o.Generation != "gen-A" {
		t.Fatal(o, e)
	}
}
func TestMediaOriginSameSongOnNextCallDoesNotReuseConsumedHandoff(t *testing.T) {
	c := NewCodex(&caplessSink{}, "codex")
	s := session.NewRegistry().Create("f", "f", t.TempDir(), backend.Codex, "", "", "native")
	mediaHandoff(c, s, "A", "gen-A", "same", "hA")
	mediaUser(c, s, "turn-A", "same")
	mediaCommand(c, s, "turn-A", "vop_call_A_song")
	mediaHandoff(c, s, "B", "gen-B", "same", "hB")
	mediaUser(c, s, "turn-B", "same")
	mediaCommand(c, s, "turn-B", "vop_call_B_song")
	o, e := c.ResolveMediaOrigin("native", "vop_call_B_song")
	if e != nil || o.Generation != "gen-B" {
		t.Fatal(o, e)
	}
	o, e = c.ResolveMediaOrigin("native", "vop_call_A_song")
	if e != nil || o.Generation != "gen-A" {
		t.Fatal(o, e)
	}
}
func TestSimpleMediaCommandRejectsShellPrograms(t *testing.T) {
	for _, s := range []string{"echo /fixture/bridge-media; /fixture/bridge-media", "/fixture/bridge-media $(whoami)", "/fixture/bridge-media | cat", "/fixture/bridge-media > file"} {
		if _, ok := simpleMediaCommand(s); ok {
			t.Fatal(s)
		}
	}
	args, ok := simpleMediaCommand("'/fixture/path with spaces/bridge-media' --id vop_valid_intent --title 'a & b'")
	if !ok || len(args) != 5 || args[0] != "/fixture/path with spaces/bridge-media" || args[4] != "a & b" {
		t.Fatal(args, ok)
	}
}
