package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"testing"
)

func TestCallbackSpeakerPromptDoesNotBecomeBackingAgentInstruction(t *testing.T) {
	input := backend.RealtimeVoiceStart{VoiceID: "callback-voice", SDP: "v=0", SpeakerInstructions: "Read the sealed report aloud", MediaInstructions: "Existing media authority"}
	p := realtimeVoiceStartParams("original-thread", input)
	if p["prompt"] != input.SpeakerInstructions || p["includeStartupContext"] != false || p["realtimeStartInstructions"] != input.MediaInstructions || p["threadId"] != "original-thread" || p["version"] != "v3" {
		t.Fatal("speaker and backing instructions mixed", p)
	}
	p = realtimeVoiceStartParams("original-thread", backend.RealtimeVoiceStart{})
	if _, ok := p["prompt"]; ok {
		t.Fatal("ordinary voice prompt overridden")
	}
}

func TestCallbackSpeechUsesOnlyOriginalOwnedRealtimeOutput(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	s := session.NewRegistry().Create("speech-source", "fixture", t.TempDir(), backend.Codex, "", "read-only", "original-thread")
	c.voiceCalls = map[string]*codexVoiceCall{"original-thread": {voiceID: "original-voice", threadID: "original-thread", sessionID: s.ID}}
	w := &toolTestWriter{c: c}
	w.reply = func(method string, raw json.RawMessage) (any, error) {
		var p map[string]any
		_ = json.Unmarshal(raw, &p)
		if method != "thread/realtime/appendSpeech" || p["threadId"] != "original-thread" || p["text"] != "Sealed result" || len(p) != 2 {
			t.Error("speech admitted work or changed source", method, p)
		}
		return map[string]any{}, nil
	}
	c.rpc.setWriter(w)
	if err := c.AppendRealtimeVoiceSpeech(context.Background(), s, "foreign-voice", "Sealed result"); err == nil {
		t.Fatal("foreign voice accepted")
	}
	if len(w.methods) != 0 {
		t.Fatal("foreign voice caused RPC")
	}
	if err := c.AppendRealtimeVoiceSpeech(context.Background(), s, "original-voice", "Sealed result"); err != nil {
		t.Fatal(err)
	}
	if len(w.methods) != 1 {
		t.Fatal("not one speech effect", w.methods)
	}
}

func TestCanonicalRealtimeTranscriptIsBoundToItsVoiceGeneration(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	events := []backend.RealtimeVoiceEvent{}
	c.voiceCalls = map[string]*codexVoiceCall{"original-thread": {voiceID: "current-voice", threadID: "original-thread", callback: func(e backend.RealtimeVoiceEvent) { events = append(events, e) }}}
	for _, voice := range []string{"old-voice", "current-voice"} {
		b, _ := json.Marshal(map[string]any{"threadId": "original-thread", "item": map[string]any{"type": "transcriptSegment", "realtimeSessionId": voice, "role": "assistant", "text": "Exact result"}})
		c.dispatchVoice("thread/realtime/itemCompleted", b)
	}
	if len(events) != 1 || events[0].Text != "Exact result" || events[0].Role != "assistant" || events[0].State != "transcript_done" {
		t.Fatal("late transcript changed current report", events)
	}
}
