package core

import (
	"bytes"
	"context"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type voiceExec struct {
	*fakeExec
	starts, stops, cancels atomic.Int32
	threadID               string
	selectedVoice          string
	startInstructions      string
	callback               func(backend.RealtimeVoiceEvent)
}

func (f *voiceExec) StartRealtimeVoice(_ context.Context, s *session.Session, input backend.RealtimeVoiceStart) (backend.RealtimeVoiceAnswer, error) {
	f.starts.Add(1)
	f.threadID = s.ResumeID()
	f.selectedVoice = input.VoiceName
	f.startInstructions = input.SpeakerInstructions
	f.callback = input.OnEvent
	return backend.RealtimeVoiceAnswer{ThreadID: s.ResumeID(), VoiceID: input.VoiceID, SDP: "v=0\r\nprivate-answer"}, nil
}
func (f *voiceExec) StopRealtimeVoice(context.Context, *session.Session, string) error {
	f.stops.Add(1)
	if f.callback != nil {
		f.callback(backend.RealtimeVoiceEvent{State: "closed"})
	}
	return nil
}
func (f *voiceExec) AppendRealtimeVoiceText(context.Context, *session.Session, string, string) error {
	return nil
}
func (f *voiceExec) Stop(context.Context, *session.Session) error { f.cancels.Add(1); return nil }
func nextVoiceState(t *testing.T, c *Client, state string) map[string]any {
	t.Helper()
	for i := 0; i < 8; i++ {
		event := waitForType(t, c, "codex_voice_event")
		if event["state"] == state {
			return event
		}
	}
	t.Fatal("expected voice state absent")
	return nil
}

func TestVoiceUsesRegistryNativeThreadAndOnlyOwningClientCanStop(t *testing.T) {
	h, base := newControlTestHub(t, t.TempDir())
	f := &voiceExec{fakeExec: base}
	h.SetExecutor(f)
	h.registry.Create("one", "one", t.TempDir(), backend.Codex, "", "", "native-one")
	h.registry.Create("two", "two", t.TempDir(), backend.Codex, "", "", "native-two")
	c := newTestClient(h)
	other := newTestClient(h)
	route(h, c, `{"type":"codex_voice_start","session_id":"one","request_id":"request-one","voice_id":"voice-one","thread_id":"native-two","sdp":"v=0\r\n"}`)
	nextVoiceState(t, c, "error")
	if f.starts.Load() != 0 {
		t.Fatal("client-selected thread reached executor")
	}
	route(h, c, `{"type":"codex_voice_start","session_id":"one","request_id":"request-two","voice_id":"voice-one","thread_id":"native-one","sdp":"v=0\r\n"}`)
	answer := nextVoiceState(t, c, "answer")
	if answer["thread_id"] != "native-one" || f.threadID != "native-one" {
		t.Fatal("wrong native binding")
	}
	route(h, other, `{"type":"codex_voice_stop","session_id":"one","request_id":"request-other","voice_id":"voice-one"}`)
	nextVoiceState(t, other, "closed")
	if f.stops.Load() != 0 {
		t.Fatal("other client stopped owner voice")
	}
	route(h, c, `{"type":"codex_voice_stop","session_id":"two","request_id":"request-wrong","voice_id":"voice-one"}`)
	nextVoiceState(t, c, "error")
	if f.stops.Load() != 0 {
		t.Fatal("wrong session stopped owner voice")
	}
	route(h, c, `{"type":"codex_voice_stop","session_id":"one","request_id":"request-stop","voice_id":"voice-one"}`)
	nextVoiceState(t, c, "closed")
	nextVoiceState(t, c, "closed") // Executor callback and gateway closure are distinct events.
	if f.stops.Load() != 1 || f.cancels.Load() != 0 {
		t.Fatal("voice stop cancelled development or repeated")
	}
}
func TestDisconnectStopsOnlyOwnedVoice(t *testing.T) {
	h, base := newControlTestHub(t, t.TempDir())
	f := &voiceExec{fakeExec: base}
	h.SetExecutor(f)
	h.registry.Create("one", "one", t.TempDir(), backend.Codex, "", "", "native-one")
	c := newTestClient(h)
	route(h, c, `{"type":"codex_voice_start","session_id":"one","request_id":"request-one","voice_id":"voice-one","sdp":"v=0\r\n"}`)
	nextVoiceState(t, c, "answer")
	h.cleanupClientVoice(c)
	nextVoiceState(t, c, "closed") // Executor callback.
	nextVoiceState(t, c, "closed") // Join the gateway stop goroutine before the following frame-log test.
	deadline := time.Now().Add(time.Second)
	for f.stops.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if f.stops.Load() != 1 || f.cancels.Load() != 0 {
		t.Fatal("disconnect cleanup touched development")
	}
}
func TestVoiceSDPAndTranscriptNeverAppearInFrameLog(t *testing.T) {
	oldFlag, oldWriter := frameLog, log.Writer()
	frameLog = true
	var buffer bytes.Buffer
	log.SetOutput(&buffer)
	defer func() { frameLog = oldFlag; log.SetOutput(oldWriter) }()
	logOutbound(protocol.RealtimeVoiceEvent{Type: "codex_voice_event", SessionID: "one", State: "answer", SDP: "PRIVATE-ICE-SECRET", Text: "PRIVATE-TRANSCRIPT"})
	if bytes.Contains(buffer.Bytes(), []byte("PRIVATE")) {
		t.Fatal("voice signaling or transcript leaked into frame log")
	}
}

func TestVoiceToneIsDecodedAndValidatedBeforeStarting(t *testing.T) {
	h, base := newControlTestHub(t, t.TempDir())
	f := &voiceExec{fakeExec: base}
	h.SetExecutor(f)
	h.registry.Create("one", "one", t.TempDir(), backend.Codex, "", "", "native-one")
	c := newTestClient(h)
	route(h, c, `{"type":"codex_voice_start","session_id":"one","request_id":"request-bad","voice_id":"voice-bad","voice_name":"marin","sdp":"v=0"}`)
	nextVoiceState(t, c, "error")
	if f.starts.Load() != 0 {
		t.Fatal("unsupported tone started")
	}
	route(h, c, `{"type":"codex_voice_start","session_id":"one","request_id":"request-good","voice_id":"voice-good","voice_name":"maple","sdp":"v=0"}`)
	nextVoiceState(t, c, "answer")
	if f.selectedVoice != "maple" {
		t.Fatal(f.selectedVoice)
	}
	h.cleanupClientVoice(c)
}
