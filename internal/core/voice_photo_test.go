package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"sync/atomic"
	"testing"
	"time"
)

type photoExec struct {
	*fakeExec
	images  chan []backend.ImageAttachment
	notices chan string
}

func (f *photoExec) Steer(_ context.Context, s *session.Session, id, content string, images []backend.ImageAttachment, _ []backend.FileAttachment) (backend.SteerResult, error) {
	f.images <- images
	return f.fakeExec.onSteer(s, id, content)
}
func (f *photoExec) StartRealtimeVoice(context.Context, *session.Session, backend.RealtimeVoiceStart) (backend.RealtimeVoiceAnswer, error) {
	return backend.RealtimeVoiceAnswer{}, nil
}
func (f *photoExec) StopRealtimeVoice(context.Context, *session.Session, string) error { return nil }
func (f *photoExec) AppendRealtimeVoiceText(_ context.Context, _ *session.Session, _ string, text string) error {
	f.notices <- text
	return nil
}
func TestVoicePhotoDirectDeliveryUsesOriginalDurableImageExactlyOnce(t *testing.T) {
	h, fe := newTestHub(t)
	c := sharedReadClient(t, h, "photo-owner")
	s := h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("thread-original")
	h.voiceClients = map[*Client]*clientVoiceCall{c: {session: s, voiceID: "voice-original"}}
	f := &photoExec{fakeExec: fe, images: make(chan []backend.ImageAttachment, 4), notices: make(chan string, 4)}
	h.SetExecutor(f)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	fe.onSend = func(s *session.Session, id, _ string) {
		if id == "a" {
			close(started)
			<-release
		}
		h.Emit(protocol.NewDone(s.ID, id))
	}
	fe.onSteer = func(s *session.Session, id, content string) (backend.SteerResult, error) {
		calls.Add(1)
		e, _, _ := h.messageQueue.Get("s1", "photo")
		var saved queuedPayload
		if err := json.Unmarshal(e.Payload, &saved); err != nil || saved.VoiceID != "voice-original" || saved.VoiceThread != "thread-original" || len(saved.Images) != 1 {
			t.Errorf("frozen original voice and image missing: %v", err)
		}
		if id != "photo" || content != "看這張圖" {
			t.Errorf("wrong original content")
		}
		return backend.SteerResult{StrictACK: true, TurnID: "turn", RequestID: "a"}, nil
	}
	enqueueTestMessage(t, h, c, "a")
	<-started
	wire := `{"type":"message","session_id":"s1","request_id":"photo","message_purpose":"instruction","content":"看這張圖","voice_context":true,"voice_id":"voice-original","thread_id":"thread-original","images":[{"media_type":"image/png","data":"aW1hZ2U="}]}`
	route(h, c, wire)
	waitForType(t, c, "message_ack")
	select {
	case images := <-f.images:
		if len(images) != 1 || images[0].Data != "aW1hZ2U=" {
			t.Fatal(images)
		}
	case <-time.After(time.Second):
		t.Fatal("normal Send did not deliver image")
	}
	waitVoicePhotoState(t, h, messagequeue.Steered)

	select {
	case <-f.notices:
	case <-time.After(time.Second):
		t.Fatal("original voice not notified after acceptance")
	}
	route(h, c, wire)
	waitForType(t, c, "message_ack")
	if calls.Load() != 1 {
		t.Fatal("duplicate image delivery")
	}
	close(release)
}
func TestVoicePhotoRejectsForeignOrRetiredCallBeforeAdmission(t *testing.T) {
	h, _ := newTestHub(t)
	c := sharedReadClient(t, h, "photo-owner")
	other := sharedReadClient(t, h, "other-device")
	s := h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("thread-original")
	h.voiceClients = map[*Client]*clientVoiceCall{c: {session: s, voiceID: "voice-original"}}
	if h.voicePhotoOwner(other, "s1", "voice-original", "thread-original") || h.voicePhotoOwner(c, "s1", "voice-original", "foreign-thread") {
		t.Fatal("foreign owner accepted")
	}
	h.voiceClients[c].stopping = true
	route(h, c, `{"type":"message","session_id":"s1","request_id":"expired-photo","content":"x","voice_context":true,"voice_id":"voice-original","thread_id":"thread-original","images":[{"media_type":"image/png","data":"eA=="}]}`)
	waitForType(t, c, "error")
	if _, found, _ := h.messageQueue.Get("s1", "expired-photo"); found {
		t.Fatal("retired voice image admitted")
	}
}
func TestVoicePhotoUnknownAcceptanceNeverReplays(t *testing.T) {
	h, fe := newTestHub(t)
	c := sharedReadClient(t, h, "photo-owner")
	s := h.registry.Create("s1", "test", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("thread-original")
	h.voiceClients = map[*Client]*clientVoiceCall{c: {session: s, voiceID: "voice-original"}}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	fe.onSend = func(s *session.Session, id, _ string) {
		if id == "a" {
			close(started)
			<-release
		}
		h.Emit(protocol.NewDone(s.ID, id))
	}
	fe.onSteer = func(*session.Session, string, string) (backend.SteerResult, error) {
		calls.Add(1)
		return backend.SteerResult{}, errors.New("ack lost")
	}
	enqueueTestMessage(t, h, c, "a")
	<-started
	wire := `{"type":"message","session_id":"s1","request_id":"photo","content":"x","voice_context":true,"voice_id":"voice-original","thread_id":"thread-original","images":[{"media_type":"image/png","data":"eA=="}]}`
	route(h, c, wire)
	waitForType(t, c, "message_ack")
	waitVoicePhotoState(t, h, messagequeue.Uncertain)
	route(h, c, wire)
	waitForType(t, c, "message_ack")
	close(release)
	time.Sleep(20 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("unknown image replayed")
	}
	expectState(t, h, "photo", messagequeue.Uncertain)
}

func waitVoicePhotoState(t *testing.T, h *Hub, want messagequeue.State) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		e, ok, _ := h.messageQueue.Get("s1", "photo")
		if ok && e.State == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	expectState(t, h, "photo", want)
}
func TestVoicePhotoInternalDeliveryDoesNotExposeManualQueueControls(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	h.queueResult(c, clientproto.Command{SessionID: "s1", RequestID: "photo", AutoVoicePhoto: true}, "promote", "rejected", "no longer waiting", messagequeue.Entry{})
	select {
	case <-c.send:
		t.Fatal("internal steering exposed a manual action banner")
	default:
	}
	h.queueResult(c, clientproto.Command{SessionID: "s1", RequestID: "photo"}, "promote", "rejected", "no longer waiting", messagequeue.Entry{})
	waitForType(t, c, "queue_action_result")
}
