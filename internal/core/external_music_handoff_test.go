package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"testing"
)

func TestIOSOfficialMusicSelectsExactNativeProvider(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	for c, call := range h.voiceClients {
		owner = c
		c.clientSurface = "ios"
		call.mediaCapable = true
		call.externalMusicCapable = true
	}
	done := make(chan any, 1)
	input := backend.VoiceMediaRequest{URL: "https://music.youtube.com/watch?v=M7lc1UVf-VE"}
	go func() {
		r, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", input)
		if e != nil {
			done <- e
		} else {
			done <- r
		}
	}()
	event := nextVoiceState(t, owner, "media")
	var command protocol.VoiceMediaCommand
	b, _ := json.Marshal(event["media"])
	_ = json.Unmarshal(b, &command)
	if command.Provider != "ytmusic_ios" || command.Action != "play_uri" {
		t.Fatal(command)
	}
	x := &protocol.ExternalMusicState{Provider: "ytmusic_ios", PackageName: "com.google.ios.youtubemusic", State: "unknown", HandoffOpened: true, Error: "external_playback_unverified", OperationID: command.CommandID, ObservationAgeMS: -1}
	ack := clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: command.CommandID, MediaSequence: command.Sequence, ExternalMusic: x}
	wire, err := json.Marshal(map[string]any{"type": "codex_voice_media_state", "session_id": ack.SessionID, "thread_id": ack.ThreadID, "voice_id": ack.VoiceID, "media_command_id": ack.MediaCommandID, "media_sequence": ack.MediaSequence, "external_music": x})
	if err != nil {
		t.Fatal(err)
	}
	var inbound protocol.Inbound
	if err := json.Unmarshal(wire, &inbound); err != nil {
		t.Fatal(err)
	}
	decoded := clientproto.NewAppV1().ParseCommand(inbound)
	if decoded.ExternalMusic == nil || !decoded.ExternalMusic.HandoffOpened {
		t.Fatal("native acknowledgement lost in actual wire adapter")
	}
	h.handleVoiceMediaState(owner, decoded)
	got := <-done
	r, ok := got.(backend.VoiceMediaResult)
	if !ok || !r.HandoffOpened || r.PlaybackConfirmed || r.ActionConfirmed || r.Observed || r.ExternalMusic != nil {
		t.Fatalf("unobservable external player falsely confirmed: %+v", got)
	}
	if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", input); e != nil {
		t.Fatal(e)
	}
	select {
	case <-owner.send:
		t.Fatal("same-intent duplicate handoff")
	default:
	}
}
func TestIOSOfficialMusicRejectsUnavailableControlBeforeDispatch(t *testing.T) {
	for _, input := range []backend.VoiceMediaRequest{{Action: "pause"}, {Action: "resume"}, {Action: "next"}, {Action: "stop"}} {
		h, _, caller, _ := controllerFixture(t)
		var owner *Client
		for c, call := range h.voiceClients {
			owner = c
			c.clientSurface = "ios"
			call.mediaCapable = true
			call.externalMusicCapable = true
		}
		if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "music_control", input); e == nil {
			t.Fatal("iOS must not claim Android remote control", input)
		}
		select {
		case <-owner.send:
			t.Fatal("unsupported action dispatched")
		default:
		}
	}
}
func TestIOSOfficialMusicCannotAttestExternalPlayerState(t *testing.T) {
	x := &protocol.ExternalMusicState{Provider: "ytmusic_ios", PackageName: "com.google.ios.youtubemusic", State: "unknown", ObservationAgeMS: -1}
	if !validExternalMusic(x) {
		t.Fatal("valid unobserved iOS handoff refused")
	}
	for _, mutate := range []func(*protocol.ExternalMusicState){func(x *protocol.ExternalMusicState) { x.PlaybackConfirmed = true }, func(x *protocol.ExternalMusicState) { x.ActionConfirmed = true }, func(x *protocol.ExternalMusicState) { x.TrackMatched = true }, func(x *protocol.ExternalMusicState) { x.State = "playing" }, func(x *protocol.ExternalMusicState) { x.MediaID = "M7lc1UVf-VE" }, func(x *protocol.ExternalMusicState) { x.PackageName = "other" }} {
		bad := *x
		mutate(&bad)
		if validExternalMusic(&bad) {
			t.Fatalf("fabricated player observation accepted: %+v", bad)
		}
	}
}
