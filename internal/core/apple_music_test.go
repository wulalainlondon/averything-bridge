package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"testing"
)

func TestAppleMusicCanonicalSongScope(t *testing.T) {
	for raw, want := range map[string]string{
		"https://music.apple.com/tw/song/fixture/987654321":              "987654321",
		"https://music.apple.com/us/album/fixture/123456789?i=987654321": "987654321",
		"https://music.apple.com.evil/us/song/987654321":                 "",
		"https://x@music.apple.com/us/song/987654321":                    "",
		"https://music.apple.com:443/us/song/987654321":                  "",
		"https://music.apple.com/us/song/987654321#":                     "",
		"https://music.apple.com/us/album/123456789?i=987654321&":        "",
		"https://music.apple.com/us/song/987654321#x":                    "",
		"https://music.apple.com/us/song/987654321?x=1":                  "",
		"https://music.apple.com/us/album/123456789?i=987654321&i=111":   "",
		"https://music.apple.com/us/song/not-numeric":                    "",
		"https://music.apple.com/us/playlist/pl.test":                    "",
	} {
		if got := appleMusicSongID(raw); got != want {
			t.Fatalf("%s: %s != %s", raw, got, want)
		}
	}
}
func TestAppleMusicOriginalCallWireReceiptAndNoReplay(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	for c, v := range h.voiceClients {
		owner = c
		c.clientSurface = "ios"
		v.mediaCapable = true
		v.externalMusicCapable = true
		v.musicProvider = "apple_music_ios"
	}
	input := backend.VoiceMediaRequest{URL: "https://music.apple.com/tw/album/fixture/123456789?i=987654321"}
	done := make(chan any, 1)
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
	if command.Provider != "apple_music_ios" || command.Action != "play_uri" || command.ExpectedID != "987654321" {
		t.Fatal(command)
	}
	state := &protocol.ExternalMusicState{Provider: "apple_music_ios", PackageName: "com.apple.Music", State: "playing", MediaID: "987654321", TrackMatched: true, ActionConfirmed: true, PlaybackConfirmed: true, ObservationAgeMS: 0, OperationID: command.CommandID}
	ack := clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: command.CommandID, MediaSequence: command.Sequence, ExternalMusic: state}
	// A different track cannot attest the requested resource, even on the exact call.
	state.MediaID = "111111111"
	h.handleVoiceMediaState(owner, ack)
	h.voiceMu.Lock()
	refused := h.voiceClients[owner].externalMusic.current.result.PlaybackConfirmed
	h.voiceMu.Unlock()
	if refused {
		t.Fatal("wrong exact song was confirmed")
	}
	state.MediaID = "987654321"
	wire, _ := json.Marshal(map[string]any{"type": "codex_voice_media_state", "session_id": ack.SessionID, "thread_id": ack.ThreadID, "voice_id": ack.VoiceID, "media_command_id": command.CommandID, "media_sequence": command.Sequence, "external_music": state})
	var inbound protocol.Inbound
	if e := json.Unmarshal(wire, &inbound); e != nil {
		t.Fatal(e)
	}
	h.handleVoiceMediaState(owner, clientproto.NewAppV1().ParseCommand(inbound))
	got := <-done
	r, ok := got.(backend.VoiceMediaResult)
	if !ok || !r.ActionConfirmed || !r.PlaybackConfirmed || !r.Observed || r.Provider != "apple_music_ios" {
		t.Fatalf("%+v", got)
	}
	if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", input); e != nil {
		t.Fatal(e)
	}
	select {
	case <-owner.send:
		t.Fatal("same intent replayed Apple queue")
	default:
	}
}
func TestAppleMusicPermissionRefusalIsNotPlayerObservation(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	for c, v := range h.voiceClients {
		owner = c
		c.clientSurface = "ios"
		v.mediaCapable = true
		v.externalMusicCapable = true
		v.musicProvider = "apple_music_ios"
	}
	done := make(chan any, 1)
	go func() {
		r, e := h.ExecuteVoiceMedia(context.Background(), caller, "music_control", backend.VoiceMediaRequest{Action: "pause"})
		if e != nil {
			done <- e
		} else {
			done <- r
		}
	}()
	e := nextVoiceState(t, owner, "media")
	b, _ := json.Marshal(e["media"])
	var cmd protocol.VoiceMediaCommand
	_ = json.Unmarshal(b, &cmd)
	if cmd.Provider != "apple_music_ios" || cmd.Action != "pause" {
		t.Fatal(cmd)
	}
	h.handleVoiceMediaState(owner, clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: cmd.CommandID, MediaSequence: cmd.Sequence, ExternalMusic: &protocol.ExternalMusicState{Provider: "apple_music_ios", PackageName: "com.apple.Music", State: "unavailable", Error: "music_permission_required", ObservationAgeMS: -1, OperationID: cmd.CommandID}})
	got := <-done
	r, ok := got.(backend.VoiceMediaResult)
	if !ok || r.Observed || r.LastObservedMS != 0 || r.ActionConfirmed || !r.NeedsUserAction {
		t.Fatalf("permission refusal falsely fresh: %+v", got)
	}
}
func TestAppleMusicCannotAttestStaleOrDifferentProvider(t *testing.T) {
	good := protocol.ExternalMusicState{Provider: "apple_music_ios", PackageName: "com.apple.Music", State: "playing", MediaID: "987654321", ObservationAgeMS: 0, ActionConfirmed: true, PlaybackConfirmed: true, TrackMatched: true}
	if !validExternalMusic(&good) {
		t.Fatal("native sample refused")
	}
	for _, mutate := range []func(*protocol.ExternalMusicState){func(x *protocol.ExternalMusicState) { x.PackageName = "com.google.ios.youtubemusic" }, func(x *protocol.ExternalMusicState) { x.ObservationAgeMS = 5001 }, func(x *protocol.ExternalMusicState) { x.HandoffOpened = true }, func(x *protocol.ExternalMusicState) { x.State = "paused" }, func(x *protocol.ExternalMusicState) { x.MediaID = "not-a-catalog-id" }} {
		bad := good
		mutate(&bad)
		if validExternalMusic(&bad) {
			t.Fatalf("false native attestation accepted %+v", bad)
		}
	}
}

func TestAppleMusicNativeControlActionsRemainOnOriginalCall(t *testing.T) {
	for _, action := range []string{"pause", "resume", "stop", "next", "previous", "seek"} {
		t.Run(action, func(t *testing.T) {
			h, _, caller, _ := controllerFixture(t)
			var owner *Client
			for c, v := range h.voiceClients {
				owner = c
				c.clientSurface = "ios"
				v.mediaCapable = true
				v.externalMusicCapable = true
				v.musicProvider = "apple_music_ios"
			}
			input := backend.VoiceMediaRequest{Action: action}
			if action == "seek" {
				n := int64(12000)
				input.PositionMS = &n
			}
			done := make(chan any, 1)
			go func() {
				r, e := h.ExecuteVoiceMedia(context.Background(), caller, "music_control", input)
				if e != nil {
					done <- e
				} else {
					done <- r
				}
			}()
			e := nextVoiceState(t, owner, "media")
			b, _ := json.Marshal(e["media"])
			var cmd protocol.VoiceMediaCommand
			_ = json.Unmarshal(b, &cmd)
			if cmd.Provider != "apple_music_ios" || cmd.Action != action {
				t.Fatal(cmd)
			}
			x := &protocol.ExternalMusicState{Provider: "apple_music_ios", PackageName: "com.apple.Music", State: "paused", MediaID: "987654321", ObservationAgeMS: 0, ActionConfirmed: true, OperationID: cmd.CommandID}
			ack := clientproto.Command{SessionID: caller.Parent.ID, ThreadID: "different-thread", VoiceID: caller.VoiceID, MediaCommandID: cmd.CommandID, MediaSequence: cmd.Sequence, ExternalMusic: x}
			h.handleVoiceMediaState(owner, ack)
			h.voiceMu.Lock()
			closed := h.voiceClients[owner].externalMusic.current.closed
			h.voiceMu.Unlock()
			if closed {
				t.Fatal("foreign thread settled native command")
			}
			ack.ThreadID = caller.Parent.ResumeID()
			h.handleVoiceMediaState(owner, ack)
			r, ok := (<-done).(backend.VoiceMediaResult)
			if !ok || !r.ActionConfirmed || !r.Observed {
				t.Fatal(r)
			}
		})
	}
}
func TestAppleMusicWrongServiceURLRefusedBeforeEffect(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	for c, v := range h.voiceClients {
		owner = c
		c.clientSurface = "ios"
		v.mediaCapable = true
		v.externalMusicCapable = true
		v.musicProvider = "apple_music_ios"
	}
	_, err := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: "https://music.youtube.com/watch?v=M7lc1UVf-VE"})
	if err == nil {
		t.Fatal("Apple source accepted YouTube URI")
	}
	select {
	case <-owner.send:
		t.Fatal("mismatched provider dispatched")
	default:
	}
}
