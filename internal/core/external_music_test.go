package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"testing"
	"time"
)

func TestExternalMusicUsesOriginalCallAndRequiresMatchedTrack(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	for c, call := range h.voiceClients {
		owner = c
		call.mediaCapable = true
		call.externalMusicCapable = true
	}
	done := make(chan any, 1)
	go func() {
		r, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: "https://music.youtube.com/watch?v=M7lc1UVf-VE"})
		if e != nil {
			done <- e
		} else {
			done <- r
		}
	}()
	event := nextVoiceState(t, owner, "media")
	var command protocol.VoiceMediaCommand
	b, _ := json.Marshal(event["media"])
	json.Unmarshal(b, &command)
	if command.Provider != "ytmusic_android" || command.ExpectedID != "M7lc1UVf-VE" || command.Action != "play_uri" {
		t.Fatal(command)
	}
	x := &protocol.ExternalMusicState{Provider: "ytmusic_android", PackageName: "com.google.android.apps.youtube.music", BindingID: "opaque-native-lease", State: "playing", Title: "a different song", OperationID: command.CommandID, PlaybackConfirmed: true, ActionConfirmed: true, ObservationAgeMS: 0}
	ack := clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: command.CommandID, MediaSequence: command.Sequence, ExternalMusic: x}
	h.handleVoiceMediaState(newTestClient(h), ack)
	select {
	case <-done:
		t.Fatal("foreign endpoint accepted")
	case <-time.After(20 * time.Millisecond):
	}
	// PLAYING without exact track matching must not be reported as successful playback.
	h.handleVoiceMediaState(owner, ack)
	got := <-done
	r, ok := got.(backend.VoiceMediaResult)
	if !ok || r.PlaybackConfirmed || r.ActionConfirmed {
		t.Fatalf("wrong song confirmed: %+v", got)
	}
	x2 := *x
	x2.TrackMatched = true
	x2.MediaID = command.ExpectedID
	ack.ExternalMusic = &x2
	h.handleVoiceMediaState(owner, ack)
	h.voiceMu.Lock()
	call := h.voiceClients[owner]
	observed := externalResult(call.externalMusic.current)
	h.voiceMu.Unlock()
	if !observed.PlaybackConfirmed {
		t.Fatal(observed)
	}
	// Same intent does not dispatch twice even after a fresh observation.
	if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{URL: "https://music.youtube.com/watch?v=M7lc1UVf-VE"}); e != nil {
		t.Fatal(e)
	}
	select {
	case <-owner.send:
		t.Fatal("duplicate playback")
	default:
	}
}
func TestExternalMusicRejectsUnsupportedOriginsTargetsAndPlayerVolume(t *testing.T) {
	for _, input := range []backend.VoiceMediaRequest{{URL: "https://music.youtube.com.evil/watch?v=M7lc1UVf-VE"}, {URL: "https://music.youtube.com/watch?v=M7lc1UVf-VE&x=1"}, {URL: "https://music.youtube.com/watch?v=M7lc1UVf-VE", Query: "both"}, {Query: "query", Volume: func() *int { v := 100; return &v }()}} {
		h, _, caller, _ := controllerFixture(t)
		for _, call := range h.voiceClients {
			call.mediaCapable = true
			call.externalMusicCapable = true
		}
		if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", input); e == nil {
			t.Fatal(input)
		}
	}
	h, _, caller, _ := controllerFixture(t)
	for _, call := range h.voiceClients {
		call.mediaCapable = true
	}
	if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{Query: "song"}); e == nil {
		t.Fatal("old phone accepted external control")
	}
}
func TestExternalMusicDoesNotAcceptForeignPackageOrLargePayload(t *testing.T) {
	x := &protocol.ExternalMusicState{Provider: "ytmusic_android", PackageName: "other", State: "playing"}
	if validExternalMusic(x) {
		t.Fatal("foreign package")
	}
	x.PackageName = "com.google.android.apps.youtube.music"
	if !validExternalMusic(x) {
		t.Fatal("valid snapshot refused")
	}
	x.Title = string(make([]byte, 400))
	if validExternalMusic(x) {
		t.Fatal("oversized title")
	}
}

func TestExternalMusicTerminalFailureSurvivesObservation(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	for c, call := range h.voiceClients {
		owner = c
		call.mediaCapable = true
		call.externalMusicCapable = true
	}
	done := make(chan any, 1)
	go func() {
		v, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_music", backend.VoiceMediaRequest{Query: "requested song", Title: "requested song"})
		if e != nil {
			done <- e
		} else {
			done <- v
		}
	}()
	event := nextVoiceState(t, owner, "media")
	var command protocol.VoiceMediaCommand
	b, _ := json.Marshal(event["media"])
	json.Unmarshal(b, &command)
	x := &protocol.ExternalMusicState{Provider: "ytmusic_android", PackageName: "com.google.android.apps.youtube.music", BindingID: "original", State: "playing", Title: "old song", Error: "playback_unconfirmed", OperationID: command.CommandID, ObservationAgeMS: 0}
	ack := clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: command.CommandID, MediaSequence: command.Sequence, ExternalMusic: x}
	h.handleVoiceMediaState(owner, ack)
	first := <-done
	if v, ok := first.(backend.VoiceMediaResult); !ok || v.ErrorCode != "playback_unconfirmed" {
		t.Fatal(first)
	}
	fresh := *x
	fresh.Error = ""
	ack.ExternalMusic = &fresh
	h.handleVoiceMediaState(owner, ack)
	h.voiceMu.Lock()
	out := externalResult(h.voiceClients[owner].externalMusic.current)
	h.voiceMu.Unlock()
	if out.ErrorCode != "playback_unconfirmed" || out.ExternalMusic.Error != "playback_unconfirmed" || out.PlaybackConfirmed {
		t.Fatalf("terminal outcome erased: %+v", out)
	}
}
