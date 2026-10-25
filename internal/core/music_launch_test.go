package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"testing"
)

func TestMusicLaunchUserActionDoesNotSettleAsPlaybackOrBlockOriginalConfirmation(t *testing.T) {
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
	raw, _ := json.Marshal(event["media"])
	var command protocol.VoiceMediaCommand
	json.Unmarshal(raw, &command)
	ack := clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: command.CommandID, MediaSequence: command.Sequence, ExternalMusic: &protocol.ExternalMusicState{Provider: "ytmusic_android", PackageName: "com.google.android.apps.youtube.music", OperationID: command.CommandID, State: "unavailable", Error: "music_open_required", NeedsUserAction: true}}
	h.handleVoiceMediaState(owner, ack)
	result, ok := (<-done).(backend.VoiceMediaResult)
	if !ok || !result.NeedsUserAction || result.PlaybackConfirmed {
		t.Fatal(result)
	}
	ack.ExternalMusic = &protocol.ExternalMusicState{Provider: "ytmusic_android", PackageName: "com.google.android.apps.youtube.music", OperationID: command.CommandID, State: "playing", MediaID: command.ExpectedID, TrackMatched: true, PlaybackConfirmed: true, ActionConfirmed: true, ObservationAgeMS: 0}
	h.handleVoiceMediaState(owner, ack)
	h.voiceMu.Lock()
	got := externalResult(h.voiceClients[owner].externalMusic.current)
	h.voiceMu.Unlock()
	if !got.PlaybackConfirmed || got.NeedsUserAction || got.ErrorCode != "" {
		t.Fatal(got)
	}
}
func TestMusicLaunchCannotLabelPlayingAsAwaitingUserAction(t *testing.T) {
	x := &protocol.ExternalMusicState{Provider: "ytmusic_android", PackageName: "com.google.android.apps.youtube.music", NeedsUserAction: true, Error: "music_open_required", PlaybackConfirmed: true}
	if validExternalMusic(x) {
		t.Fatal("pending interaction attested playback")
	}
}
