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

func TestYouTubeTargetParsingRejectsForeignOrigins(t *testing.T) {
	for _, u := range []string{"https://youtu.be/M7lc1UVf-VE", "https://www.youtube.com/watch?v=M7lc1UVf-VE&feature=share", "https://m.youtube.com/shorts/M7lc1UVf-VE"} {
		id, e := youtubeID(u)
		if e != nil || id != "M7lc1UVf-VE" {
			t.Fatal(u, id, e)
		}
	}
	for _, u := range []string{"http://youtube.com/watch?v=M7lc1UVf-VE", "https://youtube.com.evil/watch?v=M7lc1UVf-VE", "https://www.youtube.com@127.0.0.1/watch?v=M7lc1UVf-VE", "https://youtube.com:444/watch?v=M7lc1UVf-VE", "https://youtube.com/results?search_query=music", "https://youtube.com/watch?v=M7lc1UVf-VE&v=abcdefghijk", "https://youtu.be/M7lc1UVf-VE/extra"} {
		if _, e := youtubeID(u); e == nil {
			t.Fatal("unsafe or ambiguous target", u)
		}
	}
}
func TestVoiceMusicWaitsForObservedPlayerAndRejectsForeignFeedback(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var c *Client
	for owner, call := range h.voiceClients {
		c = owner
		call.mediaCapable = true
	}
	input := backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE", Title: "test song"}
	result := make(chan any, 1)
	go func() {
		v, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_youtube", input)
		if e != nil {
			result <- e
		} else {
			result <- v
		}
	}()
	event := nextVoiceState(t, c, "media")
	var command protocol.VoiceMediaCommand
	data, _ := json.Marshal(event["media"])
	if e := json.Unmarshal(data, &command); e != nil {
		t.Fatal(e)
	}
	ack := clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: command.CommandID, MediaSequence: command.Sequence, MediaVideoID: command.VideoID, MediaState: "playing"}
	h.handleVoiceMediaState(newTestClient(h), ack)
	bad := ack
	bad.MediaSequence++
	h.handleVoiceMediaState(c, bad)
	bad = ack
	bad.ThreadID = "foreign-thread"
	h.handleVoiceMediaState(c, bad)
	select {
	case v := <-result:
		t.Fatal("foreign/stale feedback completed playback", v)
	case <-time.After(20 * time.Millisecond):
	}
	buffering := ack
	buffering.MediaState = "buffering"
	h.handleVoiceMediaState(c, buffering)
	select {
	case v := <-result:
		t.Fatal("buffering reported as playing", v)
	case <-time.After(20 * time.Millisecond):
	}
	wire, _ := json.Marshal(protocol.Inbound{Type: "codex_voice_media_state", SessionID: ack.SessionID, ThreadID: ack.ThreadID, VoiceID: ack.VoiceID, MediaCommandID: ack.MediaCommandID, MediaSequence: ack.MediaSequence, MediaVideoID: ack.MediaVideoID, MediaState: ack.MediaState})
	route(h, c, string(wire))
	value := (<-result).(backend.VoiceMediaResult)
	if !value.PlaybackConfirmed || !value.ActionConfirmed || !value.Observed {
		t.Fatalf("valid player report lost:%+v", value)
	}
	replay, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_youtube", input)
	if e != nil || !replay.(backend.VoiceMediaResult).PlaybackConfirmed {
		t.Fatal(replay, e)
	}
	select {
	case <-c.send:
		t.Fatal("same tool intent reloaded the video")
	default:
	}
	input.URL = "https://youtu.be/abcdefghijk"
	if _, e = h.ExecuteVoiceMedia(context.Background(), caller, "play_youtube", input); e == nil {
		t.Fatal("same intent changed target")
	}
	h.voiceMu.Lock()
	h.voiceClients[c].media.observedAt = time.Now().Add(-31 * time.Second).UnixMilli()
	h.voiceMu.Unlock()
	caller.ToolCallID = "status-current"
	status, e := h.ExecuteVoiceMedia(context.Background(), caller, "status", backend.VoiceMediaRequest{})
	if e != nil || status.(backend.VoiceMediaResult).State != "unverified" || status.(backend.VoiceMediaResult).PlaybackConfirmed {
		t.Fatal("stale status promoted", status, e)
	}
}
func TestVoiceMusicCannotUseOldClientsOrOtherVoice(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	input := backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"}
	if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_youtube", input); e == nil {
		t.Fatal("old client got media command")
	}
	for _, call := range h.voiceClients {
		call.mediaCapable = true
	}
	caller.VoiceID = "other-voice"
	if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_youtube", input); e == nil {
		t.Fatal("other voice used player")
	}
	caller.VoiceID = ""
	if _, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_youtube", input); e == nil {
		t.Fatal("text-only caller played music")
	}
}
func TestVoiceMusicVolumeAcknowledgementMustMatchTarget(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var c *Client
	for owner, call := range h.voiceClients {
		c = owner
		call.mediaCapable = true
		call.media = &voiceMediaSession{receipts: map[string]*voiceMediaReceipt{}, video: "M7lc1UVf-VE", state: "paused", volume: 35}
	}
	wanted := 55
	done := make(chan any, 1)
	go func() {
		v, e := h.ExecuteVoiceMedia(context.Background(), caller, "control", backend.VoiceMediaRequest{Action: "volume", Volume: &wanted})
		if e != nil {
			done <- e
		} else {
			done <- v
		}
	}()
	event := nextVoiceState(t, c, "media")
	var cmd protocol.VoiceMediaCommand
	data, _ := json.Marshal(event["media"])
	_ = json.Unmarshal(data, &cmd)
	old := 35
	ack := clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: cmd.CommandID, MediaSequence: cmd.Sequence, MediaVideoID: cmd.VideoID, MediaState: "paused", MediaVolume: &old}
	h.handleVoiceMediaState(c, ack)
	select {
	case v := <-done:
		t.Fatal("old volume confirmed", v)
	case <-time.After(20 * time.Millisecond):
	}
	ack.MediaVolume = &wanted
	h.handleVoiceMediaState(c, ack)
	v := (<-done).(backend.VoiceMediaResult)
	if !v.ActionConfirmed || v.Volume != 55 || v.PlaybackConfirmed {
		t.Fatal(v)
	}
}

func TestVoiceMusicAudioStateDoesNotConfusePlayingWithSound(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	for c, call := range h.voiceClients {
		owner = c
		call.mediaCapable = true
	}
	result := make(chan any, 1)
	go func() {
		v, e := h.ExecuteVoiceMedia(context.Background(), caller, "play_youtube", backend.VoiceMediaRequest{URL: "https://youtu.be/M7lc1UVf-VE"})
		if e != nil {
			result <- e
		} else {
			result <- v
		}
	}()
	event := nextVoiceState(t, owner, "media")
	var command protocol.VoiceMediaCommand
	raw, _ := json.Marshal(event["media"])
	json.Unmarshal(raw, &command)
	yes, no := true, false
	systemVolume, playerVolume := 30, 35
	ack := protocol.Inbound{Type: "codex_voice_media_state", SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: command.CommandID, MediaSequence: command.Sequence, MediaVideoID: command.VideoID, MediaState: "playing", MediaVolume: &playerVolume, MediaAudio: &protocol.MediaAudioInfo{PlayerMuted: &yes, SystemMediaVolume: &systemVolume, SystemMediaMuted: &no, VoiceRoute: "bluetooth"}}
	raw, _ = json.Marshal(ack)
	route(h, owner, string(raw))
	got := (<-result).(backend.VoiceMediaResult)
	if !got.PlaybackConfirmed || got.AudioCheck != "player_muted" || got.Audio == nil || got.Volume != 35 {
		t.Fatalf("playing notaudible distinction lost:%+v", got)
	}
	systemVolume = 0
	ack.MediaAudio.PlayerMuted = &no
	raw, _ = json.Marshal(ack)
	route(h, owner, string(raw))
	status, e := h.ExecuteVoiceMedia(context.Background(), caller, "status", backend.VoiceMediaRequest{})
	if e != nil || status.(backend.VoiceMediaResult).AudioCheck != "system_media_volume_zero" {
		t.Fatal(status, e)
	}
	ack.MediaAudio.VoiceRoute = "untrusted-device-name"
	systemVolume = 80
	raw, _ = json.Marshal(ack)
	route(h, owner, string(raw))
	status, _ = h.ExecuteVoiceMedia(context.Background(), caller, "status", backend.VoiceMediaRequest{})
	if status.(backend.VoiceMediaResult).Audio.VoiceRoute != "bluetooth" || *status.(backend.VoiceMediaResult).Audio.SystemMediaVolume != 0 {
		t.Fatal("invaliddiagnostics accepted", status)
	}
	h.voiceMu.Lock()
	h.voiceClients[owner].media.observedAt = time.Now().Add(-31 * time.Second).UnixMilli()
	h.voiceMu.Unlock()
	status, _ = h.ExecuteVoiceMedia(context.Background(), caller, "status", backend.VoiceMediaRequest{})
	if status.(backend.VoiceMediaResult).Audio != nil || status.(backend.VoiceMediaResult).AudioCheck != "stale" {
		t.Fatal("staleaudio reused", status)
	}
}
