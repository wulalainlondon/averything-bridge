package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"testing"
)

func TestSystemMediaAudioAdjustsOnlyOriginalCapableCall(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	var owner *Client
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		owner = c
		call = v
	}
	call.systemVolumeCapable = true
	delta := 5
	done := make(chan backend.VoiceMediaResult, 1)
	go func() {
		r, e := h.executeSystemMediaVolume(context.Background(), caller.Parent, caller.VoiceID, "original-request", "vop_voice_audio_one", "fixture", call, backend.VoiceMediaRequest{Delta: &delta})
		if e != nil {
			t.Error(e)
			return
		}
		done <- r.(backend.VoiceMediaResult)
	}()
	event := nextVoiceState(t, owner, "media")
	raw, _ := json.Marshal(event["media"])
	var cmd protocol.VoiceMediaCommand
	json.Unmarshal(raw, &cmd)
	if cmd.Provider != "call_system_media" || cmd.SystemMediaDelta == nil || *cmd.SystemMediaDelta != 5 || cmd.SystemMediaVolume != nil || cmd.URI != "" {
		t.Fatal(cmd)
	}
	ack := clientproto.Command{SessionID: caller.Parent.ID, ThreadID: caller.Parent.ResumeID(), VoiceID: caller.VoiceID, MediaCommandID: cmd.CommandID, MediaSequence: cmd.Sequence, SystemMediaAudio: &protocol.SystemMediaAudioState{State: "applied", Previous: 70, Configured: 75, Applied: 73, MediaIndex: 11, MediaMax: 15, AssistantGain: 70}}
	h.handleVoiceMediaState(newTestClient(h), ack)
	if _, ok := h.systemVolumeReceipt(call, "vop_voice_audio_one"); !ok {
		t.Fatal("missing original receipt")
	}
	h.handleVoiceMediaState(owner, ack)
	result := <-done
	if !result.ActionConfirmed || !result.Observed || result.SystemMediaAudio.Applied != 73 || result.PlaybackConfirmed || call.musicProvider != "" {
		t.Fatal(result)
	}
	_, e := h.executeSystemMediaVolume(context.Background(), caller.Parent, caller.VoiceID, "original-request", "vop_voice_audio_one", "fixture", call, backend.VoiceMediaRequest{Delta: &delta})
	if e != nil {
		t.Fatal(e)
	}
	select {
	case <-owner.send:
		t.Fatal("relative change dispatched twice")
	default:
	}
	changed := 6
	if _, e = h.executeSystemMediaVolume(context.Background(), caller.Parent, caller.VoiceID, "original-request", "vop_voice_audio_one", "fixture", call, backend.VoiceMediaRequest{Delta: &changed}); e == nil {
		t.Fatal("changed intent accepted")
	}
}
func TestSystemMediaAudioLegacyAndBadTargetsRefuseBeforeDispatch(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	for _, v := range h.voiceClients {
		v.systemVolumeCapable = false
	}
	value := 70
	if _, e := h.executeSystemMediaVolume(context.Background(), caller.Parent, caller.VoiceID, "request", "vop_voice_audio_legacy", "fixture", nil, backend.VoiceMediaRequest{Volume: &value}); e == nil {
		t.Fatal("legacy device accepted")
	}
	for _, v := range h.voiceClients {
		v.systemVolumeCapable = true
	}
	if _, e := h.executeSystemMediaVolume(context.Background(), caller.Parent, "foreign-voice", "request", "vop_voice_audio_foreign", "fixture", nil, backend.VoiceMediaRequest{Volume: &value}); e == nil {
		t.Fatal("foreign call accepted")
	}
	bad := 101
	if _, e := h.executeSystemMediaVolume(context.Background(), caller.Parent, caller.VoiceID, "request", "vop_voice_audio_invalid", "fixture", nil, backend.VoiceMediaRequest{Volume: &bad}); e == nil {
		t.Fatal("invalid gain accepted")
	}
}
