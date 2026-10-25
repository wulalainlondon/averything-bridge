package core

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type voiceAudioReceipt struct {
	sentAt      time.Time
	command     protocol.VoiceMediaCommand
	fingerprint string
	result      backend.VoiceMediaResult
	done        chan struct{}
	settled     bool
}

func voiceAudioResult(r *voiceAudioReceipt) backend.VoiceMediaResult {
	v := r.result
	if v.LastObservedMS > 0 {
		age := time.Now().UnixMilli() - v.LastObservedMS
		v.ObservationAgeMS = &age
		if age < 0 || age > 5000 {
			v.Observed = false
			v.ActionConfirmed = false
			v.VoiceAudio = nil
			v.AudioCheck = "stale"
		}
	}
	return v
}

func (h *Hub) voiceVolumeReceipt(call *clientVoiceCall, id string) (backend.VoiceMediaResult, bool) {
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if call == nil {
		return backend.VoiceMediaResult{}, false
	}
	r := call.voiceAudioReceipts[id]
	if r == nil {
		return backend.VoiceMediaResult{}, false
	}
	return voiceAudioResult(r), true
}

func (h *Hub) executeVoiceVolume(ctx context.Context, parent *session.Session, voice, request, id, transport string, expected *clientVoiceCall, in backend.VoiceMediaRequest) (any, error) {
	refuse := func(code string) (any, error) {
		return nil, &backend.VoiceMediaNotDispatchedError{Cause: errors.New(code)}
	}
	if ctx.Err() != nil {
		return refuse("voice_volume_context_expired")
	}
	s, ok := h.registry.Get(parent.ID)
	if !ok || s != parent || !h.controls.MobileMayWrite(s.ID) {
		return refuse("voice_volume_owner_invalid")
	}
	if in.Action != "" || in.URL != "" || in.Query != "" || in.Title != "" || in.Artist != "" || in.PositionMS != nil || in.IntentID != "" || in.Volume != nil && in.Delta != nil {
		return refuse("voice_volume_arguments_invalid")
	}
	if in.Volume != nil && (*in.Volume < 0 || *in.Volume > 100) || in.Delta != nil && (*in.Delta < -100 || *in.Delta > 100) {
		return refuse("voice_volume_range_invalid")
	}
	h.voiceMu.Lock()
	var owner *Client
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		if c.live() && !v.stopping && v.session == parent && v.voiceID == voice && v.kind != "line" {
			if call != nil {
				h.voiceMu.Unlock()
				return refuse("voice_volume_endpoint_ambiguous")
			}
			owner = c
			call = v
		}
	}
	if call == nil || expected != nil && expected != call {
		h.voiceMu.Unlock()
		return refuse("voice_volume_call_retired")
	}
	if !call.voiceAudioCapable {
		h.voiceMu.Unlock()
		return refuse("voice_volume_unsupported")
	}
	if call.voiceAudioReceipts == nil {
		call.voiceAudioReceipts = map[string]*voiceAudioReceipt{}
	}
	b, _ := json.Marshal(in)
	fp := fmt.Sprintf("%x", sha256.Sum256(b))
	if prior := call.voiceAudioReceipts[id]; prior != nil {
		h.voiceMu.Unlock()
		if prior.fingerprint != fp {
			return refuse("voice_volume_intent_conflict")
		}
		h.voiceMu.Lock()
		v := voiceAudioResult(prior)
		h.voiceMu.Unlock()
		return v, nil
	}
	if len(call.voiceAudioReceipts) >= 128 {
		h.voiceMu.Unlock()
		return refuse("voice_volume_receipt_limit")
	}
	call.voiceAudioSequence++
	cmd := protocol.VoiceMediaCommand{Provider: "call_audio", Action: "assistant_volume", CommandID: fmt.Sprintf("va_%x", sha256.Sum256([]byte(parent.ID+"\x00"+voice+"\x00"+id))), Sequence: call.voiceAudioSequence, AssistantVolume: in.Volume, AssistantDelta: in.Delta}
	r := &voiceAudioReceipt{sentAt: time.Now(), command: cmd, fingerprint: fp, done: make(chan struct{}), result: backend.VoiceMediaResult{Provider: "call_audio", Endpoint: h.mediaEndpoint(owner), Transport: transport, CommandID: cmd.CommandID, Sequence: cmd.Sequence, Action: cmd.Action, State: "queued", AudioCheck: "assistant_output_unverified"}}
	call.voiceAudioReceipts[id] = r
	event := voiceEvent(clientproto.Command{SessionID: parent.ID, RequestID: request, VoiceID: voice}, "media", "")
	event.ThreadID = parent.ResumeID()
	event.Media = &cmd
	h.voiceMu.Unlock()
	owner.enqueueEvent(event)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	case <-r.done:
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if h.voiceClients[owner] != call || call.stopping {
		v := r.result
		v.State = "voice_ended"
		return v, nil
	}
	return voiceAudioResult(r), nil
}

func (h *Hub) handleVoiceAudioState(c *Client, cmd clientproto.Command) {
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	call := h.voiceClients[c]
	if call == nil || call.stopping || !call.voiceAudioCapable || call.voiceID != cmd.VoiceID || call.session.ID != cmd.SessionID || call.session.ResumeID() != cmd.ThreadID || !h.controls.MobileMayWrite(cmd.SessionID) {
		return
	}
	var r *voiceAudioReceipt
	for _, v := range call.voiceAudioReceipts {
		if v.command.CommandID == cmd.MediaCommandID && v.command.Sequence == cmd.MediaSequence {
			r = v
			break
		}
	}
	x := cmd.VoiceAudio
	if r == nil || r.settled || x == nil || x.Previous < 0 || x.Previous > 100 || x.Configured < 0 || x.Configured > 100 || x.Applied < 0 || x.Applied > 100 || x.MediaIndex < 0 {
		return
	}
	if x.State != "observed" && x.State != "applied" && x.State != "refused" {
		return
	}
	if x.Error != "" && x.Error != "voice_volume_invalid" && x.Error != "voice_audio_unavailable" && x.Error != "voice_audio_intent_conflict" && x.Error != "voice_audio_retired" && x.Error != "voice_volume_storage_failed" {
		return
	}
	if x.State == "applied" && (x.Error != "" || x.Configured != x.Applied || r.command.AssistantVolume != nil && x.Applied != *r.command.AssistantVolume) {
		return
	}
	if x.State == "applied" && r.command.AssistantDelta != nil {
		expected := max(0, min(100, x.Previous+*r.command.AssistantDelta))
		if x.Applied != expected {
			return
		}
	}
	if time.Since(r.sentAt) > 5*time.Second {
		r.result.State = "unverified"
		r.result.ErrorCode = "voice_volume_observation_stale"
		r.result.AudioCheck = "stale"
		r.settled = true
		close(r.done)
		return
	}
	copy := *x
	r.result.VoiceAudio = &copy
	r.result.State = x.State
	r.result.ErrorCode = x.Error
	r.result.LastObservedMS = r.sentAt.UnixMilli()
	r.result.Observed = true
	r.result.ActionConfirmed = x.State == "applied" && x.Error == ""
	r.result.AudioCheck = "assistant_output_unverified"
	r.settled = true
	close(r.done)
}
