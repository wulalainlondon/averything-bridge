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

type systemMediaReceipt struct {
	sentAt      time.Time
	command     protocol.VoiceMediaCommand
	fingerprint string
	result      backend.VoiceMediaResult
	done        chan struct{}
	settled     bool
}

func systemMediaResult(r *systemMediaReceipt) backend.VoiceMediaResult {
	v := r.result
	if v.LastObservedMS > 0 {
		age := time.Now().UnixMilli() - v.LastObservedMS
		v.ObservationAgeMS = &age
		if age < 0 || age > 5000 {
			v.Observed = false
			v.ActionConfirmed = false
			v.SystemMediaAudio = nil
			v.AudioCheck = "stale"
		}
	}
	return v
}

func (h *Hub) systemVolumeReceipt(call *clientVoiceCall, id string) (backend.VoiceMediaResult, bool) {
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if call == nil {
		return backend.VoiceMediaResult{}, false
	}
	r := call.systemMediaReceipts[id]
	if r == nil {
		return backend.VoiceMediaResult{}, false
	}
	return systemMediaResult(r), true
}

func (h *Hub) executeSystemMediaVolume(ctx context.Context, parent *session.Session, voice, request, id, transport string, expected *clientVoiceCall, in backend.VoiceMediaRequest) (any, error) {
	refuse := func(code string) (any, error) {
		return nil, &backend.VoiceMediaNotDispatchedError{Cause: errors.New(code)}
	}
	if ctx.Err() != nil {
		return refuse("system_media_volume_context_expired")
	}
	s, ok := h.registry.Get(parent.ID)
	if !ok || s != parent || !h.controls.MobileMayWrite(s.ID) {
		return refuse("system_media_volume_owner_invalid")
	}
	if in.Action != "" || in.URL != "" || in.Query != "" || in.Title != "" || in.Artist != "" || in.PositionMS != nil || in.IntentID != "" || in.Volume != nil && in.Delta != nil {
		return refuse("system_media_volume_arguments_invalid")
	}
	if in.Volume != nil && (*in.Volume < 0 || *in.Volume > 100) || in.Delta != nil && (*in.Delta < -100 || *in.Delta > 100) {
		return refuse("system_media_volume_range_invalid")
	}
	h.voiceMu.Lock()
	var owner *Client
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		if c.live() && !v.stopping && v.session == parent && v.voiceID == voice && v.kind != "line" {
			if call != nil {
				h.voiceMu.Unlock()
				return refuse("system_media_volume_endpoint_ambiguous")
			}
			owner = c
			call = v
		}
	}
	if call == nil || expected != nil && expected != call {
		h.voiceMu.Unlock()
		return refuse("system_media_volume_call_retired")
	}
	if !call.systemVolumeCapable {
		h.voiceMu.Unlock()
		return refuse("system_media_volume_unsupported")
	}
	if call.systemMediaReceipts == nil {
		call.systemMediaReceipts = map[string]*systemMediaReceipt{}
	}
	b, _ := json.Marshal(in)
	fp := fmt.Sprintf("%x", sha256.Sum256(b))
	if prior := call.systemMediaReceipts[id]; prior != nil {
		h.voiceMu.Unlock()
		if prior.fingerprint != fp {
			return refuse("system_media_volume_intent_conflict")
		}
		h.voiceMu.Lock()
		v := systemMediaResult(prior)
		h.voiceMu.Unlock()
		return v, nil
	}
	if len(call.systemMediaReceipts) >= 128 {
		h.voiceMu.Unlock()
		return refuse("system_media_volume_receipt_limit")
	}
	call.systemVolumeSequence++
	cmd := protocol.VoiceMediaCommand{Provider: "call_system_media", Action: "system_media_volume", CommandID: fmt.Sprintf("vs_%x", sha256.Sum256([]byte(parent.ID+"\x00"+voice+"\x00"+id))), Sequence: call.systemVolumeSequence, SystemMediaVolume: in.Volume, SystemMediaDelta: in.Delta}
	r := &systemMediaReceipt{sentAt: time.Now(), command: cmd, fingerprint: fp, done: make(chan struct{}), result: backend.VoiceMediaResult{Provider: "call_system_media", Endpoint: h.mediaEndpoint(owner), Transport: transport, CommandID: cmd.CommandID, Sequence: cmd.Sequence, Action: cmd.Action, State: "queued", AudioCheck: "system_media_output_unverified"}}
	call.systemMediaReceipts[id] = r
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
	return systemMediaResult(r), nil
}

func (h *Hub) handleSystemMediaState(c *Client, cmd clientproto.Command) {
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	call := h.voiceClients[c]
	if call == nil || call.stopping || !call.systemVolumeCapable || call.voiceID != cmd.VoiceID || call.session.ID != cmd.SessionID || call.session.ResumeID() != cmd.ThreadID || !h.controls.MobileMayWrite(cmd.SessionID) {
		return
	}
	var r *systemMediaReceipt
	for _, v := range call.systemMediaReceipts {
		if v.command.CommandID == cmd.MediaCommandID && v.command.Sequence == cmd.MediaSequence {
			r = v
			break
		}
	}
	x := cmd.SystemMediaAudio
	if r == nil || r.settled || x == nil || x.Previous < 0 || x.Previous > 100 || x.Configured < 0 || x.Configured > 100 || x.Applied < 0 || x.Applied > 100 || x.MediaIndex < 0 || x.MediaMax < 1 || x.MediaMax > 1000 || x.MediaIndex > x.MediaMax || x.AssistantGain < 0 || x.AssistantGain > 100 || x.Applied != (x.MediaIndex*100+x.MediaMax/2)/x.MediaMax {
		return
	}
	if x.State != "observed" && x.State != "applied" && x.State != "refused" {
		return
	}
	if x.Error != "" && x.State != "refused" {
		return
	}
	if x.State == "applied" {
		if x.Error != "" {
			return
		}
		wanted := x.Configured
		if r.command.SystemMediaVolume != nil && wanted != *r.command.SystemMediaVolume {
			return
		}
		if r.command.SystemMediaDelta != nil && wanted != max(0, min(100, x.Previous+*r.command.SystemMediaDelta)) {
			return
		}
		if x.MediaIndex != (wanted*x.MediaMax+50)/100 {
			return
		}
	}
	if time.Since(r.sentAt) > 5*time.Second {
		r.result.State = "unverified"
		r.result.ErrorCode = "system_media_volume_observation_stale"
		r.result.AudioCheck = "stale"
		r.settled = true
		close(r.done)
		return
	}
	copy := *x
	r.result.SystemMediaAudio = &copy
	r.result.State = x.State
	r.result.ErrorCode = x.Error
	r.result.LastObservedMS = r.sentAt.UnixMilli()
	r.result.Observed = true
	r.result.ActionConfirmed = x.State == "applied" && x.Error == ""
	r.result.AudioCheck = "system_media_output_unverified"
	r.settled = true
	close(r.done)
}
