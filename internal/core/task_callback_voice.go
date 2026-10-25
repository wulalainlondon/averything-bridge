package core

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/callbacks"
	"everything-go/internal/clientproto"
	"everything-go/internal/session"
	"strings"
	"time"
)

// A callback offer is accepted only under its original claimed invitation.
// Ordinary user-started calls retain their existing path and permissions.
func (h *Hub) bindCallbackVoice(ctx context.Context, c *Client, s *session.Session, cmd clientproto.Command) error {
	if cmd.CallbackID == "" && cmd.CallbackInvitation == "" && cmd.CallbackClaim == "" {
		return nil
	}
	if h.callbacks == nil || cmd.CallbackID == "" || cmd.CallbackInvitation == "" || cmd.CallbackClaim == "" {
		return errors.New("callback_claim_required")
	}
	r, e := h.callbacks.Mutate(ctx, "bind_"+cmd.CallbackInvitation, callbacks.Digest(cmd.CallbackClaim+cmd.VoiceID), cmd.CallbackID, callbacks.AnyRevision, func(r *callbacks.Record, exists bool) error {
		if !exists || !r.Enabled || r.Invite == nil || r.Invite.State != "claimed" || r.Invite.ID != cmd.CallbackInvitation || r.Invite.Claim != cmd.CallbackClaim || r.Recipient.Device != h.pairedTaskDevice(c) || r.Recipient.Binding != callbackBinding(c) || r.Session != s.ID || r.Epoch != h.callbackEpoch(s.ID) || r.Thread != s.ResumeID() || r.Config != s.SettingsSnapshot().ConfigRevision || s.State() != session.Idle || s.QueueLen() > 0 {
			return errors.New("callback_claim_revoked_or_changed")
		}
		r.Invite.Voice = cmd.VoiceID
		r.Invite.State = "connecting"
		r.State = "connecting"
		return nil
	})
	if e == nil {
		h.publishCallbackRecord(r)
	}
	return e
}

// The device's RMS proves output activity, not which result was spoken. Check
// the original provider's completed assistant transcript independently.
func (h *Hub) observeCallbackSpeech(call *clientVoiceCall, event backend.RealtimeVoiceEvent) {
	if event.State != "transcript_done" || event.Role != "assistant" || event.TranscriptVoiceID != call.voiceID {
		return
	}
	h.voiceMu.Lock()
	if call.callbackPolicy == "" || !call.callbackReporting || call.callbackSpeechSettled || call.callbackExpected == "" {
		h.voiceMu.Unlock()
		return
	}
	if len(call.callbackSpoken)+len(event.Text) > 24000 {
		call.callbackSpeechSettled = true
		h.voiceMu.Unlock()
		return
	}
	call.callbackSpoken += event.Text
	want, got := callbacks.SpokenKey(call.callbackExpected), callbacks.SpokenKey(call.callbackSpoken)
	if got == "" || (got != want && strings.HasPrefix(want, got)) {
		h.voiceMu.Unlock()
		return
	}
	verified := got == want
	call.callbackSpeechSettled = true
	policy, invitation := call.callbackPolicy, call.callbackReport
	h.voiceMu.Unlock()
	r, e := h.callbacks.Mutate(context.Background(), "transcript_"+invitation, "transcript_"+callbacks.Digest(got), policy, callbacks.AnyRevision, func(x *callbacks.Record, _ bool) error {
		if x.Invite == nil || x.Invite.ID != invitation || x.Invite.Voice != call.voiceID || x.Invite.ContentVerified || x.Invite.ContentMismatch {
			return callbacks.ErrUnavailable
		}
		x.Invite.ContentVerified = verified
		x.Invite.ContentMismatch = !verified
		if verified {
			x.State = "report_text_verified"
			if x.Invite.AudioObserved {
				x.State = "report_audio_observed"
			}
		} else {
			x.State = "report_delivery_mismatch"
			x.Invite.Reason = "語音內容與封存結果不符，請以文字報告為準；不會自動重播"
		}
		if !callbacks.Terminal(x.Invite.State) {
			x.Invite.State = x.State
		}
		if !x.Enabled {
			x.State = "disabled"
		}
		return nil
	})
	if e == nil {
		h.publishCallbackRecord(r)
	}
}

func (h *Hub) readCallbackSpeech(c *Client, s *session.Session, r callbacks.Record, in callbackInput) {
	if r.Invite == nil {
		return
	}
	h.voiceMu.Lock()
	call := h.voiceClients[c]
	valid := call != nil && call.voiceID == in.Voice && call.callbackReport == r.Invite.ID
	h.voiceMu.Unlock()
	if !valid {
		return
	}
	h.refreshCallbackSpeech(r)
}

// Readback also works after closure and across a service restart. Its only
// effect is evidence for the original invitation, never another call or task.
func (h *Hub) refreshCallbackSpeech(r callbacks.Record) {
	if r.Invite == nil || r.Report == nil || r.Invite.Voice == "" || r.Invite.ReportOperation == "" || r.Invite.ContentVerified || r.Invite.ContentMismatch || r.State == "report_delivery_mismatch" {
		return
	}
	s, exists := h.registry.Get(r.Session)
	if !exists || s.ResumeID() != r.Report.Thread {
		return
	}
	reader, ok := h.exec.(backend.RealtimeSpeechReader)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	since := r.Invite.ClaimedAt
	if since == 0 {
		since = r.Invite.Issued
	}
	for attempt := 0; attempt < 3; attempt++ {
		proof, e := reader.ReadRealtimeVoiceSpeech(ctx, s, r.Invite.Voice, since)
		if e != nil {
			return
		}
		if proof.ThreadID == r.Thread && proof.VoiceID == r.Invite.Voice && proof.Text != "" {
			want, got := callbacks.SpokenKey(callbacks.Speech(r.Name, r.Report.Status, r.Report.Text)), callbacks.SpokenKey(proof.Text)
			if got == want || !strings.HasPrefix(want, got) {
				updated, e := h.callbacks.Mutate(ctx, "readback_"+r.Invite.ID, "readback_"+callbacks.Digest(got+r.Report.Hash), r.ID, callbacks.AnyRevision, func(x *callbacks.Record, _ bool) error {
					if x.Invite == nil || x.Report == nil || x.Invite.ID != r.Invite.ID || x.Invite.Voice != proof.VoiceID || x.Report.Hash != r.Report.Hash || x.Invite.ContentVerified || x.Invite.ContentMismatch {
						return callbacks.ErrUnavailable
					}
					x.Invite.ContentVerified = got == want
					x.Invite.ContentMismatch = got != want
					if got == want {
						x.State = "report_text_verified"
						if x.Invite.AudioObserved {
							x.State = "report_audio_observed"
						}
					} else {
						x.State = "report_delivery_mismatch"
						x.Invite.Reason = "語音內容未完全核對，請以文字結果為準；不會自動重播"
					}
					if !callbacks.Terminal(x.Invite.State) {
						x.Invite.State = x.State
					}
					if !x.Enabled {
						x.State = "disabled"
					}
					return nil
				})
				if e == nil {
					h.publishCallbackRecord(updated)
				}
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}
func (h *Hub) cancelConnectingCallback(r callbacks.Record) {
	if r.Invite == nil || r.Invite.State != "revoked" || r.Invite.Voice == "" {
		return
	}
	h.voiceMu.Lock()
	var owner *Client
	var voice *clientVoiceCall
	for c, v := range h.voiceClients {
		if v.voiceID == r.Invite.Voice && v.session.ID == r.Session && v.callbackPolicy == r.ID && h.pairedTaskDevice(c) == r.Recipient.Device && callbackBinding(c) == r.Recipient.Binding {
			owner = c
			voice = v
			break
		}
	}
	h.voiceMu.Unlock()
	if owner != nil {
		h.stopClientVoice(owner, voice)
	}
}
func (h *Hub) callbackVoiceClosed(call *clientVoiceCall) {
	h.callbackVoiceOutcome(call, "ended", "")
}
func (h *Hub) callbackVoiceFailed(call *clientVoiceCall) {
	h.callbackVoiceOutcome(call, "failed", "語音未接通，未確認朗讀；原任務結果仍保留")
}
func (h *Hub) callbackVoiceOutcome(call *clientVoiceCall, outcome, reason string) {
	if h.callbacks == nil || call.callbackPolicy == "" {
		return
	}
	ctx := context.Background()
	r, found, e := h.callbacks.Get(ctx, call.callbackPolicy)
	if e != nil || !found || r.Invite == nil || r.Invite.Voice != call.voiceID {
		return
	}
	updated, e := h.callbacks.Mutate(ctx, outcome+"_"+r.Invite.ID, outcome+"_"+call.voiceID, r.ID, callbacks.AnyRevision, func(x *callbacks.Record, _ bool) error {
		if x.Invite == nil || x.Invite.ID != r.Invite.ID || x.Invite.Voice != call.voiceID {
			return callbacks.ErrUnavailable
		}
		x.Invite.State = outcome
		x.Invite.Reason = reason
		if !x.Enabled {
			x.State = "disabled"
		} else if x.State != "report_audio_observed" && x.State != "report_delivery_mismatch" {
			if x.Invite.ContentVerified {
				x.State = "report_text_verified"
			} else {
				x.State = "report_pending"
			}
		}
		return nil
	})
	if e == nil {
		h.publishCallbackRecord(updated)
	}
}
