package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"time"
)

// Enter with voiceMu locked. Inspection never creates an operation, changes its
// sequence, starts playback or migrates to another socket after reconnect.
func (h *Hub) inspectMediaLocked(ctx context.Context, owner *Client, call *clientVoiceCall, m *voiceMediaSession) (any, error) {
	r := m.current
	if !call.mediaObserveCapable || time.Now().UnixMilli()-m.observedAt < 2000 {
		result := mediaResult(m, r)
		h.voiceMu.Unlock()
		return result, nil
	}
	if m.observation == nil {
		m.observation = make(chan struct{})
	}
	observed := m.observation
	event := voiceEvent(clientproto.Command{SessionID: call.session.ID, RequestID: call.requestID, VoiceID: call.voiceID}, "media", "")
	event.ThreadID = call.session.ResumeID()
	event.Media = &protocol.VoiceMediaCommand{Action: "observe", CommandID: r.result.CommandID, Sequence: r.result.Sequence, VideoID: m.video, Title: m.title, Volume: m.volume}
	h.voiceMu.Unlock()
	owner.enqueueEvent(event)
	timer := time.NewTimer(800 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-observed:
	case <-timer.C:
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if h.voiceClients[owner] != call || call.stopping {
		var endpoint *backend.MediaEndpoint
		if r.result.Endpoint != nil {
			copy := *r.result.Endpoint
			copy.Verification = "retired_call"
			endpoint = &copy
		}
		return backend.VoiceMediaResult{State: "voice_ended", AudioCheck: "stale", CommandID: r.result.CommandID, Sequence: r.result.Sequence, Endpoint: endpoint}, nil
	}
	return mediaResult(m, r), nil
}
