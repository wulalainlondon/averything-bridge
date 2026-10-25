package core

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var youtubeVideoID = regexp.MustCompile(`^[A-Za-z0-9_-]{11}$`)

func youtubeID(raw string) (string, error) {
	if len(raw) > 2048 {
		return "", errors.New("voice_media_url_invalid")
	}
	u, e := url.Parse(strings.TrimSpace(raw))
	if e != nil || u.Scheme != "https" || u.User != nil || (u.Port() != "" && u.Port() != "443") {
		return "", errors.New("voice_media_youtube_https_required")
	}
	var id string
	switch strings.ToLower(u.Hostname()) {
	case "youtu.be":
		id = strings.TrimPrefix(u.Path, "/")
	case "youtube.com", "www.youtube.com", "m.youtube.com":
		if u.Path == "/watch" {
			if len(u.Query()["v"]) != 1 {
				return "", errors.New("voice_media_video_required")
			}
			id = u.Query().Get("v")
		} else {
			for _, prefix := range []string{"/embed/", "/shorts/", "/live/"} {
				if strings.HasPrefix(u.Path, prefix) {
					id = strings.TrimPrefix(u.Path, prefix)
					break
				}
			}
		}
	default:
		return "", errors.New("voice_media_youtube_url_required")
	}
	if !youtubeVideoID.MatchString(id) {
		return "", errors.New("voice_media_video_required")
	}
	return id, nil
}

type voiceMediaReceipt struct {
	fingerprint   string
	desiredVolume int
	result        backend.VoiceMediaResult
	settled       chan struct{}
	closed        bool
}
type voiceMediaSession struct {
	observation                    chan struct{}
	audio                          *protocol.MediaAudioInfo
	sequence                       uint64
	current                        *voiceMediaReceipt
	receipts                       map[string]*voiceMediaReceipt
	video, title, state, errorCode string
	volume                         int
	observedAt                     int64
}

func (h *Hub) VoiceMediaEnabled() bool { return true }
func mediaResult(m *voiceMediaSession, r *voiceMediaReceipt) backend.VoiceMediaResult {
	result := r.result
	if r == m.current {
		result.State = m.state
		result.Volume = m.volume
		result.LastObservedMS = m.observedAt
		result.Observed = m.observedAt > 0
		result.ErrorCode = m.errorCode
		result.Audio = m.audio
	}
	if r == m.current && m.observedAt > 0 {
		result.LastObservedState = m.state
	}
	age := time.Now().UnixMilli() - result.LastObservedMS
	if result.LastObservedMS > 0 {
		result.ObservationAgeMS = &age
	}
	fresh := result.Observed && result.LastObservedMS > 0 && age >= 0 && age <= 30000
	if result.Endpoint != nil {
		endpoint := *result.Endpoint
		endpoint.Verification = "bound_current_call"
		if fresh && result.Audio != nil && result.Audio.DeviceModel != "" {
			endpoint.Model = result.Audio.DeviceModel
		}
		result.Endpoint = &endpoint
	}
	result.PlaybackConfirmed = result.State == "playing" && fresh && result.ErrorCode == ""
	result.ActionConfirmed = false
	if fresh && result.ErrorCode == "" {
		switch result.Action {
		case "play", "resume":
			result.ActionConfirmed = result.State == "playing"
		case "pause":
			result.ActionConfirmed = result.State == "paused"
		case "stop":
			result.ActionConfirmed = result.State == "stopped"
		case "volume":
			result.ActionConfirmed = result.State != "queued" && result.State != "superseded" && result.Volume == r.desiredVolume
		}
	}

	result.AudioCheck = "unknown"
	if fresh && result.Audio != nil {
		a := result.Audio
		switch {
		case a.PlayerMuted != nil && *a.PlayerMuted:
			result.AudioCheck = "player_muted"
		case result.Volume == 0:
			result.AudioCheck = "player_volume_zero"
		case a.SystemMediaMuted != nil && *a.SystemMediaMuted:
			result.AudioCheck = "system_media_muted"
		case a.SystemMediaVolume != nil && *a.SystemMediaVolume == 0:
			result.AudioCheck = "system_media_volume_zero"
		case a.AudioProfile == "bluetooth_media" && a.SharedRouteVerified != nil && *a.SharedRouteVerified:
			result.AudioCheck = "shared_media_output_unverified"
		case a.AudioProfile == "bluetooth_media":
			result.AudioCheck = "shared_media_route_unverified"
		case a.CommunicationProfile == "bluetooth_hfp":
			result.AudioCheck = "bluetooth_call_media_unverified"
		case a.VoiceRoute == "bluetooth" && a.CommunicationProfile == "":
			result.AudioCheck = "bluetooth_media_route_unknown"
		case a.PlayerMuted != nil && a.SystemMediaVolume != nil && a.SystemMediaMuted != nil:
			result.AudioCheck = "enabled_output_unverified"
		}
	}
	if !fresh {
		result.Audio = nil
	}
	result.NeedsUserAction = result.ErrorCode == "autoplay_blocked" || result.ErrorCode == "foreground_required"
	if result.Observed && !fresh {
		result.Audio = nil
		result.AudioCheck = "stale"
		result.State = "unverified"
		result.Observed = false
	}
	return result
}
func (h *Hub) ExecuteVoiceMedia(ctx context.Context, caller backend.SessionControlCaller, method string, input backend.VoiceMediaRequest) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if caller.Parent == nil || caller.ToolCallID == "" || caller.VoiceID == "" {
		return nil, errors.New("voice_media_active_voice_required")
	}
	return h.executeVoiceMedia(ctx, caller.Parent, caller.VoiceID, caller.RequestID, caller.ToolCallID, "native_tool", nil, method, input)
}

// local_user_socket authority is the private per-call OS-user channel, never a
// fabricated SessionControlCaller. Both transports share the same scope checks.
func (h *Hub) executeVoiceMedia(ctx context.Context, parent *session.Session, voiceID, requestID, operationID, transport string, expectedCall *clientVoiceCall, method string, input backend.VoiceMediaRequest) (any, error) {
	if method == "media_volume" {
		return h.executeSystemMediaVolume(ctx, parent, voiceID, requestID, operationID, transport, expectedCall, input)
	}
	if method == "voice_volume" {
		return h.executeVoiceVolume(ctx, parent, voiceID, requestID, operationID, transport, expectedCall, input)
	}
	if method == "play_music" || method == "music_control" {
		return h.executeExternalMusic(ctx, parent, voiceID, requestID, operationID, transport, expectedCall, method, input)
	}
	if method == "status" || method == "control" {
		h.voiceMu.Lock()
		external := false
		for c, v := range h.voiceClients {
			if c.live() && v.session == parent && v.voiceID == voiceID && (v.musicProvider == "ytmusic_android" || v.musicProvider == "ytmusic_ios" || v.musicProvider == "apple_music_ios") {
				external = true
				break
			}
		}
		h.voiceMu.Unlock()
		if external {
			mapped := method
			if method == "control" {
				mapped = "music_control"
			}
			return h.executeExternalMusic(ctx, parent, voiceID, requestID, operationID, transport, expectedCall, mapped, input)
		}
	}
	// Use only on the validation path below. Once enqueueEvent is reached, an
	// error cannot prove that the device did not receive the command.
	refuse := func(err error) (any, error) {
		return nil, &backend.VoiceMediaNotDispatchedError{Cause: err}
	}
	if err := ctx.Err(); err != nil {
		return refuse(err)
	}
	s, ok := h.registry.Get(parent.ID)
	if !ok || s != parent || !h.controls.MobileMayWrite(s.ID) {
		return refuse(errors.New("voice_media_caller_not_owned"))
	}
	if input.Query != "" || input.Artist != "" || input.PositionMS != nil || input.Delta != nil {
		return refuse(errors.New("voice_media_arguments_invalid"))
	}
	action := input.Action
	video := ""
	switch method {
	case "play_youtube":
		if action != "" {
			return refuse(errors.New("voice_media_arguments_invalid"))
		}
		action = "play"
		var err error
		video, err = youtubeID(input.URL)
		if err != nil {
			return refuse(err)
		}
	case "control":
		switch action {
		case "pause", "resume", "stop", "volume":
		default:
			return refuse(errors.New("voice_media_action_invalid"))
		}
		if input.URL != "" || input.Title != "" {
			return refuse(errors.New("voice_media_arguments_invalid"))
		}
	case "status":
		if input.Action != "" || input.URL != "" || input.Title != "" || input.Volume != nil {
			return refuse(errors.New("voice_media_arguments_invalid"))
		}
	default:
		return refuse(errors.New("voice_media_tool_unknown"))
	}
	if !utf8.ValidString(input.Title) || len([]rune(input.Title)) > 200 {
		return refuse(errors.New("voice_media_title_invalid"))
	}
	if input.Volume != nil && (*input.Volume < 0 || *input.Volume > 100) {
		return refuse(errors.New("voice_media_volume_invalid"))
	}
	if action == "volume" && input.Volume == nil {
		return refuse(errors.New("voice_media_volume_required"))
	}
	h.voiceMu.Lock()
	var owner *Client
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		if v.session == s && v.voiceID == voiceID && c.live() {
			owner = c
			call = v
			break
		}
	}
	if call == nil || call.stopping || call.kind == "line" || (expectedCall != nil && call != expectedCall) {
		h.voiceMu.Unlock()
		return refuse(errors.New("voice_media_active_voice_required"))
	}
	if !call.mediaCapable {
		result := backend.VoiceMediaResult{Endpoint: h.mediaEndpoint(owner), State: "failed", AudioCheck: "unknown", ErrorCode: "voice_media_player_not_ready"}
		h.voiceMu.Unlock()
		return result, &backend.VoiceMediaNotDispatchedError{Cause: errors.New(result.ErrorCode)}
	}
	if call.media == nil {
		call.media = &voiceMediaSession{receipts: map[string]*voiceMediaReceipt{}, state: "idle", volume: 35}
	}
	m := call.media
	if method == "play_youtube" {
		if (call.musicProvider == "ytmusic_android" || call.musicProvider == "ytmusic_ios" || call.musicProvider == "apple_music_ios") && call.externalMusic != nil && call.externalMusic.current != nil && (call.externalMusic.current.result.State == "playing" || call.externalMusic.current.result.HandoffOpened) {
			h.voiceMu.Unlock()
			return refuse(errors.New("media_provider_switch_requires_stop"))
		}
		call.musicProvider = "youtube"
	}
	if method == "status" {
		if m.current == nil {
			h.voiceMu.Unlock()
			return backend.VoiceMediaResult{State: "idle", Volume: 35, Observed: true, Endpoint: h.mediaEndpoint(owner)}, nil
		}
		return h.inspectMediaLocked(ctx, owner, call, m)
	}
	encoded, _ := json.Marshal([]any{method, input})
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(encoded))
	if old := m.receipts[transport+"|"+operationID]; old != nil {
		if old.fingerprint != fingerprint {
			h.voiceMu.Unlock()
			return refuse(errors.New("voice_media_intent_conflict"))
		}
		result := mediaResult(m, old)
		h.voiceMu.Unlock()
		return result, nil
	}
	if len(m.receipts) >= 64 {
		h.voiceMu.Unlock()
		return refuse(errors.New("voice_media_command_limit"))
	}
	if action != "play" && action != "stop" && m.video == "" {
		h.voiceMu.Unlock()
		return refuse(errors.New("voice_media_no_track"))
	}
	if old := m.current; old != nil {
		old.result.State = "superseded"
		old.result.Observed = false
		if !old.closed {
			old.closed = true
			close(old.settled)
		}
	}
	if action == "play" {
		m.video = video
		m.title = strings.TrimSpace(input.Title)
	}
	if input.Volume != nil {
		m.volume = *input.Volume
	}
	m.sequence++
	m.state = "queued"
	m.errorCode = ""
	m.observedAt = 0
	m.audio = nil
	id := fmt.Sprintf("vm_%x", sha256.Sum256([]byte(h.cfg.InstanceID+"\x00"+s.ID+"\x00"+voiceID+"\x00"+transport+"\x00"+operationID)))
	receipt := &voiceMediaReceipt{fingerprint: fingerprint, desiredVolume: m.volume, settled: make(chan struct{}), result: backend.VoiceMediaResult{Endpoint: h.mediaEndpoint(owner), Transport: transport, Action: action, CommandID: id, Sequence: m.sequence, State: "queued", Title: m.title, VideoURL: "https://www.youtube.com/watch?v=" + m.video, Volume: m.volume}}
	if m.video == "" {
		receipt.result.VideoURL = ""
	}
	m.current = receipt
	m.receipts[transport+"|"+operationID] = receipt
	event := voiceEvent(clientproto.Command{SessionID: s.ID, RequestID: requestID, VoiceID: voiceID}, "media", "")
	event.ThreadID = s.ResumeID()
	event.Media = &protocol.VoiceMediaCommand{Action: action, CommandID: id, Sequence: m.sequence, VideoID: m.video, Title: m.title, Volume: m.volume}
	h.voiceMu.Unlock()
	owner.enqueueEvent(event)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-receipt.settled:
	case <-timer.C:
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	if h.voiceClients[owner] != call || call.stopping {
		return backend.VoiceMediaResult{Action: action, CommandID: id, Sequence: receipt.result.Sequence, State: "voice_ended"}, nil
	}
	return mediaResult(m, receipt), nil
}
func (h *Hub) handleVoiceMediaState(c *Client, cmd clientproto.Command) {
	if cmd.SystemMediaAudio != nil {
		h.handleSystemMediaState(c, cmd)
		return
	}
	if cmd.VoiceAudio != nil {
		h.handleVoiceAudioState(c, cmd)
		return
	}
	if cmd.ExternalMusic != nil {
		h.handleExternalMusicState(c, cmd)
		return
	}
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	call := h.voiceClients[c]
	if call == nil || call.stopping || call.kind == "line" || !call.mediaCapable || call.voiceID != cmd.VoiceID || call.session.ID != cmd.SessionID || call.session.ResumeID() != cmd.ThreadID || !h.controls.MobileMayWrite(cmd.SessionID) || call.media == nil {
		return
	}
	m := call.media
	r := m.current
	if r == nil || r.result.CommandID != cmd.MediaCommandID || r.result.Sequence != cmd.MediaSequence || cmd.MediaVideoID != m.video {
		return
	}
	switch cmd.MediaState {
	case "playing", "paused", "buffering", "cued", "blocked", "error", "stopped", "ended":
	default:
		return
	}
	switch cmd.MediaErrorCode {
	case "", "autoplay_blocked", "foreground_required", "video_not_found", "embed_blocked", "client_identity", "player_error", "network_error", "audio_route_unverified":
	default:
		return
	}
	if a := cmd.MediaAudio; a != nil {
		if !validMediaAudio(a) {
			return
		}
		if a.SystemMediaVolume != nil && (*a.SystemMediaVolume < 0 || *a.SystemMediaVolume > 100) {
			return
		}
		switch a.VoiceRoute {
		case "", "speaker", "earpiece", "wired", "bluetooth", "unknown":
		default:
			return
		}
	}
	if cmd.MediaVolume != nil && (*cmd.MediaVolume < 0 || *cmd.MediaVolume > 100) {
		return
	}
	if cmd.MediaVolume != nil {
		m.volume = *cmd.MediaVolume
	}
	m.audio = cmd.MediaAudio
	m.state = cmd.MediaState
	m.errorCode = cmd.MediaErrorCode
	m.observedAt = time.Now().UnixMilli()
	if m.observation != nil {
		close(m.observation)
		m.observation = nil
	}
	if m.state == "stopped" {
		m.video = ""
		m.title = ""
	}
	r.result.Audio = m.audio
	if m.audio != nil && m.audio.DeviceModel != "" && r.result.Endpoint != nil {
		endpoint := *r.result.Endpoint
		endpoint.Model = m.audio.DeviceModel
		r.result.Endpoint = &endpoint
	}
	r.result.State = m.state
	r.result.LastObservedState = m.state
	r.result.ErrorCode = m.errorCode
	r.result.LastObservedMS = m.observedAt
	r.result.Observed = true
	r.result.Volume = m.volume
	if !r.closed && (mediaResult(m, r).ActionConfirmed || m.state == "blocked" || m.state == "error" || m.state == "ended") {
		r.closed = true
		close(r.settled)
	}
}
