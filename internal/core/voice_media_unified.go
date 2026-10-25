package core

import (
	"context"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/mediaservice"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type unifiedVoiceMedia struct {
	store    *mediaservice.Store
	server   *mediaservice.Server
	resolver backend.MediaOriginResolver
	launcher string
}

func (h *Hub) StartUnifiedVoiceMedia(resolver backend.MediaOriginResolver, manifest string) (func(), error) {
	if resolver == nil || h.unifiedMedia != nil {
		return nil, errors.New("media_configuration_invalid")
	}
	store, e := mediaservice.Open(h.cfg.DataDir)
	if e != nil {
		return nil, e
	}
	home, e := os.UserHomeDir()
	if e != nil {
		store.Close()
		return nil, e
	}
	u := &unifiedVoiceMedia{store: store, resolver: resolver, launcher: filepath.Join(home, ".local", "bin", "bridge-media")}
	server, e := mediaservice.OpenServer(h.cfg.DataDir, h.cfg.InstanceID, manifest, func(ctx context.Context, q mediaservice.Request) (mediaservice.Result, error) {
		return h.executeUnifiedMedia(ctx, u, q)
	})
	if e != nil {
		store.Close()
		return nil, e
	}
	u.server = server
	binary, e := os.Executable()
	if e != nil {
		server.Close()
		store.Close()
		return nil, e
	}
	if e = os.MkdirAll(filepath.Dir(u.launcher), 0700); e != nil {
		server.Close()
		store.Close()
		return nil, e
	}
	// Fixed path, atomically replaced alongside the adopted signed release.
	const marker = "# Everything Go managed media entry v1\n"
	if st, err := os.Lstat(u.launcher); err == nil {
		old, readErr := os.ReadFile(u.launcher)
		if !st.Mode().IsRegular() || st.Mode()&os.ModeSymlink != 0 || readErr != nil || !strings.HasPrefix(string(old), "#!/bin/sh\n"+marker) {
			server.Close()
			store.Close()
			return nil, errors.New("media_launcher_conflict")
		}
	} else if !os.IsNotExist(err) {
		server.Close()
		store.Close()
		return nil, err
	}
	script := "#!/bin/sh\n" + marker + "exec " + shellArgument(binary) + " media --manifest " + shellArgument(manifest) + " \"$@\"\n"
	tmp, e := os.CreateTemp(filepath.Dir(u.launcher), ".bridge-media-*")
	if e != nil {
		server.Close()
		store.Close()
		return nil, e
	}
	name := tmp.Name()
	if e = tmp.Chmod(0700); e == nil {
		_, e = tmp.WriteString(script)
	}
	if e == nil {
		e = tmp.Sync()
	}
	tmp.Close()
	if e == nil {
		e = os.Rename(name, u.launcher)
	}
	os.Remove(name)
	if e != nil {
		server.Close()
		store.Close()
		return nil, e
	}
	h.unifiedMedia = u
	return func() { server.Close(); store.Close() }, nil
}

func (h *Hub) executeUnifiedMedia(ctx context.Context, u *unifiedVoiceMedia, q mediaservice.Request) (mediaservice.Result, error) {
	// Native command admission can reach us slightly after process startup.
	var origin backend.MediaOrigin
	var e error
	timer := time.NewTicker(25 * time.Millisecond)
	defer timer.Stop()
	for {
		origin, e = u.resolver.ResolveMediaOrigin(q.Thread, q.ID)
		if e == nil {
			break
		}
		select {
		case <-ctx.Done():
			return mediaservice.Result{}, errors.New("origin_context_unavailable")
		case <-timer.C:
		}
	}
	return h.executeMediaWithOrigin(ctx, u, origin, q, "local_user_socket")
}
func (h *Hub) UnifiedVoiceMediaEnabled() bool { return h.unifiedMedia != nil }
func (h *Hub) ExecuteScopedVoiceMedia(ctx context.Context, caller backend.SessionControlCaller, origin backend.MediaOrigin, intent, method string, input backend.VoiceMediaRequest) (any, error) {
	if h.unifiedMedia == nil || caller.Parent == nil || caller.ToolCallID == "" || caller.TurnID != origin.TurnID || caller.Parent.ID != origin.SessionID || caller.Parent.ResumeID() != origin.ThreadID || !mediaservice.IntentID.MatchString(intent) {
		return nil, errors.New("media_origin_invalid")
	}
	return h.executeMediaWithOrigin(ctx, h.unifiedMedia, origin, mediaservice.Request{ID: intent, Thread: origin.ThreadID, Method: method, Input: input}, "native_tool")
}
func (h *Hub) executeMediaWithOrigin(ctx context.Context, u *unifiedVoiceMedia, origin backend.MediaOrigin, q mediaservice.Request, transport string) (mediaservice.Result, error) {
	s, ok := h.registry.Get(origin.SessionID)
	if !ok || s.ResumeID() != origin.ThreadID || origin.ThreadID != q.Thread || !h.controls.MobileMayWrite(s.ID) {
		return mediaservice.Result{}, errors.New("media_origin_invalid")
	}
	h.voiceMu.Lock()
	var call *clientVoiceCall
	for c, v := range h.voiceClients {
		if c.live() && !v.stopping && v.session == s && v.voiceID == origin.VoiceID && v.mediaGeneration == origin.Generation && v.kind != "line" {
			if call != nil {
				h.voiceMu.Unlock()
				return mediaservice.Result{}, errors.New("media_endpoint_ambiguous")
			}
			call = v
		}
	}
	h.voiceMu.Unlock()
	if call == nil && q.Method != "status" {
		return mediaservice.Result{}, errors.New("media_voice_expired")
	}
	return u.store.Execute(ctx, origin, q, func() (backend.VoiceMediaResult, error) {
		if call == nil {
			return backend.VoiceMediaResult{}, errors.New("media_voice_expired")
		}
		if q.Method == "status" && q.Operation != "" {
			if result, ok := h.systemVolumeReceipt(call, q.Operation); ok {
				return result, nil
			}
			if result, ok := h.voiceVolumeReceipt(call, q.Operation); ok {
				return result, nil
			}
		}
		value, e := h.executeVoiceMedia(ctx, s, origin.VoiceID, call.requestID, q.ID, transport, call, q.Method, q.Input)
		if e != nil {
			result, _ := value.(backend.VoiceMediaResult)
			return result, e
		}
		result, ok := value.(backend.VoiceMediaResult)
		if !ok {
			return result, errors.New("media_result_invalid")
		}
		return result, nil
	})
}

func (u *unifiedVoiceMedia) instructions() string {
	return fmt.Sprintf(`YouTube music control is installed at the fixed local entry point %s, available through the existing exec_command tool. This is the primary control transport for this call, including older native conversations. No dynamic namespace update, per-call socket recipe, thread ID, voice ID or new conversation is required. The service derives the original request from provider-observed command events and refuses unknown/retired origins; do not substitute IDs or route to another device.
When delegating a music action, include this stable entry point and the requested action in the backing-agent instruction so it can execute rather than merely search. Do not claim playback before an actual result. Project instruction hot reload is not required or assumed.
On capable Android media-profile calls, --method media_volume queries phone STREAM_MUSIC volume; --volume 0..100 sets it, --delta -100..100 adjusts once. Use this ONLY for the user's explicit phone/media/Bluetooth output-volume request, never assistant voice requests. Report returned system_media_audio configured/applied percent and mediaIndex/mediaMax; discrete phone steps can round. This can affect both official Music and the media-mode call base output, never changes the route, and does not control independent earbud hardware volume. Ambiguous volume targets require clarification. No automatic increase, retry, route switch or microphone change. On uncertainty use status --operation ORIGINAL_INTENT. Unsupported/fixed/route-unready refusals are not success.
Assistant playback volume is separate: --method voice_volume queries it; --method voice_volume --volume 0..100 sets it; --method voice_volume --delta -100..100 adjusts percentage points once atomically. This affects only this original call assistant AudioTrack gain, never Music, OS volume, microphone or mute state. If user asks to make your voice slightly louder use delta 5; ambiguous volume targets must be clarified. Report only returned voice_audio configured/applied values; 100 is the gain limit. The device must advertise support. For uncertain results use status --operation ORIGINAL_INTENT; never repeat a relative increase under another ID. A volume setting does not prove audible output.
Use the fixed command with --id vop_UNIQUE_INTENT --method play_youtube --url REAL_HTTPS_YOUTUBE_VIDEO_URL --title LABEL [--volume 0..100]. First search for a real video URL or use the user's supplied URL. Never invent a video ID. For controls: --id vop_UNIQUE_INTENT --method control --action pause|resume|stop|volume [--volume 0..100]. For status: --id vop_UNIQUE_QUERY --method status [--operation ORIGINAL_INTENT]. Use a new intent only for a new user action; uncertain results must be checked by original operation, never blindly replayed or retried through another transport. IDs require 8-100 letters/digits/underscore/hyphen after vop_.
Every result identifies the actual bound endpoint (inventory ID/client/platform/model/version). Verify that identity before inspecting any USB device; USB attachment, a device display name or the user's guess does not establish that it owns this call. For stale output, report last_observed_state and observation_age_ms, not current playback. Status can ask that exact capable endpoint for a read-only observation; it never replays a play intent. Bluetooth HFP call audio and media output are distinct. Predicted media routes are not actual routed or audible output. If suggested_action=try_phone_speaker, the user can tap the App recovery button; never switch their OS route automatically. Results are JSON. queued/accepted are not playback success. playback_confirmed=true is a fresh actual device PLAYING report, not proof of audible sound. If silent, inspect audio_check and audio.player_muted/system_media_muted/system_media_volume. voice_route describes the voice call only. enabled_output_unverified is not proof of audibility. Never automatically raise OS volume or force routes. For autoplay_blocked ask to tap the official player's Play once; foreground_required means bring the App to foreground. Use only the visible official YouTube player; no Spotify or LINE routing. If execution fails, report the actual error, not an assumed disconnection.`, shellArgument(u.launcher))
}
