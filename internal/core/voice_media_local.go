package core

import (
	"context"
	"errors"
	"everything-go/internal/voicemediaipc"
	"fmt"
	"os"
	"strings"
)

func shellArgument(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
func (h *Hub) openVoiceMediaLocal(c *Client, call *clientVoiceCall) (string, error) {
	if !call.mediaCapable {
		return "", nil
	}
	if h.unifiedMedia != nil {
		instructions := h.unifiedMedia.instructions()
		if call.externalMusicCapable && call.musicProvider == "apple_music_ios" {
			instructions += `
This original iOS call selected native Apple Music. Use the SAME installed bridge-media entry with --method play_music --url VERIFIED_HTTPS_MUSIC_APPLE_COM_SONG_OR_ALBUM_TRACK_URL --title EXACT_TITLE. Resolve a real Apple Music recording first; never invent a catalog ID. Album links must identify the exact song with ?i=SONG_ID. For native pause/resume/stop/next/previous/seek use --method music_control --action ACTION (seek needs --position-ms). Status observes actual system Music player state on this original device and never repeats an effect. Permission or subscription required means the user must authorize in foreground settings or use their existing eligible account; never authorize or buy automatically. Controls can reach the native runtime during an active background voice call, but background/locked coexistence still requires real device validation. Playback confirmed means fresh player state and the exact song ID for requested songs, not human audibility. A single-song queue may have no next/previous item; unconfirmed is not success. No Music volume setter, route forcing, other-device fallback or Siri/Shortcuts/Mac automation. On uncertain operation observe the original intent only.
`
		} else if call.externalMusicCapable && call.externalMusicPlatform == "ios" {
			instructions += `
This iOS call supports official YouTube Music song handoff through the SAME installed bridge-media entry. For a user-requested official Music song, resolve a real canonical https://music.youtube.com/watch?v=VIDEO_ID recording first, then use --method play_music --url REAL_MUSIC_URL --title EXACT_TITLE. The exact current native call opens only the official App via a universal link, with no browser or embedded-player fallback. handoff_opened=true proves the official App accepted the link, NOT that playback is confirmed. iOS exposes no public generic cross-app now-playing observation; playback_confirmed remains false and state unverified. Never label handoff_opened as audible playback. Official background/locked continuity belongs to the official App and original membership. The visible embed foreground restriction applies only to play_youtube. Official iOS pause/resume/stop/next/previous/seek/query/volume controls are unsupported; do not substitute Android MediaSession or another device. foreground_required requires the user to bring Averything forward/unlock; do not replay automatically. A status query observes only the original handoff receipt and never repeats opening. No OS volume/route changes.
`
		}
		if call.externalMusicCapable && call.externalMusicPlatform != "ios" {
			instructions += `
This Android call also supports official YouTube Music app control. For a NEW requested song, first resolve a real official recording through public web search (prefer its official artist/Topic release), then use the SAME installed bridge-media entry with --method play_music --url REAL_HTTPS_MUSIC_YOUTUBE_WATCH_URL --title EXACT_EXPECTED_TITLE [--artist EXACT_EXPECTED_ARTIST]. Never invent a video ID or use a raw audio stream URL. Use the canonical music.youtube.com/watch?v=VIDEO_ID for the verified recording. Match the full official title and artist; do not shorten an artist credit to a guessed alias. Direct --query search playback is best-effort and has been observed to be ignored by the official app despite its advertised action flag; do not use --query as the normal song-switch path. Resolve the URL BEFORE the first playback dispatch. If an already-dispatched query is unconfirmed, observe the original intent only; do not silently retry it with a URL, another ID, ADB or another transport. A fresh requested song or an explicitly authorized new diagnostic is a separate intent. Do not supply --volume for this provider. For pause/resume/stop/next/previous use --method music_control --action ACTION. --method status [--operation ORIGINAL_INTENT] remains read-only and never replays. Capable Android clients can attempt the official song link once when no session exists. music_open_required/needs_user_action means the user must tap the current call notification or native controls to open Music; never issue a second play command to work around it. Opening the official App is not playback, and launch/playback uncertainty must only be observed by the original intent. No default playlist or startup autoplay is implied. Official Music must expose a supported native media session and the user must grant notification-listener access. Unsupported, session unavailable or permission-required results are refusals, never playback. A match requires the correct track plus fresh native state; audibility remains unverified. Do not use private APIs, cookies, ADB or another endpoint as a fallback. The visible-player foreground rule applies to play_youtube only; official Music background support is owned by the official App and membership. No automatic route/system volume changes.
`
			if call.musicProvider == "ytmusic_android" {
				instructions += "The user selected official YouTube Music for this call: prefer play_music for new music requests. Existing iframe play_youtube is a separate explicitly chosen source.\n"
			}
		}
		return instructions, nil
	}
	thread := call.session.ResumeID()
	voice := call.voiceID
	channel, err := voicemediaipc.Open(thread, voice, func(ctx context.Context, request voicemediaipc.Request) (any, error) {
		h.voiceMu.Lock()
		current := h.voiceClients[c] == call && !call.stopping && call.session.ResumeID() == thread && c.live()
		h.voiceMu.Unlock()
		if !current {
			return nil, errors.New("voice_media_active_voice_required")
		}
		return h.executeVoiceMedia(ctx, call.session, voice, call.requestID, request.ID, "local_user_socket", call, request.Method, request.Input)
	})
	if err != nil {
		return "", err
	}
	h.voiceMu.Lock()
	if h.voiceClients[c] != call || call.stopping {
		h.voiceMu.Unlock()
		channel.Close()
		return "", errors.New("voice_media_active_voice_required")
	}
	call.mediaIPC = channel
	h.voiceMu.Unlock()
	binary, err := os.Executable()
	if err != nil {
		channel.Close()
		return "", err
	}
	command := shellArgument(binary) + " voice-media --socket " + shellArgument(channel.Path) + " --thread " + shellArgument(thread) + " --voice " + shellArgument(voice)
	return fmt.Sprintf(`For this exact voice call, YouTube music playback is available through the existing exec_command tool, even if this older conversation has no bridge_media tool namespace. When the user requests music, delegate to the backing agent and have it execute this local control CLI. No extra namespace installation or new conversation is needed. Use this CLI as the primary control path for the call; do not switch transports to retry an uncertain playback intent.
Base command: %s
Append --id vop_UNIQUE_INTENT --method play_youtube --url REAL_HTTPS_YOUTUBE_VIDEO_URL --title LABEL [--volume 0..100]. First use web search to find a real video URL, or use the user's supplied URL; never invent a video ID.
For music controls append --id vop_UNIQUE_INTENT --method control --action pause|resume|stop|volume [--volume 0..100]. For observations append --id vop_UNIQUE_QUERY --method status.
Each distinct intent needs a unique vop_ ID with8-100 letters/digits/underscore/hyphen after vop_. Keep the original ID on a same-intent retry. A lost result must be checked using status, not blindly replayed as a new intent. Do not print the private command recipe or socket path to the user.
The local transport is local_user_socket, distinct from native_tool. Results are JSON. queued/observed/playing do not prove the user heard audio; only playback_confirmed=true is a fresh actual device PLAYING report. For autoplay_blocked, ask the user to tap the official player's Play once. After a queued result, check status before reporting success. If the user hears no music, inspect audio_check and audio: player_muted, system_media_muted and system_media_volume. A nonzero player volume does not prove unmuted. voice_route describes the voice call, not the actual music output. enabled_output_unverified means gains appear enabled, never proof of audibility. Do not automatically raise OS volume or force routes. Use the visible official player only; foreground is required. Do not route Spotify or LINE through this channel. These instructions add no filesystem/network permissions; if execution is unavailable, report the actual failure. The channel expires with this exact call and cannot be reused on another voice.`, command), nil
}
func (h *Hub) closeVoiceMediaLocal(call *clientVoiceCall) {
	h.voiceMu.Lock()
	channel := call.mediaIPC
	h.voiceMu.Unlock()
	if channel != nil {
		channel.Close()
	}
}
