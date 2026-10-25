package goexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"io"
	"strings"
	"time"
)

func (c *Codex) applyVoiceMediaTools(s *session.Session, params map[string]any) {
	p, ok := c.controlProvider.(backend.VoiceMediaProvider)
	if !ok || !p.VoiceMediaEnabled() || s == nil || strings.HasPrefix(s.ID, "s_dg_") {
		return
	}
	fields := map[string]any{"url": map[string]any{"type": "string", "maxLength": 2048, "description": "A real YouTube video URL found through web search or supplied by the user. Never invent a video ID; do not use a search-results URL."}, "title": map[string]any{"type": "string", "maxLength": 200}, "action": map[string]any{"type": "string", "enum": []string{"pause", "resume", "stop", "volume"}}, "volume": map[string]any{"type": "integer", "minimum": 0, "maximum": 100}}
	fields["intent"] = map[string]any{"type": "string", "pattern": "^vop_[A-Za-z0-9_-]{8,100}$", "description": "Original intent ID. Preserve it across retries; never switch transports to retry an uncertain effect."}
	defs := []struct {
		name, description string
		props, required   []string
	}{
		{"play_youtube", "Play user-requested YouTube music in the visible official player alongside this exact active voice call. First find a real video URL using web search if needed. queued is not playing: playback_confirmed=true requires a fresh device PLAYING report. For autoplay_blocked ask the user to tap Play once; do not blindly resend or claim it is audible. Stops/pause on background or voice end.", []string{"url", "title", "volume"}, []string{"url"}},
		{"control", "Control music in the current voice call: pause, resume, stop or volume0-100. Changes music only, never the AI voice or another device. Observed status is authoritative; a queued instruction is not playback proof.", []string{"action", "volume"}, []string{"action"}},
		{"status", "Read the exact active voice endpoint's current YouTube playback state. playback_confirmed means a fresh device PLAYING observation, not proof the human heard it. Stale observations are unverified. If the user hears no music, inspect audio_check and audio; player_muted and system media volume/mute are separate from PLAYING. enabled_output_unverified is not proof of human audibility. No playback effect.", nil, nil}}
	tools := []map[string]any{}
	for _, d := range defs {
		props := map[string]any{}
		for _, name := range d.props {
			props[name] = fields[name]
		}
		if d.name != "status" {
			props["intent"] = fields["intent"]
		}
		required := d.required
		if required == nil {
			required = []string{}
		}
		tools = append(tools, map[string]any{"type": "function", "name": d.name, "description": d.description, "inputSchema": map[string]any{"type": "object", "additionalProperties": false, "properties": props, "required": required}})
	}
	existing, _ := params["dynamicTools"].([]map[string]any)
	params["dynamicTools"] = append(existing, map[string]any{"type": "namespace", "name": "bridge_media", "description": "User-requested YouTube playback on the exact active voice device, with observed player-state receipts. Uses the official visible player; never extracts audio.", "tools": tools})
}
func (c *Codex) handleVoiceMediaTool(id any, method string, raw json.RawMessage) bool {
	if method != "item/tool/call" {
		return false
	}
	var call struct {
		ThreadID, Namespace, Tool, CallID, TurnID string
		Arguments                                 json.RawMessage
	}
	if json.Unmarshal(raw, &call) != nil || call.Namespace != "bridge_media" {
		return false
	}
	p, ok := c.controlProvider.(backend.VoiceMediaProvider)
	if !ok {
		return false
	}
	s := c.sessionForCodexParams(codexParams(raw))
	go func() {
		caller, err := c.sessionControlCaller(s, call.TurnID, call.CallID)
		var result any
		if err == nil {
			if len(call.Arguments) > 4096 {
				err = errors.New("voice_media_arguments_invalid")
			} else {
				var input backend.VoiceMediaRequest
				d := json.NewDecoder(bytes.NewReader(call.Arguments))
				d.DisallowUnknownFields()
				err = d.Decode(&input)
				if err == nil && d.Decode(&struct{}{}) != io.EOF {
					err = errors.New("voice_media_arguments_invalid")
				}
				if err == nil {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					if unified, ok := p.(backend.UnifiedVoiceMediaProvider); ok && unified.UnifiedVoiceMediaEnabled() {
						intent := input.IntentID
						input.IntentID = ""
						if intent == "" {
							intent = fmt.Sprintf("vop_native_%x", sha256.Sum256([]byte(call.ThreadID+"|"+call.CallID)))
						}
						var origin backend.MediaOrigin
						for {
							origin, err = c.mediaOriginForTurn(call.ThreadID, call.TurnID)
							if err == nil || ctx.Err() != nil {
								break
							}
							select {
							case <-ctx.Done():
							case <-time.After(25 * time.Millisecond):
							}
						}
						if err == nil {
							result, err = unified.ExecuteScopedVoiceMedia(ctx, caller, origin, intent, call.Tool, input)
						}
					} else {
						result, err = p.ExecuteVoiceMedia(ctx, caller, call.Tool, input)
					}
					cancel()
				}
			}
		}
		output := ""
		if err != nil {
			output = err.Error()
		} else {
			value, _ := json.Marshal(result)
			output = string(value)
		}
		if s != nil {
			c.tools.Start(s.ID, caller.RequestID, call.CallID, "bridge_media__"+call.Tool, "[voice media arguments omitted]")
			c.tools.Result(s.ID, caller.RequestID, call.CallID, output)
			c.tools.End(s.ID, caller.RequestID, call.CallID)
		}
		_ = c.rpc.write(map[string]any{"id": id, "result": map[string]any{"success": err == nil, "contentItems": []map[string]any{{"type": "inputText", "text": output}}}})
	}()
	return true
}
