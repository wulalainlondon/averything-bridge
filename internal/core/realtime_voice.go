package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"everything-go/internal/voicemediaipc"
	"log"
	"regexp"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/callbacks"
	"everything-go/internal/clientproto"
	"everything-go/internal/linevoice"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

var voiceIdentifier = regexp.MustCompile(`^[A-Za-z0-9_-]{8,100}$`)

type clientVoiceCall struct {
	systemVolumeCapable   bool
	systemVolumeSequence  uint64
	systemMediaReceipts   map[string]*systemMediaReceipt
	voiceAudioCapable     bool
	voiceAudioSequence    uint64
	voiceAudioReceipts    map[string]*voiceAudioReceipt
	callbackPolicy        string
	callbackReport        string
	callbackReporting     bool
	callbackExpected      string
	callbackSpoken        string
	callbackSpeechSettled bool
	externalMusicCapable  bool
	externalMusicPlatform string
	musicProvider         string
	externalMusic         *externalMusicCall
	mediaObserveCapable   bool
	mediaDeviceModel      string
	mediaGeneration       string
	mediaIPC              *voicemediaipc.Channel
	mediaCapable          bool
	media                 *voiceMediaSession
	handoffCapable        bool
	handoffPeer           string
	handoffAccount        string
	handoffTarget         string
	handoffID             string
	session               *session.Session
	voiceID, requestID    string
	cancel                context.CancelFunc
	stopping              bool
	kind                  string
	line                  *linevoice.Call
}

func voiceEvent(cmd clientproto.Command, state, message string) protocol.RealtimeVoiceEvent {
	return protocol.RealtimeVoiceEvent{Type: "codex_voice_event", SessionID: cmd.SessionID, RequestID: cmd.RequestID, VoiceID: cmd.VoiceID, State: state, Message: message}
}
func (h *Hub) ownsVoice(c *Client, call *clientVoiceCall) bool {
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	return h.voiceClients[c] == call
}

func (h *Hub) handleVoiceStart(c *Client, cmd clientproto.Command) {
	if cmd.VoiceMode == "line" {
		h.handleLineVoiceStart(c, cmd)
		return
	}
	if cmd.VoiceMode != "" && cmd.VoiceMode != "codex" {
		c.enqueueEvent(voiceEvent(cmd, "error", "不支援的通話模式"))
		return
	}
	if !backend.ValidRealtimeVoiceName(cmd.VoiceName) {
		c.enqueueEvent(voiceEvent(cmd, "error", "此音色不支援目前的語音版本"))
		return
	}
	if !voiceIdentifier.MatchString(cmd.VoiceID) || !voiceIdentifier.MatchString(cmd.RequestID) || len(cmd.SDP) > 64*1024 || !safeMediaLabel(cmd.VoiceDeviceModel) {
		c.enqueueEvent(voiceEvent(cmd, "error", "語音請求格式無效"))
		return
	}
	s, ok := h.registry.Get(cmd.SessionID)
	if !ok || s.Backend() != backend.Codex || s.ResumeID() == "" || h.cfg.CodexRemote == "" {
		c.enqueueEvent(voiceEvent(cmd, "error", "此對話沒有可使用的 Codex thread"))
		return
	}
	if cmd.ThreadID != "" && cmd.ThreadID != s.ResumeID() {
		c.enqueueEvent(voiceEvent(cmd, "error", "對話對應已變更，請重新開啟後再連線"))
		return
	}
	if !h.controls.MobileMayWrite(s.ID) {
		c.enqueueEvent(voiceEvent(cmd, "error", "此對話由桌面控制，請先取回手機控制权"))
		return
	}
	exec, ok := h.exec.(backend.RealtimeVoiceExecutor)
	if !ok {
		c.enqueueEvent(voiceEvent(cmd, "error", "此 Bridge 不支援雙向語音"))
		return
	}
	if !h.consumeVoicePermit(c, s, cmd) {
		c.enqueueEvent(voiceEvent(cmd, "error", "通話準備已失效，請從原對話重新開始"))
		return
	}
	if err := h.bindCallbackVoice(context.Background(), c, s, cmd); err != nil {
		c.enqueueEvent(voiceEvent(cmd, "error", err.Error()))
		return
	}
	parent := c.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	call := &clientVoiceCall{callbackPolicy: cmd.CallbackID, mediaCapable: cmd.VoiceMediaCapable, handoffCapable: cmd.LineHandoffCapable, session: s, voiceID: cmd.VoiceID, requestID: cmd.RequestID, cancel: cancel}
	call.mediaObserveCapable = cmd.VoiceMediaObserveCapable
	call.externalMusicCapable = cmd.VoiceExternalMusicCapable
	call.voiceAudioCapable = cmd.VoiceAudioCapable
	call.systemVolumeCapable = cmd.VoiceSystemVolumeCapable
	h.voiceMu.Lock()
	call.externalMusicPlatform = h.mediaEndpoint(c).Platform
	h.voiceMu.Unlock()
	if call.externalMusicCapable {
		if cmd.VoiceMusicProvider == "ytmusic_android" {
			call.musicProvider = "ytmusic_android"
		}
		if (cmd.VoiceMusicProvider == "ytmusic_ios" || cmd.VoiceMusicProvider == "apple_music_ios") && call.externalMusicPlatform == "ios" {
			call.musicProvider = cmd.VoiceMusicProvider
		}
	}
	call.mediaDeviceModel = cmd.VoiceDeviceModel
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		cancel()
		c.enqueueEvent(voiceEvent(cmd, "error", "無法建立媒體通話識別"))
		return
	}
	call.mediaGeneration = hex.EncodeToString(nonce)
	h.voiceMu.Lock()
	if h.voiceClients == nil {
		h.voiceClients = map[*Client]*clientVoiceCall{}
	}
	if h.voiceClients[c] != nil {
		h.voiceMu.Unlock()
		cancel()
		c.enqueueEvent(voiceEvent(cmd, "error", "請先結束上一個語音對話"))
		return
	}
	h.voiceClients[c] = call
	h.voiceMu.Unlock()
	mediaInstructions, mediaError := h.openVoiceMediaLocal(c, call)
	if call.externalMusicPlatform == "android" {
		mediaInstructions += voiceControlInstructions()
	}
	if mediaError != nil {
		log.Printf("[voice-media] private local control unavailable")
	}
	speakerInstructions := ""
	if call.callbackPolicy != "" {
		r, found, e := h.callbacks.Get(context.Background(), call.callbackPolicy)
		if e != nil || !found || r.Report == nil {
			h.stopClientVoice(c, call)
			return
		}
		call.callbackExpected = callbacks.Speech(r.Name, r.Report.Status, r.Report.Text)
		speakerInstructions = callbacks.SpeakerPrompt(call.callbackExpected)
	}
	if h.lineControl != nil {
		mediaInstructions += "\n" + h.lineControl.instructions()
	}
	c.enqueueEvent(voiceEvent(cmd, "starting", ""))
	go func() {
		defer cancel()
		answer, err := exec.StartRealtimeVoice(ctx, s, backend.RealtimeVoiceStart{SpeakerInstructions: speakerInstructions, MediaGeneration: call.mediaGeneration, MediaInstructions: mediaInstructions, VoiceID: cmd.VoiceID, VoiceName: cmd.VoiceName, SDP: cmd.SDP, OnEvent: func(event backend.RealtimeVoiceEvent) {
			if !c.live() || !h.ownsVoice(c, call) {
				return
			}
			out := voiceEvent(cmd, event.State, event.Message)
			h.observeCallbackSpeech(call, event)
			out.Text = event.Text
			out.Role = event.Role
			out.ThreadID = s.ResumeID()
			if event.State == "closed" {
				go h.callbackVoiceClosed(call)
				h.closeVoiceMediaLocal(call)
				h.voiceMu.Lock()
				if h.voiceClients[c] == call {
					delete(h.voiceClients, c)
				}
				h.voiceMu.Unlock()
			}
			// Publish terminal only after releasing the exact server voice owner.
			c.enqueueEvent(out)
		}})
		if !h.ownsVoice(c, call) {
			cleanup, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			_ = exec.StopRealtimeVoice(cleanup, s, call.voiceID)
			return
		}
		if err != nil {
			go h.callbackVoiceFailed(call)
			h.closeVoiceMediaLocal(call)
			h.voiceMu.Lock()
			if h.voiceClients[c] == call {
				delete(h.voiceClients, c)
			}
			h.voiceMu.Unlock()
			if c.live() {
				c.enqueueEvent(voiceEvent(cmd, "error", err.Error()))
			}
			return
		}
		if answer.ThreadID != s.ResumeID() || answer.VoiceID != call.voiceID {
			h.stopClientVoice(c, call)
			c.enqueueEvent(voiceEvent(cmd, "error", "語音回應與目前對話不一致"))
			return
		}
		if !c.live() {
			h.stopClientVoice(c, call)
			return
		}
		out := voiceEvent(cmd, "answer", "")
		out.ThreadID = answer.ThreadID
		out.SDP = answer.SDP
		c.enqueueEvent(out)
	}()
}
func (h *Hub) stopClientVoice(c *Client, call *clientVoiceCall) {
	h.voiceMu.Lock()
	if call.stopping {
		h.voiceMu.Unlock()
		return
	}
	call.stopping = true
	h.voiceMu.Unlock()
	call.cancel()
	h.closeVoiceMediaLocal(call)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		var err error
		if call.kind == "line" {
			h.voiceMu.Lock()
			line := call.line
			h.voiceMu.Unlock()
			if line != nil {
				line.Close()
				err = line.WaitClosed(ctx)
			}
		} else if exec, ok := h.exec.(backend.RealtimeVoiceExecutor); ok {
			err = exec.StopRealtimeVoice(ctx, call.session, call.voiceID)
		}
		h.voiceMu.Lock()
		call.stopping = false
		if err == nil && h.voiceClients[c] == call {
			delete(h.voiceClients, c)
		}
		h.voiceMu.Unlock()
		if err == nil {
			go h.callbackVoiceClosed(call)
		}
		if c.live() {
			cmd := clientproto.Command{SessionID: call.session.ID, RequestID: call.requestID, VoiceID: call.voiceID}
			if err != nil {
				c.enqueueEvent(voiceEvent(cmd, "error", "未能確認語音已結束，請再次點結束語音"))
			} else {
				c.enqueueEvent(voiceEvent(cmd, "closed", ""))
			}
		}
	}()
}
func (h *Hub) cleanupClientVoice(c *Client) {
	h.voiceMu.Lock()
	call := h.voiceClients[c]
	h.voiceMu.Unlock()
	if call != nil {
		h.stopClientVoice(c, call)
	}
}
func (h *Hub) handleVoiceStop(c *Client, cmd clientproto.Command) {
	h.voiceMu.Lock()
	call := h.voiceClients[c]
	h.voiceMu.Unlock()
	if call == nil {
		c.enqueueEvent(voiceEvent(cmd, "closed", ""))
		return
	}
	if call.voiceID != cmd.VoiceID || call.session.ID != cmd.SessionID {
		c.enqueueEvent(voiceEvent(cmd, "error", "語音停止請求不屬於目前連線"))
		return
	}
	c.enqueueEvent(voiceEvent(cmd, "stopping", ""))
	h.stopClientVoice(c, call)
}
func (h *Hub) handleVoiceText(c *Client, cmd clientproto.Command) {
	h.voiceMu.Lock()
	call := h.voiceClients[c]
	h.voiceMu.Unlock()
	if call == nil || call.kind == "line" || call.voiceID != cmd.VoiceID || call.session.ID != cmd.SessionID || !h.controls.MobileMayWrite(cmd.SessionID) {
		c.enqueueEvent(voiceEvent(cmd, "error", "語音對話已失效或沒有寫入權限"))
		return
	}
	exec, ok := h.exec.(backend.RealtimeVoiceExecutor)
	if !ok {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
		defer cancel()
		if err := exec.AppendRealtimeVoiceText(ctx, call.session, call.voiceID, cmd.Content); err != nil && c.live() {
			c.enqueueEvent(voiceEvent(cmd, "error", "語音文字未能送出；不會自動重送"))
		}
	}()
}
