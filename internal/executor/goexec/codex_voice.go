package goexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/session"
	"everything-go/internal/taskapi"
)

type codexVoiceCall struct {
	callbackSpeech               bool
	mediaGeneration              string
	voiceID, threadID, sessionID string
	answer                       chan backend.RealtimeVoiceAnswer
	failure                      chan error
	closed                       chan struct{}
	once                         sync.Once
	callback                     func(backend.RealtimeVoiceEvent)
}

// Exact restoration plus owning-session permission synchronization. No fork,
// unrelated settings reset, or cancellation of active work.
func (c *Codex) voiceThread(s *session.Session) (string, error) {
	return c.voiceThreadContext(context.Background(), s)
}

func (c *Codex) voiceThreadContext(ctx context.Context, s *session.Session) (string, error) {
	if s.Backend() != backend.Codex || s.ResumeID() == "" {
		return "", errors.New("此對話尚未建立 Codex thread，請先傳送一則文字訊息")
	}
	if err := c.ensureServer(); err != nil {
		return "", err
	}
	diagnostics, _ := c.RuntimeDiagnostics()["codex"].(map[string]any)
	if version, _ := diagnostics["running_version"].(string); !taskapi.SupportedNativeProviderVersion("codex", version) && c.authPath != "" {
		c.refreshRuntimeDiagnostics(filepath.Dir(c.authPath))
		diagnostics, _ = c.RuntimeDiagnostics()["codex"].(map[string]any)
	}
	if version, _ := diagnostics["running_version"].(string); !taskapi.SupportedNativeProviderVersion("codex", version) {
		return "", fmt.Errorf("共享 Codex app-server 為 %s；雙向語音需要已驗證的 CLI 介面版本", version)
	}
	id, err := c.resumeExactControllerThread(s)
	if err != nil {
		return "", err
	}
	if err = c.syncVoiceSessionPermissions(ctx, s, id); err != nil {
		return "", err
	}
	return id, nil
}

// Exact restoration shared by voice and pinned dispatches. Never rebuild a
// missing native conversation, override its permissions, or accept a new ID.
func (c *Codex) resumeExactControllerThread(s *session.Session) (string, error) {
	if s.Backend() != backend.Codex || s.ResumeID() == "" {
		return "", errors.New("controller_native_thread_missing")
	}
	if c.pmProvider != nil {
		policy, err := c.pmProvider.PMConfiguration(s.ID)
		if err != nil {
			return "", err
		}
		if policy != nil {
			return "", errors.New("此協作管理對話暫不支援直接語音指揮")
		}
	}
	st := c.state(s.ID)
	st.ensureMu.Lock()
	defer st.ensureMu.Unlock()
	st.mu.Lock()
	existing := st.threadID
	st.mu.Unlock()
	id := s.ResumeID()
	if existing != "" {
		if existing != id {
			return "", errors.New("session 與 native thread 不一致，拒絕語音連線")
		}
		return id, nil
	}
	if err := c.claimActiveThread(id, s); err != nil {
		return "", err
	}
	params := map[string]any{"threadId": id, "excludeTurns": true}
	c.applyDelegationThreadTools(s, params)
	c.applySessionControlThreadTools(s, params)
	raw, err := c.rpcCall("thread/resume", params, 15*time.Second)
	if err != nil {
		c.releaseActiveThreads(s)
		return "", err
	}
	var resumed struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if json.Unmarshal(raw, &resumed) != nil || resumed.Thread.ID != id {
		c.releaseActiveThreads(s)
		return "", errors.New("native thread 續接身分不一致")
	}
	st.mu.Lock()
	st.threadID = id
	st.mu.Unlock()
	c.mu.Lock()
	c.threadToSession[id] = s
	c.mu.Unlock()
	return id, nil
}

func (c *Codex) StartRealtimeVoice(ctx context.Context, s *session.Session, input backend.RealtimeVoiceStart) (backend.RealtimeVoiceAnswer, error) {
	if err := ctx.Err(); err != nil {
		return backend.RealtimeVoiceAnswer{}, err
	}
	if !backend.ValidRealtimeVoiceName(input.VoiceName) {
		return backend.RealtimeVoiceAnswer{}, errors.New("unsupported v3 voice")
	}
	if len(input.SDP) > 64*1024 || !strings.HasPrefix(input.SDP, "v=0") || input.VoiceID == "" {
		return backend.RealtimeVoiceAnswer{}, errors.New("無效的語音協商資料")
	}
	// Reserve the native identity before any settings mutation. Duplicate or
	// concurrent starts must not reconfigure an already active voice.
	original := s.ResumeID()
	if original == "" {
		return backend.RealtimeVoiceAnswer{}, errors.New("此對話尚未建立 Codex thread，請先傳送一則文字訊息")
	}
	c.voiceMu.Lock()
	if c.voiceCalls[original] != nil || c.voicePreparing[original] {
		c.voiceMu.Unlock()
		return backend.RealtimeVoiceAnswer{}, errors.New("此對話已有語音連線，請先結束舊連線")
	}
	if c.voicePreparing == nil {
		c.voicePreparing = map[string]bool{}
	}
	c.voicePreparing[original] = true
	c.voiceMu.Unlock()
	defer func() { c.voiceMu.Lock(); delete(c.voicePreparing, original); c.voiceMu.Unlock() }()
	id, err := c.voiceThreadContext(ctx, s)
	if err != nil {
		return backend.RealtimeVoiceAnswer{}, err
	}
	if id != original {
		return backend.RealtimeVoiceAnswer{}, errors.New("voice_session_configuration_changed")
	}
	if err := ctx.Err(); err != nil {
		return backend.RealtimeVoiceAnswer{}, err
	}
	call := &codexVoiceCall{callbackSpeech: input.SpeakerInstructions != "", mediaGeneration: input.MediaGeneration, voiceID: input.VoiceID, threadID: id, sessionID: s.ID, answer: make(chan backend.RealtimeVoiceAnswer, 1), failure: make(chan error, 1), closed: make(chan struct{}), callback: input.OnEvent}
	c.voiceMu.Lock()
	if c.voiceCalls == nil {
		c.voiceCalls = map[string]*codexVoiceCall{}
	}
	if c.voiceCalls[id] != nil {
		c.voiceMu.Unlock()
		return backend.RealtimeVoiceAnswer{}, errors.New("此對話已有語音連線，請先結束舊連線")
	}
	c.voiceCalls[id] = call
	c.voiceMu.Unlock()
	params := realtimeVoiceStartParams(id, input)
	_, err = c.rpcCall("thread/realtime/start", params, 30*time.Second)
	if call.callbackSpeech {
		log.Printf("[callback] voice=%s stage=start_rpc_ack accepted=%t", call.voiceID, err == nil)
	}

	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err == nil {
		timer := time.NewTimer(45 * time.Second)
		defer timer.Stop()
		select {
		case answer := <-call.answer:
			if ctx.Err() == nil {
				return answer, nil
			}
			err = ctx.Err()
		case err = <-call.failure:
		case <-call.closed:
			err = errors.New("語音協商已結束")
		case <-ctx.Done():
			err = ctx.Err()
		case <-timer.C:
			err = errors.New("語音協商逾時；不會自動重送指令")
		}
	}
	cleanup, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if call.callbackSpeech && err != nil {
		log.Printf("[callback] voice=%s stage=start_failed category=%s", call.voiceID, realtimeFailureCategory(err.Error()))
	}
	_ = c.StopRealtimeVoice(cleanup, s, input.VoiceID)
	return backend.RealtimeVoiceAnswer{}, err
}

func (c *Codex) ownedVoice(s *session.Session, voiceID string) *codexVoiceCall {
	c.voiceMu.Lock()
	defer c.voiceMu.Unlock()
	for _, call := range c.voiceCalls {
		if call.voiceID == voiceID && call.sessionID == s.ID {
			return call
		}
	}
	return nil
}
func (c *Codex) StopRealtimeVoice(ctx context.Context, s *session.Session, voiceID string) error {
	call := c.ownedVoice(s, voiceID)
	if call == nil {
		return nil
	}
	if _, err := c.rpcCall("thread/realtime/stop", map[string]any{"threadId": call.threadID}, 5*time.Second); err != nil {
		return err
	}
	select {
	case <-call.closed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return errors.New("等待語音結束確認逾時，請檢查連線狀態")
	}
}
func (c *Codex) AppendRealtimeVoiceText(ctx context.Context, s *session.Session, voiceID, text string) error {
	if len(text) > 16000 || strings.TrimSpace(text) == "" {
		return errors.New("語音文字訊息無效")
	}
	call := c.ownedVoice(s, voiceID)
	if call == nil || call.threadID != s.ResumeID() {
		return errors.New("語音連線已失效")
	}
	_, err := c.rpcCall("thread/realtime/appendText", map[string]any{"threadId": call.threadID, "text": text, "role": "user"}, 10*time.Second)
	return err
}

func (c *Codex) dispatchVoice(method string, raw json.RawMessage) bool {
	if !strings.HasPrefix(method, "thread/realtime/") {
		return false
	}
	var p struct {
		ThreadID string `json:"threadId"`
		VoiceID  string `json:"realtimeSessionId"`
		SDP      string `json:"sdp"`
		Text     string `json:"text"`
		Delta    string `json:"delta"`
		Role     string `json:"role"`
		Message  string `json:"message"`
		Item     struct {
			Type  string `json:"type"`
			Voice string `json:"realtimeSessionId"`
			Role  string `json:"role"`
			Text  string `json:"text"`
		} `json:"item"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return true
	}
	c.voiceMu.Lock()
	call := c.voiceCalls[p.ThreadID]
	c.voiceMu.Unlock()
	if call == nil || (p.VoiceID != "" && p.VoiceID != call.voiceID) {
		return true
	}
	if call.callbackSpeech && (method == "thread/realtime/sdp" || method == "thread/realtime/started" || method == "thread/realtime/closed") {
		log.Printf("[callback] voice=%s native_event=%s", call.voiceID, method)
	}
	event := backend.RealtimeVoiceEvent{}
	if method == "thread/realtime/itemAdded" {
		c.observeMediaHandoff(call, raw)
	}
	switch method {
	case "thread/realtime/itemCompleted":
		if p.Item.Type != "transcriptSegment" || p.Item.Voice != call.voiceID {
			return true
		}
		event.State = "transcript_done"
		event.Text = p.Item.Text
		event.Role = p.Item.Role
		event.TranscriptVoiceID = p.Item.Voice
	case "thread/realtime/sdp":
		if len(p.SDP) > 64*1024 {
			return true
		}
		select {
		case call.answer <- backend.RealtimeVoiceAnswer{ThreadID: call.threadID, VoiceID: call.voiceID, SDP: p.SDP}:
		default:
		}
		return true
	case "thread/realtime/error":
		if call.callbackSpeech {
			log.Printf("[callback] realtime failure voice=%s category=%s", call.voiceID, realtimeFailureCategory(p.Message))
		}
		event.State = "error"
		event.Message = "Codex 語音服務中斷，請結束語音後重新連線"
		select {
		case call.failure <- errors.New(event.Message):
		default:
		}
	case "thread/realtime/closed":
		event.State = "closed"
		call.once.Do(func() { close(call.closed) })
		c.voiceMu.Lock()
		if c.voiceCalls[p.ThreadID] == call {
			delete(c.voiceCalls, p.ThreadID)
		}
		c.voiceMu.Unlock()
	case "thread/realtime/transcript/delta":
		event.State = "transcript"
		event.Text = p.Delta
		event.Role = p.Role
	case "thread/realtime/transcript/done":
		event.State = "transcript_done"
		event.Text = p.Text
		event.Role = p.Role
	default:
		return true
	}
	if call.callback != nil {
		call.callback(event)
	}
	return true
}

func (c *Codex) failVoiceCalls() {
	c.voiceMu.Lock()
	calls := c.voiceCalls
	c.voiceCalls = map[string]*codexVoiceCall{}
	c.voiceMu.Unlock()
	for _, call := range calls {
		select {
		case call.failure <- errors.New("Codex 連線中斷，語音不會自動重送"):
		default:
		}
		call.once.Do(func() { close(call.closed) })
		if call.callback != nil {
			call.callback(backend.RealtimeVoiceEvent{State: "error", Message: "Codex 已重新連線，請重新開始語音"})
		}
	}
}

func realtimeVoiceStartParams(thread string, input backend.RealtimeVoiceStart) map[string]any {
	params := map[string]any{"threadId": thread, "outputModality": "audio", "transport": map[string]any{"type": "webrtc", "sdp": input.SDP}, "version": "v3", "realtimeSessionId": input.VoiceID, "clientManagedHandoffs": false, "includeStartupContext": true, "flushTranscriptTailOnSessionEnd": false}
	// prompt configures the realtime speaker. realtimeStartInstructions below
	// configures the backing agent and cannot bootstrap proactive speech.
	if input.SpeakerInstructions != "" {
		params["prompt"] = input.SpeakerInstructions
		params["includeStartupContext"] = false
	}
	if input.MediaInstructions != "" {
		params["realtimeStartInstructions"] = input.MediaInstructions
	}
	if input.VoiceName != "" {
		params["voice"] = input.VoiceName
	}
	return params
}
