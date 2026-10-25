package core

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/pion/webrtc/v4"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/linebridge"
	"everything-go/internal/linevoice"
)

// SetLineVoiceRuntime receives only the manifest-validated signed bundle paths.
func (h *Hub) SetLineVoiceRuntime(node, worker string) {
	h.lineVoiceNode = node
	h.lineVoiceWorker = worker
}
func (h *Hub) lineVoiceRuntime() (string, string) {
	if h.lineVoiceWorker != "" {
		return h.lineVoiceNode, h.lineVoiceWorker
	}
	return os.Getenv("LINEJS_NODE"), os.Getenv("LINEJS_VOICE_WORKER")
}
func (h *Hub) LineVoiceEnabled() bool {
	if h.lineService == nil {
		return false
	}
	if h.lineVoiceWorker != "" {
		info, e := os.Stat(h.lineVoiceWorker)
		return filepath.IsAbs(h.lineVoiceNode) && filepath.IsAbs(h.lineVoiceWorker) && e == nil && info.Mode().IsRegular()
	}
	return lineVoiceEnabled()
}
func (h *Hub) HandoffLineVoice(ctx context.Context, caller backend.SessionControlCaller, account, target string) (any, error) {
	return h.handoffLineVoice(ctx, caller, account, target, caller.ToolCallID)
}
func (h *Hub) handoffLineVoice(ctx context.Context, caller backend.SessionControlCaller, account, target, intent string) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !h.LineVoiceEnabled() || caller.Parent == nil || caller.VoiceID == "" || caller.ToolCallID == "" {
		return nil, linebridge.Fail("line_voice_handoff_not_available")
	}
	s, ok := h.registry.Get(caller.Parent.ID)
	if !ok || s != caller.Parent || !h.controls.MobileMayWrite(s.ID) {
		return nil, linebridge.Fail("line_voice_handoff_not_owned")
	}
	if !h.lineService.Store().Allowed(ctx, linebridge.Caller{Subject: "session:" + s.ID}, account, "call") {
		return nil, linebridge.Fail("line_forbidden")
	}
	t, err := h.lineService.Store().Target(ctx, account, target)
	if err != nil {
		return nil, err
	}
	peer := t.Name
	h.voiceMu.Lock()
	defer h.voiceMu.Unlock()
	for owner, call := range h.voiceClients {
		if call.session != s || call.voiceID != caller.VoiceID || !owner.live() {
			continue
		}
		if call.kind == "line" || call.stopping || !call.handoffCapable {
			return nil, linebridge.Fail("line_voice_handoff_phone_not_ready")
		}
		if !h.lineService.Store().Allowed(ctx, linebridge.Caller{Subject: "device:" + owner.deviceID}, account, "call") {
			return nil, linebridge.Fail("line_forbidden")
		}
		if call.handoffPeer != "" {
			if call.handoffTarget != target || call.handoffAccount != account {
				return nil, linebridge.Fail("line_voice_handoff_already_pending")
			}
			return map[string]any{"state": "handoff_requested", "replayed": true, "connected": false, "call_id": call.handoffID}, nil
		}
		reservation, err := h.lineService.ReserveHandoff(ctx, linebridge.Caller{Subject: "session:" + s.ID}, "device:"+owner.deviceID, s.ID, call.voiceID, account, target, intent)
		if err != nil {
			return nil, err
		}
		if reservation.State != "handoff_pending" {
			return reservation, nil
		}
		call.handoffPeer = peer
		call.handoffAccount = account
		call.handoffTarget = target
		call.handoffID = reservation.ID
		cmd := clientproto.Command{SessionID: s.ID, RequestID: call.requestID, VoiceID: call.voiceID}
		out := voiceEvent(cmd, "line_handoff", "")
		out.ThreadID = s.ResumeID()
		out.LinePeerName = peer
		out.LineCallID = reservation.ID
		out.LineAccountRef = account
		out.LineTargetRef = target
		owner.enqueueEvent(out)
		return map[string]any{"state": "handoff_requested", "replayed": false, "connected": false, "call_id": reservation.ID, "note": "Phone must stop AI audio and complete LINE dialing; this acknowledgement is not a connected call."}, nil
	}
	return nil, linebridge.Fail("line_voice_handoff_active_phone_missing")
}

func lineVoiceEnabled() bool {
	path := os.Getenv("LINEJS_VOICE_WORKER")
	if os.Getenv("LINEJS_ALLOW_CALLS") != "1" || !filepath.IsAbs(path) {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func lineVoiceICE(fallback []webrtc.ICEServer) ([]webrtc.ICEServer, error) {
	raw := os.Getenv("LINEJS_ICE_SERVERS_JSON")
	if raw == "" {
		return fallback, nil
	}
	if len(raw) > 16384 {
		return nil, linebridge.Fail("invalid_line_voice_ice_configuration")
	}
	var servers []webrtc.ICEServer
	if json.Unmarshal([]byte(raw), &servers) != nil || len(servers) > 16 {
		return nil, linebridge.Fail("invalid_line_voice_ice_configuration")
	}
	for _, server := range servers {
		if len(server.URLs) == 0 || len(server.URLs) > 4 {
			return nil, linebridge.Fail("invalid_line_voice_ice_configuration")
		}
		for _, url := range server.URLs {
			if len(url) > 1024 || !(strings.HasPrefix(url, "stun:") || strings.HasPrefix(url, "stuns:") || strings.HasPrefix(url, "turn:") || strings.HasPrefix(url, "turns:")) {
				return nil, linebridge.Fail("invalid_line_voice_ice_configuration")
			}
		}
	}
	return servers, nil
}

func (h *Hub) handleLineVoiceStart(c *Client, cmd clientproto.Command) {
	reject := func(message string) { c.enqueueEvent(voiceEvent(cmd, "error", message)) }
	if !h.LineVoiceEnabled() {
		reject("此 Bridge 尚未設定 LINE 通話服務")
		return
	}
	ice, iceError := lineVoiceICE(h.iceServers)
	if iceError != nil {
		reject("LINE 通話的 ICE 設定無效")
		return
	}
	if !voiceIdentifier.MatchString(cmd.VoiceID) || !voiceIdentifier.MatchString(cmd.RequestID) || !utf8.ValidString(cmd.LinePeerName) || len([]rune(cmd.LinePeerName)) > 128 || cmd.LineAccountRef == "" || cmd.LineTargetRef == "" || len(cmd.SDP) > 65536 {
		reject("LINE 通話請求格式無效")
		return
	}
	s, ok := h.registry.Get(cmd.SessionID)
	if !ok || s.Backend() != backend.Codex || s.ResumeID() == "" || cmd.ThreadID != s.ResumeID() || !h.controls.MobileMayWrite(cmd.SessionID) {
		reject("目前對話沒有 LINE 通話控制權")
		return
	}
	parent := c.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	call := &clientVoiceCall{session: s, voiceID: cmd.VoiceID, requestID: cmd.RequestID, cancel: cancel, kind: "line"}
	h.voiceMu.Lock()
	if h.voiceClients == nil {
		h.voiceClients = map[*Client]*clientVoiceCall{}
	}
	for owner, existing := range h.voiceClients {
		if owner == c || existing.kind == "line" || existing.session.ID == s.ID {
			h.voiceMu.Unlock()
			cancel()
			reject("請先結束目前的語音或 LINE 通話")
			return
		}
	}
	h.voiceClients[c] = call
	h.voiceMu.Unlock()
	c.enqueueEvent(voiceEvent(cmd, "starting", "正在建立 LINE 音訊連線"))
	id := fmt.Sprintf("line_%x", sha256.Sum256([]byte(h.cfg.InstanceID+"\x00"+s.ID+"\x00"+cmd.VoiceID)))
	go func() {
		prepareCtx, prepareCancel := context.WithTimeout(ctx, 40*time.Second)
		var receipt linebridge.CallReceipt
		var material linebridge.CallMaterial
		var prepareErr error
		if cmd.LineCallID != "" {
			receipt, material, prepareErr = h.lineService.PrepareHandoffCall(prepareCtx, linebridge.Caller{Subject: "device:" + c.deviceID, Admin: true}, cmd.LineCallID, s.ID, cmd.LineAccountRef, cmd.LineTargetRef)
		} else {
			receipt, material, prepareErr = h.lineService.PrepareCall(prepareCtx, linebridge.Caller{Subject: "device:" + c.deviceID, Admin: true}, cmd.LineAccountRef, cmd.LineTargetRef, id)
		}
		prepareCancel()
		if prepareErr != nil {
			cancel()
			h.voiceMu.Lock()
			if h.voiceClients[c] == call {
				delete(h.voiceClients, c)
			}
			h.voiceMu.Unlock()
			reject("無法準備 LINE 通話（" + linebridge.SafeCode(prepareErr) + "）")
			return
		}
		id = receipt.ID
		releaseOwner, registerErr := h.lineService.RegisterCall(ctx, linebridge.Caller{Subject: "device:" + c.deviceID, Admin: true}, receipt, cancel)
		if registerErr != nil {
			cancel()
			_ = h.lineService.UpdateCall(context.Background(), id, "failed_before_effect", linebridge.SafeCode(registerErr))
			h.voiceMu.Lock()
			if h.voiceClients[c] == call {
				delete(h.voiceClients, c)
			}
			h.voiceMu.Unlock()
			reject("LINE 通話授權已變更")
			return
		}
		defer releaseOwner()
		defer func() { _ = h.lineService.UpdateCall(context.Background(), id, "closed", "") }()
		payload, _ := json.Marshal(material)
		env := []string{"LINEJS_ALLOW_CALLS=1"}
		for _, key := range []string{"PATH", "HOME", "TMPDIR", "LANG", "TZ"} {
			if value, ok := os.LookupEnv(key); ok {
				env = append(env, key+"="+value)
			}
		}
		node, worker := h.lineVoiceRuntime()
		line, answer, err := linevoice.Offer(ctx, linevoice.Config{WorkerProtocol: 2, Node: node, Worker: worker, ICE: ice, Env: env, Material: payload, BeforeStart: func(ctx context.Context) error {
			return h.lineService.ValidatePreparedCall(ctx, linebridge.Caller{Subject: "device:" + c.deviceID, Admin: true}, id)
		}}, id, cmd.LinePeerName, cmd.SDP, func(event linevoice.Event) {
			if event.State == "diagnostic" {
				_ = h.lineService.RecordCallTelemetry(context.Background(), id, linebridge.CallTelemetry{Code: event.Code, Stage: event.Stage, Status: event.Status, ElapsedMS: event.ElapsedMS, DurationMS: event.DurationMS, ReleaseCode: event.RelCode, DroppedPackets: event.DroppedPackets, PayloadLength: event.PayloadLength, TXFrames: event.TXFrames, RXFrames: event.RXFrames})
				return
			}
			if event.State == "starting" || event.State == "ringing" || event.State == "connected" || event.State == "closed" || event.State == "error" {
				state := event.State
				if state == "starting" {
					state = "dispatching"
				}
				_ = h.lineService.UpdateCall(context.Background(), id, state, event.Code)
			}
			if !h.ownsVoice(c, call) {
				return
			}
			message := ""
			if event.State == "ringing" {
				message = "正在等待 LINE 對方接聽"
			}
			if event.State == "error" {
				message = "LINE 通話中斷或無法建立，請確認帳號、好友名稱與網路"
				switch event.Code {
				case "peer_not_unique":
					message = "找不到唯一相符的 LINE 好友，請輸入完整顯示名稱"
				case "keychain_unavailable", "existing_login_required":
					message = "LINE 登入資料無法使用，請先恢復伺服器的登入與金鑰存取"
				case "credential_store_busy":
					message = "LINE 帳號正由另一個程序使用，請稍後再試"
				case "call_intent_already_used":
					message = "這次撥號已處理，不會自動重撥"
				case "line_media_unavailable":
					message = "對方已接聽，但 LINE 音訊連線未能建立"
				case "call_route_unavailable":
					message = "LINE 沒有提供可用的通話路徑"
				case "phone_connect_timeout", "phone_disconnected":
					message = "手機音訊網路連線失敗或已中斷"
				case "call_duration_limit":
					message = "本次 LINE 通話已達時限，已結束"
				}
			}
			out := voiceEvent(cmd, event.State, message)
			out.ThreadID = s.ResumeID()
			if c.live() {
				c.enqueueEvent(out)
			}
			if event.State == "closed" {
				h.voiceMu.Lock()
				if h.voiceClients[c] == call {
					delete(h.voiceClients, c)
				}
				h.voiceMu.Unlock()
				cancel()
			}
		})
		if err != nil {
			cancel()
			h.voiceMu.Lock()
			if h.voiceClients[c] == call {
				delete(h.voiceClients, c)
			}
			h.voiceMu.Unlock()
			if c.live() {
				reject("無法建立 LINE 音訊連線")
			}
			return
		}
		// Keep the durable call lease until the actual child and media sockets
		// have terminated, rather than just until the SDP answer is returned.
		defer func() {
			line.Close()
			wait, stop := context.WithTimeout(context.Background(), 8*time.Second)
			defer stop()
			_ = line.WaitClosed(wait)
		}()
		h.voiceMu.Lock()
		owned := h.voiceClients[c] == call && !call.stopping
		if owned {
			call.line = line
		}
		h.voiceMu.Unlock()
		if !owned || !c.live() {
			line.Close()
			return
		}
		out := voiceEvent(cmd, "answer", "")
		out.ThreadID = s.ResumeID()
		out.SDP = answer
		c.enqueueEvent(out)
		<-ctx.Done()
	}()
}
