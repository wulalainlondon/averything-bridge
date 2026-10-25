package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/callbacks"
	"everything-go/internal/session"
)

type callbackInput struct {
	intent     string
	result     *callbackEvent
	Type       string `json:"type"`
	Operation  string `json:"operation_id"`
	Action     string `json:"action"`
	Session    string `json:"session_id,omitempty"`
	Request    string `json:"task_request_id,omitempty"`
	Expected   uint64 `json:"expected_revision"`
	Enabled    bool   `json:"enabled"`
	Failures   bool   `json:"include_failure"`
	Recipient  string `json:"recipient_inventory_id,omitempty"`
	Invitation string `json:"invitation_id,omitempty"`
	Voice      string `json:"voice_id,omitempty"`
	State      string `json:"state,omitempty"`
	Native     bool   `json:"native_support"`
	Compact    bool   `json:"compact,omitempty"`
	Cursor     string `json:"cursor,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}
type callbackInvitationSummary struct {
	ID         string `json:"id"`
	Session    string `json:"session_id"`
	Request    string `json:"task_request_id"`
	Revision   uint64 `json:"revision"`
	Invitation string `json:"invitation_id"`
	Expires    int64  `json:"expires_at"`
}
type callbackTaskSource struct {
	ExecutionRequest string `json:"execution_request_id,omitempty"`
	Request          string `json:"task_request_id"`
	Title            string `json:"title"`
	State            string `json:"state"`
	Created          int64  `json:"created_at"`
}
type callbackEvent struct {
	Tasks       []callbackTaskSource        `json:"tasks,omitempty"`
	Type        string                      `json:"type"`
	Authority   string                      `json:"authority_instance_id"`
	Operation   string                      `json:"operation_id"`
	Accepted    bool                        `json:"accepted"`
	Error       string                      `json:"error,omitempty"`
	Record      *callbacks.Record           `json:"record,omitempty"`
	Records     []callbacks.Record          `json:"records,omitempty"`
	Devices     []callbacks.Recipient       `json:"devices,omitempty"`
	Invitations []callbackInvitationSummary `json:"invitations,omitempty"`
	NextCursor  string                      `json:"next_cursor,omitempty"`
}

func callbackBinding(c *Client) string {
	p := c.readIdentity.Load()
	if p == nil {
		return ""
	}
	sum := sha256.Sum256([]byte(p.token))
	return hex.EncodeToString(sum[:])
}
func (h *Hub) callbackRecipient(c *Client) callbacks.Recipient {
	b := c.inventoryBinding.Load()
	r := callbacks.Recipient{Device: h.pairedTaskDevice(c), Binding: callbackBinding(c), Name: "這台裝置"}
	if b != nil {
		r.Inventory = b.key
		if b.name != "" {
			r.Name = b.name
		}
	}
	return r
}
func (h *Hub) callbackRespond(c *Client, in callbackInput, r *callbacks.Record, e error) {
	out := callbackEvent{Type: "task_callback", Authority: h.cfg.InstanceID, Operation: in.Operation, Accepted: e == nil, Record: h.callbackView(c, r)}
	if e != nil {
		out.Error = e.Error()
	}
	if in.result != nil {
		*in.result = out
	}
	c.enqueueEvent(out)
}
func (h *Hub) callbackRecordID(session, epoch, request string) string {
	return callbacks.ID(h.cfg.InstanceID, session, epoch, request)
}
func (h *Hub) callbackEpoch(session string) string {
	epoch, e := h.runtimes.SourceEpoch(session)
	if e != nil {
		return ""
	}
	return epoch
}

func (h *Hub) callbackDevices() []callbacks.Recipient {
	out := []callbacks.Recipient{}
	seen := map[string]bool{}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		if !c.callbackCapable.Load() || !c.callbackNative.Load() {
			continue
		}
		r := h.callbackRecipient(c)
		if r.Device == "" || seen[r.Inventory] {
			continue
		}
		seen[r.Inventory] = true
		out = append(out, r)
	}
	return out
}
func (h *Hub) callbackTarget(c *Client, inventory string) (callbacks.Recipient, error) {
	own := h.callbackRecipient(c)
	if inventory == "" || inventory == own.Inventory {
		if !c.callbackNative.Load() {
			return own, errors.New("此裝置尚未支援接聽，請選擇已連線的接收手機")
		}
		return own, nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for other := range h.clients {
		if other.callbackNative.Load() && other.callbackCapable.Load() {
			r := h.callbackRecipient(other)
			if r.Device != "" && r.Inventory == inventory {
				return r, nil
			}
		}
	}
	return callbacks.Recipient{}, errors.New("接收裝置未連線或尚未支援來電")
}

// A socket is not a prerequisite for notifying a previously enrolled recipient.
// Only the same policy's server-resolved recipient may be reused offline; a
// caller-supplied inventory ID or push token alone never establishes support.
func (h *Hub) callbackTargetForRecord(ctx context.Context, c *Client, inventory, id string) (callbacks.Recipient, error) {
	target, liveErr := h.callbackTarget(c, inventory)
	if liveErr == nil {
		return target, nil
	}
	if inventory == "" || h.callbacks == nil || h.fcm == nil {
		return callbacks.Recipient{}, liveErr
	}
	// An explicitly unsupported current client is not an offline client.
	if h.callbackRecipient(c).Inventory == inventory {
		return callbacks.Recipient{}, liveErr
	}
	h.mu.RLock()
	connected := false
	for other := range h.clients {
		if h.callbackRecipient(other).Inventory == inventory {
			connected = true
			break
		}
	}
	h.mu.RUnlock()
	if connected {
		return callbacks.Recipient{}, liveErr
	}
	r, found, err := h.callbacks.Get(ctx, id)
	if err != nil {
		return callbacks.Recipient{}, err
	}
	if !found || r.Recipient.Inventory != inventory || r.Recipient.Device == "" || r.Recipient.Binding == "" {
		return callbacks.Recipient{}, liveErr
	}
	for _, binding := range h.pairing.DeviceBindings() {
		if binding.DeviceID == r.Recipient.Device && callbacks.Digest(binding.Token) == r.Recipient.Binding && h.fcm.HasRegisteredTaskCallbackDevice(binding.DeviceID) {
			return r.Recipient, nil
		}
	}
	return callbacks.Recipient{}, liveErr
}

// This negotiated extension is parsed before the legacy frame union, just as
// TaskAPI is. Every action still requires the original paired transport.
func (h *Hub) handleTaskCallback(ctx context.Context, c *Client, raw []byte) {
	var in callbackInput
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	e := d.Decode(&in)
	if e != nil || len(raw) > 16<<10 || in.Type != "task_callback_request" || !voiceIdentifier.MatchString(in.Operation) {
		h.callbackRespond(c, in, nil, errors.New("invalid_callback_request"))
		return
	}
	h.executeTaskCallback(ctx, c, in)
}
func (h *Hub) executeTaskCallback(ctx context.Context, c *Client, in callbackInput) {
	var e error
	owner := h.pairedTaskDevice(c)
	if h.callbacks == nil || owner == "" || c.enrollmentOnly || c.inventoryProbe {
		h.callbackRespond(c, in, nil, errors.New("callback_pairing_required"))
		return
	}
	c.callbackCapable.Store(true)
	if in.Action == "sync" {
		c.callbackNative.Store(in.Native)
		if in.Compact {
			limit := in.Limit
			if limit == 0 {
				limit = 64
			}
			rows, next, err := h.callbacks.ActiveInvitationPage(ctx, owner, callbackBinding(c), in.Cursor, limit, time.Now().UnixMilli())
			out := callbackEvent{Type: "task_callback", Authority: h.cfg.InstanceID, Operation: in.Operation, Accepted: err == nil, NextCursor: next}
			if err != nil {
				out.Error = err.Error()
			} else {
				for _, r := range rows {
					source, exists := h.registry.Get(r.Session)
					if !exists || source.Snapshot().Hidden || source.State() == session.Closed || !h.cwdInScope(source.Cwd()) || !h.controls.MobileMayWrite(source.ID) {
						continue
					}
					out.Invitations = append(out.Invitations, callbackInvitationSummary{ID: r.ID, Session: r.Session, Request: r.Request, Revision: r.Revision, Invitation: r.Invite.ID, Expires: r.Invite.Expires})
				}
			}
			if in.result != nil {
				*in.result = out
			}
			c.enqueueEvent(out)
			if err == nil && in.Cursor == "" {
				go h.recoverCallbackAdmissions(context.Background())
			}
			// Each full record is retrieved with the original get request. Do not
			// embed report bodies or replay a whole history in this registration ACK.
			return
		}
		go h.recoverCallbackAdmissions(context.Background())
		all, e := h.callbacks.List(ctx)
		out := callbackEvent{Type: "task_callback", Authority: h.cfg.InstanceID, Operation: in.Operation, Accepted: e == nil, Devices: h.callbackDevices()}
		for _, r := range all {
			if (r.Requester == owner && r.RequesterBinding == callbackBinding(c)) || (r.Recipient.Device == owner && r.Recipient.Binding == callbackBinding(c)) {
				r := *h.callbackView(c, &r)
				out.Records = append(out.Records, r)
				if len(out.Records) <= 4 {
					go h.refreshCallbackSpeech(r)
				}
			}
		}
		if in.result != nil {
			*in.result = out
		}
		c.enqueueEvent(out)
		go h.publishCallbackInvitations(context.Background())
		return
	}
	if in.Session == "" || len(in.Session) > 512 || len(in.Request) > 200 {
		h.callbackRespond(c, in, nil, errors.New("callback_source_required"))
		return
	}
	s, ok := h.registry.Get(in.Session)
	if !ok || s.Snapshot().Hidden || s.State() == session.Closed || !h.cwdInScope(s.Cwd()) || !h.controls.MobileMayWrite(s.ID) {
		h.callbackRespond(c, in, nil, errors.New("callback_source_unavailable"))
		return
	}
	if s.Backend() != backend.Codex || h.cfg.CodexRemote == "" {
		h.callbackRespond(c, in, nil, errors.New("這個對話尚未支援原來源語音匯報"))
		return
	}
	if in.Action == "sources" {
		snapshot, e := h.messageQueue.Snapshot(s.ID)
		out := callbackEvent{Type: "task_callback", Authority: h.cfg.InstanceID, Operation: in.Operation, Accepted: e == nil}
		if e != nil {
			out.Error = "原指令暫時無法讀取"
		} else {
			for i := len(snapshot.Items) - 1; i >= 0 && len(out.Tasks) < 20; i-- {
				entry := snapshot.Items[i]
				admission := h.taskAdmission(entry)
				if admission.MessagePurpose != "instruction" || admission.Origin != nil {
					continue
				}
				projected := h.projectSessionTask(s, entry, owner, nil)
				state := string(entry.State)
				if projected.Transport == "ordinary_steer" {
					state = projected.State
				}
				out.Tasks = append(out.Tasks, callbackTaskSource{ExecutionRequest: projected.ExecutionRequestID, Request: entry.RequestID, Title: truncateGraphemes(entry.Content, 80), State: state, Created: entry.CreatedAt})
			}
		}
		// An explicitly queried active root must not disappear behind twenty
		// newer queued inputs. This is a read of the same scoped instruction.
		if in.Request != "" && e == nil {
			entry, found, err := h.messageQueue.Get(s.ID, in.Request)
			seen := false
			for _, row := range out.Tasks {
				if row.Request == in.Request {
					seen = true
				}
			}
			admission := h.taskAdmission(entry)
			if err == nil && found && !seen && admission.MessagePurpose == "instruction" && admission.Origin == nil {
				projected := h.projectSessionTask(s, entry, owner, nil)
				state := string(entry.State)
				if projected.Transport == "ordinary_steer" {
					state = projected.State
				}
				out.Tasks = append([]callbackTaskSource{{ExecutionRequest: projected.ExecutionRequestID, Request: entry.RequestID, Title: truncateGraphemes(entry.Content, 80), State: state, Created: entry.CreatedAt}}, out.Tasks...)
			}
		}
		if h.work != nil && e == nil {
			workSources, err := h.work.CallbackSources(ctx, s.ID)
			if err != nil {
				out.Accepted = false
				out.Error = "工作項目來源暫時無法核對"
			} else {
				seen := map[string]bool{}
				for _, t := range out.Tasks {
					seen[t.Request] = true
				}
				for _, w := range workSources {
					if !seen[w.Request] && w.Latest && len(out.Tasks) < 20 {
						out.Tasks = append(out.Tasks, callbackTaskSource{Request: w.Request, Title: w.Title, State: w.Lifecycle, Created: w.Created})
					}
				}
			}
		}
		if in.result != nil {
			*in.result = out
		}
		c.enqueueEvent(out)
		return
	}
	epoch := h.callbackEpoch(s.ID)
	if epoch == "" {
		h.callbackRespond(c, in, nil, errors.New("callback_source_epoch_unknown"))
		return
	}
	id := h.callbackRecordID(s.ID, epoch, in.Request)
	if in.Action == "get" {
		r, found, e := h.callbacks.Get(ctx, id)
		if !found && e == nil {
			r = callbacks.Record{ID: id, Authority: h.cfg.InstanceID, Session: s.ID, Request: in.Request, State: "disabled"}
		}
		h.callbackRespond(c, in, &r, e)
		if found && e == nil {
			go h.refreshCallbackSpeech(r)
		}
		return
	}
	var target callbacks.Recipient
	if in.Action == "set" || in.Action == "request_now" {
		target, e = h.callbackTargetForRecord(ctx, c, in.Recipient, id)
		if e != nil {
			h.callbackRespond(c, in, nil, e)
			return
		}
	}
	payload, _ := json.Marshal(in)
	fingerprint := callbacks.Digest(string(payload) + callbackBinding(c))
	if in.intent != "" {
		fingerprint = in.intent
	}
	op := callbacks.ID(owner, in.Operation)
	expected := in.Expected
	switch in.Action {
	case "claim", "presented", "decline", "missed", "busy", "connected", "report", "report_finished", "audio_observed", "unavailable", "ended":
		expected = callbacks.AnyRevision
	}
	r, e := h.callbacks.Mutate(ctx, op, fingerprint, id, expected, func(r *callbacks.Record, exists bool) error {
		if !exists {
			entry, found, err := h.messageQueue.Get(s.ID, in.Request)
			_, workFound, workErr := h.callbackWorkSource(ctx, s.ID, in.Request)
			if err != nil || workErr != nil || (!found && !workFound) {
				return errors.New("callback_task_not_found")
			}
			admission := h.taskAdmission(entry)
			if !workFound && admission.MessagePurpose != "instruction" {
				return errors.New("請選擇原任務指令，不對進度或補充訊息單獨來電")
			}
			if admission.Origin != nil {
				return errors.New("請對原交辦任務設定匯報，子工作不單獨來電")
			}
			*r = callbacks.Record{Authority: h.cfg.InstanceID, Session: s.ID, Request: in.Request, Epoch: epoch, Thread: s.ResumeID(), Config: s.SettingsSnapshot().ConfigRevision, Origin: admission.OwnerDevice, Name: s.Name(), State: "disabled"}
		}
		if r.Session != s.ID || r.Epoch != epoch {
			return errors.New("callback_source_changed")
		}
		switch in.Action {
		case "set", "request_now":
			if (in.Action == "request_now" || in.Enabled) && r.Thread != "" && r.Thread != s.ResumeID() {
				return errors.New("原任務的native thread已變更，不能轉接其他對話匯報")
			}
			if (in.Action == "request_now" || in.Enabled) && r.Invite != nil && r.Invite.Claim != "" && !callbacks.Terminal(r.Invite.State) {
				return errors.New("匯報已接聽，請先結束目前通話")
			}
			if in.Action == "set" && !in.Enabled {
				callbacks.Cancel(r)
				return nil
			}
			if in.Action == "request_now" && r.Report == nil {
				return errors.New("正式結果尚未準備好")
			}
			if in.Action == "request_now" && !h.callbackReportCurrent(ctx, *r) {
				return errors.New("來源或必要驗收條件已改變，請回原任務核對")
			}
			if r.Invite != nil && !callbacks.Terminal(r.Invite.State) {
				r.Invite.State = "revoked"
			}
			r.Enabled = true
			r.Thread = s.ResumeID()
			r.Config = s.SettingsSnapshot().ConfigRevision
			r.Failures = in.Failures
			r.Requester = owner
			r.RequesterBinding = callbackBinding(c)
			r.Recipient = target
			r.State = "armed"
			if r.Report != nil {
				callbacks.NewInvitation(r, time.Now().UnixMilli(), in.Operation)
				r.Invite.Manual = in.Action == "request_now"
			}
		case "cancel":
			callbacks.Cancel(r)
		case "claim":
			if !h.callbackReportCurrent(ctx, *r) {
				return errors.New("匯報來源或驗收條件已改變，請回原任務核對")
			}
			if s.ResumeID() != r.Thread || s.SettingsSnapshot().ConfigRevision != r.Config || s.State() != session.Idle || s.QueueLen() > 0 {
				return errors.New("來源對話正在處理工作或設定已改變，請稍後匯報")
			}
			if e := callbacks.Claim(r, owner, callbackBinding(c), in.Invitation, in.Operation, time.Now().UnixMilli()); e != nil {
				return e
			}
		case "presented", "decline", "missed", "busy", "connected", "report", "report_finished", "audio_observed", "unavailable", "ended":
			if r.Invite == nil || r.Recipient.Device != owner || r.Recipient.Binding != callbackBinding(c) || r.Invite.ID != in.Invitation {
				return callbacks.ErrUnavailable
			}
			i := r.Invite
			if callbacks.Terminal(i.State) {
				return callbacks.ErrUnavailable
			}
			if time.Now().UnixMilli() >= i.Expires && i.Claim == "" {
				return errors.New("callback_expired")
			}
			switch in.Action {
			case "presented":
				if i.Claim != "" {
					return callbacks.ErrUnavailable
				}
				i.State = "ringing"
				i.PresentedAt = time.Now().UnixMilli()
			case "decline":
				if i.Claim != "" {
					return callbacks.ErrUnavailable
				}
				i.State = "declined"
				r.State = "report_pending"
			case "missed":
				if i.Claim != "" {
					return callbacks.ErrUnavailable
				}
				i.State = "missed"
				r.State = "report_pending"
			case "unavailable":
				if i.Claim != "" {
					return callbacks.ErrUnavailable
				}
				i.State = "failed"
				i.Reason = "手機通知未開啟，請在系統設定允許任務匯報來電"
				r.State = "report_pending"
			case "busy":
				if i.Claim != "" {
					return callbacks.ErrUnavailable
				}
				i.State = "busy_deferred"
				r.State = "report_pending"
			case "connected", "report", "report_finished", "audio_observed", "ended":
				h.voiceMu.Lock()
				call := h.voiceClients[c]
				owned := call != nil && call.voiceID == in.Voice && call.session.ID == s.ID
				h.voiceMu.Unlock()
				if !owned || i.Claim == "" {
					return errors.New("callback_voice_unverified")
				}
				if i.Voice != "" && i.Voice != in.Voice {
					return callbacks.ErrUnavailable
				}
				i.Voice = in.Voice
				if in.Action == "connected" {
					i.State = "connected"
					r.State = "connected"
				}
				if in.Action == "report" {
					if i.ReportOperation != "" {
						return errors.New("callback_report_already_requested")
					}
					if r.Report == nil || r.Report.Thread != s.ResumeID() {
						return errors.New("callback_result_changed")
					}
					i.ReportOperation = in.Operation
					i.State = "report_requested"
					r.State = "report_requested"
				}
				if in.Action == "audio_observed" {
					if i.ReportOperation == "" {
						return callbacks.ErrUnavailable
					}
					i.AudioObserved = true
					if i.ContentVerified {
						i.State = "report_audio_observed"
						r.State = i.State
					} else if r.State != "report_delivery_mismatch" {
						i.State = "report_output_unverified"
						r.State = i.State
					}
					if !r.Enabled {
						r.State = "disabled"
					}
				}
				if in.Action == "report_finished" {
					if i.ReportOperation == "" || !i.ContentVerified || !i.AudioObserved {
						return callbacks.ErrUnavailable
					}
					i.State = "report_finished"
					r.State = "report_finished"
				}
				if in.Action == "ended" {
					i.State = "ended"
					r.State = "report_pending"
				}
			}
		default:
			return errors.New("unsupported_callback_action")
		}
		return nil
	})
	h.callbackRespond(c, in, &r, e)
	if e != nil {
		return
	}
	h.publishCallbackRecord(r)
	if in.Action == "cancel" || (in.Action == "set" && !in.Enabled) {
		h.cancelConnectingCallback(r)
	}
	if in.Action == "report" {
		go h.speakCallback(c, s, r, in)
	}
	if in.Action == "audio_observed" {
		go h.readCallbackSpeech(c, s, r, in)
	}
	if in.Action == "set" && r.Enabled && r.Report == nil {
		go h.reconcileTaskCallbacks(context.Background(), s.ID, in.Request)
	}
}

func (h *Hub) speakCallback(c *Client, s *session.Session, r callbacks.Record, in callbackInput) {
	text := callbacks.Speech(r.Name, r.Report.Status, r.Report.Text)
	h.voiceMu.Lock()
	call := h.voiceClients[c]
	if call == nil || call.voiceID != in.Voice {
		h.voiceMu.Unlock()
		return
	}
	if call.callbackReport == r.Invite.ID {
		h.voiceMu.Unlock()
		return
	}
	call.callbackReport = r.Invite.ID
	call.callbackPolicy = r.ID
	call.callbackReporting = true
	call.callbackExpected = text
	call.callbackSpoken = ""
	call.callbackSpeechSettled = false
	h.voiceMu.Unlock()
	exec, ok := h.exec.(backend.RealtimeSpeechExecutor)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	e := exec.AppendRealtimeVoiceSpeech(ctx, s, in.Voice, text)
	state := "report_dispatched"
	if e != nil {
		state = "report_delivery_unknown"
	}
	receipt, receiptErr := h.callbacks.Mutate(context.Background(), "speech_"+r.Invite.ID, "speech_"+r.Invite.ReportOperation, r.ID, callbacks.AnyRevision, func(x *callbacks.Record, _ bool) error {
		if x.Invite == nil || x.Invite.ID != r.Invite.ID || x.Invite.ReportOperation != in.Operation || callbacks.Terminal(x.Invite.State) {
			return callbacks.ErrUnavailable
		}
		if !x.Invite.ContentVerified && !x.Invite.AudioObserved && x.State != "report_delivery_mismatch" {
			x.Invite.State = state
			x.State = state
		}
		return nil
	})
	if receiptErr == nil {
		h.publishCallbackRecord(receipt)
	}

	if e != nil {
		log.Printf("[callback] report append unconfirmed invitation=%s", r.Invite.ID)
		h.callbackRespond(c, in, nil, errors.New("匯報送達未確認，不會自動重播"))
	}
}
func (h *Hub) publishCallbackRecord(r callbacks.Record) {
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()
	for _, c := range clients {
		if !c.callbackCapable.Load() {
			continue
		}
		owner := h.pairedTaskDevice(c)
		if (owner == r.Requester && callbackBinding(c) == r.RequesterBinding) || (owner == r.Recipient.Device && callbackBinding(c) == r.Recipient.Binding) {
			x := r
			c.enqueueEvent(callbackEvent{Type: "task_callback", Authority: h.cfg.InstanceID, Accepted: true, Record: h.callbackView(c, &x)})
		}
	}
}
func (h *Hub) reconcileTaskCallbacks(ctx context.Context, sessionID, requestID string) {
	if h.callbacks == nil || h.messageQueue == nil {
		return
	}
	all, e := h.callbacks.List(ctx)
	if e != nil {
		return
	}
	for _, r := range all {
		if !r.Enabled || r.Report != nil || r.Session != sessionID {
			continue
		}
		s, ok := h.registry.Get(r.Session)
		if !ok || s.ResumeID() == "" || h.callbackEpoch(s.ID) != r.Epoch {
			continue
		}
		entry, found, e := h.messageQueue.Get(r.Session, r.Request)
		if e != nil {
			continue
		}
		status, text, ready := h.callbackResult(ctx, r, entry, found)
		if !ready {
			continue
		}
		effective, _, executionFound := h.callbackExecution(r)
		if status == "result_ready" && !executionFound {
			continue
		}
		turn := ""
		executionRequest := ""
		if executionFound {
			executionRequest = effective.Request
			turn = h.nativeTaskTurn(s, effective.Request)
		}
		if status == "result_ready" {
			if turn == "" {
				continue
			}
			reader, ok := h.exec.(interface {
				FinalAnswerForSession(*session.Session, string) (string, bool, error)
			})
			if !ok {
				continue
			}
			text, found, e = reader.FinalAnswerForSession(s, effective.Request)
			if e != nil || !found || strings.TrimSpace(text) == "" {
				continue
			}
		}
		work, workFound, _ := h.callbackWorkSource(ctx, r.Session, effective.Request)
		hash := callbacks.Digest(text)
		updated, e := h.callbacks.Mutate(ctx, "seal_"+r.ID+hash, "seal_"+hash, r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
			if !x.Enabled || x.Report != nil {
				return callbacks.ErrUnavailable
			}
			if x.Thread != "" && x.Thread != s.ResumeID() {
				return errors.New("callback_thread_changed")
			}
			if x.Config != s.SettingsSnapshot().ConfigRevision {
				return errors.New("callback_configuration_changed")
			}
			x.Thread = s.ResumeID()
			x.Report = &callbacks.Report{ExecutionRequest: executionRequest, ID: callbacks.ID(x.ID, turn, hash), Hash: hash, Text: text, Thread: s.ResumeID(), Turn: turn, Status: status, At: time.Now().UnixMilli()}
			if workFound {
				x.Report.SourceKind = "workitem"
				x.Report.SourceID = work.ItemID
				x.Report.SourceRevision = work.Revision
			}
			callbacks.NewInvitation(x, time.Now().UnixMilli(), "sealed_"+hash)
			return nil
		})
		if e == nil {
			h.publishCallbackRecord(updated)
		}
	}
	h.publishCallbackInvitations(ctx)
}
func (h *Hub) publishCallbackInvitations(ctx context.Context) {
	if h.callbacks == nil {
		return
	}
	all, e := h.callbacks.List(ctx)
	if e != nil {
		return
	}
	now := time.Now().UnixMilli()
	for _, r := range all {
		if !r.Enabled || r.Invite == nil || r.Report == nil {
			continue
		}
		i := r.Invite
		if i.State == "group_waiting" {
			attempted, checkErr := h.callbacks.DeliveryAttempted(ctx, r)
			if checkErr != nil || attempted || callbacks.GroupBlocked(r, all) {
				continue
			}
			updated, err := h.callbacks.Mutate(ctx, "group_promote_"+i.ID+fmt.Sprint(r.Revision), "group_promote", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
				if !x.Enabled || x.Invite == nil || x.Invite.ID != i.ID || x.Invite.State != "group_waiting" {
					return callbacks.ErrUnavailable
				}
				x.Invite.State = "delivery_pending"
				x.Invite.Reason = ""
				x.Invite.Expires = now + 60000
				x.State = "invitation_pending"
				return nil
			})
			if err == nil {
				h.publishCallbackRecord(updated)
			}
			continue
		}
		if i.Claim == "" && !callbacks.Terminal(i.State) && now >= i.Expires {
			updated, e := h.callbacks.Mutate(ctx, "expire_"+i.ID, "expire", r.ID, r.Revision, func(x *callbacks.Record, _ bool) error {
				x.Invite.State = "expired"
				x.State = "report_pending"
				return nil
			})
			if e == nil {
				h.publishCallbackRecord(updated)
			}
			continue
		}
		if i.State != "delivery_pending" {
			continue
		}
		if !h.callbackReportCurrent(ctx, r) {
			continue
		}
		source, found := h.registry.Get(r.Session)
		valid := found && source.Backend() == backend.Codex && !source.Snapshot().Hidden && source.State() != session.Closed && h.controls.MobileMayWrite(r.Session) && h.callbackEpoch(r.Session) == r.Epoch && source.ResumeID() == r.Thread && source.SettingsSnapshot().ConfigRevision == r.Config
		paired := false
		for _, b := range h.pairing.DeviceBindings() {
			if b.DeviceID == r.Recipient.Device && callbacks.Digest(b.Token) == r.Recipient.Binding {
				paired = true
			}
		}
		if !valid || !paired {
			updated, e := h.callbacks.Mutate(ctx, "revoke_"+i.ID, "source_or_recipient_changed", r.ID, callbacks.AnyRevision, func(x *callbacks.Record, _ bool) error {
				if x.Invite == nil || x.Invite.ID != i.ID || x.Invite.Claim != "" {
					return callbacks.ErrUnavailable
				}
				x.Invite.State = "revoked"
				x.Invite.Reason = "來源或接收裝置已變更，請重新設定匯報"
				x.State = "report_pending"
				return nil
			})
			if e == nil {
				h.publishCallbackRecord(updated)
			}
			continue
		}
		if source.State() != session.Idle || source.QueueLen() > 0 {
			continue
		}
		h.publishCallbackRecord(r)
		if h.fcm != nil {
			valid := false
			for _, b := range h.pairing.DeviceBindings() {
				if b.DeviceID == r.Recipient.Device && callbacks.Digest(b.Token) == r.Recipient.Binding {
					valid = true
				}
			}
			if valid {
				h.fcm.NotifyTaskCallback(r.Authority, r.Session, r.Request, r.ID, i.ID, r.Recipient.Device, i.Expires)
			}
		}
	}
}
func (h *Hub) StartTaskCallbacks(ctx context.Context) {
	if h.callbacks == nil {
		return
	}
	go func() {
		timer := time.NewTicker(3 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				all, e := h.callbacks.List(ctx)
				if e != nil {
					continue
				}
				for _, r := range all {
					if r.Enabled && r.Report == nil {
						h.reconcileTaskCallbacks(ctx, r.Session, r.Request)
					}
				}
				h.publishCallbackInvitations(ctx)
			}
		}
	}()
}

func (h *Hub) armAdmittedCallback(ctx context.Context, c *Client, s *session.Session, request string) error {
	if h.callbacks == nil || !c.callbackNative.Load() || h.pairedTaskDevice(c) == "" {
		return errors.New("來電未登記：此裝置尚未支援接聽")
	}
	if s.Backend() != backend.Codex {
		return errors.New("來電未登記：原對話不支援語音")
	}
	epoch := h.callbackEpoch(s.ID)
	if epoch == "" {
		h.runtimes.Ensure(s.ID, s.State().String(), s.QueueLen())
		epoch = h.callbackEpoch(s.ID)
	}
	if epoch == "" {
		return errors.New("callback_epoch_unknown")
	}
	id := h.callbackRecordID(s.ID, epoch, request)
	if _, found, e := h.callbacks.Get(ctx, id); e != nil || found {
		return e
	}
	entry, found, e := h.messageQueue.Get(s.ID, request)
	if e != nil || !found {
		return errors.New("callback_task_not_found")
	}
	payload := h.taskAdmission(entry)
	if payload.Origin != nil {
		return errors.New("子任務不單獨來電")
	}
	r, e := h.callbacks.Mutate(ctx, "admit_"+id, "admit_"+id, id, 0, func(r *callbacks.Record, _ bool) error {
		*r = callbacks.Record{Authority: h.cfg.InstanceID, Session: s.ID, Request: request, Epoch: epoch, Thread: s.ResumeID(), Config: s.SettingsSnapshot().ConfigRevision, Origin: payload.OwnerDevice, Requester: h.pairedTaskDevice(c), RequesterBinding: callbackBinding(c), Recipient: h.callbackRecipient(c), Name: s.Name(), Enabled: true, State: "armed"}
		return nil
	})
	if e == nil {
		h.publishCallbackRecord(r)
	}
	return e
}
func (h *Hub) recoverCallbackAdmissions(ctx context.Context) {
	if h.callbacks == nil || h.messageQueue == nil {
		return
	}
	admissions, e := h.messageQueue.CallbackAdmissions()
	if e != nil {
		return
	}
	for _, a := range admissions {
		s, ok := h.registry.Get(a.Session)
		if !ok {
			continue
		}
		id := h.callbackRecordID(s.ID, h.callbackEpoch(s.ID), a.Request)
		if _, found, e := h.callbacks.Get(ctx, id); found || e != nil {
			continue
		}
		h.mu.RLock()
		var owner *Client
		for c := range h.clients {
			if h.pairedTaskDevice(c) == a.Owner && callbackBinding(c) == a.Binding && c.callbackNative.Load() {
				owner = c
				break
			}
		}
		h.mu.RUnlock()
		if owner != nil {
			_ = h.armAdmittedCallback(ctx, owner, s, a.Request)
		}
	}
}

func (h *Hub) callbackView(c *Client, r *callbacks.Record) *callbacks.Record {
	if r == nil {
		return nil
	}
	copy := *r
	if r.Invite != nil {
		i := *r.Invite
		copy.Invite = &i
		if (copy.State == "report_audio_observed" || copy.State == "report_finished") && (!i.ContentVerified || !i.AudioObserved) {
			copy.State = "report_output_unverified"
		}
	}
	copy.RecipientEligible = h.pairedTaskDevice(c) == r.Recipient.Device && callbackBinding(c) == r.Recipient.Binding
	return &copy
}
