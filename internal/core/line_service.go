package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/linebridge"
	"everything-go/internal/session"
	"net/http"
	"strings"
)

func (h *Hub) SetLineService(service *linebridge.Service) { h.lineService = service }
func (h *Hub) LineServiceEnabled() bool                   { return h.lineService != nil }
func (h *Hub) LineSubjectAllowed(subject string) bool {
	if strings.HasPrefix(subject, "session:") {
		return h.LineSessionAllowed(strings.TrimPrefix(subject, "session:"))
	}
	if strings.HasPrefix(subject, "device:") && h.pairing != nil {
		return h.pairedPushDevices()[strings.TrimPrefix(subject, "device:")]
	}
	return false
}
func (h *Hub) LineSubjects() []linebridge.Subject {
	out := []linebridge.Subject{}
	if h.pairing == nil {
		return out
	}
	seen := map[string]bool{}
	for _, binding := range h.pairing.DeviceBindings() {
		if binding.DeviceID == "" || seen[binding.DeviceID] {
			continue
		}
		seen[binding.DeviceID] = true
		out = append(out, linebridge.Subject{Ref: "device:" + binding.DeviceID, Name: binding.DeviceID, Kind: "paired_device"})
	}
	for _, item := range h.registry.List() {
		snap := item.Snapshot()
		policy, err := h.PMConfiguration(item.ID)
		if h.LineSessionAllowed(item.ID) && h.controllerInScope(item) && !snap.Hidden && item.State() != session.Closed && !strings.HasPrefix(item.ID, "pm_") && err == nil && policy == nil {
			out = append(out, linebridge.Subject{Ref: "session:" + item.ID, Name: snap.Name, Kind: "session"})
		}
	}
	return out
}
func (h *Hub) LineSessionAllowed(id string) bool {
	if h.workspaceReservedSession(id) {
		return false
	}
	_, ok := h.registry.Get(id)
	return ok && !strings.HasPrefix(id, "s_dg_") && h.controls.MobileMayWrite(id)
}

// Global voice dispatch belongs only to this Go's paired devices and ordinary
// conversations the mobile user can operate. Delegates/PM/hidden/closed scopes
// do not inherit it, even if a caller presents a valid native tool origin.
func (h *Hub) LineVoiceDispatchSubjectAllowed(subject string) bool {
	if !h.LineSubjectAllowed(subject) {
		return false
	}
	if strings.HasPrefix(subject, "device:") {
		return true
	}
	if !strings.HasPrefix(subject, "session:") {
		return false
	}
	id := strings.TrimPrefix(subject, "session:")
	s, ok := h.registry.Get(id)
	if !ok || strings.HasPrefix(id, "pm_") || !h.controllerInScope(s) {
		return false
	}
	snap := s.Snapshot()
	policy, err := h.PMConfiguration(id)
	return !snap.Hidden && s.State() != session.Closed && err == nil && policy == nil
}
func (h *Hub) ServeLineAPI(w http.ResponseWriter, r *http.Request) {
	authorize := func(r *http.Request) (linebridge.Caller, bool) {
		fields := strings.Fields(r.Header.Get("Authorization"))
		if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
			return linebridge.Caller{}, false
		}
		token := fields[1]
		device := r.Header.Get("X-Bridge-Device-ID")
		if token == "" || device == "" || h.pairing == nil || !h.pairing.MatchesDevice(token, device) {
			return linebridge.Caller{}, false
		}
		return linebridge.Caller{Subject: "device:" + device, Admin: true}, true
	}
	linebridge.Handler(h.lineService, authorize).ServeHTTP(w, r)
}
func (h *Hub) ExecuteLineTool(ctx context.Context, caller backend.SessionControlCaller, method string, args json.RawMessage) (any, error) {
	if h.lineService == nil || caller.Parent == nil || caller.ToolCallID == "" {
		return nil, linebridge.Fail("line_caller_unavailable")
	}
	if h.workspaceReservedSession(caller.Parent.ID) || h.workspaceManagedRequest(caller.Parent.ID, caller.RequestID) {
		return nil, linebridge.Fail("line_caller_not_owned")
	}
	registered, ok := h.registry.Get(caller.Parent.ID)
	if !ok || registered != caller.Parent || strings.HasPrefix(registered.ID, "s_dg_") || !h.controls.MobileMayWrite(registered.ID) {
		return nil, linebridge.Fail("line_caller_not_owned")
	}
	allowed := map[string]bool{"get_message_page": true, "set_target_alias": true, "query_operations": true, "get_operation_detail": true, "update_scheduled_message": true, "resolve_schedule_time": true, "access": true, "list_subscriptions": true, "list_attachments": true, "download_resource": true, "import_resource": true, "reschedule_operation": true, "handoff_call": true, "cancel_call": true, "get_call": true, "subscribe": true, "accounts": true, "status": true, "health": true, "find_openchats": true, "find_targets": true, "get_messages": true, "send_message": true, "get_operation": true, "list_operations": true, "schedule_message": true, "cancel_operation": true}
	if !allowed[method] {
		return nil, linebridge.Fail("line_tool_unavailable")
	}
	var input map[string]json.RawMessage
	if len(args) > 65536 || json.Unmarshal(args, &input) != nil || input == nil {
		return nil, linebridge.Fail("line_request_invalid")
	}
	if _, exists := input["method"]; exists {
		return nil, linebridge.Fail("line_request_invalid")
	}
	input["method"], _ = json.Marshal(method)
	data, _ := json.Marshal(input)
	request, err := linebridge.DecodeRequest(data)
	if err != nil {
		return nil, err
	}
	if request.Limit == 0 && (method == "get_messages" || method == "find_targets" || method == "find_openchats") {
		request.Limit = 20
	}
	// Preserve this exact native invocation as the default idempotency key.
	if (method == "send_message" || method == "schedule_message") && request.Intent == "" {
		request.Intent = caller.ToolCallID
	}
	if method == "list_attachments" {
		limit := request.Limit
		if limit == 0 {
			limit = 20
		}
		if limit < 1 || limit > 50 || h.attachments == nil {
			return nil, linebridge.Fail("line_limit_invalid")
		}
		out := []map[string]any{}
		for _, record := range h.attachments.ListSession(registered.ID, limit) {
			out = append(out, map[string]any{"attachment_ref": record.AttachmentID, "name": record.DisplayName, "mime_type": record.MIMEType, "created_at": record.CreatedAt})
		}
		return out, nil
	}
	if method == "handoff_call" {
		return h.HandoffLineVoice(ctx, caller, request.Account, request.Target)
	}
	result, err := h.lineService.Execute(ctx, linebridge.Caller{Subject: "session:" + registered.ID}, request)
	capMessages := func(messages []linebridge.Message) {
		budget := 24000
		for i := range messages {
			runes := []rune(messages[i].Text)
			limit := 2000
			if budget < limit {
				limit = budget
			}
			if len(runes) > limit {
				messages[i].Text = string(runes[:limit])
				messages[i].TextTruncated = true
			}
			budget -= len([]rune(messages[i].Text))
		}
	}
	if messages, ok := result.([]linebridge.Message); ok {
		capMessages(messages)
		result = messages
	}
	if page, ok := result.(linebridge.MessagePage); ok {
		capMessages(page.Messages)
		result = page
	}

	return result, err
}
