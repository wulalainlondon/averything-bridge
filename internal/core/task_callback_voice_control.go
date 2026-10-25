package core

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"everything-go/internal/callbacks"
	"everything-go/internal/messagequeue"
)

func (h *Hub) ControlTaskCallback(ctx context.Context, caller backend.SessionControlCaller, input backend.TaskCallbackControl) (any, error) {
	if h.callbacks == nil || caller.Parent == nil || caller.VoiceID == "" || caller.ToolCallID == "" || caller.TurnID == "" {
		return nil, errors.New("callback_live_voice_caller_required")
	}
	h.voiceMu.Lock()
	var client *Client
	for c, call := range h.voiceClients {
		if c.live() && call.session == caller.Parent && call.voiceID == caller.VoiceID && !call.stopping {
			client = c
			break
		}
	}
	h.voiceMu.Unlock()
	if client == nil || h.pairedTaskDevice(client) == "" {
		return nil, errors.New("callback_original_voice_expired")
	}
	operation := "callback_" + callbacks.Digest(caller.VoiceID + caller.ToolCallID)[:32]
	body, _ := json.Marshal(input)
	intent := callbacks.Digest(string(body) + caller.Parent.ID + caller.VoiceID + callbackBinding(client))
	original, hash, found, e := h.callbacks.Operation(ctx, callbacks.ID(h.pairedTaskDevice(client), operation))
	if e != nil {
		return nil, e
	}
	if found {
		if hash != intent {
			return nil, callbacks.ErrOperation
		}
		return callbackEvent{Type: "task_callback", Authority: h.cfg.InstanceID, Operation: operation, Accepted: true, Record: h.callbackView(client, &original)}, nil
	}
	request := input.TaskRequestID
	if request == "" && input.Action != "list_tasks" {
		request = caller.Parent.ActiveQueuedID()
		if request == "" {
			snapshot, e := h.messageQueue.Snapshot(caller.Parent.ID)
			if e != nil {
				return nil, e
			}
			active := []string{}
			for _, r := range snapshot.Items {
				a := h.taskAdmission(r)
				if a.MessagePurpose == "instruction" && a.Origin == nil && (r.State == messagequeue.Queued || r.State == messagequeue.Running) {
					active = append(active, r.RequestID)
				}
			}
			if len(active) != 1 {
				return map[string]any{"status": "selection_required", "message": "請先使用 list_tasks 選定原指令；未改動來電設定"}, nil
			}
			request = active[0]
		}
	}
	action := input.Action
	switch action {
	case "set":
		if input.Enabled == nil {
			return nil, errors.New("callback_enabled_required")
		}
	case "cancel":
	case "status":
		action = "get"
	case "list_tasks":
		action = "sources"
	default:
		return nil, errors.New("callback_action_unsupported")
	}
	epoch := h.callbackEpoch(caller.Parent.ID)
	r, found, e := h.callbacks.Get(ctx, h.callbackRecordID(caller.Parent.ID, epoch, request))
	if e != nil {
		return nil, e
	}
	revision := uint64(0)
	if found {
		revision = r.Revision
	}
	var result callbackEvent
	in := callbackInput{intent: intent, Type: "task_callback_request", Operation: operation, Action: action, Session: caller.Parent.ID, Request: request, Expected: revision, result: &result}
	if input.Enabled != nil {
		in.Enabled = *input.Enabled
	}
	h.executeTaskCallback(ctx, client, in)
	if result.Type == "" {
		return nil, errors.New("callback_result_unconfirmed")
	}
	return result, nil
}
