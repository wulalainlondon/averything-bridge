package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/callbacks"
	"everything-go/internal/messagequeue"
	"testing"
)

func TestVoiceCallbackReplayKeepsOriginalReceiptWithoutRearmingCancelledTask(t *testing.T) {
	h, c, s, _ := callbackFixture(t)
	taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Queued)
	h.voiceClients = map[*Client]*clientVoiceCall{c: {session: s, voiceID: "voice-callback-settings"}}
	caller := backend.SessionControlCaller{Parent: s, VoiceID: "voice-callback-settings", TurnID: "voice-turn-settings", ToolCallID: "tool-callback-setting"}
	enabled := true
	input := backend.TaskCallbackControl{Action: "set", TaskRequestID: "r_callbacktask", Enabled: &enabled}
	if _, e := h.ControlTaskCallback(context.Background(), caller, input); e != nil {
		t.Fatal(e)
	}
	id := h.callbackRecordID(s.ID, h.callbackEpoch(s.ID), "r_callbacktask")
	r, _, _ := h.callbacks.Get(context.Background(), id)
	if !r.Enabled || r.Recipient.Device != "phone-a" {
		t.Fatal("not bound to actual voice recipient", r)
	}
	got := callbackAction(t, h, c, callbackInput{Action: "cancel", Operation: "callback_cancel_after_voice_setting", Session: s.ID, Request: r.Request, Expected: r.Revision})
	if got["accepted"] != true {
		t.Fatal(got)
	}
	cancelled, _, _ := h.callbacks.Get(context.Background(), id)
	if _, e := h.ControlTaskCallback(context.Background(), caller, input); e != nil {
		t.Fatal("same tool call lost original receipt", e)
	}
	replayed, _, _ := h.callbacks.Get(context.Background(), id)
	if replayed.Enabled || replayed.Revision != cancelled.Revision {
		t.Fatal("old voice tool replay rearmed cancelled callback")
	}
	enabled = false
	if _, e := h.ControlTaskCallback(context.Background(), caller, input); e != callbacks.ErrOperation {
		t.Fatal("same tool call accepted changed arguments", e)
	}
}

func TestVoiceCallbackRequiresActualLiveOriginalVoice(t *testing.T) {
	h, _, s, _ := callbackFixture(t)
	if _, e := h.ControlTaskCallback(context.Background(), backend.SessionControlCaller{Parent: s, VoiceID: "nonexistent-voice", TurnID: "turn", ToolCallID: "tool"}, backend.TaskCallbackControl{Action: "list_tasks"}); e == nil {
		t.Fatal("fabricated voice caller accepted")
	}
	rows, _ := h.callbacks.List(context.Background())
	if len(rows) != 0 {
		t.Fatal("unverified caller changed policies")
	}
}
