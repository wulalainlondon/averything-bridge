package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/voicecontrol"
	"everything-go/internal/voicetransfer"
	"testing"
)

type fixedVoiceCaller struct{ caller backend.SessionControlCaller }

func (f fixedVoiceCaller) ResolveVoiceControlCaller(thread, id, fp string) (backend.SessionControlCaller, error) {
	return f.caller, nil
}
func TestVoiceControlOriginalIntentIsQueryOnlyAfterAcceptance(t *testing.T) {
	h, c, caller, input := transferFixture(t)
	ctx := context.Background()
	input.Action = ""
	params, _ := json.Marshal(input)
	q := voicecontrol.Request{ID: "vtr_original_transfer", Thread: caller.Parent.ResumeID(), Method: "voice_transfer", Params: params}
	result, e := h.executeVoiceControl(ctx, fixedVoiceCaller{caller}, q)
	if e != nil {
		t.Fatal(e)
	}
	r := result.Result.(voicetransfer.Record)
	if r.State != "confirmation_required" || h.voiceClients[c] == nil {
		t.Fatal("CLI changed live voice", r)
	}
	another := caller
	another.ToolCallID = "second-native-query"
	result, e = h.executeVoiceControl(ctx, fixedVoiceCaller{another}, q)
	if e != nil || result.Result.(voicetransfer.Record).ID != r.ID {
		t.Fatal("uncertain receipt recreated intent", e)
	}
	input.ExpectedThreadID = "replacement"
	q.Params, _ = json.Marshal(input)
	if _, e = h.executeVoiceControl(ctx, fixedVoiceCaller{another}, q); e == nil {
		t.Fatal("intent parameter substitution")
	}
	q.Method = "dispatch_to_session"
	if q.Valid() {
		t.Fatal("text dispatch admitted")
	}
}
func TestVoiceControlRejectsCallerOverrideAndNoLiveOriginal(t *testing.T) {
	h, c, caller, _ := transferFixture(t)
	ctx := context.Background()
	q := voicecontrol.Request{ID: "vtr_forbidden_caller", Thread: caller.Parent.ResumeID(), Method: "list_sessions", Params: json.RawMessage(`{"caller":"other"}`)}
	if _, e := h.executeVoiceControl(ctx, fixedVoiceCaller{caller}, q); e == nil {
		t.Fatal("caller override accepted")
	}
	q.Params = json.RawMessage(`{}`)
	delete(h.voiceClients, c)
	if _, e := h.executeVoiceControl(ctx, fixedVoiceCaller{caller}, q); e == nil {
		t.Fatal("retired voice accepted")
	}
}
