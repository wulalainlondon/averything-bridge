package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/linebridge"
	"everything-go/internal/linecontrol"
	"os"
	"path/filepath"
	"testing"
)

func TestLINEFixedHandoffBindsOriginalVoiceAndBothGrants(t *testing.T) {
	h, _ := newControlTestHub(t, t.TempDir())
	store, a := testLineCatalog(h, t)
	ctx := context.Background()
	worker := filepath.Join(t.TempDir(), "worker.mjs")
	if e := os.WriteFile(worker, []byte("// fixture"), 0600); e != nil {
		t.Fatal(e)
	}
	h.SetLineVoiceRuntime("/fixture/node", worker)
	s := h.registry.Create("line-voice-parent", "parent", t.TempDir(), backend.Codex, "", "read-only", "native-line")
	c := newTestClient(h)
	c.deviceID = "fixture-device"
	h.latestMu.Lock()
	h.latestByDevice[c.deviceID] = c
	h.latestMu.Unlock()
	target, e := store.SaveTarget(ctx, a.ID, "fixture-peer", "Morrie", "user")
	if e != nil {
		t.Fatal(e)
	}
	caller := backend.SessionControlCaller{Parent: s, RequestID: "request", TurnID: "turn", ToolCallID: "native-tool", VoiceID: "voice-original"}
	original := linebridge.Caller{Subject: "device:fixture-device"}
	if e = store.Grant(ctx, original, a.ID, "session:"+s.ID, []string{"status"}); e != nil {
		t.Fatal(e)
	}
	h.voiceClients = map[*Client]*clientVoiceCall{c: {session: s, voiceID: "voice-original", requestID: "request", handoffCapable: true}}
	args, _ := json.Marshal(map[string]string{"account_ref": a.ID, "target_ref": target.Ref})
	q := linecontrol.Request{ID: "lop_handoff_original", Thread: s.ResumeID(), Method: "handoff_call", Params: args}
	result, e := h.executeLineControl(ctx, fixtureLineOrigin{caller}, q)
	if e != nil || result.OK || result.Code != "line_forbidden" {
		t.Fatal("scope bypass", result, e)
	}
	if e = store.Grant(ctx, original, a.ID, "session:"+s.ID, []string{"status", "call"}); e != nil {
		t.Fatal(e)
	}
	result, e = h.executeLineControl(ctx, fixtureLineOrigin{caller}, q)
	if e != nil || !result.OK {
		t.Fatal(result, e)
	}
	receipt := result.Result.(map[string]any)
	if receipt["connected"] != false {
		t.Fatal("reservation called connected")
	}
	id := receipt["call_id"].(string)
	event := nextVoiceState(t, c, "line_handoff")
	if event["line_call_id"] != id || event["voice_id"] != caller.VoiceID || event["line_target_ref"] != target.Ref {
		t.Fatal(event)
	}
	caller.ToolCallID = "different-native-observation"
	replay, e := h.executeLineControl(ctx, fixtureLineOrigin{caller}, q)
	if e != nil || !replay.OK || replay.Result.(map[string]any)["call_id"] != id {
		t.Fatal(replay, e)
	}
	caller.VoiceID = "wrong-voice"
	if got, e := h.executeLineControl(ctx, fixtureLineOrigin{caller}, q); e != nil || got.OK {
		t.Fatal("different voice accepted", got, e)
	}
	query, _ := json.Marshal(map[string]string{"account_ref": a.ID, "original_intent": q.ID})
	caller.VoiceID = ""
	read, e := h.executeLineControl(ctx, fixtureLineOrigin{caller}, linecontrol.Request{ID: "lop_call_receipt", Thread: s.ResumeID(), Method: "call_intent_status", Params: query})
	if e != nil || !read.OK || read.Result.(map[string]any)["found"] != true {
		t.Fatal(read, e)
	}
}
