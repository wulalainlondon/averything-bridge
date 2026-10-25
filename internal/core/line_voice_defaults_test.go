package core

import (
	"bytes"
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/linebridge"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGlobalLineVoicePolicyAuthorizesNewVoiceWithoutPerConversationOrPhoneGrants(t *testing.T) {
	h, _ := newControlTestHub(t, t.TempDir())
	ctx := context.Background()
	store, err := linebridge.OpenStore(t.TempDir(), "fixture", bytes.Repeat([]byte{8}, 32))
	if err != nil {
		t.Fatal(err)
	}
	a, err := store.CreateAccount(ctx, "original", "device:original-owner", "/fixture/profile", "/fixture/helper")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.BindPrincipal(ctx, a.ID, "u"+strings.Repeat("1", 32)); err != nil {
		t.Fatal(err)
	}
	a, _ = store.Account(ctx, a.ID)
	svc := linebridge.NewService(store, nil, linebridge.Config{SubjectAllowed: h.LineSubjectAllowed, VoiceDispatchSubjectAllowed: h.LineVoiceDispatchSubjectAllowed, VoiceDispatchAccounts: []linebridge.VoiceDispatchAccount{{Account: a.ID, Binding: a.Revision}}})
	h.SetLineService(svc)
	t.Cleanup(func() { svc.Close(); store.Close() })
	h.pairing.OpenEnrollment(0)
	if err = h.pairing.Claim("original-phone-bearer", "new-phone"); err != nil {
		t.Fatal(err)
	}
	worker := filepath.Join(t.TempDir(), "worker.mjs")
	os.WriteFile(worker, []byte("// no actual network worker in fixture"), 0600)
	h.SetLineVoiceRuntime("/fixture/node", worker)
	s := h.registry.Create("new-ordinary-conversation", "new voice", t.TempDir(), backend.Codex, "", "read-only", "native-new-voice")
	caller := backend.SessionControlCaller{Parent: s, RequestID: "new-request", ToolCallID: "original-native-tool", VoiceID: "original-voice"}
	result, err := h.ExecuteLineTool(ctx, caller, "accounts", json.RawMessage(`{}`))
	if err != nil || len(result.([]linebridge.Account)) != 1 {
		t.Fatal(result, err)
	}
	access, err := h.ExecuteLineTool(ctx, caller, "access", json.RawMessage(`{"account_ref":"`+a.ID+`"}`))
	if err != nil || !access.(linebridge.Access).VoiceDispatch.Enabled {
		t.Fatal(access, err)
	}
	for _, method := range []string{"grant", "set_grants", "transfer_owner", "create_account", "pair"} {
		if _, err = h.ExecuteLineTool(ctx, caller, method, json.RawMessage(`{}`)); err == nil {
			t.Fatal("voice became administrator", method)
		}
	}
	target, err := store.SaveTarget(ctx, a.ID, "user-peer", "Morrie", "user")
	if err != nil {
		t.Fatal(err)
	}
	c := newTestClient(h)
	c.deviceID = "new-phone"
	h.latestMu.Lock()
	h.latestByDevice[c.deviceID] = c
	h.latestMu.Unlock()
	h.voiceClients = map[*Client]*clientVoiceCall{c: {session: s, voiceID: caller.VoiceID, requestID: caller.RequestID, handoffCapable: true}}
	result, err = h.HandoffLineVoice(ctx, caller, a.ID, target.Ref)
	if err != nil {
		t.Fatal("new voice still needed individual grants", err)
	}
	receipt := result.(map[string]any)
	if receipt["state"] != "handoff_requested" || receipt["connected"] != false {
		t.Fatal(receipt)
	}
	event := nextVoiceState(t, c, "line_handoff")
	if event["line_call_id"] != receipt["call_id"] || event["voice_id"] != caller.VoiceID {
		t.Fatal("source binding lost", event)
	}
	caller.VoiceID = "foreign-voice"
	if _, err = h.HandoffLineVoice(ctx, caller, a.ID, target.Ref); err == nil {
		t.Fatal("unbound voice accepted")
	}
}
func TestLineGlobalVoiceEligibilityExcludesDelegatesAndManagedOrClosedScopes(t *testing.T) {
	h, _ := newControlTestHub(t, t.TempDir())
	normal := h.registry.Create("normal", "normal", t.TempDir(), backend.Codex, "", "", "native-normal")
	if !h.LineVoiceDispatchSubjectAllowed("session:" + normal.ID) {
		t.Fatal("ordinary conversation excluded")
	}
	for _, id := range []string{"s_dg_child", "pm_managed", "unknown"} {
		if id != "unknown" {
			h.registry.Create(id, id, t.TempDir(), backend.Codex, "", "", "native-"+id)
		}
		if h.LineVoiceDispatchSubjectAllowed("session:" + id) {
			t.Fatal(id)
		}
	}
	normal.Close()
	if h.LineVoiceDispatchSubjectAllowed("session:" + normal.ID) {
		t.Fatal("closed conversation inherited access")
	}
	if h.LineVoiceDispatchSubjectAllowed("device:not-paired") {
		t.Fatal("unknown phone")
	}
}
