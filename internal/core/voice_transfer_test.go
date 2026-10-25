package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/sessiondispatch"
	"everything-go/internal/voicetransfer"
	"testing"
)

func transferFixture(t *testing.T) (*Hub, *Client, backend.SessionControlCaller, backend.SessionControlRequest) {
	h, _, caller, target := controllerFixture(t)
	c := sharedReadClient(t, h, "transfer-phone")
	h.voiceClients = map[*Client]*clientVoiceCall{c: {session: caller.Parent, voiceID: caller.VoiceID, externalMusicPlatform: "android"}}
	s, err := voicetransfer.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h.transfers = s
	t.Cleanup(func() { s.Close() })
	if _, err = h.dispatches.SetGrant(context.Background(), caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0); err != nil {
		t.Fatal(err)
	}
	return h, c, caller, backend.SessionControlRequest{Action: "voice_transfer", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), ExpectedConfigRevision: target.SettingsSnapshot().ConfigRevision}
}
func transferAction(t *testing.T, h *Hub, c *Client, r voicetransfer.Record, action string, release, connected bool, voice string) voicetransfer.Record {
	t.Helper()
	input, _ := json.Marshal(map[string]any{"transfer_id": r.ID, "action": action, "revision": r.Revision, "native_release_confirmed": release, "native_connected": connected, "target_voice_id": voice})
	h.handleVoiceTransfer(c, clientproto.Command{SessionID: r.Source.Session, RequestID: "human-action", Data: string(input)})
	v, err := h.transfers.Get(context.Background(), r.ID, h.pairedTaskDevice(c))
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func TestLegacyTransferRequiresConfirmationExactRemoteReleaseAndNativeConnected(t *testing.T) {
	h, c, caller, input := transferFixture(t)
	value, err := h.ControlSession(context.Background(), caller, input)
	if err != nil {
		t.Fatal(err)
	}
	r := value.(voicetransfer.Record)
	if r.State != "confirmation_required" || h.voiceClients[c] == nil {
		t.Fatal("model tool changed active call", r)
	}
	if got := transferAction(t, h, c, r, "released", true, false, ""); got.State != r.State {
		t.Fatal("skipped human tap")
	}
	r = transferAction(t, h, c, r, "confirm", false, false, "")
	if r.State != "confirmed" {
		t.Fatal(r)
	}
	if got := transferAction(t, h, c, r, "released", true, false, ""); got.State != r.State {
		t.Fatal("remote call still active")
	}
	delete(h.voiceClients, c)
	if got := transferAction(t, h, c, r, "released", false, false, ""); got.State != r.State {
		t.Fatal("native release not confirmed")
	}
	r = transferAction(t, h, c, r, "released", true, false, "")
	if r.State != "a_released" {
		t.Fatal(r)
	}
	r = transferAction(t, h, c, r, "starting", true, false, "")
	if r.State != "starting_b" {
		t.Fatal(r)
	}
	if got := transferAction(t, h, c, r, "connected", true, true, "new-voice"); got.State != r.State {
		t.Fatal("navigation upgraded to connected")
	}
	target, _ := h.registry.Get(input.SessionID)
	h.voiceClients[c] = &clientVoiceCall{session: target, voiceID: "new-voice"}
	if got := transferAction(t, h, c, r, "connected", true, false, "new-voice"); got.State != r.State {
		t.Fatal("native connected absent")
	}
	r = transferAction(t, h, c, r, "connected", true, true, "new-voice")
	if r.State != "completed" || r.TargetVoice != "new-voice" {
		t.Fatal(r)
	}
}
func TestDirectSpokenTransferUsesOriginalLiveCallWithoutSecondHumanTap(t *testing.T) {
	h, c, caller, input := transferFixture(t)
	value, err := h.ControlSession(context.Background(), caller, input)
	if err != nil {
		t.Fatal(err)
	}
	r := value.(voicetransfer.Record)
	if r.Authorization != "voice_command" {
		t.Fatal("explicit live intent not identified")
	}
	data, _ := json.Marshal(map[string]any{"transfer_id": r.ID, "action": "confirm", "revision": r.Revision, "automatic": true})
	h.handleVoiceTransfer(c, clientproto.Command{SessionID: r.Source.Session, RequestID: "native-direct", Data: string(data)})
	got, err := h.transfers.Get(context.Background(), r.ID, h.pairedTaskDevice(c))
	if err != nil || got.State != "confirmed" || h.voiceClients[c] == nil {
		t.Fatal("native authorization lost original call", got, err)
	}
	if next := transferAction(t, h, c, got, "released", true, false, ""); next.State != "confirmed" {
		t.Fatal("direct authorization skipped actual A release")
	}
}
func TestLegacyReceiptCannotAutoConfirmEvenOnOriginalDevice(t *testing.T) {
	h, c, caller, input := transferFixture(t)
	target, _ := h.registry.Get(input.SessionID)
	r, err := h.transfers.Create(context.Background(), voicetransfer.Record{ID: voicetransfer.ID("legacy", "request", "tool"), Authority: h.cfg.InstanceID, Source: transferTarget(caller.Parent), Target: transferTarget(target), SourceVoice: caller.VoiceID, Device: h.pairedTaskDevice(c)})
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"transfer_id": r.ID, "action": "confirm", "revision": r.Revision, "automatic": true})
	h.handleVoiceTransfer(c, clientproto.Command{SessionID: r.Source.Session, RequestID: "legacy-auto", Data: string(data)})
	got, _ := h.transfers.Get(context.Background(), r.ID, h.pairedTaskDevice(c))
	if got.State != "confirmation_required" {
		t.Fatal("legacy receipt became direct permission", got)
	}
}
func TestTransferRejectsScopeDriftRevocationAndForgedCaller(t *testing.T) {
	h, c, caller, input := transferFixture(t)
	ctx := context.Background()
	bad := caller
	bad.VoiceID = "forged"
	if _, err := h.ControlSession(ctx, bad, input); err == nil {
		t.Fatal("forged voice accepted")
	}
	input.InstanceID = "remote"
	if _, err := h.ControlSession(ctx, caller, input); err == nil {
		t.Fatal("cross-bridge accepted")
	}
	input.InstanceID = ""
	input.ExpectedThreadID = "wrong"
	if _, err := h.ControlSession(ctx, caller, input); err == nil {
		t.Fatal("stale target accepted")
	}
	input.ExpectedThreadID = "target-thread"
	v, err := h.ControlSession(ctx, caller, input)
	if err != nil {
		t.Fatal(err)
	}
	r := v.(voicetransfer.Record)
	grant, _ := h.dispatches.Grant(ctx, caller.Parent.ID)
	if _, err = h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{}, grant.Revision); err != nil {
		t.Fatal(err)
	}
	if got := transferAction(t, h, c, r, "confirm", false, false, ""); got.State != "confirmation_required" {
		t.Fatal("revoked target acquired transfer")
	}
	r = transferAction(t, h, c, r, "cancel", false, false, "")
	if r.State != "cancelled" || h.voiceClients[c] == nil {
		t.Fatal("cancelled preflight affected A", r)
	}
}
