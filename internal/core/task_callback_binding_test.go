package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/callbacks"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"strings"
	"testing"
	"time"
)

func TestAnsweredCallbackExplainsProactiveSpeechWithoutChangingOrdinaryCalls(t *testing.T) {
	h, c, e, r := callbackReady(t)
	claimID := "callback_claim_for_proactive_speech"
	got := callbackAction(t, h, c, callbackInput{Action: "claim", Operation: claimID, Session: r.Session, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID})
	if got["accepted"] != true {
		t.Fatal(got)
	}
	h.handleVoiceStart(c, clientproto.Command{SessionID: r.Session, ThreadID: r.Thread, VoiceID: "voice-callback-proactive", RequestID: "request-callback-proactive", SDP: "v=0\r\n", CallbackID: r.ID, CallbackInvitation: r.Invite.ID, CallbackClaim: claimID})
	nextVoiceState(t, c, "answer")
	if !strings.Contains(e.startInstructions, callbacks.VoiceInstructions) || e.appends.Load() != 0 || e.cancels.Load() != 0 {
		t.Fatal("callback bootstrap must prepare only the original voice speaker")
	}
}

func callbackReady(t *testing.T) (*Hub, *Client, *callbackExec, callbacks.Record) {
	t.Helper()
	h, c, s, e := callbackFixture(t)
	taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Completed)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: "r_callbacktask", ThreadID: s.ResumeID(), TurnID: "callback-turn"})
	if err := h.armAdmittedCallback(context.Background(), c, s, "r_callbacktask"); err != nil {
		t.Fatal(err)
	}
	h.reconcileTaskCallbacks(context.Background(), s.ID, "r_callbacktask")
	rows, _ := h.callbacks.List(context.Background())
	return h, c, e, rows[0]
}
func TestCallbackRevokedOfferAcquiresNoMediaAndCancelsNoTask(t *testing.T) {
	h, c, e, r := callbackReady(t)
	claim := callbackAction(t, h, c, callbackInput{Action: "claim", Operation: "callback_claim_before_cancel", Session: r.Session, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID})
	if claim["accepted"] != true {
		t.Fatal(claim)
	}
	r, _, _ = h.callbacks.Get(context.Background(), r.ID)
	cancel := callbackAction(t, h, c, callbackInput{Action: "cancel", Operation: "callback_cancel_before_offer", Session: r.Session, Request: r.Request, Expected: r.Revision})
	if cancel["accepted"] != true {
		t.Fatal(cancel)
	}
	h.handleVoiceStart(c, clientproto.Command{SessionID: r.Session, ThreadID: r.Thread, VoiceID: "voice-revoked-callback", RequestID: "request-revoked-offer", SDP: "v=0\r\n", CallbackID: r.ID, CallbackInvitation: r.Invite.ID, CallbackClaim: "callback_claim_before_cancel"})
	nextVoiceState(t, c, "error")
	if e.starts.Load() != 0 || e.appends.Load() != 0 || e.cancels.Load() != 0 {
		t.Fatal("revoked report acquired media or affected work")
	}
}
func TestCallbackWireSourceIsCanonicalAndAudienceIsPairGenerationBound(t *testing.T) {
	h, c, _, r := callbackReady(t)
	view := h.callbackView(c, &r)
	if !view.RecipientEligible {
		t.Fatal("original recipient ineligible")
	}
	wire, e := marshalEvent(callbackEvent{Type: "task_callback", Authority: "i1", Record: view}, "i1")
	if e != nil {
		t.Fatal(e)
	}
	var decoded struct {
		Record struct {
			Session  string `json:"session_id"`
			Eligible bool   `json:"recipient_eligible"`
		}
	}
	if json.Unmarshal(wire, &decoded) != nil || decoded.Record.Session != "sk1:i1:s_callback" || !decoded.Record.Eligible {
		t.Fatal(string(wire))
	}
	if e = h.pairing.Unclaim("qa-read-phone-a"); e != nil {
		t.Fatal(e)
	}
	h.pairing.OpenEnrollment(time.Minute)
	if e = h.pairing.Claim("second-pair-generation", "phone-a"); e != nil {
		t.Fatal(e)
	}
	c.readIdentity.Store(&pairedReadIdentity{token: "second-pair-generation", deviceID: "phone-a"})
	if h.pairedTaskDevice(c) != "phone-a" || h.callbackView(c, &r).RecipientEligible {
		t.Fatal("same device string acquired old invitation")
	}
	for len(c.send) > 0 {
		<-c.send
	}
	h.publishCallbackRecord(r)
	assertNoTypeWithin(t, c, "task_callback", 30*time.Millisecond)
	response := callbackAction(t, h, c, callbackInput{Action: "sync", Operation: "callback_sync_repaired_device", Native: true})
	if rows, ok := response["records"].([]any); ok && len(rows) > 0 {
		t.Fatal("new pairing inherited old callback state", response)
	}
}
