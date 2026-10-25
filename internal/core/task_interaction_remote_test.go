package core

import (
	"bytes"
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/relay"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	ti "everything-go/internal/taskinteraction"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInteractionCrossInstanceAnswerUnknownReceiptAndNoReplay(t *testing.T) {
	ctx := context.Background()
	t.Setenv("BRIDGE_RELAY_ALLOW_LOOPBACK", "1")
	t.Setenv("INTERACTION_TEST_SECRET", "synthetic-test-secret")
	childHub, childExec, _, child := controllerFixture(t)
	childHub.cfg.InstanceID = "remote"
	parentHub, parentExec, caller, _ := controllerFixture(t)
	for _, h := range []*Hub{childHub, parentHub} {
		s, e := ti.Open(t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		h.interactions = s
		t.Cleanup(func() { s.Close() })
	}
	defer child.CloseAndWait()
	defer caller.Parent.CloseAndWait()
	childExec.onSend = func(*session.Session, string, string) {}
	parentExec.onSend = func(*session.Session, string, string) {}
	var drop atomic.Bool
	var answerCalls atomic.Int32
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/session-control/v1/interaction" && drop.CompareAndSwap(true, false) {
			answerCalls.Add(1)
			capture := httptest.NewRecorder()
			childHub.ServeSessionControlAPI(capture, r)
			if capture.Code != 200 {
				t.Errorf("receiver rejected accepted answer: %d %s", capture.Code, capture.Body.String())
			}
			conn, _, e := w.(http.Hijacker).Hijack()
			if e == nil {
				conn.Close()
			}
			return
		}
		childHub.ServeSessionControlAPI(w, r)
	}))
	defer receiver.Close()
	source := httptest.NewServer(http.HandlerFunc(parentHub.ServeSessionControlAPI))
	defer source.Close()
	childHub.relayPeers = relay.Peers{"i1": {InstanceID: "i1", BaseURL: source.URL, SecretRef: "env:INTERACTION_TEST_SECRET"}}
	parentHub.relayPeers = relay.Peers{"remote": {InstanceID: "remote", BaseURL: receiver.URL, SecretRef: "env:INTERACTION_TEST_SECRET"}}
	_, e := childHub.dispatches.SetGrant(ctx, "peer:i1", sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	if e != nil {
		t.Fatal(e)
	}
	_, e = parentHub.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Instances: []string{"remote"}}, 0)
	if e != nil {
		t.Fatal(e)
	}
	// No reverse general controller grant exists on parentHub.
	value, e := parentHub.ControlSession(ctx, caller, backend.SessionControlRequest{Action: "dispatch_to_session", InstanceID: "remote", SessionID: child.ID, ExpectedThreadID: child.ResumeID(), Content: "original cross-instance task"})
	if e != nil {
		t.Fatal(e)
	}
	outbound := value.(sessiondispatch.Record)
	request := "scjob_" + remoteControllerID("i1", outbound.ID)
	waitActor(t, child, request)
	q := resultRecord(t, interactionCall(t, childHub, child, request, "ask", map[string]any{"action": "ask_parent", "question": strings.Repeat("問", 3000), "kind": "task_clarification", "blocking": false}))
	childHub.reconcileInteractions(ctx)
	imported, e := parentHub.interactions.Get(ctx, q.ID)
	if e != nil {
		t.Fatal("question not relayed", e)
	}
	if imported.Binding.Authority != "remote" || imported.AckToken != "" {
		t.Fatal(imported)
	}
	parentHub.reconcileInteractions(ctx)
	wake := "tiwake_" + q.ID
	waitActor(t, caller.Parent, wake)
	drop.Store(true)
	input := map[string]any{"action": "answer", "interaction_id": q.ID, "expected_revision": 1, "answer": "cross-fixture"}
	res := interactionCall(t, parentHub, caller.Parent, wake, "answer-once", input)
	if !res.OK || res.Result.(map[string]any)["answer_submission"] != "unknown" {
		t.Fatal(res)
	}
	actual, e := childHub.interactions.Get(ctx, q.ID)
	if e != nil || actual.Answer != "cross-fixture" {
		t.Fatal("remote effect missing", actual, e)
	}
	// The same original key only performs a receipt query after uncertainty.
	res = interactionCall(t, parentHub, caller.Parent, wake, "answer-once", input)
	if !res.OK || res.Result.(map[string]any)["answer_submission"] != "accepted" || answerCalls.Load() != 1 {
		t.Fatal(res, answerCalls.Load())
	}
	childResponse := interactionCall(t, childHub, child, request, "child-read", map[string]any{"action": "get", "interaction_id": q.ID})
	if !childResponse.OK {
		t.Fatal(childResponse)
	}
	token := childResponse.Result.(map[string]any)["ack_token"]
	ack := interactionCall(t, childHub, child, request, "ack", map[string]any{"action": "ack", "interaction_id": q.ID, "ack_token": token})
	if !ack.OK {
		t.Fatal(ack)
	}
	childHub.reconcileInteractions(ctx)
	final, e := parentHub.interactions.Get(ctx, q.ID)
	if e != nil || final.State != "resolved" || final.AckToken != "" {
		t.Fatal("terminal question not synchronized", final, e)
	}
	// A changed or foreign original target cannot introduce a question.
	forged := q
	forged.ID = ti.ID("ti_")
	forged.Binding.ChildThread = "foreign"
	payload, _ := json.Marshal(controllerEnvelope{ParentID: q.Binding.Parent, Interaction: &interactionPeerPayload{Action: "sync", Record: &forged}})
	headers := relay.Sign("synthetic-test-secret", "remote", http.MethodPost, "/api/session-control/v1/interaction", payload, time.Now())
	req := httptest.NewRequest(http.MethodPost, "/api/session-control/v1/interaction", bytes.NewReader(payload))
	req.RemoteAddr = "127.0.0.1:1"
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rw := httptest.NewRecorder()
	parentHub.ServeSessionControlAPI(rw, req)
	if rw.Code != 403 {
		t.Fatal("forged lineage accepted", rw.Code)
	}
}
