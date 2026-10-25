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
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Two isolated test processes on different hosts. Native execution is a fixture;
// this test qualifies the signed relay, persistence and unknown-receipt path only.
func TestInteractionTwoHostTransport(t *testing.T) {
	path := os.Getenv("BRIDGE_INTERACTION_TRANSPORT_INPUT")
	if path == "" {
		t.Skip("explicit isolated two-host qualification required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var in struct{ Role, Listen, Peer, Secret, Report, Host string }
	if json.Unmarshal(raw, &in) != nil || (in.Role != "parent" && in.Role != "child") || in.Report == "" || in.Secret == "" {
		t.Fatal("invalid persisted probe input")
	}
	marker, err := os.OpenFile(in.Report+".admitted", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal("probe already admitted; inspect original evidence", err)
	}
	marker.Close()
	t.Setenv("BRIDGE_RELAY_ALLOW_LOOPBACK", "1")
	t.Setenv("INTERACTION_TRANSPORT_FIXTURE_SECRET", in.Secret)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	h, fe, caller, target := controllerFixture(t)
	defer target.CloseAndWait()
	defer caller.Parent.CloseAndWait()
	h.interactions, err = ti.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer h.interactions.Close()
	mine, peer := "transport-parent", "transport-child"
	if in.Role == "child" {
		mine, peer = peer, mine
	}
	h.cfg.InstanceID = mine
	h.relayPeers = relay.Peers{peer: {InstanceID: peer, BaseURL: in.Peer, SecretRef: "env:INTERACTION_TRANSPORT_FIXTURE_SECRET"}}
	fe.onSend = func(*session.Session, string, string) {}
	if in.Role == "child" {
		_, err = h.dispatches.SetGrant(ctx, "peer:"+peer, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	} else {
		_, err = h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Instances: []string{peer}}, 0)
	}
	if err != nil {
		t.Fatal(err)
	}
	var answerCalls atomic.Int32
	listener, err := net.Listen("tcp", in.Listen)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if in.Role == "child" && strings.HasSuffix(r.URL.Path, "/interaction") {
			body, e := io.ReadAll(io.LimitReader(r.Body, 2*1024*1024))
			if e != nil {
				t.Error(e)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			var envelope controllerEnvelope
			if json.Unmarshal(body, &envelope) == nil && envelope.Interaction != nil && envelope.Interaction.Action == "answer" {
				if answerCalls.Add(1) == 1 {
					// Commit the authentic original request and deliberately lose its response.
					capture := &transportResponseCapture{header: make(http.Header)}
					h.ServeSessionControlAPI(capture, r)
					if capture.status != http.StatusOK {
						t.Errorf("answer rejected %d", capture.status)
					}
					conn, _, e := w.(http.Hijacker).Hijack()
					if e == nil {
						conn.Close()
					}
					return
				}
			}
		}
		h.ServeSessionControlAPI(w, r)
	})}
	go server.Serve(listener)
	defer server.Close()
	if err = os.WriteFile(in.Report+".ready", []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	report := map[string]any{"host": in.Host, "role": in.Role, "native_executor": "fixture", "transport": "authenticated relay over original SSH loopback tunnel", "production_state_modified": false}
	finish := func(q ti.Record) {
		report["interaction_id"] = q.ID
		report["state"] = q.State
		report["actor"] = q.Actor
		report["answer_calls"] = answerCalls.Load()
		report["passed"] = true
		data, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(in.Report, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if in.Role == "parent" {
		request := backend.SessionControlRequest{Action: "dispatch_to_session", InstanceID: peer, SessionID: "target", ExpectedThreadID: "target-thread", Content: "two-host synthetic question fixture"}
		value, err := h.ControlSession(ctx, caller, request)
		if err != nil {
			t.Fatal(err)
		}
		record := value.(sessiondispatch.Record)
		if record.State == "rejected" {
			t.Fatal(record.Error)
		}
		answered, receiptSeen := false, false
		for ctx.Err() == nil {
			h.reconcileInteractions(ctx)
			rows, err := h.interactions.List(ctx, caller.Parent.ID, 100)
			if err != nil {
				t.Fatal(err)
			}
			for _, q := range rows {
				wake := "tiwake_" + q.ID
				if !answered {
					waitActor(t, caller.Parent, wake)
					res := interactionCall(t, h, caller.Parent, wake, "two-host-answer-once", map[string]any{"action": "answer", "interaction_id": q.ID, "expected_revision": 1, "answer": "transport-alpha"})
					if !res.OK || res.Result.(map[string]any)["answer_submission"] != "unknown" {
						t.Fatal("lost response not unknown", res)
					}
					answered = true
				}
				if !receiptSeen {
					res := interactionCall(t, h, caller.Parent, wake, "two-host-answer-once", map[string]any{"action": "answer", "interaction_id": q.ID, "expected_revision": 1, "answer": "transport-alpha"})
					if res.OK && res.Result.(map[string]any)["answer_submission"] == "accepted" {
						receiptSeen = true
					}
				}
				if q.State == "resolved" && receiptSeen {
					report["original_receipt_recovered"] = true
					finish(q)
					return
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	} else {
		var q ti.Record
		for ctx.Err() == nil {
			if q.ID == "" && strings.HasPrefix(target.ActiveQueuedID(), "scjob_") {
				q = resultRecord(t, interactionCall(t, h, target, target.ActiveQueuedID(), "two-host-ask", map[string]any{"action": "ask_parent", "question": "Which transport fixture?", "kind": "task_clarification", "blocking": false}))
			}
			h.reconcileInteractions(ctx)
			if q.ID != "" {
				current, err := h.interactions.Get(ctx, q.ID)
				if err != nil {
					t.Fatal(err)
				}
				if current.Answer != "" {
					resultRecord(t, interactionCall(t, h, target, q.ProducerRequest, "two-host-ack", map[string]any{"action": "ack", "interaction_id": q.ID, "ack_token": current.AckToken}))
					h.reconcileInteractions(ctx)
					final, err := h.interactions.Get(ctx, q.ID)
					if err != nil || final.State != "resolved" {
						t.Fatal(err)
					}
					if answerCalls.Load() != 1 {
						t.Fatal("answer replayed", answerCalls.Load())
					}
					finish(final)
					time.Sleep(2 * time.Second) // Keep exact receipt readable by the other process.
					return
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	t.Fatal("two-host qualification timed out; original report/input retained")
}

type transportResponseCapture struct {
	header http.Header
	status int
}

func (w *transportResponseCapture) Header() http.Header    { return w.header }
func (w *transportResponseCapture) WriteHeader(status int) { w.status = status }
func (w *transportResponseCapture) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return len(b), nil
}
