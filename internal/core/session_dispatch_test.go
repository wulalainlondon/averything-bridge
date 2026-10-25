package core

import (
	"bytes"
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/protocol"
	"everything-go/internal/relay"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func controllerFixture(t *testing.T) (*Hub, *fakeExec, backend.SessionControlCaller, *session.Session) {
	h, fe := newTestHub(t)
	store, e := sessiondispatch.Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	h.dispatches = store
	t.Cleanup(func() { store.Close() })
	parent := h.registry.Create("controller", "Controller", t.TempDir(), backend.Codex, "", "danger-full-access", "")
	parent.SetResumeID("parent-thread")
	target := h.registry.Create("target", "Target", t.TempDir(), backend.Codex, "", "read-only", "")
	target.SetResumeID("target-thread")
	c := newTestClient(h)
	h.voiceClients = map[*Client]*clientVoiceCall{c: {session: parent, voiceID: "active-voice"}}
	caller := backend.SessionControlCaller{Parent: parent, RequestID: "observed-request", TurnID: "native-turn", ToolCallID: "tool-call", VoiceID: "active-voice"}
	return h, fe, caller, target
}
func TestControllerVoiceDispatchUsesTargetPermissionsAndDeduplicates(t *testing.T) {
	h, fe, caller, target := controllerFixture(t)
	ctx := context.Background()
	if _, e := h.ControlSession(ctx, caller, backend.SessionControlRequest{Action: "list_sessions"}); e == nil {
		t.Fatal("default grant open")
	}
	h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	var count atomic.Int32
	fe.onSend = func(s *session.Session, id, text string) {
		count.Add(1)
		if s.Snapshot().Sandbox != "read-only" {
			t.Error("parent sandbox leaked")
		}
		fe.sink.Emit(protocol.NewDone(s.ID, id))
	}
	req := backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), ExpectedConfigRevision: target.SettingsSnapshot().ConfigRevision, Content: "bounded user task"}
	value, e := h.ControlSession(ctx, caller, req)
	if e != nil {
		t.Fatal(e)
	}
	receipt := value.(sessiondispatch.Record)
	deadline := time.Now().Add(2 * time.Second)
	for count.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if count.Load() != 1 {
		t.Fatal(count.Load())
	}
	again, e := h.ControlSession(ctx, caller, req)
	if e != nil || again.(sessiondispatch.Record).ID != receipt.ID {
		t.Fatal(again, e)
	}
	req.Content = "different instruction"
	if _, e = h.ControlSession(ctx, caller, req); e == nil {
		t.Fatal("intent replacement accepted")
	}
	if count.Load() != 1 {
		t.Fatal("duplicated execution")
	}
}
func TestControllerRejectsStaleThreadAndRevokedVoice(t *testing.T) {
	h, _, caller, target := controllerFixture(t)
	ctx := context.Background()
	h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	req := backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: target.ID, ExpectedThreadID: "wrong-thread", Content: "must not execute"}
	value, e := h.ControlSession(ctx, caller, req)
	if e != nil {
		t.Fatal(e)
	}
	if value.(sessiondispatch.Record).State != "rejected" {
		t.Fatal(value)
	}
	h.voiceClients = map[*Client]*clientVoiceCall{}
	if _, e = h.ControlSession(ctx, caller, backend.SessionControlRequest{Action: "list_sessions"}); e == nil {
		t.Fatal("expired voice became owned caller")
	}
}
func TestControllerRemoteAPIRequiresExplicitPeerGrantAndRejectsReplay(t *testing.T) {
	h, _, _, _ := controllerFixture(t)
	t.Setenv("CTRL_TEST_SECRET", "secret")
	t.Setenv("BRIDGE_RELAY_ALLOW_LOOPBACK", "1")
	h.relayPeers = relay.Peers{"origin": {InstanceID: "origin", BaseURL: "http://100.64.0.1", SecretRef: "env:CTRL_TEST_SECRET"}}
	payload, _ := json.Marshal(controllerEnvelope{ParentID: "parent", Input: backend.SessionControlRequest{Action: "list_sessions"}})
	headers := relay.Sign("secret", "origin", http.MethodPost, "/api/session-control/v1/read", payload, time.Now())
	send := func(hdr map[string]string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/api/session-control/v1/read", bytes.NewReader(payload))
		r.RemoteAddr = "127.0.0.1:1"
		for k, v := range hdr {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		h.ServeSessionControlAPI(w, r)
		return w
	}
	if w := send(headers); w.Code != 403 {
		t.Fatal(w.Code, w.Body.String())
	}
	h.dispatches.SetGrant(context.Background(), "peer:origin", sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	headers = relay.Sign("secret", "origin", http.MethodPost, "/api/session-control/v1/read", payload, time.Now())
	if w := send(headers); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := send(headers); w.Code != 401 {
		t.Fatal("nonce replay", w.Code)
	}
}
func TestControllerRemoteDispatchAndReplyAreIdempotentAcrossTwoBridges(t *testing.T) {
	receiver, fe, _, target := controllerFixture(t)
	receiver.cfg.InstanceID = "remote"
	t.Setenv("CTRL_REMOTE_SECRET", "shared")
	t.Setenv("BRIDGE_RELAY_ALLOW_LOOPBACK", "1")
	receiver.relayPeers = relay.Peers{"i1": {InstanceID: "i1", BaseURL: "http://100.64.0.1", SecretRef: "env:CTRL_REMOTE_SECRET"}}
	receiver.dispatches.SetGrant(context.Background(), "peer:i1", sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	var count atomic.Int32
	fe.onSend = func(s *session.Session, id, text string) {
		count.Add(1)
		fe.sink.Emit(backend.CompletedAnswer{SessionID: s.ID, RequestID: id, Text: "exact remote final"})
		fe.sink.Emit(protocol.NewDone(s.ID, id))
	}
	server := httptest.NewServer(http.HandlerFunc(receiver.ServeSessionControlAPI))
	defer server.Close()
	sender, _, caller, _ := controllerFixture(t)
	sender.relayPeers = relay.Peers{"remote": {InstanceID: "remote", BaseURL: server.URL, SecretRef: "env:CTRL_REMOTE_SECRET"}}
	sender.dispatches.SetGrant(context.Background(), caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Instances: []string{"remote"}}, 0)
	req := backend.SessionControlRequest{Action: "dispatch_to_session", InstanceID: "remote", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), ExpectedConfigRevision: target.SettingsSnapshot().ConfigRevision, Content: "remote bounded user task"}
	value, e := sender.ControlSession(context.Background(), caller, req)
	if e != nil {
		t.Fatal(e)
	}
	receipt := value.(sessiondispatch.Record)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		receipt = sender.refreshSessionDispatch(context.Background(), receipt)
		if receipt.State == "completed" && receipt.Result != "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if count.Load() != 1 || receipt.State != "completed" || receipt.Result != "exact remote final" {
		t.Fatal(count.Load(), receipt)
	}
	again, e := sender.ControlSession(context.Background(), caller, req)
	if e != nil || again.(sessiondispatch.Record).ID != receipt.ID || count.Load() != 1 {
		t.Fatal(again, e, count.Load())
	}
}

func TestControllerQueuedTargetBindingIsRecheckedBeforeExecution(t *testing.T) {
	for _, change := range []string{"thread", "configuration"} {
		t.Run(change, func(t *testing.T) {
			h, fe, caller, target := controllerFixture(t)
			ctx := context.Background()
			h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
			started, release := make(chan struct{}), make(chan struct{})
			target.SubmitNamed("hold", func() { close(started); <-release; target.EndTurn() })
			<-started
			var sends atomic.Int32
			fe.onSend = func(s *session.Session, id, text string) { sends.Add(1); fe.sink.Emit(protocol.NewDone(s.ID, id)) }
			req := backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), ExpectedConfigRevision: target.SettingsSnapshot().ConfigRevision, Content: "must stay bound"}
			value, e := h.ControlSession(ctx, caller, req)
			if e != nil {
				t.Fatal(e)
			}
			receipt := value.(sessiondispatch.Record)
			if change == "thread" {
				target.SetResumeID("different-thread")
			} else {
				config := session.ConfigurationFrom(target.SettingsSnapshot())
				config.Model = "different-model"
				if _, e := target.SetFutureConfiguration(config, nil); e != nil {
					t.Fatal(e)
				}
			}
			close(release)
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				receipt = h.refreshSessionDispatch(ctx, receipt)
				if receipt.State == "failed" {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if receipt.State != "failed" || sends.Load() != 0 {
				t.Fatal(receipt, sends.Load())
			}
		})
	}
}

func TestControllerCancelOnlyOwnWaitingInstructionAndNoSelfDispatch(t *testing.T) {
	h, _, caller, target := controllerFixture(t)
	ctx := context.Background()
	h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	started, release := make(chan struct{}), make(chan struct{})
	target.SubmitNamed("hold", func() { close(started); <-release; target.EndTurn() })
	<-started
	defer close(release)
	req := backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), ExpectedConfigRevision: target.SettingsSnapshot().ConfigRevision, Content: "waiting task"}
	value, e := h.ControlSession(ctx, caller, req)
	if e != nil {
		t.Fatal(e)
	}
	receipt := value.(sessiondispatch.Record)
	value, e = h.ControlSession(ctx, caller, backend.SessionControlRequest{Action: "cancel_waiting_dispatch", DispatchID: receipt.ID})
	if e != nil || value.(sessiondispatch.Record).State != "cancelled" {
		t.Fatal(value, e)
	}
	if _, e = h.ControlSession(ctx, caller, backend.SessionControlRequest{Action: "cancel_waiting_dispatch", DispatchID: receipt.ID}); e == nil {
		t.Fatal("cancelled task accepted again")
	}
	req.SessionID = caller.Parent.ID
	req.ExpectedThreadID = caller.Parent.ResumeID()
	caller.ToolCallID = "other-call"
	if _, e = h.ControlSession(ctx, caller, req); e == nil {
		t.Fatal("self dispatch allowed")
	}
}

func TestControllerLostRemoteAcknowledgementDoesNotRepeatTask(t *testing.T) {
	receiver, fe, _, target := controllerFixture(t)
	receiver.cfg.InstanceID = "remote"
	t.Setenv("CTRL_LOST_SECRET", "shared")
	t.Setenv("BRIDGE_RELAY_ALLOW_LOOPBACK", "1")
	receiver.relayPeers = relay.Peers{"i1": {InstanceID: "i1", BaseURL: "http://100.64.0.1", SecretRef: "env:CTRL_LOST_SECRET"}}
	receiver.dispatches.SetGrant(context.Background(), "peer:i1", sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	var count atomic.Int32
	fe.onSend = func(s *session.Session, id, text string) {
		count.Add(1)
		fe.sink.Emit(backend.CompletedAnswer{SessionID: s.ID, RequestID: id, Text: "one execution"})
		fe.sink.Emit(protocol.NewDone(s.ID, id))
	}
	var lose atomic.Bool
	lose.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/dispatch") && lose.Swap(false) {
			rec := httptest.NewRecorder()
			receiver.ServeSessionControlAPI(rec, r)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		receiver.ServeSessionControlAPI(w, r)
	}))
	defer server.Close()
	sender, _, caller, _ := controllerFixture(t)
	sender.relayPeers = relay.Peers{"remote": {InstanceID: "remote", BaseURL: server.URL, SecretRef: "env:CTRL_LOST_SECRET"}}
	sender.dispatches.SetGrant(context.Background(), caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Instances: []string{"remote"}}, 0)
	value, e := sender.ControlSession(context.Background(), caller, backend.SessionControlRequest{Action: "dispatch_to_session", InstanceID: "remote", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), Content: "one task"})
	if e != nil {
		t.Fatal(e)
	}
	receipt := value.(sessiondispatch.Record)
	if receipt.State != "uncertain" {
		t.Fatal("lost ACK must not claim success", receipt)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		receipt = sender.refreshSessionDispatch(context.Background(), receipt)
		if receipt.State == "completed" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if count.Load() != 1 || receipt.Result != "one execution" || receipt.State != "completed" {
		t.Fatal(count.Load(), receipt)
	}
	// A configured peer cannot change content while retaining an old claimed hash.
	altered := receipt
	altered.Content = "replacement task"
	var remote sessiondispatch.Record
	if e := sender.controllerRemoteRequest(context.Background(), "remote", "dispatch", controllerEnvelope{ParentID: caller.Parent.ID, Record: altered}, &remote); e == nil {
		t.Fatal("receiver trusted caller digest")
	}
	if count.Load() != 1 {
		t.Fatal("task repeated")
	}
}

func TestControllerWaitsForObservedVoiceTurnWithoutStartingOverlappingWork(t *testing.T) {
	h, fe, caller, target := controllerFixture(t)
	ctx := context.Background()
	h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	h.Emit(backend.ObservedTurn{SessionID: target.ID, RequestID: "native-external", Phase: "running"})
	var count atomic.Int32
	fe.onSend = func(s *session.Session, id, text string) {
		count.Add(1)
		fe.sink.Emit(backend.CompletedAnswer{SessionID: s.ID, RequestID: id, Text: "safe final"})
		fe.sink.Emit(protocol.NewDone(s.ID, id))
	}
	value, e := h.ControlSession(ctx, caller, backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), Content: "wait for voice background task"})
	if e != nil {
		t.Fatal(e)
	}
	receipt := value.(sessiondispatch.Record)
	if receipt.State != "pending" || count.Load() != 0 || target.IsStreaming() {
		t.Fatal(receipt, count.Load())
	}
	h.Emit(backend.ObservedTurn{SessionID: target.ID, RequestID: "native-external", Phase: "completed"})
	h.reconcileSessionDispatches(ctx)
	deadline := time.Now().Add(time.Second)
	for count.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if count.Load() != 1 {
		t.Fatal(count.Load())
	}
}

func TestControllerCannotEscapeConfiguredBridgeRoot(t *testing.T) {
	h, _, caller, target := controllerFixture(t)
	ctx := context.Background()
	h.cfg.RootDir = caller.Parent.Cwd()
	h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	catalog := h.controllerCatalog(sessiondispatch.Grant{Enabled: true, Local: true}, "")
	for _, row := range catalog {
		if row["session_id"] == target.ID {
			t.Fatal("outside target leaked")
		}
	}
	value, e := h.ControlSession(ctx, caller, backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), Content: "outside root"})
	if e != nil {
		t.Fatal(e)
	}
	if value.(sessiondispatch.Record).State != "rejected" {
		t.Fatal(value)
	}
}

func TestControllerDiscoversOnlyGrantedInstancesWithoutSecretsOrEndpoints(t *testing.T) {
	h, _, caller, _ := controllerFixture(t)
	ctx := context.Background()
	h.relayPeers = relay.Peers{"granted": {InstanceID: "granted", InstanceName: "Morrie", BaseURL: "http://100.64.0.1", SecretRef: "env:SECRET"}, "private": {InstanceID: "private", BaseURL: "http://100.64.0.2", SecretRef: "env:PRIVATE"}}
	h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Instances: []string{"granted"}}, 0)
	value, e := h.ControlSession(ctx, caller, backend.SessionControlRequest{Action: "list_instances"})
	if e != nil {
		t.Fatal(e)
	}
	rows := value.([]map[string]any)
	if len(rows) != 1 || rows[0]["instance_id"] != "granted" || rows[0]["instance_name"] != "Morrie" {
		t.Fatal(value)
	}
	raw, _ := json.Marshal(value)
	if strings.Contains(string(raw), "100.64") || strings.Contains(string(raw), "SECRET") || strings.Contains(string(raw), "private") {
		t.Fatal("peer connection material leaked")
	}
}

func TestControllerResultDoesNotEnterAChangedParentThread(t *testing.T) {
	h, _, caller, target := controllerFixture(t)
	ctx := context.Background()
	r := sessiondispatch.Record{ID: "sealed", ParentID: caller.Parent.ID, ParentThreadID: caller.Parent.ResumeID(), InstanceID: h.cfg.InstanceID, SessionID: target.ID, ThreadID: target.ResumeID(), RequestID: "job", IntentHash: "intent", State: "completed", Result: "sealed result", DeliveryState: "pending"}
	if _, e := h.dispatches.Create(ctx, r); e != nil {
		t.Fatal(e)
	}
	caller.Parent.SetResumeID("new-parent-thread")
	h.reconcileSessionDispatches(ctx)
	fresh, _, e := h.dispatches.Get(ctx, r.ID)
	if e != nil || fresh.DeliveryState != "failed" || fresh.Result != "sealed result" || caller.Parent.QueueLen() != 0 {
		t.Fatal(fresh, e)
	}
}

func TestControllerReceiptDoesNotUploadCachedLargeResult(t *testing.T) {
	h, _, _, _ := controllerFixture(t)
	t.Setenv("CTRL_SMALL_SECRET", "shared")
	t.Setenv("BRIDGE_RELAY_ALLOW_LOOPBACK", "1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		if len(raw) > 2048 {
			t.Error("large receipt request", len(raw))
		}
		var input controllerEnvelope
		json.Unmarshal(raw, &input)
		if input.Record.ID != "receipt" || input.Record.Result != "" || input.Record.Content != "" {
			t.Error("receipt uploaded cached content")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer server.Close()
	h.relayPeers = relay.Peers{"remote": {InstanceID: "remote", BaseURL: server.URL, SecretRef: "env:CTRL_SMALL_SECRET"}}
	var result sessiondispatch.Record
	e := h.controllerRemoteRequest(context.Background(), "remote", "receipt", controllerEnvelope{ParentID: "parent", Record: sessiondispatch.Record{ID: "receipt", Content: "cached instruction", Result: strings.Repeat("large result", 20000)}}, &result)
	if e != nil {
		t.Fatal(e)
	}
}

func TestControllerActualTailscaleTransportIntegration(t *testing.T) {
	url := os.Getenv("BRIDGE_CONTROLLER_TEST_PEER_URL")
	if url == "" {
		t.Skip("explicit isolated peer required")
	}
	h, _, caller, _ := controllerFixture(t)
	h.cfg.InstanceID = "lab-source"
	h.relayPeers = relay.Peers{"lab-receiver": {InstanceID: "lab-receiver", BaseURL: url, SecretRef: "env:BRIDGE_CONTROLLER_TRANSPORT_TEST_SECRET"}}
	h.dispatches.SetGrant(context.Background(), caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Instances: []string{"lab-receiver"}}, 0)
	req := backend.SessionControlRequest{Action: "dispatch_to_session", InstanceID: "lab-receiver", SessionID: "lab-target", ExpectedThreadID: "lab-thread", Content: "isolated transport fixture"}
	value, e := h.ControlSession(context.Background(), caller, req)
	if e != nil {
		t.Fatal(e)
	}
	receipt := value.(sessiondispatch.Record)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		receipt = h.refreshSessionDispatch(context.Background(), receipt)
		if receipt.State == "completed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if receipt.State != "completed" || receipt.Result != `{"executions":1,"fixture":"transport_only_no_model"}` {
		t.Fatal(receipt.State, receipt.Error)
	}
	again, e := h.ControlSession(context.Background(), caller, req)
	if e != nil || again.(sessiondispatch.Record).ID != receipt.ID || again.(sessiondispatch.Record).Result != receipt.Result {
		t.Fatal("duplicate transport operation")
	}
	t.Log("actual Tailscale HMAC dispatch and sealed result passed; executor is a fixture, not Codex voice")
}

func TestControllerMissingThreadTerminalDrainsOriginalQueueAndPreservesReceipt(t *testing.T) {
	h, fe, caller, target := controllerFixture(t)
	defer target.CloseAndWait()
	ctx := context.Background()
	if _, err := h.dispatches.SetGrant(ctx, caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0); err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int32
	fe.onSend = func(s *session.Session, id, _ string) {
		sends.Add(1)
		h.Emit(backend.NewError(s.ID, id, backend.ErrProcessDied, "failed to start codex thread: no rollout found for thread id target-thread"))
	}
	input := backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: target.ID,
		ExpectedThreadID: target.ResumeID(), ExpectedConfigRevision: target.SettingsSnapshot().ConfigRevision, Content: "original instruction"}
	value, err := h.ControlSession(ctx, caller, input)
	if err != nil {
		t.Fatal(err)
	}
	original := value.(sessiondispatch.Record)
	deadline := time.Now().Add(2 * time.Second)
	for {
		receipt := h.refreshSessionDispatch(ctx, original)
		if receipt.State == "failed" && !target.IsStreaming() {
			if receipt.RequestID != original.RequestID || receipt.ID != original.ID ||
				!strings.Contains(receipt.Error, "no rollout found") || target.QueueLen() != 0 {
				t.Fatal("original failed receipt or queue incorrect", receipt)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("request did not terminate", receipt)
		}
		time.Sleep(time.Millisecond)
	}
	again, err := h.ControlSession(ctx, caller, input)
	if err != nil {
		t.Fatal(err)
	}
	receipt := again.(sessiondispatch.Record)
	if receipt.ID != original.ID || receipt.RequestID != original.RequestID || receipt.State != "failed" ||
		sends.Load() != 1 || target.ResumeID() != "target-thread" {
		t.Fatal("failed request replayed or original identity changed", receipt, sends.Load())
	}
}
