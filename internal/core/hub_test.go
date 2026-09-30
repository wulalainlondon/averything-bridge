package core

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/executor"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

// fakeExec is a scriptable Executor for hub behavior tests. Send delegates to an
// injectable onSend so a test decides what a "turn" emits (a full turn with
// done, or a long-running turn that emits nothing until stopped).
type fakeExec struct {
	sink          executor.Sink
	onSend        func(s *session.Session, reqID, content string)
	onSendContext func(ctx context.Context, s *session.Session, reqID, content string)
	onSteer       func(s *session.Session, reqID, content string) (backend.SteerResult, error)
}

func (f *fakeExec) Send(ctx context.Context, s *session.Session, reqID, content string, _ []backend.ImageAttachment, _ []backend.FileAttachment) error {
	if f.onSendContext != nil {
		f.onSendContext(ctx, s, reqID, content)
	}
	if f.onSend != nil {
		f.onSend(s, reqID, content)
	}
	return nil
}

func (f *fakeExec) Stop(_ context.Context, s *session.Session) error {
	f.sink.Emit(protocol.NewStopped(s.ID, ""))
	return nil
}

func (f *fakeExec) Clear(_ context.Context, s *session.Session) error {
	f.sink.Emit(protocol.NewSessionWarning(s.ID, "Session history cleared."))
	return nil
}

func (f *fakeExec) Close(_ context.Context, s *session.Session) error { return nil }

func (f *fakeExec) Steer(_ context.Context, s *session.Session, reqID, content string, _ []backend.ImageAttachment, _ []backend.FileAttachment) (backend.SteerResult, error) {
	if f.onSteer == nil {
		return backend.SteerResult{}, backend.ErrUnsupportedSteer
	}
	return f.onSteer(s, reqID, content)
}

func newTestHub(t *testing.T) (*Hub, *fakeExec) {
	t.Helper()
	reg := session.NewRegistry()
	pairing := governance.NewPairing(filepath.Join(t.TempDir(), "pairing.json"))
	h := NewHub(reg, Config{InstanceID: "i1", InstanceName: "test"}, pairing, 0)
	t.Cleanup(func() { h.messageQueue.Close() })
	fe := &fakeExec{sink: h}
	h.SetExecutor(fe)
	return h, fe
}

type diagnosticExec struct{ *fakeExec }

func (d *diagnosticExec) RuntimeDiagnostics() map[string]any {
	return map[string]any{"codex": map[string]any{
		"status": "restart_required", "managed_version": "0.153.0", "running_version": "0.149.0",
	}}
}

type rejectingConfigExec struct{ *fakeExec }

func (r *rejectingConfigExec) UpdateSessionSettings(context.Context, *session.Session) error {
	return errors.New("runtime refused settings")
}

func TestStatusResultIncludesBackendRuntimeDiagnostics(t *testing.T) {
	h, fe := newTestHub(t)
	h.SetExecutor(&diagnosticExec{fakeExec: fe})
	status := h.statusResult("s1")
	runtimes, ok := status.Status["backend_runtimes"].(map[string]any)
	if !ok {
		t.Fatalf("backend runtime diagnostics missing: %+v", status.Status)
	}
	codex, ok := runtimes["codex"].(map[string]any)
	if !ok || codex["status"] != "restart_required" || codex["managed_version"] != "0.153.0" {
		t.Fatalf("unexpected Codex diagnostics: %+v", runtimes)
	}
}

// newTestClient registers a client whose send channel we drain directly. The
// buffer is large enough that enqueue never overflows (so the nil conn is never
// touched).
func newTestClient(h *Hub) *Client {
	c := &Client{hub: h, send: make(chan []byte, 1024), quit: make(chan struct{}), clientID: "test-client"}
	h.addClient(c)
	return c
}

func route(h *Hub, c *Client, frame string) {
	in, err := protocol.ParseInbound([]byte(frame))
	if err != nil {
		panic(err)
	}
	h.route(context.Background(), c, h.client.ParseCommand(in))
}

// waitForType drains the client's send channel until it sees an event of the
// given type, returning it. Fails the test on timeout.
func waitForType(t *testing.T, c *Client, typ string) map[string]any {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case data := <-c.send:
			var m map[string]any
			if err := json.Unmarshal(data, &m); err != nil {
				t.Fatalf("bad event JSON: %v", err)
			}
			if m["type"] == typ {
				return m
			}
		case <-deadline:
			t.Fatalf("timed out waiting for event type %q", typ)
		}
	}
}

func waitState(t *testing.T, s *session.Session, want session.State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.State() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("session state = %s, want %s", s.State(), want)
}

// Regression: a background goroutine (sendHistory/sendUsage/…) that captured a
// client and enqueues after the client disconnected must never panic. The real
// 4G app triggered this: it requested history then immediately switched bridges,
// and the late sendHistory enqueue hit a closed send channel → process crash.
// The fix is to never close send and gate on quit instead.
func TestEnqueueAfterShutdownDoesNotPanic(t *testing.T) {
	h, _ := newTestHub(t)
	c := &Client{hub: h, send: make(chan []byte, 2), quit: make(chan struct{}), clientID: "bg"}

	c.shutdown()
	c.shutdown() // idempotent

	// These would panic ("send on closed channel") under the old close(send) model.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			c.enqueueEvent(protocol.NewPong())
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue after shutdown blocked")
	}
}

func TestHubResolvesLocalGeneratedMediaURL(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	c.deviceID = "phone"
	h.registerLatest(c)
	path := filepath.Join(t.TempDir(), "generated image.png")
	if err := os.WriteFile(path, []byte("png"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.Emit(protocol.Media{
		Type: "media", SessionID: "s1", RequestID: "r1",
		MediaType: "image", Path: path,
	})

	event := waitForType(t, c, "media")
	url, _ := event["url"].(string)
	if url == "" || !strings.Contains(url, "/media/") || !strings.Contains(url, "generated%20image.png") {
		t.Fatalf("local media URL was not resolved: %q", url)
	}
}

// pending_interactions_list must return a valid empty array (never null) even
// when the wired executor can't answer interactions (the hub's fakeExec).
func TestPendingInteractionsListEmptyWhenUnsupported(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	route(h, c, `{"type":"pending_interactions_list"}`)
	ev := waitForType(t, c, "pending_interactions_list")
	arr, ok := ev["interactions"].([]any)
	if !ok {
		t.Fatalf("interactions must be an array, got %T", ev["interactions"])
	}
	if len(arr) != 0 {
		t.Fatalf("interactions should be empty, got %v", arr)
	}
}

func TestSteerMessageBypassesTurnQueueAndAcknowledgesAcceptedTurn(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	sessionUnderTest := h.registry.Create("s1", "codex", t.TempDir(), backend.Codex, "", "", "")
	called := make(chan struct{}, 1)
	callCount := 0
	fe.onSteer = func(s *session.Session, reqID, content string) (backend.SteerResult, error) {
		callCount++
		if s.ID != "s1" || reqID != "r-steer" || content != "change direction" {
			t.Fatalf("unexpected steer args session=%s req=%s content=%q", s.ID, reqID, content)
		}
		called <- struct{}{}
		return backend.SteerResult{TurnID: "turn-1", RequestID: "r-active"}, nil
	}

	route(h, c, `{"type":"steer_message","session_id":"s1","request_id":"r-steer","content":"change direction"}`)
	event := waitForType(t, c, "steer_result")
	if event["status"] != "accepted" || event["turn_id"] != "turn-1" || event["active_request_id"] != "r-active" {
		t.Fatalf("unexpected steer result: %+v", event)
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("steer executor was not called")
	}
	if sessionUnderTest.QueueLen() != 0 {
		t.Fatal("steer command entered the ordinary turn queue")
	}

	// Re-sending the same command after a reconnect/ack loss must replay the
	// cached accepted result instead of injecting the user message twice.
	route(h, c, `{"type":"steer_message","session_id":"s1","request_id":"r-steer","content":"change direction"}`)
	replayed := waitForType(t, c, "steer_result")
	if replayed["status"] != "accepted" || callCount != 1 {
		t.Fatalf("duplicate steer was not deduplicated: event=%+v calls=%d", replayed, callCount)
	}
}

// The Phase 5 read commands the app polls on connect must return valid empty
// lists (arrays, never null) so the app's z.array schemas accept them, instead
// of being left unhandled.
func TestPhase5ReadStubsReturnEmptyArrays(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)

	cases := []struct{ send, want, field string }{
		{`{"type":"list_instances"}`, "instances_list", "instances"},
		{`{"type":"get_inbox"}`, "inbox_list", "items"},
		{`{"type":"feed_list_request"}`, "feed_list", "items"},
	}
	for _, tc := range cases {
		route(h, c, tc.send)
		ev := waitForType(t, c, tc.want)
		arr, ok := ev[tc.field].([]any)
		if !ok {
			t.Fatalf("%s.%s must be an array (not null), got %T: %v", tc.want, tc.field, ev[tc.field], ev[tc.field])
		}
		if len(arr) != 0 {
			t.Fatalf("%s.%s should be empty, got %v", tc.want, tc.field, arr)
		}
	}
}

func TestNewSessionBroadcastsSessionsList(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)

	route(h, c, `{"type":"new_session","session_id":"s1","name":"Fresh","backend":"claude"}`)
	waitForType(t, c, "session_created")
	ev := waitForType(t, c, "sessions_list")
	sessions, _ := ev["sessions"].([]any)
	if len(sessions) != 1 {
		t.Fatalf("new_session should broadcast one-session sessions_list, got %d", len(sessions))
	}
	ss, _ := sessions[0].(map[string]any)
	if ss["id"] != "s1" {
		t.Fatalf("sessions_list id = %v, want s1", ss["id"])
	}
}

func TestNewSessionResumeReusesCanonicalSession(t *testing.T) {
	h, _ := newTestHub(t)
	existing := h.registry.Create("s_existing", "Existing", "/work", "codex", "", "", "thread-1")
	c := newTestClient(h)

	route(h, c, `{"type":"new_session","session_id":"s_duplicate","name":"Duplicate","backend":"codex","resume_claude_id":"thread-1"}`)

	created := waitForType(t, c, "session_created")
	if created["session_id"] != existing.ID {
		t.Fatalf("session_created id = %v, want canonical %s", created["session_id"], existing.ID)
	}
	closed := waitForType(t, c, "session_closed")
	if closed["session_id"] != "s_duplicate" {
		t.Fatalf("session_closed id = %v, want optimistic duplicate", closed["session_id"])
	}
	if _, ok := h.registry.Get("s_duplicate"); ok {
		t.Fatal("duplicate resume created a second registry session")
	}
}

func TestRequestHistoryMissingSessionReturnsEmptySnapshot(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)

	route(h, c, `{"type":"request_history","session_id":"missing","mode":"snapshot"}`)
	ev := waitForType(t, c, "history_snapshot")
	if ev["session_id"] != "missing" {
		t.Fatalf("session_id = %v, want missing", ev["session_id"])
	}
	msgs, ok := ev["messages"].([]any)
	if !ok || len(msgs) != 0 {
		t.Fatalf("missing session history should be empty array, got %#v", ev["messages"])
	}
}

func TestRenameSessionBroadcastsToAllClients(t *testing.T) {
	h, _ := newTestHub(t)
	c1 := newTestClient(h)
	c2 := newTestClient(h)

	route(h, c1, `{"type":"new_session","session_id":"s1","name":"Old","backend":"claude"}`)
	waitForType(t, c1, "session_created")

	c1.deviceID = "device-a"
	route(h, c1, `{"type":"rename_session","session_id":"s1","name":"New","mutation_id":"m1","expected_revision":0}`)
	for _, c := range []*Client{c1, c2} {
		ev := waitForType(t, c, "session_renamed")
		if ev["session_id"] != "s1" || ev["name"] != "New" {
			t.Fatalf("bad rename event: %v", ev)
		}
		if ev["authority_instance_id"] != "i1" || ev["mutation_id"] != "m1" || ev["revision"] != float64(1) || ev["updated_by"] != "device-a" {
			t.Fatalf("rename authority metadata missing: %v", ev)
		}
	}
}

func TestRenameSessionRejectsStaleRevisionWithoutBroadcast(t *testing.T) {
	h, _ := newTestHub(t)
	c1 := newTestClient(h)
	c2 := newTestClient(h)
	route(h, c1, `{"type":"new_session","session_id":"s1","name":"Old","backend":"claude"}`)
	waitForType(t, c1, "session_created")
	route(h, c1, `{"type":"rename_session","session_id":"s1","name":"First","mutation_id":"m1","expected_revision":0}`)
	waitForType(t, c1, "session_renamed")
	waitForType(t, c2, "session_renamed")
	route(h, c1, `{"type":"rename_session","session_id":"s1","name":"Stale","mutation_id":"m2","expected_revision":0}`)
	rejected := waitForType(t, c1, "session_rename_rejected")
	if rejected["reason"] != "revision_conflict" || rejected["current_name"] != "First" || rejected["current_revision"] != float64(1) {
		t.Fatalf("bad stale rename rejection: %v", rejected)
	}
	select {
	case raw := <-c2.send:
		t.Fatalf("stale rename leaked broadcast: %s", raw)
	default:
	}
}

func TestSetSessionMetaBroadcastsAndUpdatesSummaries(t *testing.T) {
	h, _ := newTestHub(t)
	c1 := newTestClient(h)
	c2 := newTestClient(h)

	route(h, c1, `{"type":"new_session","session_id":"s1","name":"One","backend":"claude"}`)
	waitForType(t, c1, "session_created")

	route(h, c1, `{"type":"set_session_meta","session_id":"s1","pinned":true,"hidden":true}`)
	for _, c := range []*Client{c1, c2} {
		ev := waitForType(t, c, "session_meta_updated")
		if ev["session_id"] != "s1" || ev["pinned"] != true || ev["hidden"] != true {
			t.Fatalf("bad meta event: %v", ev)
		}
	}

	route(h, c1, `{"type":"request_sessions_list"}`)
	ev := waitForType(t, c1, "sessions_list")
	sessions, _ := ev["sessions"].([]any)
	if len(sessions) != 1 {
		t.Fatalf("sessions_list len = %d, want 1", len(sessions))
	}
	ss, _ := sessions[0].(map[string]any)
	if ss["pinned"] != true || ss["hidden"] != true {
		t.Fatalf("sessions_list should include updated meta, got %v", ss)
	}
}

func TestSwitchSessionConfigReturnsAuthoritativeSnapshotToEveryDevice(t *testing.T) {
	h, _ := newTestHub(t)
	c1 := newTestClient(h)
	c2 := newTestClient(h)
	route(h, c1, `{"type":"new_session","session_id":"s1","name":"One","backend":"claude"}`)
	waitForType(t, c1, "session_created")

	route(h, c1, `{"type":"switch_session_config","session_id":"s1","mutation_id":"cfg-1","effort":"high","sandbox":"workspace-write","service_tier":"fast","collaboration_mode":"plan"}`)
	for _, c := range []*Client{c1, c2} {
		ev := waitForType(t, c, "session_config_result")
		if ev["accepted"] != true || ev["mutation_id"] != "cfg-1" || ev["effort"] != "high" || ev["sandbox"] != "workspace-write" || ev["service_tier"] != "fast" || ev["collaboration_mode"] != "plan" {
			t.Fatalf("bad authoritative config result: %v", ev)
		}
	}

	route(h, c1, `{"type":"switch_session_config","session_id":"s1","mutation_id":"cfg-2","collaboration_mode":"default"}`)
	for _, c := range []*Client{c1, c2} {
		ev := waitForType(t, c, "session_config_result")
		if ev["accepted"] != true || ev["mutation_id"] != "cfg-2" || ev["collaboration_mode"] != "default" {
			t.Fatalf("Default mode was not broadcast authoritatively: %v", ev)
		}
	}
}

func TestSwitchSessionConfigRollsBackWhenRuntimeRejects(t *testing.T) {
	h, fe := newTestHub(t)
	h.SetExecutor(&rejectingConfigExec{fakeExec: fe})
	c := newTestClient(h)
	route(h, c, `{"type":"new_session","session_id":"s1","name":"One","backend":"codex","effort":"medium","sandbox":"danger-full-access"}`)
	waitForType(t, c, "session_created")

	route(h, c, `{"type":"switch_session_config","session_id":"s1","mutation_id":"cfg-2","effort":"","sandbox":"read-only"}`)
	ev := waitForType(t, c, "session_config_result")
	if ev["accepted"] != false || ev["mutation_id"] != "cfg-2" || !strings.HasPrefix(ev["reason"].(string), "runtime_rejected") {
		t.Fatalf("bad rejected config result: %v", ev)
	}
	snap, _ := h.registry.Get("s1")
	if got := snap.Snapshot(); got.Effort != "medium" || got.Sandbox != "danger-full-access" {
		t.Fatalf("rejected config was not rolled back: %+v", got)
	}
}

func TestProtocolV3ScopesOutboundIdentityAndAcceptsItOnInbound(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	c.protocolVersion = 3

	route(h, c, `{"type":"new_session","session_id":"same","name":"Scoped","backend":"claude"}`)
	created := waitForType(t, c, "session_created")
	key, _ := created["session_id"].(string)
	if key != "sk1:i1:same" || created["authority_instance_id"] != "i1" {
		t.Fatalf("scoped created event=%v", created)
	}

	// A client stores the compound key and sends it back unchanged. ParseInbound
	// must restore the Bridge-local id before registry routing.
	route(h, c, `{"type":"set_session_meta","session_id":"sk1:i1:same","pinned":true}`)
	updated := waitForType(t, c, "session_meta_updated")
	if updated["session_id"] != "sk1:i1:same" || updated["pinned"] != true {
		t.Fatalf("scoped round trip=%v", updated)
	}
	if _, ok := h.registry.Get("same"); !ok {
		t.Fatal("compound wire id leaked into local registry")
	}
}

func TestProtocolNegotiationScopesPerClientWithoutBreakingLegacyClients(t *testing.T) {
	h, _ := newTestHub(t)
	legacy := newTestClient(h)
	legacy.clientID = "legacy"
	legacy.protocolVersion = 1
	modern := newTestClient(h)
	modern.clientID = "modern"
	modern.protocolVersion = 3

	h.Emit(protocol.NewTextChunk("same", "r1", "hello"))
	legacyEvent := waitForType(t, legacy, "text_chunk")
	modernEvent := waitForType(t, modern, "text_chunk")
	if legacyEvent["session_id"] != "same" {
		t.Fatalf("legacy id=%v", legacyEvent["session_id"])
	}
	if modernEvent["session_id"] != "sk1:i1:same" || modernEvent["authority_instance_id"] != "i1" {
		t.Fatalf("modern event=%v", modernEvent)
	}
}

func TestSetSessionMetaIgnoresUnknownSession(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)

	route(h, c, `{"type":"set_session_meta","session_id":"missing","hidden":true}`)
	select {
	case data := <-c.send:
		t.Fatalf("unknown session meta should be silent, got %s", string(data))
	case <-time.After(80 * time.Millisecond):
	}
}

// A full turn: session_created, then the streamed events, then done — and the
// session returns to Idle (the Hub drives EndTurn off the done event).
func TestMessageStreamsAndEndsTurn(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	fe.onSend = func(s *session.Session, reqID, content string) {
		h.Emit(protocol.NewTextChunk(s.ID, reqID, "hello "+content))
		h.Emit(protocol.NewDone(s.ID, reqID))
	}

	route(h, c, `{"type":"new_session","session_id":"s1","backend":"claude"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"world"}`)

	chunk := waitForType(t, c, "text_chunk")
	if chunk["content"] != "hello world" {
		t.Fatalf("unexpected chunk: %v", chunk)
	}
	waitForType(t, c, "done")

	s, _ := h.registry.Get("s1")
	waitState(t, s, session.Idle)
	snap := s.Snapshot()
	if snap.PreviewText != "hello world" || snap.PreviewRole != "assistant" || snap.PreviewRevision != 2 || snap.PreviewUpdatedAt == 0 {
		t.Fatalf("terminal row projection was not committed after accepted request: %+v", snap)
	}
}

func TestDoneWakesExactTranscriptPath(t *testing.T) {
	h, _ := newTestHub(t)
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	fx := &forkExec{fakeExec: fakeExec{sink: h}, prov: &forkProv{path: path}}
	h.SetExecutor(fx)
	s := h.registry.Create("s1", "Work", "/work", "codex", "", "", "thread-1")
	if s.ResumeID() == "" {
		t.Fatal("test session missing resume id")
	}
	woke := make(chan string, 1)
	h.SetTranscriptChangeNotifier(func(got string) { woke <- got })

	h.Emit(protocol.NewTextChunk("s1", "r1", "finished"))
	h.Emit(protocol.NewDone("s1", "r1"))
	select {
	case got := <-woke:
		if got != path {
			t.Fatalf("notifier path=%q want=%q", got, path)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal turn did not wake transcript indexer")
	}
}

func TestMessageAckConfirmsBridgeQueueAcceptance(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	release := make(chan struct{})
	fe.onSend = func(s *session.Session, reqID, content string) {
		<-release
		h.Emit(protocol.NewDone(s.ID, reqID))
	}

	route(h, c, `{"type":"new_session","session_id":"s1","backend":"codex"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r-ack","content":"hello"}`)
	ack := waitForType(t, c, "message_ack")
	if ack["session_id"] != "s1" || ack["request_id"] != "r-ack" || ack["status"] != "queued" {
		t.Fatalf("unexpected message ACK: %+v", ack)
	}
	close(release)
	waitForType(t, c, "done")
}

// stop on a long-running turn (one that emits nothing until interrupted) must
// emit stopped and return the session to Idle.
func TestStopEndsTurn(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	fe.onSend = func(s *session.Session, reqID, content string) {
		h.Emit(protocol.NewTextChunk(s.ID, reqID, "thinking..."))
		// no done — the turn stays in flight until stop
	}

	route(h, c, `{"type":"new_session","session_id":"s1","backend":"claude"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"go"}`)
	waitForType(t, c, "text_chunk")

	s, _ := h.registry.Get("s1")
	waitState(t, s, session.Streaming)

	route(h, c, `{"type":"stop","session_id":"s1"}`)
	waitForType(t, c, "stopped")
	waitState(t, s, session.Idle)
}

func TestClearWarnsAndEndsTurn(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	fe.onSend = func(s *session.Session, reqID, content string) {
		h.Emit(protocol.NewTextChunk(s.ID, reqID, "..."))
	}

	route(h, c, `{"type":"new_session","session_id":"s1","backend":"claude"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"go"}`)
	waitForType(t, c, "text_chunk")

	s, _ := h.registry.Get("s1")
	waitState(t, s, session.Streaming)

	route(h, c, `{"type":"clear_session","session_id":"s1"}`)
	waitForType(t, c, "session_warning")
	waitState(t, s, session.Idle) // router calls EndTurn after Clear
}

func TestCloseRemovesSession(t *testing.T) {
	h, _ := newTestHub(t)
	c := newTestClient(h)
	route(h, c, `{"type":"new_session","session_id":"s1","backend":"claude"}`)
	waitForType(t, c, "session_created")

	s, _ := h.registry.Get("s1")
	route(h, c, `{"type":"close_session","session_id":"s1"}`)
	waitForType(t, c, "session_closed")

	if _, ok := h.registry.Get("s1"); ok {
		t.Fatal("session should be removed from the registry after close")
	}
	// The worker is shut down: further turns are rejected.
	if s.Submit(func() {}) {
		t.Fatal("Submit on a closed session must fail")
	}
}

// Events emitted while no client is connected are buffered and replayed to the
// next client after its sessions_list (the offline-recovery path).
func TestReconnectReplaysOfflineEvents(t *testing.T) {
	h, _ := newTestHub(t)
	// No clients connected: this event must be buffered, not dropped.
	h.Emit(protocol.NewTextChunk("s1", "r1", "missed while offline"))

	c := newTestClient(h)
	route(h, c, `{"type":"hello","device_id":"d1"}`)

	// hello replies with hello_ack + sessions_list, then replays the buffered chunk.
	waitForType(t, c, "hello_ack")
	waitForType(t, c, "sessions_list")
	replayed := waitForType(t, c, "text_chunk")
	if replayed["content"] != "missed while offline" {
		t.Fatalf("offline event not replayed correctly: %v", replayed)
	}
}

func TestReliableReplayBatchesAndCommitsOnlyAfterAck(t *testing.T) {
	h, _ := newTestHub(t)
	for i := 0; i < 2050; i++ {
		h.Emit(protocol.NewDone("s1", "r"+itoa(i)))
	}
	c := newTestClient(h)
	c.deviceID = "phone"
	c.supportsReplayAck = true
	h.registerLatest(c)
	h.startOfflineReplay(c)

	batches := 0
	for h.offline.Len() > 0 {
		before := h.offline.Len()
		batch := waitForType(t, c, "offline_replay_batch")
		events, _ := batch["events"].([]any)
		if len(events) == 0 || len(events) > replayBatchSize {
			t.Fatalf("invalid replay batch size %d", len(events))
		}
		if h.offline.Len() != before {
			t.Fatalf("journal changed before ACK: before=%d after=%d", before, h.offline.Len())
		}
		batchID, _ := batch["batch_id"].(string)
		route(h, c, `{"type":"offline_replay_ack","batch_id":"`+batchID+`"}`)
		batches++
	}
	if batches < 30 {
		t.Fatalf("2050 events should require many bounded batches, got %d", batches)
	}
	select {
	case <-c.quit:
		t.Fatal("reliable replay must not overflow and drop the client")
	default:
	}
}

func TestReliableReplayDisconnectBeforeAckRetainsBatch(t *testing.T) {
	h, _ := newTestHub(t)
	h.Emit(protocol.NewDone("s1", "r1"))
	c := newTestClient(h)
	c.deviceID = "phone"
	c.supportsReplayAck = true
	h.registerLatest(c)
	h.startOfflineReplay(c)
	waitForType(t, c, "offline_replay_batch")
	c.shutdown()
	h.releaseReplayLease(c)
	h.removeClient(c)
	if h.offline.Len() != 1 {
		t.Fatalf("unacked event was lost, remaining=%d", h.offline.Len())
	}

	next := newTestClient(h)
	next.deviceID = "phone"
	next.supportsReplayAck = true
	h.registerLatest(next)
	h.startOfflineReplay(next)
	batch := waitForType(t, next, "offline_replay_batch")
	if events, _ := batch["events"].([]any); len(events) != 1 {
		t.Fatalf("reconnect should resend retained event: %v", batch)
	}
}

func TestHelloSendsDurableGoalsSnapshotBeforeReplay(t *testing.T) {
	h, _ := newTestHub(t)
	h.Emit(protocol.NewGoalUpdate("s1", protocol.Goal{ThreadID: "t1", Objective: "ship", Status: "complete", UpdatedAt: 10}))
	c := newTestClient(h)
	route(h, c, `{"type":"hello","device_id":"phone","replay_ack":true}`)
	waitForType(t, c, "hello_ack")
	waitForType(t, c, "sessions_list")
	snapshot := waitForType(t, c, "goals_snapshot")
	items, _ := snapshot["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("goals snapshot=%v", snapshot)
	}
	item := items[0].(map[string]any)
	goal := item["goal"].(map[string]any)
	if item["session_id"] != "s1" || goal["status"] != "complete" {
		t.Fatalf("wrong goal snapshot item: %v", item)
	}
	waitForType(t, c, "offline_replay_batch")
}

// Two messages for the same session must not interleave: the second turn only
// starts after the first emits done.
func TestPerSessionTurnsSerialize(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)

	starts := make(chan string, 2)
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	fe.onSend = func(s *session.Session, reqID, content string) {
		starts <- reqID
		if reqID == "r1" {
			<-releaseFirst
		} else {
			<-releaseSecond
		}
		h.Emit(protocol.NewTextChunk(s.ID, reqID, "answer "+content))
		h.Emit(protocol.NewDone(s.ID, reqID))
	}

	route(h, c, `{"type":"new_session","session_id":"s1","backend":"claude"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"a"}`)
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r2","content":"b"}`)

	if got := <-starts; got != "r1" {
		t.Fatalf("first turn should be r1, got %s", got)
	}
	select {
	case got := <-starts:
		t.Fatalf("second turn %s started before first finished", got)
	case <-time.After(60 * time.Millisecond):
	}
	close(releaseFirst) // let r1 finish → r2 should start
	if got := <-starts; got != "r2" {
		t.Fatalf("second turn should be r2, got %s", got)
	}
	// Turn 1's later completion must not overwrite the actor-ordered preview for
	// turn 2, which is now the active work.
	s, _ := h.registry.Get("s1")
	if snap := s.Snapshot(); snap.PreviewText != "b" || snap.PreviewRole != "user" {
		t.Fatalf("queued turn preview regressed after prior completion: %+v", snap)
	}
	close(releaseSecond)
}

// Once message_ack confirms the Bridge accepted a follow-up, the turn belongs
// to the server-side Session actor and must run even if the sending app closes.
func TestAcceptedQueuedTurnSurvivesClientDisconnect(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	starts := make(chan string, 2)
	releaseFirst := make(chan struct{})
	fe.onSend = func(s *session.Session, reqID, content string) {
		starts <- reqID
		if reqID == "r1" {
			<-releaseFirst
		}
		h.Emit(protocol.NewDone(s.ID, reqID))
	}

	route(h, c, `{"type":"new_session","session_id":"s1","backend":"codex"}`)
	waitForType(t, c, "session_created")
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r1","content":"first"}`)
	waitForType(t, c, "message_ack")
	if got := <-starts; got != "r1" {
		t.Fatalf("first start=%q", got)
	}
	route(h, c, `{"type":"message","session_id":"s1","request_id":"r2","content":"second"}`)
	ack := waitForType(t, c, "message_ack")
	if ack["request_id"] != "r2" || ack["status"] != "queued" {
		t.Fatalf("follow-up ack=%+v", ack)
	}
	view := h.runtimeSnapshot("").Items[0]
	if view.Phase != "running" || view.ActiveRequestID != "r1" || view.QueueLength != 1 {
		t.Fatalf("queued follow-up replaced active request: %+v", view)
	}

	// Simulate the phone process disappearing after the acknowledgement.
	h.removeClient(c)
	close(releaseFirst)
	select {
	case got := <-starts:
		if got != "r2" {
			t.Fatalf("second start=%q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server-owned follow-up did not run after client disconnect")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		view = h.runtimeSnapshot("").Items[0]
		if view.Phase == "completed" && view.ActiveRequestID == "r2" && view.QueueLength == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("second turn did not reach terminal runtime: %+v", view)
}

func TestDuplicateMessageRequestIsAcknowledgedWithoutSecondExecution(t *testing.T) {
	h, fe := newTestHub(t)
	c := newTestClient(h)
	executions := make(chan string, 2)
	fe.onSend = func(s *session.Session, reqID, content string) {
		executions <- reqID
		h.Emit(protocol.NewDone(s.ID, reqID))
	}
	route(h, c, `{"type":"new_session","session_id":"s1","backend":"codex"}`)
	waitForType(t, c, "session_created")
	frame := `{"type":"message","session_id":"s1","request_id":"same","content":"once"}`
	route(h, c, frame)
	waitForType(t, c, "message_ack")
	if got := <-executions; got != "same" {
		t.Fatalf("execution=%q", got)
	}
	route(h, c, frame)
	ack := waitForType(t, c, "message_ack")
	if ack["request_id"] != "same" || ack["status"] != "queued" {
		t.Fatalf("duplicate ack=%+v", ack)
	}
	select {
	case got := <-executions:
		t.Fatalf("duplicate executed as %q", got)
	case <-time.After(80 * time.Millisecond):
	}
}
