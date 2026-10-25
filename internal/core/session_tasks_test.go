package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/history"
	"everything-go/internal/session"
	"github.com/coder/websocket"
	"testing"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/messagequeue"
	"everything-go/internal/protocol"
	"everything-go/internal/sessiondispatch"
)

func taskEntry(t *testing.T, h *Hub, session, request, owner string, state messagequeue.State) messagequeue.Entry {
	t.Helper()
	payload, _ := json.Marshal(queuedPayload{MessagePurpose: "instruction", OwnerDevice: owner, Content: request})
	e, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: session, RequestID: request, Content: request, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	if state != messagequeue.Queued {
		e, _, err = h.messageQueue.Transition(session, request, []messagequeue.State{messagequeue.Queued}, state, "", "", "")
		if err != nil {
			t.Fatal(err)
		}
	}
	return e
}
func TestTaskProjectionRequiresNativeConsumptionAndExactFinalAnchor(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "same", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("thread")
	e := taskEntry(t, h, s.ID, "r_request1", "owner", messagequeue.Running)
	if got := h.projectSessionTask(s, e, "owner", nil); got.State != "handoff_unknown" || got.CanCancel {
		t.Fatalf("actor running became native acceptance: %+v", got)
	}
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: e.RequestID, ThreadID: "thread", TurnID: "native1"})
	if got := h.projectSessionTask(s, e, "owner", nil); got.State != "consumed" || got.NativeTurnID != "native1" {
		t.Fatalf("acceptance missing: %+v", got)
	}
	e, _, _ = h.messageQueue.Transition(s.ID, e.RequestID, []messagequeue.State{messagequeue.Running}, messagequeue.Completed, "", "", "")
	if got := h.projectSessionTask(s, e, "owner", nil); got.State != "completed_unanchored" {
		t.Fatal(got)
	}
	finals := map[string]map[string]any{"r_other": {"source_message_id": "wrong", "content": "DONE"}}
	if got := h.projectSessionTask(s, e, "owner", finals); got.State != "completed_unanchored" {
		t.Fatal("another turn substituted", got)
	}
	finals[e.RequestID] = map[string]any{"source_message_id": "native-final-1", "source_turn_id": "native1", "content": "Actual final"}
	if got := h.projectSessionTask(s, e, "owner", finals); got.State != "completed" || got.SourceMessageID != "native-final-1" {
		t.Fatal(got)
	}
}
func TestTaskOriginRejectsFalseOwnerMissingGrantWrongSourceAndPeer(t *testing.T) {
	h, _, caller, target := controllerFixture(t)
	owner := sharedReadClient(t, h, "owner")
	other := sharedReadClient(t, h, "other")
	source := caller.Parent
	taskEntry(t, h, source.ID, "r_parent123", "owner", messagequeue.Running)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: source.ID, RequestID: "r_parent123", ThreadID: source.ResumeID(), TurnID: "parent-native"})
	origin := &protocol.TaskOrigin{InstanceID: h.cfg.InstanceID, SessionID: source.ID, ThreadID: source.ResumeID(), ConfigRevision: source.SettingsSnapshot().ConfigRevision, RequestID: "r_parent123"}
	cmd := clientproto.Command{SessionID: target.ID, TaskOrigin: origin}
	if _, err := h.admitTaskOrigin(owner, cmd); err == nil {
		t.Fatal("missing grant admitted")
	}
	h.dispatches.SetGrant(context.Background(), source.ID, sessiondispatch.Grant{Enabled: true, Local: true}, 0)
	if _, err := h.admitTaskOrigin(other, cmd); err == nil {
		t.Fatal("false owner admitted")
	}
	if got, err := h.admitTaskOrigin(owner, cmd); err != nil || got != "owner" {
		t.Fatal(got, err)
	}
	origin.InstanceID = "other-instance"
	if _, err := h.admitTaskOrigin(owner, cmd); err == nil {
		t.Fatal("foreign origin accepted")
	}
	origin.InstanceID = h.cfg.InstanceID
	origin.ThreadID = "wrong"
	if _, err := h.admitTaskOrigin(owner, cmd); err == nil {
		t.Fatal("wrong thread accepted")
	}
}
func TestTaskCancelRejectsAnotherDeviceUnknownAndLateNativeAcceptance(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "same", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("thread")
	owner := sharedReadClient(t, h, "owner")
	other := sharedReadClient(t, h, "other")
	taskEntry(t, h, s.ID, "r_unknown1", "owner", messagequeue.Uncertain)
	cmd := clientproto.Command{Kind: "cancel_queued_message", SessionID: s.ID, RequestID: "r_unknown1"}
	h.cancelQueuedMessage(owner, cmd)
	if waitForType(t, owner, "queue_action_result")["status"] != "rejected" {
		t.Fatal("unknown cancelled")
	}
	taskEntry(t, h, s.ID, "r_waiting1", "owner", messagequeue.Queued)
	cmd.RequestID = "r_waiting1"
	h.cancelQueuedMessage(other, cmd)
	if waitForType(t, other, "queue_action_result")["status"] != "rejected" {
		t.Fatal("other device cancelled")
	}
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: cmd.RequestID, ThreadID: "thread", TurnID: "late-native"})
	h.cancelQueuedMessage(owner, cmd)
	if waitForType(t, owner, "queue_action_result")["status"] != "rejected" {
		t.Fatal("late accepted cancelled")
	}
}
func TestTaskReadEmptyUnsupportedForbiddenAndNoUnrelatedBusyChildren(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "same", t.TempDir(), backend.Ollama, "", "", "")
	owner := sharedReadClient(t, h, "owner")
	h.registry.Create("other", "same", t.TempDir(), backend.Codex, "", "", "")
	h.sendSessionTasks(owner, clientproto.Command{SessionID: s.ID, RequestID: "read1"})
	event := waitForType(t, owner, "session_tasks_snapshot")
	if event["status"] != "supported" || len(event["items"].([]any)) != 0 || len(event["children"].([]any)) != 0 || event["history_status"] != "unsupported" {
		t.Fatal(event)
	}
	unknown := newTestClient(h)
	h.sendSessionTasks(unknown, clientproto.Command{SessionID: s.ID, RequestID: "read2"})
	if waitForType(t, unknown, "session_tasks_snapshot")["status"] != "forbidden" {
		t.Fatal("unpaired read accepted")
	}
}
func TestTaskScopePreservesPeerTargetAndOrigin(t *testing.T) {
	raw := []byte(`{"type":"session_tasks_snapshot","session_id":"parent","children":[{"instance_id":"peer","session_id":"same","origin":{"instance_id":"local","session_id":"parent"}}]}`)
	out, err := scopeSessionIDsJSON(raw, "local")
	if err != nil {
		t.Fatal(err)
	}
	var event map[string]any
	json.Unmarshal(out, &event)
	child := event["children"].([]any)[0].(map[string]any)
	if child["session_id"] != "sk1:peer:same" || child["origin"].(map[string]any)["session_id"] != "sk1:local:parent" {
		t.Fatal(string(out))
	}
}

func TestTaskAuthenticatedWSQueuedConsumedAndCompletedAnchor(t *testing.T) {
	h, fe := newTestHub(t)
	if err := h.pairing.Claim("qa-read-ws-owner", "ws-owner"); err != nil {
		t.Fatal(err)
	}
	s := h.registry.Create("s1", "Task", t.TempDir(), backend.Codex, "", "read-only", "")
	s.SetResumeID("thread")
	provider := &floatingHistoryProvider{byResume: map[string][]map[string]any{"thread": {}}}
	h.SetExecutor(&floatingHistoryExec{fakeExec: fe, provider: provider})
	started := make(chan struct{})
	accept := make(chan struct{})
	finish := make(chan struct{})
	fe.onSend = func(s *session.Session, id, text string) {
		close(started)
		<-accept
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: id, ThreadID: "thread", TurnID: "native-turn"})
		<-finish
		h.Emit(protocol.NewDone(s.ID, id))
	}
	conn, ctx, cleanup := dialWS(t, h)
	defer cleanup()
	write := func(raw string) {
		if err := conn.Write(ctx, websocket.MessageText, []byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
	readType := func(kind string) map[string]any {
		for {
			e := readEvent(t, ctx, conn)
			if e["type"] == kind {
				return e
			}
		}
	}
	write(`{"type":"hello","device_id":"ws-owner","auth_token":"qa-read-ws-owner","protocol_version":3,"session_read_sync":true}`)
	readType("hello_ack")
	write(`{"type":"message","session_id":"s1","request_id":"r_wsoriginal123","message_purpose":"instruction","content":"fixture task"}`)
	readType("message_ack")
	<-started
	write(`{"type":"request_session_tasks","session_id":"s1","request_id":"read1"}`)
	e := readType("session_tasks_snapshot")
	item := e["items"].([]any)[0].(map[string]any)
	if e["session_id"] != "sk1:i1:s1" || item["state"] != "handoff_unknown" {
		t.Fatal(e)
	}
	close(accept)
	for h.messageQueue.NativeAcceptance(s.ID, "r_wsoriginal123", "thread") == "" {
		time.Sleep(time.Millisecond)
	}
	write(`{"type":"request_session_tasks","session_id":"s1","request_id":"read2"}`)
	e = readType("session_tasks_snapshot")
	if e["items"].([]any)[0].(map[string]any)["state"] != "consumed" {
		t.Fatal(e)
	}
	// A fixture final enters through the real HistoryProvider, not a ledger edit.
	m := history.CompleteMsg("codex", "thread", "final-source", "assistant", "Actual final", 1000, nil)
	m["request_id"] = "r_wsoriginal123"
	m["source_turn_id"] = "native-turn"
	m["history_read_result_verified"] = true
	provider.byResume["thread"] = []map[string]any{m}
	close(finish)
	readType("done")
	write(`{"type":"request_session_tasks","session_id":"s1","request_id":"read3"}`)
	e = readType("session_tasks_snapshot")
	item = e["items"].([]any)[0].(map[string]any)
	if item["state"] != "completed" || item["source_message_id"] != "final-source" {
		t.Fatal(e)
	}
}

func TestTaskSteeredSupplementFollowsOnlyMatchingExecutionTerminalAndFinal(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "same", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("thread")
	active := taskEntry(t, h, s.ID, "r_active123", "owner", messagequeue.Running)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: active.RequestID, ThreadID: "thread", TurnID: "turn1"})
	supplement := taskEntry(t, h, s.ID, "r_supplement123", "owner", messagequeue.Queued)
	supplement, _, _ = h.messageQueue.Transition(s.ID, supplement.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Steered, "", active.RequestID, "turn1", true)
	if got := h.projectSessionTask(s, supplement, "owner", nil); got.State != "consumed" || got.ExecutionRequestID != active.RequestID || got.RequestID != supplement.RequestID {
		t.Fatal(got)
	}
	h.messageQueue.Transition(s.ID, active.RequestID, []messagequeue.State{messagequeue.Running}, messagequeue.Completed, "", "", "")
	wrong := map[string]map[string]any{active.RequestID: {"source_message_id": "wrong", "source_turn_id": "other", "content": "DONE"}}
	if got := h.projectSessionTask(s, supplement, "owner", wrong); got.State != "completed_unanchored" || got.SourceMessageID != "" {
		t.Fatal("wrong turn became final", got)
	}
	finals := map[string]map[string]any{active.RequestID: {"source_message_id": "original-final", "source_turn_id": "turn1", "content": "Matching turn final"}}
	got := h.projectSessionTask(s, supplement, "owner", finals)
	if got.State != "completed" || got.SourceMessageID != "original-final" || got.ExecutionRequestID != active.RequestID || got.Transport != "ordinary_steer" {
		t.Fatal(got)
	}
	// Another independently accepted execution may fail; it cannot settle this supplement.
	other := taskEntry(t, h, s.ID, "r_other123", "owner", messagequeue.Failed)
	_ = other
	if got := h.projectSessionTask(s, supplement, "owner", finals); got.State != "completed" {
		t.Fatal(got)
	}
}
func TestTaskSteeredSupplementFollowsMatchingFailureAndRejectsForeignNativeTurn(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "same", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("thread")
	active := taskEntry(t, h, s.ID, "r_failedactive123", "owner", messagequeue.Failed)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: active.RequestID, ThreadID: "thread", TurnID: "failed-turn"})
	e := taskEntry(t, h, s.ID, "r_steerfailed123", "owner", messagequeue.Queued)
	e, _, _ = h.messageQueue.Transition(s.ID, e.RequestID, []messagequeue.State{messagequeue.Queued}, messagequeue.Steered, "", active.RequestID, "failed-turn", true)
	if got := h.projectSessionTask(s, e, "owner", nil); got.State != "failed" {
		t.Fatal(got)
	}
	e.TurnID = "foreign-turn"
	if got := h.projectSessionTask(s, e, "owner", nil); got.State != "legacy_unknown" {
		t.Fatal("foreign turn settled", got)
	}
}

func TestTaskControllerSteerProjectionUsesEffectiveNativeExecutionAnchor(t *testing.T) {
	h, fe, caller, target := controllerFixture(t)
	h.dispatches.SetGrant(context.Background(), caller.Parent.ID, sessiondispatch.Grant{Enabled: true, Local: true, Steer: true}, 0)
	provider := &floatingHistoryProvider{byResume: map[string][]map[string]any{target.ResumeID(): {}}}
	h.SetExecutor(&floatingHistoryExec{fakeExec: fe, provider: provider})
	activeID := "r_controlleractive123"
	turnID := "controller-native"
	started, release := make(chan struct{}), make(chan struct{})
	fe.onSend = func(s *session.Session, id, text string) {
		emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: id, ThreadID: s.ResumeID(), TurnID: turnID})
		close(started)
		<-release
		h.Emit(protocol.NewDone(s.ID, id))
	}
	fe.onSteer = func(s *session.Session, id, text string) (backend.SteerResult, error) {
		return backend.SteerResult{StrictACK: true, RequestID: activeID, TurnID: turnID}, nil
	}
	client := sharedReadClient(t, h, "controller-viewer")
	if !h.enqueueChatMessage(client, clientproto.Command{Kind: "message", SessionID: target.ID, RequestID: activeID, MessagePurpose: "instruction", Content: "fixture active"}) {
		t.Fatal("active admission")
	}
	<-started
	value, err := h.ControlSession(context.Background(), caller, backend.SessionControlRequest{Action: "dispatch_to_session", SessionID: target.ID, ExpectedThreadID: target.ResumeID(), ExpectedConfigRevision: target.SettingsSnapshot().ConfigRevision, Content: "explicit user supplement", Mode: "steer"})
	if err != nil {
		t.Fatal(err)
	}
	record := value.(sessiondispatch.Record)
	deadline := time.Now().Add(time.Second)
	for {
		e, found, _ := h.messageQueue.Get(target.ID, record.RequestID)
		if found && e.State == messagequeue.Steered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("steer not accepted")
		}
		time.Sleep(time.Millisecond)
	}
	h.sendSessionTasks(client, clientproto.Command{SessionID: caller.Parent.ID, RequestID: "before"})
	event := waitForType(t, client, "session_tasks_snapshot")
	child := event["children"].([]any)[0].(map[string]any)
	if child["state"] != "consumed" || child["transport"] != "controller_steer" || child["execution_request_id"] != activeID {
		t.Fatal(event)
	}
	final := history.CompleteMsg("codex", target.ResumeID(), "controller-final-source", "assistant", "Final of the effective native turn", 1000, nil)
	final["request_id"] = activeID
	final["source_turn_id"] = turnID
	final["history_read_result_verified"] = true
	provider.byResume[target.ResumeID()] = []map[string]any{final}
	close(release)
	waitForType(t, client, "done")
	h.sendSessionTasks(client, clientproto.Command{SessionID: caller.Parent.ID, RequestID: "after"})
	event = waitForType(t, client, "session_tasks_snapshot")
	child = event["children"].([]any)[0].(map[string]any)
	if child["state"] != "completed" || child["source_message_id"] != "controller-final-source" || child["execution_request_id"] != activeID || child["request_id"] != record.RequestID {
		t.Fatal(event)
	}
}

func TestTaskLegacyAndReturnFinalsAreNotIndependentUserResults(t *testing.T) {
	h, _ := newTestHub(t)
	s := h.registry.Create("s1", "same", t.TempDir(), backend.Codex, "", "", "")
	s.SetResumeID("thread")
	for _, purpose := range []string{"", "result_return", "question_reply", "control"} {
		id := "r_purpose_" + purpose
		payload, _ := json.Marshal(queuedPayload{OwnerDevice: "owner", MessagePurpose: purpose, Content: "not inferred from DONE"})
		e, _, err := h.messageQueue.Enqueue(messagequeue.Entry{SessionID: s.ID, RequestID: id, Payload: payload})
		if err != nil {
			t.Fatal(err)
		}
		e, _, _ = h.messageQueue.Transition(s.ID, id, []messagequeue.State{messagequeue.Queued}, messagequeue.Completed, "", "", "")
		final := map[string]map[string]any{id: {"source_message_id": "final-" + purpose, "source_turn_id": "turn", "content": "DONE"}}
		if got := h.projectSessionTask(s, e, "owner", final); got.State == "completed" {
			t.Fatal("notice became independent result", got)
		}
	}
}

func TestTaskTypedPhotoAndFloatingReceiptsTrackWithoutChangingOriginalIDs(t *testing.T) {
	for _, id := range []string{"photo_original123", "floating_original123"} {
		t.Run(id, func(t *testing.T) {
			h, _ := newTestHub(t)
			s := h.registry.Create("s1", "same", t.TempDir(), backend.Codex, "", "", "")
			s.SetResumeID("thread")
			owner := sharedReadClient(t, h, "owner")
			entry := taskEntry(t, h, s.ID, id, "owner", messagequeue.Queued)
			h.sendSessionTasks(owner, clientproto.Command{SessionID: s.ID, RequestID: "queued-read"})
			snapshot := waitForType(t, owner, "session_tasks_snapshot")
			if len(snapshot["items"].([]any)) != 1 || snapshot["items"].([]any)[0].(map[string]any)["request_id"] != id {
				t.Fatal(snapshot)
			}
			h.messageQueue.Transition(s.ID, id, []messagequeue.State{messagequeue.Queued}, messagequeue.Running, "", "", "")
			emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: id, ThreadID: "thread", TurnID: "native-photo"})
			entry, _, _ = h.messageQueue.Get(s.ID, id)
			if got := h.projectSessionTask(s, entry, "owner", nil); got.State != "consumed" {
				t.Fatal(got)
			}
			entry, _, _ = h.messageQueue.Transition(s.ID, id, []messagequeue.State{messagequeue.Running}, messagequeue.Completed, "", "", "")
			final := map[string]map[string]any{id: {"source_message_id": "native-final", "source_turn_id": "native-photo", "content": "Actual final"}}
			if got := h.projectSessionTask(s, entry, "owner", final); got.State != "completed" || got.RequestID != id {
				t.Fatal(got)
			}
		})
	}
}

// Controlled original-history delay: no real provider, task, cancellation or replay.
type taskDelayedHistory struct {
	entered chan struct{}
	release chan struct{}
}

func (p *taskDelayedHistory) LoadHistory(string, history.Opts) (*history.Result, error) {
	close(p.entered)
	<-p.release
	return &history.Result{Kind: "snapshot"}, nil
}
func (p *taskDelayedHistory) ResumableSessions(int) ([]history.ResumableSession, error) {
	return nil, nil
}

type taskDelayedExec struct {
	fakeExec
	provider *taskDelayedHistory
}

func (e *taskDelayedExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.provider, true
}
func (e *taskDelayedExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.provider}
}
func TestSessionTaskHistoryRevalidatesOriginalPairingAndSourceAfterDelay(t *testing.T) {
	for _, change := range []string{"repaired-same-device", "source-thread", "source-hidden", "source-config"} {
		t.Run(change, func(t *testing.T) {
			h, _ := newTestHub(t)
			s := h.registry.Create("parent", "Parent", t.TempDir(), backend.Codex, "", "", "original-thread")
			reader := sharedReadClient(t, h, "human")
			taskEntry(t, h, s.ID, "r_original", "human", messagequeue.Queued)
			p := &taskDelayedHistory{make(chan struct{}), make(chan struct{})}
			h.SetExecutor(&taskDelayedExec{fakeExec: fakeExec{sink: h}, provider: p})
			done := make(chan struct{})
			go func() {
				h.sendSessionTasks(reader, clientproto.Command{SessionID: s.ID, RequestID: "delayed-read"})
				close(done)
			}()
			select {
			case <-p.entered:
			case <-time.After(time.Second):
				t.Fatal("history did not enter")
			}
			switch change {
			case "repaired-same-device":
				if err := h.pairing.Unclaim("qa-read-human"); err != nil {
					t.Fatal(err)
				}
				if err := h.pairing.Claim("new-fixture-human-token", "human"); err != nil {
					t.Fatal(err)
				}
			case "source-thread":
				s.SetResumeID("replacement-thread")
			case "source-hidden":
				hidden := true
				s.SetMeta(nil, &hidden)
			case "source-config":
				_, err := s.SetFutureConfiguration(session.ConfigurationFrom(s.SettingsSnapshot()), nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			close(p.release)
			<-done
			event := waitForType(t, reader, "session_tasks_snapshot")
			if event["status"] != "forbidden" || len(event["items"].([]any)) != 0 || len(event["children"].([]any)) != 0 {
				t.Fatalf("stale read disclosed content: %+v", event)
			}
		})
	}
}
