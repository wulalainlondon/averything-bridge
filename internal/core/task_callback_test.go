package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/callbacks"
	"everything-go/internal/messagequeue"
	"everything-go/internal/session"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type callbackExec struct {
	*voiceExec
	final      string
	finalFound bool
	appends    atomic.Int32
	appended   string
}

func (e *callbackExec) FinalAnswerForSession(_ *session.Session, request string) (string, bool, error) {
	if request != "r_callbacktask" {
		return "", false, nil
	}
	return e.final, e.finalFound, nil
}
func (e *callbackExec) AppendRealtimeVoiceSpeech(_ context.Context, _ *session.Session, _, text string) error {
	e.appended = text
	e.appends.Add(1)
	return nil
}
func callbackFixture(t *testing.T) (*Hub, *Client, *session.Session, *callbackExec) {
	t.Helper()
	h, fe := newControlTestHub(t, t.TempDir())
	e := &callbackExec{voiceExec: &voiceExec{fakeExec: fe}, final: "真正的正式結果", finalFound: true}
	h.SetExecutor(e)
	c := sharedReadClient(t, h, "phone-a")
	c.callbackCapable.Store(true)
	c.callbackNative.Store(true)
	s := h.registry.Create("s_callback", "匯報測試", t.TempDir(), backend.Codex, "", "", "native-callback")
	h.runtimes.Ensure(s.ID, "idle", 0)
	return h, c, s, e
}
func callbackAction(t *testing.T, h *Hub, c *Client, in callbackInput) map[string]any {
	t.Helper()
	in.Type = "task_callback_request"
	raw, _ := json.Marshal(in)
	h.handleTaskCallback(context.Background(), c, raw)
	for n := 0; n < 12; n++ {
		event := waitForType(t, c, "task_callback")
		if event["operation_id"] == in.Operation {
			return event
		}
	}
	t.Fatal("operation response missing")
	return nil
}
func TestCallbackRefusesUnpairedAndDoesNotStartVoice(t *testing.T) {
	h, c, s, e := callbackFixture(t)
	c.readIdentity.Store(nil)
	taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Queued)
	got := callbackAction(t, h, c, callbackInput{Action: "set", Operation: "callback_set_unpaired", Session: s.ID, Request: "r_callbacktask", Enabled: true})
	if got["accepted"] != false || e.starts.Load() != 0 || e.appends.Load() != 0 {
		t.Fatal(got)
	}
	rows, _ := h.callbacks.List(context.Background())
	if len(rows) != 0 {
		t.Fatal(rows)
	}
}
func TestCallbackCompletedIsNotEnoughWithoutExactFinal(t *testing.T) {
	h, c, s, e := callbackFixture(t)
	taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Completed)
	if err := h.armAdmittedCallback(context.Background(), c, s, "r_callbacktask"); err != nil {
		t.Fatal(err)
	}
	h.reconcileTaskCallbacks(context.Background(), s.ID, "r_callbacktask")
	rows, _ := h.callbacks.List(context.Background())
	if len(rows) != 1 || rows[0].Report != nil {
		t.Fatal("terminal became final", rows)
	}
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: "r_callbacktask", ThreadID: s.ResumeID(), TurnID: "callback-turn"})
	e.finalFound = false
	h.reconcileTaskCallbacks(context.Background(), s.ID, "r_callbacktask")
	rows, _ = h.callbacks.List(context.Background())
	if rows[0].Report != nil {
		t.Fatal("commentary substituted")
	}
	e.finalFound = true
	h.reconcileTaskCallbacks(context.Background(), s.ID, "r_callbacktask")
	rows, _ = h.callbacks.List(context.Background())
	if rows[0].Report == nil || rows[0].Report.Text != e.final || rows[0].Invite == nil || e.starts.Load() != 0 || e.appends.Load() != 0 {
		t.Fatal(rows)
	}
}
func TestCallbackRecipientOnlyCanClaimAndReportExactlyOnce(t *testing.T) {
	h, c, s, e := callbackFixture(t)
	taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Completed)
	emitVerifiedNativeFixture(h, backend.NativeTaskAccepted{SessionID: s.ID, RequestID: "r_callbacktask", ThreadID: s.ResumeID(), TurnID: "callback-turn"})
	if err := h.armAdmittedCallback(context.Background(), c, s, "r_callbacktask"); err != nil {
		t.Fatal(err)
	}
	h.reconcileTaskCallbacks(context.Background(), s.ID, "r_callbacktask")
	rows, _ := h.callbacks.List(context.Background())
	r := rows[0]
	if r.Invite == nil {
		t.Fatal("no sealed invitation")
	}
	other := sharedReadClient(t, h, "phone-b")
	other.callbackNative.Store(true)
	got := callbackAction(t, h, other, callbackInput{Action: "claim", Operation: "callback_wrong_phone", Session: s.ID, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID})
	if got["accepted"] != false {
		t.Fatal("wrong recipient", got)
	}
	in := callbackInput{Action: "claim", Operation: "callback_accept_once", Session: s.ID, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID}
	got = callbackAction(t, h, c, in)
	if got["accepted"] != true {
		t.Fatal(got)
	}
	got = callbackAction(t, h, c, in)
	if got["accepted"] != true {
		t.Fatal("original receipt lost", got)
	}
	h.voiceClients = map[*Client]*clientVoiceCall{}
	h.voiceClients[c] = &clientVoiceCall{voiceID: "voice-callback", session: s}
	r, _, _ = h.callbacks.Get(context.Background(), r.ID)
	report := callbackInput{Action: "report", Operation: "callback_report_once", Session: s.ID, Request: r.Request, Expected: r.Revision, Invitation: r.Invite.ID, Voice: "voice-callback"}
	got = callbackAction(t, h, c, report)
	if got["accepted"] != true {
		t.Fatal(got)
	}
	// Run synchronously to inspect idempotency; async original races through same guard.
	latest, _, _ := h.callbacks.Get(context.Background(), r.ID)
	h.speakCallback(c, s, latest, report)
	callbackAction(t, h, c, report)
	deadline := time.Now().Add(time.Second)
	for e.appends.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.appends.Load() != 1 || !strings.Contains(e.appended, e.final) {
		t.Fatal(e.appends.Load(), e.appended)
	}
}
func TestCallbackCancelPreventsLateSealAndRecoveryRearming(t *testing.T) {
	h, c, s, _ := callbackFixture(t)
	taskEntry(t, h, s.ID, "r_callbacktask", "phone-a", messagequeue.Queued)
	if e := h.armAdmittedCallback(context.Background(), c, s, "r_callbacktask"); e != nil {
		t.Fatal(e)
	}
	rows, _ := h.callbacks.List(context.Background())
	r := rows[0]
	got := callbackAction(t, h, c, callbackInput{Action: "cancel", Operation: "callback_cancel_task", Session: s.ID, Request: r.Request, Expected: r.Revision})
	if got["accepted"] != true {
		t.Fatal(got)
	}
	if e := h.armAdmittedCallback(context.Background(), c, s, "r_callbacktask"); e != nil {
		t.Fatal(e)
	}
	latest, _, _ := h.callbacks.Get(context.Background(), r.ID)
	if latest.Enabled || latest.State != "disabled" {
		t.Fatal(latest)
	}
}
func TestCallbackFingerprintIsNotPublic(t *testing.T) {
	h, c, _, _ := callbackFixture(t)
	r := h.callbackRecipient(c)
	raw, _ := json.Marshal(r)
	if strings.Contains(string(raw), r.Binding) || !strings.Contains(string(raw), "phone-a") {
		t.Fatal(string(raw))
	}
	if callbacks.Digest("qa-read-phone-a") != r.Binding {
		t.Fatal("not original paired credential generation")
	}
}
