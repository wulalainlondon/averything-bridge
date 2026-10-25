package goexec

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func asyncFixture(t *testing.T) (*Codex, *session.Session, *capSink) {
	t.Helper()
	sink := &capSink{}
	c := NewCodex(sink, "never-launch-codex-in-tests")
	c.dataDir = t.TempDir()
	c.sessionsRoot = t.TempDir()
	s := session.NewRegistry().Create("async-session", "Async", t.TempDir(), backend.Codex, "", "read-only", "native-thread")
	c.state(s.ID).threadID = s.ResumeID()
	c.threadToSession[s.ResumeID()] = s
	return c, s, sink
}

const asyncItemJSON = `{"type":"agentMessage","id":"call-fixture","delivery":"async","text":"","questions":[{"title":"Choose a route","options":["Recommended route","Second route"]},{"title":"Explain why","options":null}]}`

func emitAsyncFixture(c *Codex, s *session.Session) {
	c.dispatch(json.RawMessage(`{"method":"item/completed","params":{"threadId":"` + s.ResumeID() + `","item":` + asyncItemJSON + `}}`))
}

func TestNativeAsyncQuestionsReceiveDeduplicateAndOther(t *testing.T) {
	c, s, sink := asyncFixture(t)
	emitAsyncFixture(c, s)
	emitAsyncFixture(c, s)
	pending := c.PendingInteractions(s.ID)
	if len(pending) != 2 {
		t.Fatalf("want 2 independently answerable questions: %+v", pending)
	}
	for _, p := range pending {
		if p.IsBlocking == nil || *p.IsBlocking || !p.Questions[0].FreeForm {
			t.Fatalf("bad async payload: %+v", p)
		}
		if p.Questions[0].QuestionID != asyncQuestionID("call-fixture", 0) && p.Questions[0].QuestionID != asyncQuestionID("call-fixture", 1) {
			t.Fatal("lost native question index")
		}
	}
	if c.hasPendingInteraction(s.ID) {
		t.Fatal("async question must not block liveness watchdog")
	}
	if got := sink.count(func(e any) bool { _, ok := e.(backend.UserInputRequest); return ok }); got != 2 {
		t.Fatalf("duplicate prompts: %d", got)
	}
	q := normalizeCodexQuestions([]map[string]any{{"id": "old", "question": "Pick", "isOther": true, "options": []any{"One"}}})
	if !q[0].FreeForm {
		t.Fatal("legacy isOther free-form was lost")
	}
}

func TestNativeAsyncReplyIsCorrelatedAndOnlyResolvedOnAck(t *testing.T) {
	c, s, _ := asyncFixture(t)
	emitAsyncFixture(c, s)
	qid := asyncQuestionID("call-fixture", 0)
	id := asyncRequestID(s.ResumeID(), qid)
	sid, content, handled, err := c.PrepareAsyncReply(id, map[string]any{qid: "My other route"}, false)
	if err != nil || !handled || sid != s.ID {
		t.Fatalf("prepare: %s %v %v", sid, handled, err)
	}
	replies := parseAsyncReply(content)
	if len(replies) != 1 || replies[0].QuestionItemID != qid || replies[0].Question != "Choose a route" || replies[0].Answer != "My other route" {
		t.Fatalf("bad native record: %+v", replies)
	}
	if len(c.PendingInteractions(s.ID)) != 2 {
		t.Fatal("preparation falsely resolved a question")
	}
	st := c.state(s.ID)
	st.turnActive = true
	st.currentTurnID = "live-turn"
	st.reqID = "owned-request"
	writer := &toolTestWriter{c: c}
	writer.reply = func(method string, raw json.RawMessage) (any, error) {
		if method != "turn/steer" {
			t.Fatalf("unexpected mutation: %s", method)
		}
		var p struct {
			Thread string `json:"threadId"`
			Turn   string `json:"expectedTurnId"`
			Input  []struct {
				Text string `json:"text"`
			} `json:"input"`
		}
		_ = json.Unmarshal(raw, &p)
		if p.Thread != "native-thread" || p.Turn != "live-turn" || len(p.Input) != 1 || p.Input[0].Text != content {
			t.Fatalf("misrouted reply: %+v", p)
		}
		return nil, errors.New("fixture rejected steer")
	}
	c.rpc.setWriter(writer)
	if _, err := c.steerActiveTurn(s, id, content, nil, nil); err == nil {
		t.Fatal("expected rejection")
	}
	if len(c.PendingInteractions(s.ID)) != 2 {
		t.Fatal("failed native send removed the question")
	}
	writer.reply = func(string, json.RawMessage) (any, error) { return map[string]any{"turnId": "another-turn"}, nil }
	if _, err := c.steerActiveTurn(s, id, content, nil, nil); err == nil {
		t.Fatal("mismatched native ACK treated as delivery")
	}
	if len(c.PendingInteractions(s.ID)) != 2 {
		t.Fatal("mismatched ACK removed the question")
	}
	writer.reply = func(string, json.RawMessage) (any, error) { return map[string]any{"turnId": "live-turn"}, nil }
	if _, err := c.steerActiveTurn(s, id, content, nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(c.PendingInteractions(s.ID)) != 1 {
		t.Fatal("ACK must resolve exactly the replied question")
	}
	emitAsyncFixture(c, s)
	if len(c.PendingInteractions(s.ID)) != 1 {
		t.Fatal("replay resurrected an answered question")
	}
	data, err := os.ReadFile(c.asyncTombstonePath(s.ResumeID()))
	if err != nil || strings.Contains(string(data), "My other route") {
		t.Fatalf("journal must contain only IDs: %v", err)
	}
	c2 := NewCodex(&capSink{}, "never-launch")
	c2.dataDir = c.dataDir
	c2.state(s.ID).threadID = s.ResumeID()
	c2.threadToSession[s.ResumeID()] = s
	emitAsyncFixture(c2, s)
	if len(c2.PendingInteractions(s.ID)) != 1 {
		t.Fatal("restart lost answered tombstone")
	}
	if _, err := c2.validateAsyncReply(s, id, content); err == nil {
		t.Fatal("accepted an already answered queued reply")
	}
}

func TestNativeAsyncReplyGuardsOriginalThreadAndCancellation(t *testing.T) {
	c, s, _ := asyncFixture(t)
	emitAsyncFixture(c, s)
	qid := asyncQuestionID("call-fixture", 0)
	id := asyncRequestID(s.ResumeID(), qid)
	if _, _, handled, err := c.PrepareAsyncReply(id, nil, true); !handled || err == nil {
		t.Fatal("invented cancellation RPC")
	}
	if _, _, _, err := c.PrepareAsyncReply(id, nil, false); err == nil {
		t.Fatal("empty answer accepted")
	}
	_, content, _, _ := c.PrepareAsyncReply(id, map[string]any{qid: "answer"}, false)
	s.SetResumeID("different-thread")
	if _, err := c.validateAsyncReply(s, id, content); err == nil {
		t.Fatal("answer can drift to another thread")
	}
	if c.RespondUserInput(id, map[string]any{qid: "answer"}, false) {
		t.Fatal("native reply used legacy RPC path")
	}
}

func TestNativeAsyncHistoryRehydratesOnlyUnansweredCanonicalItems(t *testing.T) {
	c, s, sink := asyncFixture(t)
	s.SetResumeID("00000000-0000-4000-8000-000000000001")
	c.state(s.ID).threadID = s.ResumeID()
	qid := asyncQuestionID("call-fixture", 0)
	reply, _ := json.Marshal([]nativeQuestionReply{{Answer: "fixture", Question: "Choose a route", QuestionItemID: qid}})
	text := asyncReplyOpen + "\n" + string(reply) + "\n" + asyncReplyClose
	content, _ := json.Marshal([]map[string]string{{"type": "input_text", "text": text}})
	rows := `{"timestamp":"2026-10-01T00:00:00Z","type":"event_msg","payload":{"type":"item_completed","item":` + asyncItemJSON + `}}` + "\n" +
		`{"timestamp":"2026-10-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` + strings.Repeat("padding", 2048) + `"}]}}` + "\n" +
		`{"timestamp":"2026-10-01T00:00:01Z","type":"response_item","payload":{"type":"message","role":"user","content":` + string(content) + `}}` + "\n"
	path := filepath.Join(c.sessionsRoot, "rollout-2026-10-01T00-00-00-"+s.ResumeID()+".jsonl")
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "256")
	if err := os.WriteFile(path, []byte(rows), 0600); err != nil {
		t.Fatal(err)
	}
	c.ReconcileAsyncQuestions(s)
	c.ReconcileAsyncQuestions(s)
	pending := c.PendingInteractions(s.ID)
	if len(pending) != 1 || pending[0].Questions[0].QuestionID != asyncQuestionID("call-fixture", 1) {
		t.Fatalf("history pending: %+v", pending)
	}
	if sink.count(func(e any) bool { _, ok := e.(backend.UserInputRequest); return ok }) != 1 {
		t.Fatal("answered history briefly appeared as a prompt")
	}
	c.expireDisconnectedInteractions()
	if len(c.PendingInteractions(s.ID)) != 1 {
		t.Fatal("disconnect expired an ordinary async question")
	}
}

func TestNativeAsyncAnsweredBeforeQuestionAndRetiredTurnReplay(t *testing.T) {
	c, s, _ := asyncFixture(t)
	qid := asyncQuestionID("call-fixture", 0)
	data, _ := json.Marshal([]nativeQuestionReply{{Answer: "fixture", Question: "Choose a route", QuestionItemID: qid}})
	content, _ := json.Marshal([]map[string]string{{"type": "text", "text": asyncReplyOpen + "\n" + string(data) + "\n" + asyncReplyClose}})
	c.dispatch(json.RawMessage(`{"method":"item/completed","params":{"threadId":"native-thread","item":{"type":"userMessage","content":` + string(content) + `}}}`))
	st := c.state(s.ID)
	st.retiredTurns = []string{"old-turn"}
	c.dispatch(json.RawMessage(`{"method":"item/completed","params":{"threadId":"native-thread","turnId":"old-turn","item":` + asyncItemJSON + `}}`))
	if got := c.PendingInteractions(s.ID); len(got) != 1 || got[0].Questions[0].QuestionID != asyncQuestionID("call-fixture", 1) {
		t.Fatalf("out-of-order native replay: %+v", got)
	}
}

func TestNativeAsyncQuestionsDoNotRouteChildThreadAnswersToParent(t *testing.T) {
	c, s, _ := asyncFixture(t)
	c.threadToSession["child-thread"] = s
	c.dispatch(json.RawMessage(`{"method":"item/completed","params":{"threadId":"child-thread","item":` + asyncItemJSON + `}}`))
	if len(c.PendingInteractions(s.ID)) != 0 {
		t.Fatal("child question was exposed with an invalid parent reply route")
	}
}

func TestNativeAsyncReconciliationRoutesLiveQuestionsWithoutClaimingWriter(t *testing.T) {
	c, s, _ := asyncFixture(t)
	c.state(s.ID).threadID = ""
	delete(c.threadToSession, s.ResumeID())
	c.ReconcileAsyncQuestions(s)
	emitAsyncFixture(c, s)
	if len(c.PendingInteractions(s.ID)) != 2 {
		t.Fatal("live question from an unclaimed native conversation was lost")
	}
	if c.state(s.ID).threadID != "" || len(c.activeThreadOwner) != 0 || c.rpc.hasWriter() {
		t.Fatal("receiving a question claimed or resumed the native writer")
	}
}

func TestNativeAsyncStartAcknowledgementAndStaleThreadNeverFork(t *testing.T) {
	c, s, _ := asyncFixture(t)
	emitAsyncFixture(c, s)
	qid := asyncQuestionID("call-fixture", 0)
	id := asyncRequestID(s.ResumeID(), qid)
	_, content, _, _ := c.PrepareAsyncReply(id, map[string]any{qid: "fixture"}, false)
	st := c.state(s.ID)
	st.reqID = id
	st.turnActive = true
	st.turnStartPending = true
	st.turnDone = make(chan struct{})
	w := &toolTestWriter{c: c}
	w.reply = func(method string, _ json.RawMessage) (any, error) {
		if method != "turn/start" {
			t.Fatalf("unexpected fork/resume recovery: %s", method)
		}
		return nil, errors.New("thread not found")
	}
	c.rpc.setWriter(w)
	input := []map[string]any{{"type": "text", "text": content, "text_elements": []any{}}}
	if err := c.startTurnWithStaleRetry(s, st, s.ResumeID(), input, ""); err == nil {
		t.Fatal("stale native question was sent")
	}
	if len(w.methods) != 1 || len(c.PendingInteractions(s.ID)) != 2 {
		t.Fatal("stale reply was retried or falsely resolved")
	}
	w.reply = func(string, json.RawMessage) (any, error) { return map[string]any{}, nil }
	if err := c.startTurnWithStaleRetry(s, st, s.ResumeID(), input, ""); err == nil {
		t.Fatal("missing native turn identity treated as delivery")
	}
	if len(c.PendingInteractions(s.ID)) != 2 {
		t.Fatal("malformed ACK removed the question")
	}
	w.reply = func(string, json.RawMessage) (any, error) {
		return map[string]any{"turn": map[string]any{"id": "reply-turn", "status": "inProgress"}}, nil
	}
	if err := c.startTurnWithStaleRetry(s, st, s.ResumeID(), input, ""); err != nil {
		t.Fatal(err)
	}
	if len(c.PendingInteractions(s.ID)) != 1 {
		t.Fatal("accepted native start did not resolve replied question")
	}
}

func TestNativeAsyncCorruptResolutionJournalFailsClosedWithoutOverwrite(t *testing.T) {
	c, s, _ := asyncFixture(t)
	path := c.asyncTombstonePath(s.ResumeID())
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("invalid-fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	emitAsyncFixture(c, s)
	if len(c.PendingInteractions(s.ID)) != 0 {
		t.Fatal("unreadable answered-state resurrected questions")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "invalid-fixture" {
		t.Fatal("corrupt journal overwritten silently")
	}
}

func TestLegacyUserInputWriteFailureKeepsPending(t *testing.T) {
	c, s, _ := asyncFixture(t)
	c.handleServerRequest(1, "item/tool/requestUserInput", json.RawMessage(`{"threadId":"native-thread","itemId":"legacy","questions":[{"id":"q","question":"Pick"}]}`))
	c.rpc.setWriter(nil)
	if c.RespondUserInput("legacy", map[string]any{"q": "answer"}, false) {
		t.Fatal("reported success without a writer")
	}
	if len(c.PendingInteractions(s.ID)) != 1 {
		t.Fatal("write failure lost pending question")
	}
}
