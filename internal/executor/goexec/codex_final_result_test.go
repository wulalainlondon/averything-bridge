package goexec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func TestFinalAnswerForRequestExcludesCommentaryAndOtherTurns(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.dataDir = t.TempDir()
	c.sessionsRoot = t.TempDir()
	threadID := "01a0ec71-65c0-7c71-aad2-1336d7251333"
	if err := c.rememberTurnRequest(threadID, "turn-one", "request-one"); err != nil {
		t.Fatal(err)
	}
	if err := c.rememberTurnRequest(threadID, "turn-two", "request-two"); err != nil {
		t.Fatal(err)
	}
	rows := []string{
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-one"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":[{"type":"output_text","text":"Progress only"}]}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"partial_answer","content":[{"type":"output_text","text":"Partial answer is not the native final"}]}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"Verified result"}]}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-one"}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"turn-two"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"Later answer"}]}}`,
	}
	path := filepath.Join(c.sessionsRoot, "rollout-2026-09-29T17-14-09-"+threadID+".jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s := session.NewRegistry().Create("child", "Child", t.TempDir(), backend.Codex, "", "", threadID)
	got, found, err := c.FinalAnswerForRequest(s, "request-one")
	if err != nil || !found || got != "Verified result" {
		t.Fatalf("answer=%q found=%v err=%v", got, found, err)
	}
	got, found, err = c.FinalAnswerForRequest(s, "request-two")
	if err != nil || !found || got != "Later answer" {
		t.Fatalf("second answer=%q found=%v err=%v", got, found, err)
	}
}

func TestExactQueueFinalPinsOriginalThreadRejectsAmbiguousMappingAndNeverDiscoversFiles(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.dataDir = t.TempDir()
	c.sessionsRoot = t.TempDir()
	thread := "01a0ec71-65c0-7c71-aad2-1336d7251333"
	if err := c.rememberTurnRequest(thread, "exact-turn", "original-request"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(c.sessionsRoot, "rollout-2026-09-29T17-14-09-"+thread+".jsonl")
	data := fixtureHeader() + `{"type":"event_msg","payload":{"type":"task_started","turn_id":"exact-turn"}}` + "\n" + `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"Exact original final"}]}}` + "\n" + `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"exact-turn","last_agent_message":"Exact original final"}}` + "\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	if found, err := c.ExactQueueFinal(thread, "original-request", "exact-turn"); found || err == nil {
		t.Fatal("unindexed runtime file discovered")
	}
	// Test-only provider index fixture; no public/runtime discovery operation.
	c.rolloutRoot = c.sessionsRoot
	c.rolloutByID = map[string]string{thread: path}
	if found, err := c.ExactQueueFinal(thread, "original-request", "exact-turn"); !found || err != nil {
		t.Fatal(found, err)
	}
	if found, err := c.ExactQueueFinal(thread, "original-request", "wrong-turn"); found || err == nil {
		t.Fatal("wrong original native turn accepted")
	}
	if err := c.rememberTurnRequest(thread, "duplicate-turn", "original-request"); err != nil {
		t.Fatal(err)
	}
	if found, err := c.ExactQueueFinal(thread, "original-request", "exact-turn"); found || err == nil {
		t.Fatal("ambiguous map accepted")
	}
	s := session.NewRegistry().Create("s", "Task", t.TempDir(), backend.Codex, "", "", thread)
	if turn, err := c.NativeTurnForRequest(s, "original-request"); turn != "" || err == nil {
		t.Fatal("ambiguous native consumption accepted")
	}
}
