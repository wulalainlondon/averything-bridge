package goexec

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"everything-go/internal/history"
)

func TestCodexHistoryRequestIdentitySurvivesCacheAndRestart(t *testing.T) {
	root, data := t.TempDir(), t.TempDir()
	thread := "00000000-0000-0000-0000-000000000123"
	path := filepath.Join(root, "rollout-2026-09-15T00-00-00-"+thread+".jsonl")
	rows := []string{
		`{"type":"response_item","payload":{"type":"message","role":"assistant","content":"legacy"}}`,
		`{"type":"event_msg","payload":{"type":"task_started","turn_id":"native-C"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"user","content":"same prompt"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","content":"same answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"native-C"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","content":"unbound"}}`,
		`{"type":"turn_context","payload":{"turn_id":"native-D"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"user","content":"same prompt"}}`,
		`{"type":"response_item","payload":{"type":"message","role":"assistant","content":"same answer"}}`,
	}
	if err := os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	newExecutor := func() *Codex { c := NewCodex(&capSink{}, "codex"); c.sessionsRoot = root; c.SetDataDir(data); return c }
	c := newExecutor()
	opts := history.Opts{Mode: "snapshot", Limit: 20}
	before, err := c.LoadHistory(thread, opts)
	if err != nil || len(before.Messages) != 6 {
		t.Fatalf("initial history: %v %+v", err, before)
	}
	if err := c.rememberTurnRequest(thread, "native-C", "bridge-C"); err != nil {
		t.Fatal(err)
	}
	if err := c.rememberTurnRequest(thread, "native-D", "bridge-D"); err != nil {
		t.Fatal(err)
	}
	for _, executor := range []*Codex{c, newExecutor()} {
		got, err := executor.LoadHistory(thread, opts)
		if err != nil {
			t.Fatal(err)
		}
		for i, want := range []string{"", "bridge-C", "bridge-C", "", "bridge-D", "bridge-D"} {
			actual, _ := got.Messages[i]["request_id"].(string)
			if actual != want {
				t.Fatalf("row %d request %q != %q", i, actual, want)
			}
		}
	}
	// Attaching identity must not mutate the parsed cache or a previously returned batch.
	if _, ok := before.Messages[2]["request_id"]; ok {
		t.Fatal("mutated cached message")
	}
	if err := os.WriteFile(c.turnRequestsPath(thread), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	after, _ := c.LoadHistory(thread, opts)
	if _, ok := after.Messages[2]["request_id"]; ok {
		t.Fatal("corrupt binding must not invent identity")
	}
}

func TestCodexTurnRequestBindingConflictAndAuthority(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.SetDataDir(t.TempDir())
	if err := c.rememberTurnRequest("thread-1", "turn", "request-1"); err != nil {
		t.Fatal(err)
	}
	if err := c.rememberTurnRequest("thread-1", "turn", "request-1"); err != nil {
		t.Fatal(err)
	}
	if err := c.rememberTurnRequest("thread-1", "turn", "request-2"); err == nil {
		t.Fatal("accepted conflicting identity")
	}
	if err := c.rememberTurnRequest("thread-2", "turn", "request-2"); err != nil {
		t.Fatal(err)
	}
	r, err := c.loadTurnRequests("thread-1")
	if err != nil || r.Requests["turn"] != "request-1" {
		t.Fatalf("lost original binding: %v %+v", err, r)
	}
}

func TestAcceptedJoinCannotOverwriteAnotherBridgeOrExternalTurn(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.SetDataDir(t.TempDir())
	for _, previous := range []string{"real-bridge-request", "codex_external_other-turn"} {
		thread := "thread-" + previous
		if err := c.rememberTurnRequest(thread, "turn", previous); err != nil {
			t.Fatal(err)
		}
		if err := c.rememberAcceptedTurnRequest(thread, "turn", "new-bridge-request"); err == nil {
			t.Fatal("accepted join overwrote foreign identity")
		}
		journal, err := c.loadTurnRequests(thread)
		if err != nil || journal.Requests["turn"] != previous {
			t.Fatal("binding changed after conflict")
		}
	}
}
