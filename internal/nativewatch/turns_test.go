package nativewatch

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func nativeLifecycleLine(kind, turn string) string {
	return fmt.Sprintf("{\"type\":\"event_msg\",\"payload\":{\"type\":%q,\"turn_id\":%q}}\n", kind, turn)
}

func TestNativeTurnWatcherColdStartAndTerminal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	write := func(text string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	text := nativeLifecycleLine("task_complete", "old")
	write(text)
	w := NewTurnWatcher()
	w.Track(NativeSession{ID: "s1", ResumeID: "root", Backend: BackendCodex, Path: path})
	var events []TurnActivity
	emit := func(e TurnActivity) { events = append(events, e) }
	w.poll(emit)
	if len(events) != 0 {
		t.Fatal("startup replayed an old completion")
	}
	text += nativeLifecycleLine("task_started", "new")
	write(text)
	w.poll(emit)
	w.poll(emit)
	if len(events) != 1 || events[0].Phase != "running" || events[0].TurnID != "new" {
		t.Fatalf("start: %+v", events)
	}
	// Recreating the observer in the middle of the same turn must recover it
	// without connecting to, resuming, or claiming the Codex thread.
	cold := NewTurnWatcher()
	cold.Track(NativeSession{ID: "s1", ResumeID: "root", Backend: BackendCodex, Path: path})
	cold.poll(emit)
	if len(events) != 2 || events[1].Phase != "running" {
		t.Fatal("cold-start active turn not recovered")
	}
	text += nativeLifecycleLine("task_complete", "new")
	write(text)
	w.poll(emit)
	if len(events) != 3 || events[2].Phase != "completed" {
		t.Fatalf("terminal: %+v", events)
	}
}

func TestLatestNativeTurnSkipsToolPayloadAndPartialRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	start := nativeLifecycleLine("task_started", "active")
	large := `{"type":"response_item","payload":{"text":"` + strings.Repeat("x", 1024*1024) + `"}}` + "\n"
	partial := strings.TrimSuffix(nativeLifecycleLine("task_complete", "active"), "\n")
	text := start + large + partial
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	turn, phase := latestNativeTurn(path, int64(len(text)))
	if turn != "active" || phase != "running" {
		t.Fatalf("partial terminal accepted: %q %q", turn, phase)
	}
	text += "\n"
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	turn, phase = latestNativeTurn(path, int64(len(text)))
	if turn != "active" || phase != "completed" {
		t.Fatalf("large tool hid terminal: %q %q", turn, phase)
	}
}

func TestNativeTurnWatcherDoesNotLoseFastTerminalBetweenPolls(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	text := nativeLifecycleLine("task_complete", "old")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	w := NewTurnWatcher()
	w.Track(NativeSession{ID: "s1", ResumeID: "root", Backend: BackendCodex, Path: path})
	var events []TurnActivity
	emit := func(e TurnActivity) { events = append(events, e) }
	w.poll(emit)
	text += nativeLifecycleLine("task_started", "fast") + nativeLifecycleLine("task_complete", "fast")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	w.poll(emit)
	w.poll(emit)
	if len(events) != 1 || events[0].TurnID != "fast" || events[0].Phase != "completed" {
		t.Fatalf("missed/duplicated committed terminal: %+v", events)
	}
}

func TestNativeTaskCompleteWithErrorIsNeverSuccessful(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	text := nativeLifecycleLine("task_started", "failed") + `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"failed","last_agent_message":null,"error":{"message":"model unavailable","codex_error_info":"server_overloaded"}}}` + "\n"
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	turn, phase := latestNativeTurn(path, int64(len(text)))
	if turn != "failed" || phase != "failed" {
		t.Fatalf("native error became success: %s/%s", turn, phase)
	}
}
