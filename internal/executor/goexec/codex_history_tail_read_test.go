package goexec

import (
	"everything-go/internal/history"
	"everything-go/internal/runtimejournal"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func tailLines(rows ...string) []history.TailLine {
	out := make([]history.TailLine, len(rows))
	for i, row := range rows {
		out[i] = history.TailLine{LineNo: 5700 + i, Data: []byte(row)}
	}
	return out
}

const tailFinal = `{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","content":"final tail"}}`
const tailComplete = `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"original-native-turn"}}`

func TestTruncatedCodexFinalRecoversExactClosingTurnForSharedRead(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.SetDataDir(t.TempDir())
	if err := c.rememberTurnRequest("thread", "original-native-turn", "original-request"); err != nil {
		t.Fatal(err)
	}
	lines := tailLines(`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":"progress"}}`, tailFinal, tailComplete)
	raw, truncated, err := parseCodexHistoryLines(lines, "thread", true)
	if err != nil || !truncated {
		t.Fatal(err)
	}
	got := c.withHistoryRequestIDs(raw, "thread")
	if len(got) != 1 {
		t.Fatalf("rows=%d", len(got))
	}
	if got[0]["source_turn_id"] != "original-native-turn" || got[0]["request_id"] != "original-request" {
		t.Fatalf("missing exact closing identity: turn=%v request=%v", got[0]["source_turn_id"], got[0]["request_id"])
	}
	if got[0]["source_message_id"] != "codex:thread:line:5701" || got[0]["history_read_result_verified"] != true {
		t.Fatal("changed native final identity")
	}
	store := runtimejournal.New(t.TempDir())
	store.Update("session", "running", "original-request", 0, "", "")
	done, _ := store.Update("session", "completed", "original-request", 0, "completed", "")
	views, err := store.SharedSnapshot("device", []string{"session"}, []string{"device"})
	if err != nil || len(views) != 1 {
		t.Fatal(err)
	}
	if !store.HistoryMatchesReadBoundary("session", views[0].ReadEpoch, done.Revision, got) {
		t.Fatal("actual shared-read boundary still rejects the exact persisted request")
	}
	// Recovery supplies only the exact native identity; an absent journal cannot fabricate a request.
	if _, ok := raw[0]["request_id"]; ok {
		t.Fatal("mutated parsed cache")
	}
	other := c.withHistoryRequestIDs(raw, "different-thread")
	if _, ok := other[0]["request_id"]; ok {
		t.Fatal("borrowed request from another native thread")
	}
}

func TestTruncatedClosingTurnNeverClaimsAmbiguousOrNonFinalHistory(t *testing.T) {
	tests := []struct {
		name      string
		truncated bool
		rows      []string
	}{
		{"untruncated legacy", false, []string{tailFinal, tailComplete}},
		{"aborted", true, []string{tailFinal, `{"type":"event_msg","payload":{"type":"turn_aborted","turn_id":"original-native-turn"}}`}},
		{"empty closing id", true, []string{tailFinal, `{"type":"event_msg","payload":{"type":"task_complete"}}`}},
		{"explicit unknown start", true, []string{`{"type":"event_msg","payload":{"type":"task_started"}}`, tailFinal, tailComplete}},
		{"after earlier terminal", true, []string{`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"old"}}`, tailFinal, tailComplete}},
		{"progress only", true, []string{`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","content":"progress"}}`, tailComplete}},
		{"ambiguous finals", true, []string{tailFinal, tailFinal, tailComplete}},
		{"tool after final", true, []string{tailFinal, `{"type":"response_item","payload":{"type":"function_call","call_id":"t","name":"exec","arguments":"{}"}}`, tailComplete}},
		{"error completion", true, []string{tailFinal, `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"original-native-turn","error":{"message":"error"}}}`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _, err := parseCodexHistoryLines(tailLines(tt.rows...), "thread", tt.truncated)
			if err != nil {
				t.Fatal(err)
			}
			for _, m := range got {
				if m["source_turn_id"] != nil {
					t.Fatal("claimed unsupported turn")
				}
			}
		})
	}
}

func TestClosingRecoveryLeavesKnownTurnAndProgressUntouched(t *testing.T) {
	rows := tailLines(`{"type":"event_msg","payload":{"type":"task_started","turn_id":"known-turn"}}`, tailFinal, tailComplete)
	got, _, err := parseCodexHistoryLines(rows, "thread", true)
	if err != nil || len(got) != 1 {
		t.Fatal(err)
	}
	if got[0]["source_turn_id"] != "known-turn" {
		t.Fatal("closing marker overwrote a known opening")
	}
}

func TestBoundedFileTailStillProducesAnExactSharedReadIdentity(t *testing.T) {
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "4096")
	c := NewCodex(&capSink{}, "codex")
	c.SetDataDir(t.TempDir())
	if err := c.rememberTurnRequest("thread", "original-native-turn", "original-request"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "rollout.jsonl")
	rows := []string{`{"type":"event_msg","payload":{"type":"task_started","turn_id":"original-native-turn"}}`, `{"type":"response_item","payload":{"type":"message","role":"user","content":"` + strings.Repeat("x", 8192) + `"}}`, tailFinal, tailComplete}
	if err := os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	messages, truncated, err := c.readCodexHistory(path, "thread")
	if err != nil || !truncated {
		t.Fatalf("not bounded: %v truncated=%v", err, truncated)
	}
	got := c.withHistoryRequestIDs(messages, "thread")
	if len(got) != 1 || got[0]["request_id"] != "original-request" || got[0]["source_message_id"] != "codex:thread:line:3" {
		t.Fatalf("tail identity missing: %+v", got)
	}
}

func TestClosingTurnFixDoesNotReusePreFixPersistedCache(t *testing.T) {
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "4096")
	root := t.TempDir()
	thread := "00000000-0000-0000-0000-000000000124"
	path := filepath.Join(root, "rollout-2026-10-13T00-00-00-"+thread+".jsonl")
	rows := []string{`{"type":"event_msg","payload":{"type":"task_started","turn_id":"original-native-turn"}}`, `{"type":"response_item","payload":{"type":"message","role":"user","content":"` + strings.Repeat("x", 8192) + `"}}`, tailFinal, tailComplete}
	if err := os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	key := history.FileKey{Path: path, MtimeNS: fi.ModTime().UnixNano(), Size: fi.Size()}
	history.DefaultCache().Save("codex:turns-v2:"+thread, key, []map[string]any{{"role": "assistant", "content": "final tail", "source_message_id": "codex:" + thread + ":line:3", "history_read_result_verified": true}})
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	c.SetDataDir(t.TempDir())
	if err := c.rememberTurnRequest(thread, "original-native-turn", "original-request"); err != nil {
		t.Fatal(err)
	}
	got, err := c.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 20})
	if err != nil || len(got.Messages) != 1 || got.Messages[0]["request_id"] != "original-request" {
		t.Fatalf("reused old persisted cache without closing identity: error=%v result=%+v", err, got)
	}
}
