package goexec

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/history"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeHistoryPagesCrossOriginalTailBudgetWithoutGaps(t *testing.T) {
	for _, kind := range []string{"codex", "codex-gzip", "claude"} {
		t.Run(kind, func(t *testing.T) {
			t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "4096")
			backend := kind
			if kind == "codex-gzip" {
				backend = "codex"
			}
			root := t.TempDir()
			thread := "00000000-0000-0000-0000-000000000981"
			var source strings.Builder
			const rows = 240
			for i := 1; i <= rows; i++ {
				text := fmt.Sprintf("message-%03d-%s", i, strings.Repeat("x", 140))
				if backend == "codex" {
					fmt.Fprintf(&source, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":%q}}\n", text)
				} else {
					fmt.Fprintf(&source, "{\"type\":\"user\",\"message\":{\"content\":%q}}\n", text)
				}
			}
			var provider history.Provider
			if backend == "codex" {
				p := filepath.Join(root, "rollout-2026-10-17T00-00-00-"+thread+".jsonl")
				if kind == "codex-gzip" {
					p += ".gz"
					f, e := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0600)
					if e != nil {
						t.Fatal(e)
					}
					z := gzip.NewWriter(f)
					z.Write([]byte(source.String()))
					z.Close()
					f.Close()
				} else {
					if err := os.WriteFile(p, []byte(source.String()), 0600); err != nil {
						t.Fatal(err)
					}
				}
				c := NewCodex(&capSink{}, "codex")
				c.sessionsRoot = root
				c.SetDataDir(t.TempDir())
				provider = c
			} else {
				dir := filepath.Join(root, "project")
				os.Mkdir(dir, 0700)
				os.WriteFile(filepath.Join(dir, thread+".jsonl"), []byte(source.String()), 0600)
				c := NewClaude(&capSink{}, "claude")
				c.projectsDir = root
				provider = c
			}
			page, err := provider.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 12, IncludeThinking: true})
			if err != nil {
				t.Fatal(err)
			}
			all := append([]map[string]any(nil), page.Messages...)
			for n := 0; page.HasMoreBefore && n < rows; n++ {
				if len(page.Messages) == 0 {
					t.Fatal("empty page falsely says more")
				}
				cursor := page.Messages[0]["source_message_id"].(string)
				page, err = provider.LoadHistory(thread, history.Opts{Mode: "snapshot", Before: cursor, Limit: 12, IncludeThinking: true})
				if err != nil {
					t.Fatal(err)
				}
				if !page.KnownIDFound {
					t.Fatalf("cursor fell outside tail budget: %s", cursor)
				}
				all = append(append([]map[string]any(nil), page.Messages...), all...)
			}
			if page.HasMoreBefore || len(all) != rows {
				t.Fatalf("incomplete history: got%d/%d more=%v", len(all), rows, page.HasMoreBefore)
			}
			for i, m := range all {
				want := fmt.Sprintf("%s:%s:line:%d", backend, thread, i+1)
				if m["source_message_id"] != want {
					t.Fatalf("row%d source=%v want%s", i, m["source_message_id"], want)
				}
			}
		})
	}
}

func TestCodexHistoryWindowPagesKeepCommentaryAndTurnOwnershipAtEdges(t *testing.T) {
	root := t.TempDir()
	thread := "00000000-0000-0000-0000-000000000982"
	var raw strings.Builder
	for turn := 1; turn <= 30; turn++ {
		fmt.Fprintf(&raw, "{\"type\":\"turn_context\",\"payload\":{\"turn_id\":\"turn-%d\"}}\n", turn)
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":\"human-%d\"}}\n", turn)
		for j := 1; j <= 3; j++ {
			fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":%q}}\n", fmt.Sprintf("progress-%d-%d %s", turn, j, strings.Repeat("x", 200+(turn%5)*400)))
		}
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"call-%d\",\"name\":\"exec_command\",\"arguments\":\"{}\"}}\n", turn)
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"call-%d\",\"output\":%q}}\n", turn, "tool-output"+strings.Repeat("o", 1100))
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":\"final-%d\"}}\n", turn)
		fmt.Fprintf(&raw, "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"turn-%d\"}}\n", turn)
	}
	path := filepath.Join(root, "rollout-2026-10-17T00-00-00-"+thread+".jsonl")
	os.WriteFile(path, []byte(raw.String()), 0600)
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	c.SetDataDir(t.TempDir())
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "1048576")
	full, err := c.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 1000, IncludeThinking: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Messages) != 90 {
		t.Fatalf("fixture must include30tool rows: got%d", len(full.Messages))
	}
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "4096")
	anchored, err := c.LoadHistory(thread, history.Opts{Mode: "continuation", KnownLast: full.Messages[1]["source_message_id"].(string), Limit: 4, IncludeThinking: true, Scoped: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(anchored.Messages) == 0 || anchored.Messages[0]["content_hash"] != full.Messages[1]["content_hash"] {
		t.Fatal("cursor tool/commentary body changed at lookahead edge")
	}
	page, err := c.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 4, IncludeThinking: true, Scoped: true})
	if err != nil {
		t.Fatal(err)
	}
	all := append([]map[string]any(nil), page.Messages...)
	for n := 0; page.HasMoreBefore && n < 100; n++ {
		if len(page.Messages) == 0 {
			t.Fatal("no progress")
		}
		page, err = c.LoadHistory(thread, history.Opts{Mode: "snapshot", Before: page.Messages[0]["source_message_id"].(string), Limit: 4, IncludeThinking: true, Scoped: true})
		if err != nil {
			t.Fatal(err)
		}
		if !page.KnownIDFound {
			t.Fatal("lost cursor")
		}
		all = append(append([]map[string]any(nil), page.Messages...), all...)
	}
	if len(all) != len(full.Messages) {
		t.Fatalf("count %d !=%d", len(all), len(full.Messages))
	}
	for i, m := range all {
		expected := full.Messages[i]
		if m["source_message_id"] != expected["source_message_id"] || m["content_hash"] != expected["content_hash"] || m["source_turn_id"] != expected["source_turn_id"] {
			t.Fatalf("row%d got source=%v hash=%v turn=%v;expected source=%v hash=%v turn=%v", i, m["source_message_id"], m["content_hash"], m["source_turn_id"], expected["source_message_id"], expected["content_hash"], expected["source_turn_id"])
		}
	}
}

func TestCodexGzipHistoryWindowPagesKeepCommentaryAndTurnOwnershipAtEdges(t *testing.T) {
	root := t.TempDir()
	thread := "00000000-0000-0000-0000-000000000982"
	var raw strings.Builder
	for turn := 1; turn <= 30; turn++ {
		fmt.Fprintf(&raw, "{\"type\":\"turn_context\",\"payload\":{\"turn_id\":\"turn-%d\"}}\n", turn)
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":\"human-%d\"}}\n", turn)
		for j := 1; j <= 3; j++ {
			fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":%q}}\n", fmt.Sprintf("progress-%d-%d %s", turn, j, strings.Repeat("x", 200+(turn%5)*400)))
		}
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"call-%d\",\"name\":\"exec_command\",\"arguments\":\"{}\"}}\n", turn)
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"call-%d\",\"output\":%q}}\n", turn, "tool-output"+strings.Repeat("o", 1100))
		fmt.Fprintf(&raw, "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":\"final-%d\"}}\n", turn)
		fmt.Fprintf(&raw, "{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"turn-%d\"}}\n", turn)
	}
	path := filepath.Join(root, "rollout-2026-10-17T00-00-00-"+thread+".jsonl")
	path += ".gz"
	f, e := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	z := gzip.NewWriter(f)
	z.Write([]byte(raw.String()))
	z.Close()
	f.Close()
	c := NewCodex(&capSink{}, "codex")
	c.sessionsRoot = root
	c.SetDataDir(t.TempDir())
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "1048576")
	full, err := c.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 1000, IncludeThinking: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Messages) != 90 {
		t.Fatalf("fixture must include30tool rows: got%d", len(full.Messages))
	}
	t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "4096")
	anchored, err := c.LoadHistory(thread, history.Opts{Mode: "continuation", KnownLast: full.Messages[1]["source_message_id"].(string), Limit: 4, IncludeThinking: true, Scoped: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(anchored.Messages) == 0 || anchored.Messages[0]["content_hash"] != full.Messages[1]["content_hash"] {
		t.Fatalf("cursor tool/commentary body changed at lookahead edge: got=%v expected=%v", anchored.Messages, full.Messages[1])
	}
	page, err := c.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 4, IncludeThinking: true, Scoped: true})
	if err != nil {
		t.Fatal(err)
	}
	all := append([]map[string]any(nil), page.Messages...)
	for n := 0; page.HasMoreBefore && n < 100; n++ {
		if len(page.Messages) == 0 {
			t.Fatal("no progress")
		}
		page, err = c.LoadHistory(thread, history.Opts{Mode: "snapshot", Before: page.Messages[0]["source_message_id"].(string), Limit: 4, IncludeThinking: true, Scoped: true})
		if err != nil {
			t.Fatal(err)
		}
		if !page.KnownIDFound {
			t.Fatal("lost cursor")
		}
		all = append(append([]map[string]any(nil), page.Messages...), all...)
	}
	if len(all) != len(full.Messages) {
		t.Fatalf("count %d !=%d", len(all), len(full.Messages))
	}
	for i, m := range all {
		expected := full.Messages[i]
		if m["source_message_id"] != expected["source_message_id"] || m["content_hash"] != expected["content_hash"] || m["source_turn_id"] != expected["source_turn_id"] {
			t.Fatalf("row%d got source=%v hash=%v turn=%v;expected source=%v hash=%v turn=%v", i, m["source_message_id"], m["content_hash"], m["source_turn_id"], expected["source_message_id"], expected["content_hash"], expected["source_turn_id"])
		}
	}
}

func TestScopedNativeHistoryDoesNotMintTransientOpenCommentaryAnchor(t *testing.T) {
	root := t.TempDir()
	thread := "00000000-0000-0000-0000-000000000983"
	path := filepath.Join(root, "rollout-2026-10-17T00-00-00-"+thread+".jsonl")
	open := "{\"type\":\"turn_context\",\"payload\":{\"turn_id\":\"active-turn\"}}\n" +
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":\"original question\"}}\n" +
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":\"unfinished progress\"}}\n"
	if e := os.WriteFile(path, []byte(open), 0600); e != nil {
		t.Fatal(e)
	}
	c := NewCodex(&capSink{}, "disabled-fixture")
	c.sessionsRoot = root
	c.SetDataDir(t.TempDir())
	initial, e := c.LoadHistory(thread, history.Opts{Scoped: true, Mode: "snapshot", Limit: 80, IncludeThinking: true})
	if e != nil {
		t.Fatal(e)
	}
	if len(initial.Messages) != 1 || initial.Messages[0]["source_message_id"] != "codex:"+thread+":line:2" {
		t.Fatal("Open commentary must remain live progress, not an ephemeral canonical history anchor")
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		t.Fatal(e)
	}
	f.WriteString("{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"one\",\"name\":\"exec_command\",\"arguments\":\"{}\"}}\n" +
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"one\",\"output\":\"done\"}}\n" +
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":\"one final\"}}\n" +
		"{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"active-turn\"}}\n")
	f.Close()
	page, e := c.LoadHistory(thread, history.Opts{Scoped: true, Mode: "continuation", KnownLast: initial.Messages[0]["source_message_id"].(string), Limit: 80, IncludeThinking: true})
	if e != nil {
		t.Fatal(e)
	}
	if !page.KnownIDFound || len(page.Messages) != 3 || page.Messages[2]["content"] != "one final" {
		t.Fatal("Original user anchor must survive open progress -> tool -> done without stale text or missing cursor")
	}
}

func TestScopedNativeHistoryRetainsClosedProgressWithoutInventingFinalAnswer(t *testing.T) {
	for _, kind := range []string{"task_complete", "turn_aborted"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			thread := "00000000-0000-0000-0000-000000000984"
			path := filepath.Join(root, "rollout-2026-10-17T00-00-00-"+thread+".jsonl")
			raw := "{\"type\":\"turn_context\",\"payload\":{\"turn_id\":\"closed-turn\"}}\n" +
				"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"commentary\",\"content\":\"retained closed progress\"}}\n" +
				fmt.Sprintf("{\"type\":\"event_msg\",\"payload\":{\"type\":%q,\"turn_id\":\"closed-turn\"}}\n", kind)
			os.WriteFile(path, []byte(raw), 0600)
			c := NewCodex(&capSink{}, "disabled-fixture")
			c.sessionsRoot = root
			c.SetDataDir(t.TempDir())
			result, e := c.LoadHistory(thread, history.Opts{Scoped: true, Mode: "snapshot", Limit: 80})
			if e != nil {
				t.Fatal(e)
			}
			if len(result.Messages) != 1 || result.Messages[0]["content"] != "retained closed progress" || result.Messages[0]["history_read_result_verified"] != false {
				t.Fatal("Exact closed progress must be retained without claiming a final answer")
			}
		})
	}
}

func TestScopedHistoryDefersMutableParallelToolsUntilOriginalRequestCompletes(t *testing.T) {
	root := t.TempDir()
	data := t.TempDir()
	thread := "00000000-0000-0000-0000-000000000985"
	path := filepath.Join(root, "rollout-2026-10-17T00-00-00-"+thread+".jsonl")
	raw := "{\"type\":\"turn_context\",\"payload\":{\"turn_id\":\"parallel-turn\"}}\n" +
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":\"original parallel question\"}}\n" +
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"first\",\"name\":\"exec_command\",\"arguments\":\"{}\"}}\n" +
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\",\"call_id\":\"second\",\"name\":\"exec_command\",\"arguments\":\"{}\"}}\n"
	os.WriteFile(path, []byte(raw), 0600)
	dir := filepath.Join(data, "codex-turn-requests")
	os.MkdirAll(dir, 0700)
	sum := sha256.Sum256([]byte(thread))
	bytes, _ := json.Marshal(map[string]any{"version": 1, "thread_id": thread, "requests": map[string]string{"parallel-turn": "r_parallel_original"}})
	os.WriteFile(filepath.Join(dir, hex.EncodeToString(sum[:])+".json"), bytes, 0600)
	c := NewCodex(&capSink{}, "disabled-fixture")
	c.sessionsRoot = root
	c.SetDataDir(data)
	initial, e := c.LoadHistory(thread, history.Opts{Scoped: true, Mode: "snapshot", Limit: 80, DeferredAssistantRequestID: "r_parallel_original"})
	if e != nil {
		t.Fatal(e)
	}
	if len(initial.Messages) != 1 || initial.Messages[0]["role"] != "user" {
		t.Fatal("Mutable parallel tool rows must remain live, not frozen into an older canonical prefix")
	}
	legacy, e := c.LoadHistory(thread, history.Opts{Mode: "snapshot", Limit: 80, DeferredAssistantRequestID: "r_parallel_original"})
	if e != nil || len(legacy.Messages) != 3 {
		t.Fatal("Legacy reads must preserve their original snapshot behavior")
	}
	unrelated, e := c.LoadHistory(thread, history.Opts{Scoped: true, Mode: "snapshot", Limit: 80, DeferredAssistantRequestID: "r_other"})
	if e != nil || len(unrelated.Messages) != 3 {
		t.Fatal("Only the exact active request may be deferred")
	}
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	f.WriteString("{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"first\",\"output\":\"first completed output\"}}\n" +
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"call_id\":\"second\",\"output\":\"second completed output\"}}\n" +
		"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":\"one parallel final\"}}\n" +
		"{\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"parallel-turn\"}}\n")
	f.Close()
	finished, e := c.LoadHistory(thread, history.Opts{Scoped: true, Mode: "continuation", KnownLast: initial.Messages[0]["source_message_id"].(string), Limit: 80})
	if e != nil {
		t.Fatal(e)
	}
	if !finished.KnownIDFound || len(finished.Messages) != 4 {
		t.Fatal("Stable user anchor must include both completed tools and one final")
	}
	for i, want := range []string{"first completed output", "second completed output"} {
		blocks := finished.Messages[i+1]["blocks"].([]map[string]any)
		if blocks[len(blocks)-1]["output"] != want {
			t.Fatalf("Earlier parallel tool%d output was left stale", i)
		}
	}
}
