package goexec

import (
	"bufio"
	"encoding/json"
	"everything-go/internal/history"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Explicitly local, private source snapshots. No model executor or credentials.
func TestOriginalNativeHistoryPrivateSnapshots(t *testing.T) {
	root := os.Getenv("HISTORY_ORIGINAL_PRIVATE_ROOT")
	if root == "" {
		t.Skip("requires exact private source snapshots")
	}
	paths, e := filepath.Glob(filepath.Join(root, "*.jsonl"))
	if e != nil || len(paths) != 2 {
		t.Fatal("two exact private snapshots required")
	}
	for _, path := range paths {
		thread := strings.TrimSuffix(filepath.Base(path), ".jsonl")
		thread = thread[len(thread)-36:]
		t.Run(thread, func(t *testing.T) {
			info, e := os.Stat(path)
			if e != nil {
				t.Fatal(e)
			}
			f, e := os.Open(path)
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			expected := map[string]map[string]any{}
			var fullLines []history.TailLine
			reader := bufio.NewReader(f)
			lineNo := 0
			for {
				line, e := reader.ReadBytes('\n')
				if len(line) > 0 {
					lineNo++
					ln := history.TailLine{LineNo: lineNo, Data: line}
					if info.Size() < 32<<20 {
						fullLines = append(fullLines, ln)
					} else {
						var row codexHistoryRow
						if json.Unmarshal(line, &row) == nil && row.Type == "response_item" {
							p := parseCodexHistoryPayload(row.Payload)
							if p.Type == "message" && (p.Role == "user" || p.Role == "assistant" && p.Phase != "commentary") {
								m, _, err := parseCodexHistoryLines([]history.TailLine{ln}, thread, false)
								if err != nil {
									t.Fatal(err)
								}
								for _, v := range m {
									expected[v["source_message_id"].(string)] = v
								}
							}
						}
					}
				}
				if e != nil {
					if e != io.EOF {
						t.Fatal(e)
					}
					break
				}
			}
			small := len(fullLines) > 0
			if small {
				all, _, err := parseCodexHistoryLines(fullLines, thread, false)
				if err != nil {
					t.Fatal(err)
				}
				for _, v := range all {
					expected[v["source_message_id"].(string)] = v
				}
				fullLines = nil
				t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "2097152")
			} else {
				t.Setenv("EVERYTHING_GO_HISTORY_LOAD_MAX_BYTES", "33554432")
			}
			c := NewCodex(&capSink{}, "disabled-original-read-only")
			c.sessionsRoot = root
			c.SetDataDir(t.TempDir())
			page, err := c.LoadHistory(thread, history.Opts{Scoped: true, Mode: "snapshot", Limit: 80, IncludeThinking: true})
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			matched := map[string]bool{}
			pages := 0
			total := 0
			for {
				pages++
				if pages > 10000 {
					t.Fatal("page no progress")
				}
				for _, m := range page.Messages {
					id := m["source_message_id"].(string)
					if seen[id] {
						t.Fatal("duplicate native source")
					}
					seen[id] = true
					total++
					if want, ok := expected[id]; ok {
						if m["content"] != want["content"] {
							t.Fatalf("original text changed at source %s", id)
						}
						if small && (m["content_hash"] != want["content_hash"] || m["source_turn_id"] != want["source_turn_id"]) {
							t.Fatalf("original group or ownership changed at %s", id)
						}
						matched[id] = true
					}
				}
				if !page.HasMoreBefore {
					break
				}
				if len(page.Messages) == 0 {
					t.Fatal("empty page before earliest")
				}
				cursor := page.Messages[0]["source_message_id"].(string)
				page, err = c.LoadHistory(thread, history.Opts{Scoped: true, Mode: "snapshot", Before: cursor, Limit: 80, IncludeThinking: true})
				if err != nil {
					t.Fatal(err)
				}
				if !page.KnownIDFound {
					t.Fatal("original cursor disappeared")
				}
			}
			if len(matched) != len(expected) {
				t.Fatalf("source gaps:matched=%d expected=%d total=%d", len(matched), len(expected), total)
			}
			if small && len(seen) != len(expected) {
				t.Fatal("unexpected group rows")
			}
			t.Logf("bytes=%d physical_lines=%d canonical_rows=%d reference_rows=%d pages=%d all_reference_rows_exact=true full_group_reference=%v", info.Size(), lineNo, total, len(expected), pages, small)
		})
	}
}
