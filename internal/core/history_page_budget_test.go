package core

import (
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"

	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
)

func TestOrdinaryHistoryBudgetPreservesDirectionAndCachedResult(t *testing.T) {
	rows := []map[string]any{}
	for i := 0; i < 4; i++ {
		rows = append(rows, map[string]any{"source_message_id": string(rune('a' + i)), "content": strings.Repeat("x", 300<<10)})
	}
	for _, tc := range []struct {
		mode, kind, first, last string
		count                   int
		before, after           bool
	}{
		{"snapshot", "snapshot", "d", "d", 1, true, false},
		{"delta", "delta", "d", "d", 1, true, false},
		{"beginning", "snapshot", "a", "a", 1, false, true},
		{"continuation", "snapshot", "a", "b", 2, false, true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			input := &history.Result{Kind: tc.kind, Messages: rows, SourceCount: 40, KnownIDFound: true}
			before, _ := json.Marshal(input)
			result, err := boundOrdinaryHistoryFrame(input, clientproto.Command{Mode: tc.mode})
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Messages) != tc.count || result.Messages[0]["source_message_id"] != tc.first || result.Messages[tc.count-1]["source_message_id"] != tc.last || result.HasMoreBefore != tc.before || result.HasMoreAfter != tc.after {
				t.Fatalf("wrong page boundary for %s", tc.mode)
			}
			if result.SourceCount != 40 || !result.KnownIDFound {
				t.Fatal("source metadata lost")
			}
			if tc.kind == "delta" && (result.Kind != "snapshot" || result.SnapshotReason != "byte_budget") {
				t.Fatal("shortened delta hid the gap")
			}
			after, _ := json.Marshal(input)
			if string(before) != string(after) {
				t.Fatal("cached provider result mutated")
			}
		})
	}
}

func TestOrdinaryHistoryPagesBoundWireAndPreserveEveryOriginalRow(t *testing.T) {
	h, _ := newTestHub(t)
	rows := []map[string]any{}
	for i := 1; i <= 4; i++ {
		row := scopedHistoryRow("physical", i)
		row["blocks"] = []map[string]any{{"type": "tool_call", "tool_use_id": string(rune('a' + i)), "name": "exec", "output": strings.Repeat("x", 5<<20)}}
		rows = append(rows, row)
	}
	rows = append(rows, scopedHistoryRow("physical", 5))
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": rows}}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	s := h.registry.Create("ordinary-large", "Synthetic", "/tmp", "codex", "", "read-only", "")
	s.SetResumeID("physical")
	c := newDeviceClient(h, "ordinary-large-device", 16)
	h.registerLatest(c)
	cmd := clientproto.NewAppV1().ParseCommand(protocol.Inbound{Type: "request_history", SessionID: s.ID, Mode: "snapshot"})
	got := map[string][32]byte{}
	for page := 0; page < 10; page++ {
		h.sendHistory(c, s, cmd)
		frame := accessWire(t, c)
		raw, err := json.Marshal(frame)
		if err != nil {
			t.Fatal(err)
		}
		if frame["type"] != "history_snapshot" {
			t.Fatalf("history page failed: %v", frame["type"])
		}
		if len(raw) >= 13<<20 {
			t.Fatalf("ordinary page would break native transport: %d bytes", len(raw))
		}
		messages := frame["messages"].([]any)
		if page == 0 && len(raw) > 1<<20 {
			t.Fatalf("initial tail did not stay small: %d bytes", len(raw))
		}
		if len(messages) == 0 {
			t.Fatal("empty page cannot advance")
		}
		for _, value := range messages {
			m := value.(map[string]any)
			id := m["source_message_id"].(string)
			b, _ := json.Marshal(m)
			if _, exists := got[id]; exists {
				t.Fatal("duplicate page row", id)
			}
			got[id] = sha256.Sum256(b)
		}
		if frame["has_more_before"] != true {
			break
		}
		cmd.Before = messages[0].(map[string]any)["source_message_id"].(string)
	}
	if len(got) != len(rows) {
		t.Fatalf("lost rows: %d of %d", len(got), len(rows))
	}
	for _, row := range rows {
		b, _ := json.Marshal(row)
		if got[row["source_message_id"].(string)] != sha256.Sum256(b) {
			t.Fatal("original body changed")
		}
	}
}

func TestOversizedSingleHistoryRowReturnsReadErrorWithoutClosingControl(t *testing.T) {
	h, _ := newTestHub(t)
	row := scopedHistoryRow("physical", 1)
	row["blocks"] = []map[string]any{{"type": "tool_call", "output": strings.Repeat("x", 18<<20)}}
	p := &historyRangeFixtureProvider{byID: map[string][]map[string]any{"physical": {row}}}
	h.SetExecutor(&historyRangeFixtureExec{fakeExec: fakeExec{sink: h}, provider: p})
	s := h.registry.Create("oversized-row", "Synthetic", "/tmp", "codex", "", "read-only", "")
	s.SetResumeID("physical")
	c := newDeviceClient(h, "oversized-row-device", 16)
	h.registerLatest(c)
	cmd := clientproto.NewAppV1().ParseCommand(protocol.Inbound{Type: "request_history", SessionID: s.ID, Mode: "snapshot"})
	h.sendHistory(c, s, cmd)
	frame := accessWire(t, c)
	if frame["type"] != "error" || frame["code"] != "history_frame_too_large" || frame["command_type"] != "request_history" {
		t.Fatalf("oversize must be a history-only failure: type=%v code=%v command=%v", frame["type"], frame["code"], frame["command_type"])
	}
	if !c.live() {
		t.Fatal("history error closed the original control connection")
	}
	c.enqueuePong()
	if accessWire(t, c)["type"] != "pong" {
		t.Fatal("control channel did not survive")
	}
	if len(row["blocks"].([]map[string]any)[0]["output"].(string)) != 18<<20 {
		t.Fatal("original tool output truncated")
	}
}
