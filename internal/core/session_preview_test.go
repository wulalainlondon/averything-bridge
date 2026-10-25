package core

import (
	"encoding/json"
	"os"
	"testing"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/search"
	"everything-go/internal/session"
)

type previewHistoryProvider struct{ messages []map[string]any }

func (p *previewHistoryProvider) LoadHistory(string, history.Opts) (*history.Result, error) {
	return &history.Result{Kind: "snapshot", Messages: p.messages, SourceCount: len(p.messages), KnownIDFound: true}, nil
}
func (p *previewHistoryProvider) ResumableSessions(int) ([]history.ResumableSession, error) {
	return nil, nil
}

type previewHistoryExec struct {
	fakeExec
	provider *previewHistoryProvider
}

func (e *previewHistoryExec) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.provider, true
}
func (e *previewHistoryExec) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.provider}
}

func TestSessionPreviewSharedVectors(t *testing.T) {
	data, err := os.ReadFile("../../contracts/v3/normalization_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		Previews []struct {
			Phase             string `json:"phase"`
			User              string `json:"user"`
			Assistant         string `json:"assistant"`
			TerminalAssistant string `json:"terminal_assistant"`
			ExpectedRole      string `json:"expected_role"`
			Expected          string `json:"expected"`
		} `json:"previews"`
	}
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	for _, vector := range vectors.Previews {
		messages := []protocol.RecentMessage{{Role: "user", Text: vector.User}}
		if vector.Assistant != "" {
			messages = append(messages, protocol.RecentMessage{Role: "assistant", Text: vector.Assistant})
		}
		if vector.TerminalAssistant != "" && vector.TerminalAssistant != vector.Assistant {
			messages = append(messages, protocol.RecentMessage{Role: "assistant", Text: vector.TerminalAssistant})
		}
		text, role := selectSessionPreview(messages, vector.Phase)
		if text != vector.Expected || role != vector.ExpectedRole {
			t.Fatalf("phase=%s got=(%s,%s) want=(%s,%s)", vector.Phase, role, text, vector.ExpectedRole, vector.Expected)
		}
	}
}

func TestTruncateGraphemesDoesNotSplitEmoji(t *testing.T) {
	if got := truncateGraphemes("A👨‍👩‍👧‍👦B", 2); got != "A👨‍👩‍👧‍👦" {
		t.Fatalf("got %q", got)
	}
}

func TestPersistedPreviewBeatsOlderSearchProjection(t *testing.T) {
	messages := []protocol.RecentMessage{{Role: "assistant", Text: "stale indexed result"}}
	indexed := &search.SessionPreview{LastTS: 100, LastAssistantTS: 100}
	text, role, at := resolveSessionPreview("fresh durable result", "assistant", 200_000, messages, indexed, "idle")
	if text != "fresh durable result" || role != "assistant" || at != 200_000 {
		t.Fatalf("older index replaced durable projection: (%q,%q,%d)", text, role, at)
	}
}

func TestNewerExternalCLIProjectionSupersedesPersistedPreview(t *testing.T) {
	messages := []protocol.RecentMessage{{Role: "assistant", Text: "new external result"}}
	indexed := &search.SessionPreview{LastTS: 300, LastAssistantTS: 300}
	text, role, at := resolveSessionPreview("old durable result", "assistant", 200_000, messages, indexed, "idle")
	if text != "new external result" || role != "assistant" || at != 300_000 {
		t.Fatalf("newer external result was ignored: (%q,%q,%d)", text, role, at)
	}
}

func TestHistoryAssistantPreviewRejectsToolCommandButKeepsCommentary(t *testing.T) {
	toolOnly := map[string]any{
		"role": "assistant", "content": "const r = await tools.exec_command(...)",
		"blocks": []map[string]any{{"type": "tool", "name": "exec", "command": "const r = await tools.exec_command(...)"}},
	}
	if got := historyAssistantPreview(toolOnly); got != "" {
		t.Fatalf("tool command leaked into row preview: %q", got)
	}
	withCommentary := map[string]any{
		"role": "assistant", "content": "const r = await tools.exec_command(...)",
		"blocks": []any{
			map[string]any{"type": "text", "text": "正在驗證正式簽名。"},
			map[string]any{"type": "tool", "name": "exec"},
		},
	}
	if got := historyAssistantPreview(withCommentary); got != "正在驗證正式簽名。" {
		t.Fatalf("human commentary was not preserved: %q", got)
	}
}

func TestHistoryReconciliationRepairsPersistedToolProjection(t *testing.T) {
	h, _ := newTestHub(t)
	provider := &previewHistoryProvider{messages: []map[string]any{
		{
			"role": "assistant", "content": "human-facing update", "timestamp": int64(2_000),
			"blocks": []map[string]any{{"type": "text", "text": "human-facing update"}},
		},
		{
			"role": "assistant", "content": "const r = await tools.exec_command(...)", "timestamp": int64(3_000),
			"blocks": []map[string]any{{"type": "tool", "name": "exec"}},
		},
	}}
	h.SetExecutor(&previewHistoryExec{fakeExec: fakeExec{sink: h}, provider: provider})
	s := h.registry.Create("s1", "Work", "/work", "codex", "", "", "thread-1")
	if _, changed, err := h.registry.CommitPreviewAndPersist(s.ID, "const r = await tools.exec_command(...)", "assistant", 3_000); err != nil || !changed {
		t.Fatalf("seed tool projection: changed=%v err=%v", changed, err)
	}
	c := newTestClient(h)
	h.sendHistory(c, s, clientproto.Command{SessionID: s.ID, Mode: "snapshot"})
	waitForType(t, c, "history_snapshot")
	list := waitForType(t, c, "sessions_list")

	snap := s.Snapshot()
	if snap.PreviewText != "human-facing update" || snap.PreviewUpdatedAt != 2_000 || snap.PreviewRevision != 2 {
		t.Fatalf("history did not self-heal tool projection: %+v", snap)
	}
	sessions, _ := list["sessions"].([]any)
	if len(sessions) != 1 || sessions[0].(map[string]any)["preview_text"] != "human-facing update" {
		t.Fatalf("repaired projection was not broadcast immediately: %+v", list)
	}
}
