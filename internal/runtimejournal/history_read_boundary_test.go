package runtimejournal

import (
	"testing"
	"time"
)

func TestReadBoundaryRequiresActualTargetNativeReply(t *testing.T) {
	s := New(t.TempDir())
	s.Update("s1", "running", "r1", 0, "", "")
	s.Update("s1", "completed", "r1", 0, "completed", "")
	view := sharedView(t, s, "phone")
	s.Update("s1", "running", "r2", 0, "", "")
	latest, _ := s.Update("s1", "completed", "r2", 0, "completed", "")
	old := []map[string]any{{"role": "assistant", "source": "codex", "request_id": "r1", "content": "old answer", "history_read_result_verified": true}}
	if s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, old) {
		t.Fatal("old native tail acquired latest read proof")
	}
	current := []map[string]any{{"role": "assistant", "source": "codex", "request_id": "r2", "content": "final answer", "history_read_result_verified": true}}
	if !s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, current) {
		t.Fatal("exact native turn was not readable")
	}
	current[0]["origin"] = "bridge_delegation_result"
	if s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, current) {
		t.Fatal("delegated result impersonated a native reply")
	}
}

func TestClaudeReadBoundaryRequiresVerifiedFreshNativeTimestamp(t *testing.T) {
	s := New(t.TempDir())
	s.now = func() time.Time { return time.UnixMilli(1000) }
	s.Update("s1", "running", "r1", 0, "", "")
	s.now = func() time.Time { return time.UnixMilli(2000) }
	latest, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	view := sharedView(t, s, "phone")
	message := map[string]any{"role": "assistant", "source": "claude", "timestamp": int64(1500), "history_timestamp_verified": true, "content": "final answer", "history_read_result_verified": true}
	if !s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, []map[string]any{message}) {
		t.Fatal("fresh verified Claude reply was not readable")
	}
	message["timestamp"] = int64(999)
	if s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, []map[string]any{message}) {
		t.Fatal("old reply became current")
	}
	message["timestamp"], message["history_timestamp_verified"] = int64(2500), false
	if s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, []map[string]any{message}) {
		t.Fatal("fallback file timestamp became a read proof")
	}
	message["history_timestamp_verified"] = true
	if s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, []map[string]any{message}) {
		t.Fatal("later turn became an earlier boundary")
	}
}

func TestReadBoundaryRejectsSameTurnProgressAndThinkingBeforeFinalHistoryFlush(t *testing.T) {
	s := New(t.TempDir())
	s.Update("s1", "running", "r1", 0, "", "")
	latest, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	view := sharedView(t, s, "phone")
	message := map[string]any{"role": "assistant", "source": "codex", "request_id": "r1", "content": "I am checking", "history_read_result_verified": false}
	if s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, []map[string]any{message}) {
		t.Fatal("same-request progress acquired a completion proof")
	}
	delete(message, "history_read_result_verified")
	if s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, []map[string]any{message}) {
		t.Fatal("unknown completion marker was guessed")
	}
	message["history_read_result_verified"], message["content"] = true, ""
	if s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, []map[string]any{message}) {
		t.Fatal("thinking-only/empty reply became completed content")
	}
	message["content"] = "Completed answer"
	if !s.HistoryMatchesReadBoundary("s1", view.ReadEpoch, latest.Revision, []map[string]any{message}) {
		t.Fatal("verified final native reply was not readable")
	}
}
