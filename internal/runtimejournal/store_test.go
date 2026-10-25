package runtimejournal

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPerDeviceReadAndRestart(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("s1", "running", "r1", 0, "", "")
	completed, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	if got := s.Snapshot("phone", []string{"s1"})[0].Unread; got != 1 {
		t.Fatalf("phone unread=%d", got)
	}
	if _, changed := s.Ack("desktop", "s1", completed.Revision, true); !changed {
		t.Fatal("first desktop ACK did not advance its cursor")
	}
	if got := s.Snapshot("phone", []string{"s1"})[0].Unread; got != 1 {
		t.Fatalf("desktop consumed phone unread=%d", got)
	}
	if got := s.Snapshot("desktop", []string{"s1"})[0].Unread; got != 0 {
		t.Fatalf("desktop unread=%d", got)
	}
	reloaded := New(dir)
	if got := reloaded.Snapshot("phone", []string{"s1"})[0].Unread; got != 1 {
		t.Fatalf("restart lost phone cursor: %d", got)
	}
	if _, changed := reloaded.Ack("phone", "s1", completed.Revision, true); !changed {
		t.Fatal("first phone ACK did not advance its cursor")
	}
	if got := reloaded.Snapshot("phone", []string{"s1"})[0].Unread; got != 0 {
		t.Fatalf("phone read did not clear: %d", got)
	}
}

func TestDuplicateAckDoesNotRewriteJournal(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	completed, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	if _, changed := s.Ack("phone", "s1", completed.Revision, true); !changed {
		t.Fatal("first ACK did not change the cursor")
	}
	path := filepath.Join(dir, "session_runtime.json")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, changed := s.Ack("phone", "s1", completed.Revision, true); changed {
		t.Fatal("duplicate ACK reported a change")
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		t.Fatalf("duplicate ACK rewrote journal: before=%v/%d after=%v/%d",
			before.ModTime(), before.Size(), after.ModTime(), after.Size())
	}
	cursorPath := filepath.Join(dir, "session_runtime_cursors.jsonl")
	cursorBefore, err := os.Stat(cursorPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, changed := s.Ack("phone", "s1", completed.Revision, true); changed {
		t.Fatal("second duplicate ACK reported a change")
	}
	cursorAfter, err := os.Stat(cursorPath)
	if err != nil {
		t.Fatal(err)
	}
	if cursorAfter.Size() != cursorBefore.Size() || !cursorAfter.ModTime().Equal(cursorBefore.ModTime()) {
		t.Fatalf("duplicate ACK rewrote cursor log: before=%v/%d after=%v/%d",
			cursorBefore.ModTime(), cursorBefore.Size(), cursorAfter.ModTime(), cursorAfter.Size())
	}
}

func TestCursorAppendSurvivesCrashWithoutFullSnapshotRewrite(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	completed, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	journalPath := filepath.Join(dir, "session_runtime.json")
	before, err := os.Stat(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, changed := s.Ack("phone", "s1", completed.Revision, true); !changed {
		t.Fatal("ACK did not advance cursor")
	}
	after, err := os.Stat(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		t.Fatalf("cursor ACK rewrote lifecycle snapshot: before=%v/%d after=%v/%d",
			before.ModTime(), before.Size(), after.ModTime(), after.Size())
	}
	if info, err := os.Stat(filepath.Join(dir, "session_runtime_cursors.jsonl")); err != nil || info.Size() == 0 {
		t.Fatalf("cursor append missing: info=%v err=%v", info, err)
	}
	reloaded := New(dir)
	if got := reloaded.Snapshot("phone", []string{"s1"})[0].Unread; got != 0 {
		t.Fatalf("cursor append was not replayed after crash: unread=%d", got)
	}
}

func TestTerminalHistoryIsBounded(t *testing.T) {
	s := New(t.TempDir())
	for i := 0; i < 200; i++ {
		s.Update("s1", "running", "r", 0, "", "")
		s.Update("s1", "completed", "r", 0, "completed", "")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if got := len(s.records["s1"].Terminals); got != 128 {
		t.Fatalf("terminals=%d", got)
	}
}

func TestRecoverAllStaleRunsAtStartupBoundary(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("active", "running", "r1", 0, "", "")
	s.Update("done", "completed", "r2", 0, "completed", "")

	reloaded := New(dir)
	recovered := reloaded.RecoverAllStale()
	if len(recovered) != 1 || recovered[0].SessionID != "active" || recovered[0].Phase != "interrupted" {
		t.Fatalf("recovered=%+v", recovered)
	}
	if got := reloaded.Snapshot("phone", []string{"done"})[0].Phase; got != "completed" {
		t.Fatalf("completed session changed to %q", got)
	}
	if again := reloaded.RecoverAllStale(); len(again) != 0 {
		t.Fatalf("startup recovery was not idempotent: %+v", again)
	}
}

func TestNewRequestSupersedesPreviousTerminalPresentationButKeepsLedger(t *testing.T) {
	s := New(t.TempDir())
	s.Update("s1", "running", "r-old", 0, "", "")
	interrupted, _ := s.Update("s1", "interrupted", "r-old", 0, "interrupted", "Bridge restarted")
	if interrupted.LastTerminal != "interrupted" || interrupted.CompletedAt == 0 {
		t.Fatalf("interrupted=%+v", interrupted)
	}
	queued, changed := s.Update("s1", "queued", "r-new", 1, "", "")
	if !changed || queued.LastTerminal != "" || queued.LastError != "" || queued.CompletedAt != 0 {
		t.Fatalf("new request retained stale terminal presentation: %+v", queued)
	}
	s.mu.Lock()
	terminals := append([]Terminal(nil), s.records["s1"].Terminals...)
	s.mu.Unlock()
	if len(terminals) != 1 || terminals[0].Status != "interrupted" {
		t.Fatalf("new request erased delivery ledger: %+v", terminals)
	}
}

func TestActiveViewSelfHealsLegacyTerminalFromDifferentRequest(t *testing.T) {
	s := New(t.TempDir())
	s.mu.Lock()
	s.records["s1"] = &Record{
		SessionID: "s1", Revision: 3, Phase: "running", Stage: "thinking", ActiveRequestID: "r-new",
		LastTerminal: "interrupted", LastError: "Bridge restarted", CompletedAt: 100, UpdatedAt: 200,
		Terminals:     []Terminal{{Revision: 2, RequestID: "r-old", Status: "interrupted", At: 100}},
		AckedByDevice: map[string]uint64{}, ReadByDevice: map[string]uint64{},
	}
	s.mu.Unlock()
	view := s.Snapshot("phone", []string{"s1"})[0]
	if view.LastTerminal != "" || view.LastError != "" || view.CompletedAt != 0 || view.Phase != "running" {
		t.Fatalf("legacy terminal leaked into active projection: %+v", view)
	}
	s.mu.Lock()
	if got := s.records["s1"].Terminals; len(got) != 1 || got[0].Status != "interrupted" {
		t.Fatalf("self-heal erased ledger: %+v", got)
	}
	s.mu.Unlock()
}

func TestQueueLengthUpdatePreservesTheActiveRequest(t *testing.T) {
	s := New(t.TempDir())
	running, _ := s.Update("s1", "running", "r-active", 0, "", "")
	queued, changed := s.UpdateQueueLength("s1", 2)
	if !changed || queued.Revision <= running.Revision {
		t.Fatalf("queue update did not advance: before=%+v after=%+v", running, queued)
	}
	if queued.Phase != "running" || queued.Stage != "preparing" || queued.ActiveRequestID != "r-active" || queued.QueueLength != 2 {
		t.Fatalf("queue update replaced active runtime: %+v", queued)
	}
	if _, changed := s.UpdateQueueLength("s1", 2); changed {
		t.Fatal("identical queue length should be idempotent")
	}
}

func TestCanonicalHistoryClearsOnlyRestartGeneratedInterruption(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("restart", "running", "r1", 0, "", "")
	reloaded := New(dir)
	recovered := reloaded.RecoverAllStale()[0]
	if _, changed := reloaded.ReconcileHistoryActivity("restart", recovered.CompletedAt-1, "idle"); changed {
		t.Fatal("older history cleared interruption")
	}
	view, changed := reloaded.ReconcileHistoryActivity("restart", recovered.CompletedAt+1, "idle")
	if !changed || view.Phase != "idle" || view.LastTerminal != "" || view.LastError != "" || view.CompletedAt != 0 {
		t.Fatalf("newer canonical history did not heal restart marker: %+v changed=%v", view, changed)
	}
	persisted := New(dir).Snapshot("phone", []string{"restart"})[0]
	if persisted.Phase != "idle" || persisted.LastTerminal != "" {
		t.Fatalf("history reconciliation was not durable: %+v", persisted)
	}

	manual := New(t.TempDir())
	stopped, _ := manual.Update("manual", "interrupted", "r2", 0, "interrupted", "Stopped by user")
	if _, changed := manual.ReconcileHistoryActivity("manual", stopped.CompletedAt+1, "idle"); changed {
		t.Fatal("canonical history erased a user-requested stop")
	}
}

func TestProgressIsRevisionedDedupedAndPersists(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	initial, _ := s.Update("s1", "running", "r1", 0, "", "")
	resuming, changed := s.Progress("s1", "r1", "resuming_thread", "Loading conversation")
	if !changed || resuming.Revision <= initial.Revision || resuming.Stage != "resuming_thread" {
		t.Fatalf("resuming progress=%+v changed=%v initial=%+v", resuming, changed, initial)
	}
	duplicate, changed := s.Progress("s1", "r1", "resuming_thread", "Loading conversation")
	if changed || duplicate.Revision != resuming.Revision {
		t.Fatalf("duplicate advanced revision: %+v changed=%v", duplicate, changed)
	}
	waiting, changed := s.Progress("s1", "r1", "waiting_model", "Codex accepted the turn")
	if !changed || waiting.Revision <= resuming.Revision {
		t.Fatalf("waiting progress=%+v changed=%v", waiting, changed)
	}
	regressed, changed := s.Progress("s1", "r1", "submitting_turn", "late RPC update")
	if changed || regressed.Stage != "waiting_model" || regressed.Revision != waiting.Revision {
		t.Fatalf("out-of-order stage regressed runtime: %+v changed=%v", regressed, changed)
	}
	s.Flush()
	reloaded := New(dir).Snapshot("phone", []string{"s1"})[0]
	if reloaded.Stage != "waiting_model" || reloaded.StageMessage != "Codex accepted the turn" || reloaded.StageStartedAt == 0 || reloaded.ActiveStartedAt == 0 {
		t.Fatalf("restart lost stage: %+v", reloaded)
	}
}

func TestActiveStartedAtSurvivesStagesReconnectAndChangesOnlyForNewRequest(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	now := time.UnixMilli(1_000)
	s.now = func() time.Time { return now }
	queued, _ := s.Update("s1", "queued", "r1", 1, "", "")
	if queued.ActiveStartedAt != 1_000 {
		t.Fatalf("queued active_started_at=%d", queued.ActiveStartedAt)
	}

	now = time.UnixMilli(2_000)
	thinking, _ := s.Progress("s1", "r1", "thinking", "")
	if thinking.ActiveStartedAt != 1_000 || thinking.StageStartedAt != 2_000 {
		t.Fatalf("stage reset turn clock: %+v", thinking)
	}

	now = time.UnixMilli(3_000)
	running, _ := s.Update("s1", "running", "r1", 0, "", "")
	if running.ActiveStartedAt != 1_000 {
		t.Fatalf("phase reset turn clock: %+v", running)
	}
	s.Flush()
	if got := New(dir).Snapshot("phone", []string{"s1"})[0].ActiveStartedAt; got != 1_000 {
		t.Fatalf("restart active_started_at=%d", got)
	}

	now = time.UnixMilli(4_000)
	s.Update("s1", "completed", "r1", 0, "completed", "")
	now = time.UnixMilli(5_000)
	next, _ := s.Update("s1", "queued", "r2", 1, "", "")
	if next.ActiveStartedAt != 5_000 {
		t.Fatalf("new request retained old clock: %+v", next)
	}
}

func TestProgressWritesAreCoalescedButTerminalIsImmediate(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("s1", "running", "r1", 0, "", "")
	baseline := s.writes
	s.Progress("s1", "r1", "resuming_thread", "Loading conversation")
	s.Progress("s1", "r1", "waiting_model", "Waiting for Codex")
	if got := s.writes; got != baseline {
		t.Fatalf("progress wrote synchronously: writes=%d baseline=%d", got, baseline)
	}
	s.Flush()
	if got := s.writes; got != baseline+1 {
		t.Fatalf("coalesced progress writes=%d want=%d", got, baseline+1)
	}

	completed, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	reloaded := New(dir).Snapshot("phone", []string{"s1"})[0]
	if reloaded.Revision != completed.Revision || reloaded.Phase != "completed" {
		t.Fatalf("terminal transition was not durable: got=%+v want=%+v", reloaded, completed)
	}
}

func TestProgressFlushesAutomatically(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("s1", "running", "r1", 0, "", "")
	s.Progress("s1", "r1", "waiting_model", "Waiting for Codex")

	deadline := time.Now().Add(2 * time.Second)
	for {
		reloaded := New(dir).Snapshot("phone", []string{"s1"})[0]
		if reloaded.Stage == "waiting_model" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("coalesced progress did not flush: %+v", reloaded)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
