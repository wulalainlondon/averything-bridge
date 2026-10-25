package runtimejournal

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func sharedView(t *testing.T, s *Store, device string) View {
	t.Helper()
	views, err := s.SharedSnapshot(device, []string{"s1"}, []string{"desktop", "phone"})
	if err != nil || len(views) != 1 {
		t.Fatalf("shared snapshot: %v %+v", err, views)
	}
	return views[0]
}

func markSharedRead(t *testing.T, s *Store, id, epoch string, revision uint64) (SharedReadState, bool, error) {
	t.Helper()
	token, _ := s.ReadBoundaryToken(id, epoch, revision)
	return s.MarkSharedRead(id, epoch, revision, token)
}

func TestSharedReadRejectsHistoryTokenFromReusedTerminalRevision(t *testing.T) {
	s := New(t.TempDir())
	completed, _ := s.Update("s1", "completed", "old-request", 0, "completed", "")
	view := sharedView(t, s, "phone")
	token, err := s.ReadBoundaryToken("s1", view.ReadEpoch, completed.Revision)
	if err != nil {
		t.Fatal(err)
	}
	// Model a restored ledger followed by a different result at the same number.
	s.mu.Lock()
	s.records["s1"].Terminals[0].RequestID = "different-request"
	s.mu.Unlock()
	if _, changed, err := s.MarkSharedRead("s1", view.ReadEpoch, completed.Revision, token); err != ErrSharedReadStaleBoundary || changed {
		t.Fatalf("stale content token accepted: %v %v", changed, err)
	}
	if sharedView(t, s, "phone").Unread != 1 {
		t.Fatal("different result was consumed")
	}
}

func TestSharedReadDoesNotConsumeOtherDeviceDeliveryOrHistory(t *testing.T) {
	s := New(t.TempDir())
	completed, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	phone := sharedView(t, s, "phone")
	state, changed, err := markSharedRead(t, s, "s1", phone.ReadEpoch, completed.Revision)
	if err != nil || !changed || state.Unread != 0 {
		t.Fatalf("shared read: %+v %v %v", state, changed, err)
	}
	phone = sharedView(t, s, "phone")
	if phone.Unread != 0 || !phone.DeliveryPending || !phone.HistoryReconcile {
		t.Fatalf("other device lost delivery/history: %+v", phone)
	}
	legacy := s.Snapshot("phone", []string{"s1"})[0]
	if legacy.Unread != 1 {
		t.Fatalf("legacy behavior changed: %+v", legacy)
	}
	s.Ack("desktop", "s1", completed.Revision, true)
	if !sharedView(t, s, "phone").HistoryReconcile {
		t.Fatal("desktop history ACK consumed phone history")
	}
}

func TestSharedReadRaceKeepsNewerReplyUnread(t *testing.T) {
	s := New(t.TempDir())
	first, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	view := sharedView(t, s, "phone")
	s.Update("s1", "running", "r2", 0, "", "")
	second, _ := s.Update("s1", "completed", "r2", 0, "completed", "")
	state, _, err := markSharedRead(t, s, "s1", view.ReadEpoch, first.Revision)
	if err != nil || state.Unread != 1 || state.RuntimeRevision != second.Revision {
		t.Fatalf("new reply consumed: %+v %v", state, err)
	}
	if sharedView(t, s, "desktop").Unread != 1 {
		t.Fatal("new reply absent on other device")
	}
}

func TestSharedReadIsDurableAndIndependentFromLifecycleRevision(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	completed, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	view := sharedView(t, s, "phone")
	read, _, err := markSharedRead(t, s, "s1", view.ReadEpoch, completed.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if read.RuntimeRevision != completed.Revision || read.ReadVersion != 1 {
		t.Fatalf("read advanced lifecycle: %+v", read)
	}
	reloaded := sharedView(t, New(dir), "phone")
	if reloaded.ReadEpoch != view.ReadEpoch || reloaded.ReadVersion != read.ReadVersion || reloaded.Unread != 0 || !reloaded.HistoryReconcile {
		t.Fatalf("restart lost shared state: %+v", reloaded)
	}
}

func TestSharedReadDuplicateDoesNotWrite(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	completed, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	view := sharedView(t, s, "phone")
	if _, _, err := markSharedRead(t, s, "s1", view.ReadEpoch, completed.Revision); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(s.sharedReadPath)
	state, changed, err := markSharedRead(t, s, "s1", view.ReadEpoch, completed.Revision)
	after, _ := os.Stat(s.sharedReadPath)
	if err != nil || changed || state.ReadVersion != 1 || !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("duplicate wrote: %+v %v %v", state, changed, err)
	}
}

func TestSharedReadOnlyMigratesValidatedPairedDevices(t *testing.T) {
	s := New(t.TempDir())
	first, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	s.Ack("desktop", "s1", first.Revision, true)
	s.Update("s1", "running", "r2", 0, "", "")
	second, _ := s.Update("s1", "completed", "r2", 0, "completed", "")
	s.Ack("unpaired", "s1", second.Revision, true)
	s.Ack("phone", "s1", second.Revision, false)
	view := sharedView(t, s, "phone")
	if view.ReadRevision != first.Revision || view.Unread != 1 {
		t.Fatalf("migration used unpaired read or receipt: %+v", view)
	}
	// A legacy device read after initialization is not an implicit enrollment.
	s.Ack("desktop", "s1", second.Revision, true)
	if sharedView(t, s, "phone").Unread != 1 {
		t.Fatal("legacy read silently changed shared scope")
	}
}

func TestSharedReadRejectsInvalidAndRecreatedSessionBoundaries(t *testing.T) {
	s := New(t.TempDir())
	completed, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	view := sharedView(t, s, "phone")
	for _, input := range []struct {
		id, epoch string
		revision  uint64
	}{
		{"unknown", view.ReadEpoch, completed.Revision}, {"s1", "", completed.Revision},
		{"s1", "other", completed.Revision}, {"s1", view.ReadEpoch, 0}, {"s1", view.ReadEpoch, completed.Revision + 1},
	} {
		if _, changed, err := markSharedRead(t, s, input.id, input.epoch, input.revision); err == nil || changed {
			t.Fatalf("invalid boundary accepted: %+v", input)
		}
	}
	s.Remove("s1")
	s.Update("s1", "completed", "new", 0, "completed", "")
	newView := sharedView(t, s, "phone")
	if newView.ReadEpoch == view.ReadEpoch {
		t.Fatal("recreated session reused epoch")
	}
	if _, _, err := markSharedRead(t, s, "s1", view.ReadEpoch, completed.Revision); err == nil {
		t.Fatal("offline stale read consumed recreated session")
	}
}

func TestSharedReadFailedPersistenceDoesNotAdvance(t *testing.T) {
	s := New(t.TempDir())
	completed, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	view := sharedView(t, s, "phone")
	// A directory cannot be replaced by the ledger's atomic file rename.
	s.sharedReadPath = t.TempDir()
	if _, changed, err := markSharedRead(t, s, "s1", view.ReadEpoch, completed.Revision); err == nil || changed {
		t.Fatal("failed persistence reported successful read")
	}
	if sharedView(t, s, "phone").Unread != 1 {
		t.Fatal("failure advanced memory")
	}
}

func TestSharedReadConcurrentBoundariesAreMonotonic(t *testing.T) {
	s := New(t.TempDir())
	first, _ := s.Update("s1", "completed", "r1", 0, "completed", "")
	view := sharedView(t, s, "phone")
	s.Update("s1", "running", "r2", 0, "", "")
	second, _ := s.Update("s1", "completed", "r2", 0, "completed", "")
	var wg sync.WaitGroup
	for _, revision := range []uint64{first.Revision, second.Revision, first.Revision, second.Revision} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := markSharedRead(t, s, "s1", view.ReadEpoch, revision); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if final := sharedView(t, s, "phone"); final.ReadRevision != second.Revision || final.Unread != 0 {
		t.Fatalf("read regressed: %+v", final)
	}
}

func TestSharedReadCorruptLedgerIsNotOverwritten(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("s1", "completed", "r1", 0, "completed", "")
	path := filepath.Join(dir, "session_shared_reads.json")
	if err := os.WriteFile(path, []byte("invalid ledger"), 0o600); err != nil {
		t.Fatal(err)
	}
	reloaded := New(dir)
	if _, err := reloaded.SharedSnapshot("phone", []string{"s1"}, []string{"phone"}); err == nil {
		t.Fatal("corrupt ledger was silently reset")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "invalid ledger" {
		t.Fatal("corrupt ledger overwritten")
	}
}

func TestSharedReadFlushesUnpersistedProgressBeforeRead(t *testing.T) {
	dir := t.TempDir()
	s := New(dir)
	s.Update("s1", "completed", "r1", 0, "completed", "")
	view := sharedView(t, s, "phone")
	s.Update("s1", "running", "r2", 0, "", "")
	progress, _ := s.Progress("s1", "r2", "running_tool", "tool")
	if _, _, err := markSharedRead(t, s, "s1", view.ReadEpoch, progress.Revision); err != nil {
		t.Fatal(err)
	}
	reloaded := sharedView(t, New(dir), "phone")
	if reloaded.ReadEpoch != view.ReadEpoch || reloaded.ReadRevision != progress.Revision || reloaded.Unread != 0 {
		t.Fatalf("progress/read crash boundary lost: %+v", reloaded)
	}
}
