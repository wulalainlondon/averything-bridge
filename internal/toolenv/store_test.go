package toolenv

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fixture(t *testing.T) (*Store, Identity, Snapshot) {
	t.Helper()
	s := Open(t.TempDir())
	id := Identity{"host", "phone", "s1", "t1"}
	return s, id, Snapshot{ThreadID: "t1", Generation: "g1", CheckedAt: time.Now().UnixMilli(), ReloadAllowed: true, ForkAllowed: true, Binding: "settings"}
}
func begin(t *testing.T, s *Store, id Identity, snap Snapshot) Operation {
	t.Helper()
	p, e := s.Prepare(id, "reload", snap)
	if e != nil {
		t.Fatal(e)
	}
	o, start, e := s.Begin(id, Request{Action: "reload", Token: p.Token, OperationID: "op1", MaintenanceConfirmed: true}, snap.Generation, snap.Binding)
	if e != nil || !start {
		t.Fatalf("begin=%+v,%v,%v", o, start, e)
	}
	return o
}
func TestPlanBoundToDeviceThreadGenerationSettingsAndConsent(t *testing.T) {
	s, id, snap := fixture(t)
	p, e := s.Prepare(id, "reload", snap)
	if e != nil {
		t.Fatal(e)
	}
	r := Request{Action: "reload", Token: p.Token, OperationID: "op1", MaintenanceConfirmed: true}
	other := id
	other.Device = "other"
	for _, c := range []struct {
		id           Identity
		gen, binding string
		confirm      bool
	}{{other, "g1", "settings", true}, {id, "g2", "settings", true}, {id, "g1", "changed", true}, {id, "g1", "settings", false}} {
		r.MaintenanceConfirmed = c.confirm
		if _, _, e := s.Begin(c.id, r, c.gen, c.binding); e == nil {
			t.Fatal("accepted changed binding/consent")
		}
	}
	r.MaintenanceConfirmed = true
	if _, start, e := s.Begin(id, r, "g1", "settings"); e != nil || !start {
		t.Fatal(e)
	}
	if _, start, e := s.Begin(id, r, "g1", "settings"); e != nil || start {
		t.Fatal("duplicate mutation", e)
	}
}
func TestConcurrentApplyRunsOnce(t *testing.T) {
	s, id, snap := fixture(t)
	p, _ := s.Prepare(id, "fork", snap)
	r := Request{Action: "fork", Token: p.Token, OperationID: "op1"}
	var wg sync.WaitGroup
	var starts atomic.Int32
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, start, e := s.Begin(id, r, "g1", "settings")
			if e != nil {
				t.Error(e)
			}
			if start {
				starts.Add(1)
			}
		}()
	}
	wg.Wait()
	if starts.Load() != 1 {
		t.Fatal(starts.Load())
	}
}
func TestRestartNeverReplaysMutation(t *testing.T) {
	for _, phase := range []string{"waiting_idle", "applying", "verifying"} {
		t.Run(phase, func(t *testing.T) {
			s, id, snap := fixture(t)
			begin(t, s, id, snap)
			if phase != "waiting_idle" {
				if _, e := s.Transition(id, "op1", "waiting_idle", phase, "", "none", "", ""); e != nil {
					t.Fatal(e)
				}
			}
			reopened := Open(filepath.Dir(s.path))
			o, e := reopened.Get(id, "op1")
			if e != nil {
				t.Fatal(e)
			}
			want := "indeterminate"
			if phase == "waiting_idle" {
				want = "cancelled"
			}
			if o.Phase != want {
				t.Fatal(o)
			}
			if phase != "waiting_idle" {
				if _, e := reopened.Prepare(id, "reload", snap); Code(e) != "operation_pending" {
					t.Fatal(e)
				}
			}
			if _, start, e := reopened.Begin(id, Request{Action: "reload", OperationID: "op1"}, "g1", "settings"); e != nil || start {
				t.Fatal("replayed", e)
			}
		})
	}
}
func TestCancelOnlyBeforeApply(t *testing.T) {
	s, id, snap := fixture(t)
	begin(t, s, id, snap)
	if _, e := s.Transition(id, "op1", "waiting_idle", "applying", "", "none", "", ""); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Transition(id, "op1", "waiting_idle", "cancelled", "", "none", "", ""); Code(e) != "operation_changed" {
		t.Fatal(e)
	}
}
func TestExpiredPlanCorruptJournalAndWrongOwnerFailClosed(t *testing.T) {
	s, id, snap := fixture(t)
	p, _ := s.Prepare(id, "fork", snap)
	s.now = func() time.Time { return time.Now().Add(3 * time.Minute) }
	if _, _, e := s.Begin(id, Request{Action: "fork", OperationID: "op1", Token: p.Token}, "g1", "settings"); Code(e) != "plan_expired" {
		t.Fatal(e)
	}
	if err := os.WriteFile(s.path, []byte("not-json"), 0600); err != nil {
		t.Fatal(err)
	}
	bad := Open(filepath.Dir(s.path))
	if _, e := bad.Prepare(id, "fork", snap); Code(e) != "journal_unavailable" {
		t.Fatal(e)
	}
	s, id, snap = fixture(t)
	begin(t, s, id, snap)
	id.Device = "other"
	if _, e := s.Get(id, "op1"); Code(e) != "no_operation" {
		t.Fatal(e)
	}
}
