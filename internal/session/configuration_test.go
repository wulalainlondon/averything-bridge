package session

import (
	"testing"
	"time"
)

func TestConfigurationReservationSerializesDequeueAndChecksRevision(t *testing.T) {
	s := NewRegistry().Create("cfg", "Config", t.TempDir(), "codex", "", "read-only", "")
	zero := uint64(0)
	_, release, err := s.ReserveConfiguration(&zero)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	s.SubmitNamed("message", func() { started <- struct{}{}; s.EndTurn() })
	select {
	case <-started:
		t.Fatal("run crossed reserved settings boundary")
	case <-time.After(20 * time.Millisecond):
	}
	if _, _, err = s.ReserveConfiguration(nil); err == nil {
		t.Fatal("second writer acquired config lease")
	}
	s.SetConfigRevision(1)
	release()
	release()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("queue did not resume")
	}
	for i := 0; i < 100 && s.State() != Idle; i++ {
		time.Sleep(time.Millisecond)
	}
	if _, _, err = s.ReserveConfiguration(&zero); err == nil || err.Error() != "config_revision_conflict" {
		t.Fatal("stale config accepted", err)
	}
}
