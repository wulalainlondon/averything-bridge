package runtimejournal

import "testing"

func TestUnconfirmedStopRestoresOnlyItsOwnLiveRun(t *testing.T) {
	store := New(t.TempDir())
	store.Update("s", "running", "r1", 1, "", "")
	store.Update("s", "stopping", "r1", 1, "", "")
	view, changed := store.RestoreUnconfirmedStop("s", "r1", "running")
	if !changed || view.Phase != "running" || view.ActiveRequestID != "r1" || view.QueueLength != 1 || view.LastTerminal != "" {
		t.Fatal("failed stop released or changed the run", view)
	}
	store.Update("s", "completed", "r1", 1, "completed", "")
	if _, changed = store.RestoreUnconfirmedStop("s", "r1", "running"); changed {
		t.Fatal("failed stop resurrected completed run")
	}
	store.Update("s", "running", "r2", 0, "", "")
	store.Update("s", "stopping", "r2", 0, "", "")
	if _, changed = store.RestoreUnconfirmedStop("s", "r1", "running"); changed {
		t.Fatal("old stop failure changed newer run")
	}
}
