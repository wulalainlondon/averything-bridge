package notificationreply

import "testing"

func TestOriginalCrashAfterClaimMustQuarantine(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	before, _, _ := s.Enqueue("original", "source", "SYNTHETIC")
	s.Claim("original")
	r := NewStore(dir)
	got, ok := r.Get("original")
	if !ok || got.Status != "uncertain" || got.ID != before.ID || got.SessionID != before.SessionID || len(r.Pending()) != 0 {
		t.Fatal("original Dispatching crash must retain original unresolved identity without retrying as Pending")
	}
}
