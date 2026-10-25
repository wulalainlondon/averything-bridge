package notificationreply

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNarrowRestartCrashWindows(t *testing.T) {
	for _, stage := range []string{"pending", "claimed", "sent_before_mark", "marked_sent", "failed"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			s := NewStore(dir)
			s.Enqueue("original-id", "source", "SYNTHETIC_CONTENT")
			if stage != "pending" {
				s.Claim("original-id")
			}
			if stage == "marked_sent" {
				s.MarkSent("original-id")
			}
			if stage == "failed" {
				s.MarkFailed("original-id", "synthetic_known_failure")
			}
			before, _ := s.Get("original-id")
			r := NewStore(dir)
			got, _ := r.Get("original-id")
			want := before.Status
			if stage == "claimed" || stage == "sent_before_mark" {
				want = StatusUncertain
			}
			if got.Status != want || got.ID != before.ID || got.SessionID != before.SessionID || got.Content != before.Content || got.CreatedAt != before.CreatedAt || got.UpdatedAt != before.UpdatedAt || got.Error != before.Error {
				t.Fatal("identity/history/status changed")
			}
			if len(r.Pending()) != map[bool]int{true: 1, false: 0}[stage == "pending"] {
				t.Fatal("wrong recoverable rows")
			}
			again := NewStore(dir)
			a, _ := again.Get("original-id")
			if a != got {
				t.Fatal("persist/reload changed quarantine")
			}
			if stage == "claimed" || stage == "sent_before_mark" {
				if _, yes := again.Claim("original-id"); yes {
					t.Fatal("resubmitted")
				}
				again.MarkSent("original-id")
				again.MarkFailed("original-id", "unqualified")
				a, _ = again.Get("original-id")
				if a.Status != StatusUncertain {
					t.Fatal("uncertain relabelled")
				}
			}
		})
	}
}
func TestUncertainPreservesOldHistoryAndIsNotPruned(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().Add(-30 * 24 * time.Hour).UnixMilli()
	rows := map[string]Record{"uncertain": {ID: "uncertain", SessionID: "s", Content: "SYNTHETIC", Status: StatusDispatching, Error: "prior_unknown", CreatedAt: old, UpdatedAt: old}}
	for i := 0; i < maxRecords+2; i++ {
		id := string(rune(0x1000 + i))
		rows[id] = Record{ID: id, SessionID: "s", Content: "SYNTHETIC", Status: StatusUncertain, CreatedAt: old, UpdatedAt: old}
	}
	raw, _ := json.Marshal(snapshot{Records: rows})
	os.WriteFile(filepath.Join(dir, "notification_replies.json"), raw, 0600)
	s := NewStore(dir)
	r, ok := s.Get("uncertain")
	if !ok || r.Status != StatusUncertain || r.Error != "prior_unknown" || r.RecoveryFrom != StatusDispatching || r.UpdatedAt != old {
		t.Fatal("unknown history pruned/cleared")
	}
	if len(s.records) != len(rows) {
		t.Fatal("uncertain capacity eviction")
	}
	s.Enqueue("new", "s", "new")
	if newRow, newOK := s.Get("new"); !newOK || newRow.Status != StatusPending {
		t.Fatal("uncertain soft-cap evicted genuine Pending")
	}
	if _, ok = s.Get("uncertain"); !ok {
		t.Fatal("later prune deleted unknown")
	}
}
func TestClaimPersistenceFailureCannotAuthorizeSend(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	s.Enqueue("id", "s", "SYNTHETIC")
	before, _ := s.Get("id")
	os.Mkdir(s.path+".tmp", 0700)
	r, claimed := s.Claim("id")
	if claimed || r != before {
		t.Fatal("undurable claim accepted")
	}
	disk := NewStore(dir)
	r, _ = disk.Get("id")
	if r.Status != StatusPending {
		t.Fatal("genuine pending lost")
	}
}
func TestUncertainDuplicateKeepsIDAndConflictGuard(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	s.Enqueue("id", "s", "original")
	s.Claim("id")
	r := NewStore(dir)
	a, _ := r.Get("id")
	b, created, err := r.Enqueue("id", "s", "original")
	if created || err != nil || a != b {
		t.Fatal("uncertain recreated")
	}
	if _, _, err = r.Enqueue("id", "foreign", "original"); err != ErrReplyConflict {
		t.Fatal("foreign identity")
	}
}

func TestCorruptRecoveryDoesNotRecreateOriginalRequest(t *testing.T) {
	for _, raw := range []string{`{`, `{}`, `{"records":{"old":{"id":"foreign","session_id":"s","status":"dispatching"}}}`} {
		dir := t.TempDir()
		path := filepath.Join(dir, "notification_replies.json")
		os.WriteFile(path, []byte(raw), 0600)
		s := NewStore(dir)
		if _, created, err := s.Enqueue("old", "s", "SYNTHETIC"); created || err != ErrStoreUnavailable {
			t.Fatal("unknown recovered store recreated request")
		}
		if _, claimed := s.Claim("old"); claimed {
			t.Fatal("corrupt store claimed")
		}
		after, _ := os.ReadFile(path)
		if string(after) != raw {
			t.Fatal("lost/corrupt history overwritten")
		}
	}
}
