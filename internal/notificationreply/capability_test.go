package notificationreply

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCapabilityIsSessionBoundPersistentAndExpires(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(100, 0)
	caps, err := NewCapabilities(dir, "bridge-a")
	if err != nil {
		t.Fatal(err)
	}
	caps.now = func() time.Time { return now }
	token, expires := caps.Issue("s1", time.Minute)
	if token == "" || expires != now.Add(time.Minute).UnixMilli() {
		t.Fatalf("token=%q expires=%d", token, expires)
	}
	if err := caps.Validate(token, "s1"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(caps.Validate(token, "s2"), ErrInvalidCapability) {
		t.Fatal("token crossed sessions")
	}

	reloaded, err := NewCapabilities(dir, "bridge-a")
	if err != nil {
		t.Fatal(err)
	}
	reloaded.now = func() time.Time { return now.Add(2 * time.Minute) }
	if !errors.Is(reloaded.Validate(token, "s1"), ErrExpiredCapability) {
		t.Fatal("expired token accepted")
	}
	info, err := os.Stat(filepath.Join(dir, "notification_reply_secret"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("secret permissions=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestReplyStoreIsIdempotentAndRecoversDispatching(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	if _, created, err := store.Enqueue("r1", "s1", "continue"); err != nil || !created {
		t.Fatalf("enqueue created=%v err=%v", created, err)
	}
	if _, created, err := store.Enqueue("r1", "s1", "continue"); err != nil || created {
		t.Fatalf("retry created=%v err=%v", created, err)
	}
	if _, _, err := store.Enqueue("r1", "s1", "different"); !errors.Is(err, ErrReplyConflict) {
		t.Fatalf("conflict=%v", err)
	}
	if _, claimed := store.Claim("r1"); !claimed {
		t.Fatal("pending reply not claimed")
	}

	reloaded := NewStore(dir)
	pending := reloaded.Pending()
	if len(pending) != 1 || pending[0].ID != "r1" || pending[0].Status != StatusPending {
		t.Fatalf("recovered=%+v", pending)
	}
	reloaded.MarkSent("r1")
	if record, _ := reloaded.Get("r1"); record.Status != StatusSent {
		t.Fatalf("record=%+v", record)
	}
}
