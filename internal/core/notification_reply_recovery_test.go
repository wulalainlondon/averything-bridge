package core

import (
	"bytes"
	"encoding/json"
	"everything-go/internal/notificationreply"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNotificationUncertainPublicProjectionDoesNotResend(t *testing.T) {
	h, e := newTestHub(t)
	h.cfg.TailscaleIP, h.cfg.Port = "100.64.0.10", 8766
	dir := t.TempDir()
	store := notificationreply.NewStore(dir)
	store.Enqueue("original-id", "s1", "SYNTHETIC")
	store.Claim("original-id")
	h.notificationReplies = notificationreply.NewStore(dir)
	h.registry.Create("s1", "fixture", t.TempDir(), "codex", "", "", "fixture-thread")
	_, _, cap, _ := h.notificationReplyAction("s1")
	calls := make(chan string, 3)
	e.onSend = func(s *session.Session, id, content string) { calls <- id; h.Emit(protocol.NewDone(s.ID, id)) }
	h.resumeNotificationReplies()
	body, _ := json.Marshal(notificationReplyRequest{ReplyID: "original-id", SessionID: "s1", Content: "SYNTHETIC"})
	q := httptest.NewRequest("POST", "/api/notification/v1/replies", bytes.NewReader(body))
	q.Header.Set("Authorization", "Reply "+cap)
	w := httptest.NewRecorder()
	h.ServeNotificationReplyAPI(w, q)
	var r notificationReplyResponse
	json.Unmarshal(w.Body.Bytes(), &r)
	if w.Code != http.StatusAccepted || r.Status != notificationreply.StatusUncertain || r.RequestID != "notification-reply-original-id" || r.Error != "acceptance_unknown_lookup_original" {
		t.Fatal("uncertain projection", w.Code, r)
	}
	select {
	case <-calls:
		t.Fatal("uncertain was resent")
	case <-time.After(30 * time.Millisecond):
	}
	h.notificationReplies.Enqueue("genuine-pending", "s1", "SYNTHETIC")
	h.resumeNotificationReplies()
	select {
	case id := <-calls:
		if id != "notification-reply-genuine-pending" {
			t.Fatal("wrong request")
		}
	case <-time.After(time.Second):
		t.Fatal("pending failed to resume")
	}
	// The provider callback precedes MarkSent and its durable write. Keep the
	// store and queue alive until that actor work finishes before TempDir cleanup.
	for deadline := time.Now().Add(time.Second); ; time.Sleep(time.Millisecond) {
		record, found := h.notificationReplies.Get("genuine-pending")
		if found && record.Status == notificationreply.StatusSent {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("resumed reply did not durably finish", record.Status)
		}
	}
}
func TestNotificationClaimDiskFailureBlocksProvider(t *testing.T) {
	h, e := newTestHub(t)
	dir := t.TempDir()
	h.notificationReplies = notificationreply.NewStore(dir)
	h.registry.Create("s1", "fixture", t.TempDir(), "codex", "", "", "")
	h.notificationReplies.Enqueue("id", "s1", "SYNTHETIC")
	calls := make(chan bool, 1)
	e.onSend = func(*session.Session, string, string) { calls <- true }
	if err := os.Mkdir(filepath.Join(dir, "notification_replies.json.tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	h.dispatchNotificationReply("id")
	select {
	case <-calls:
		t.Fatal("provider called after failed claim persistence")
	case <-time.After(30 * time.Millisecond):
	}
}
