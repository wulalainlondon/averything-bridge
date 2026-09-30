package core

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

func TestNotificationReplyAPIQueuesExactlyOnce(t *testing.T) {
	h, exec := newTestHub(t)
	h.cfg.TailscaleIP, h.cfg.LanIP, h.cfg.Port = "100.64.0.10", "192.168.1.20", 8766
	h.registry.Create("s1", "Reply target", t.TempDir(), "codex", "", "", "")
	replyURL, fallbackURL, capability, expiresAt := h.notificationReplyAction("s1")
	if replyURL != "http://100.64.0.10:8766/api/notification/v1/replies" || capability == "" || expiresAt <= time.Now().UnixMilli() {
		t.Fatalf("reply action url=%q capability=%t expiry=%d", replyURL, capability != "", expiresAt)
	}
	if fallbackURL != "http://192.168.1.20:8766/api/notification/v1/replies" {
		t.Fatalf("fallback url=%q", fallbackURL)
	}

	sent := make(chan string, 2)
	// fakeExec uses the concrete Session signature; keep completion inside the
	// callback so a queued duplicate would be observable as a second send.
	exec.onSend = func(s *session.Session, reqID, content string) {
		sent <- content
		h.Emit(protocol.NewDone(s.ID, reqID))
	}

	call := func(content string) *httptest.ResponseRecorder {
		body, _ := json.Marshal(notificationReplyRequest{ReplyID: "reply-1", SessionID: "s1", Content: content})
		req := httptest.NewRequest(http.MethodPost, replyURL, bytes.NewReader(body))
		req.Header.Set("Authorization", "Reply "+capability)
		response := httptest.NewRecorder()
		h.ServeNotificationReplyAPI(response, req)
		return response
	}
	first := call("接著跑 lint")
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	select {
	case content := <-sent:
		if content != "接著跑 lint" {
			t.Fatalf("content=%q", content)
		}
	case <-time.After(time.Second):
		t.Fatal("reply was not dispatched")
	}

	retry := call("接著跑 lint")
	if retry.Code != http.StatusAccepted {
		t.Fatalf("retry status=%d body=%s", retry.Code, retry.Body.String())
	}
	select {
	case duplicate := <-sent:
		t.Fatalf("idempotent retry dispatched twice: %q", duplicate)
	case <-time.After(80 * time.Millisecond):
	}
	conflict := call("不同內容")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
}

func TestNotificationReplyAPIRejectsInvalidCapabilityAndDesktopLease(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.TailscaleIP, h.cfg.Port = "100.64.0.10", 8766
	h.registry.Create("s1", "Reply target", t.TempDir(), "codex", "", "", "thread-1")
	replyURL, _, capability, _ := h.notificationReplyAction("s1")
	body := []byte(`{"reply_id":"reply-1","session_id":"s1","content":"continue"}`)

	bad := httptest.NewRequest(http.MethodPost, replyURL, bytes.NewReader(body))
	bad.Header.Set("Authorization", "Reply invalid")
	badResponse := httptest.NewRecorder()
	h.ServeNotificationReplyAPI(badResponse, bad)
	if badResponse.Code != http.StatusUnauthorized {
		t.Fatalf("bad capability status=%d", badResponse.Code)
	}

	h.controls.MarkDesktopOwner("s1", "thread-1")
	blocked := httptest.NewRequest(http.MethodPost, replyURL, bytes.NewReader(body))
	blocked.Header.Set("Authorization", "Reply "+capability)
	blockedResponse := httptest.NewRecorder()
	h.ServeNotificationReplyAPI(blockedResponse, blocked)
	if blockedResponse.Code != http.StatusConflict || !bytes.Contains(blockedResponse.Body.Bytes(), []byte("session_controlled_by_desktop")) {
		t.Fatalf("desktop lease status=%d body=%s", blockedResponse.Code, blockedResponse.Body.String())
	}
}
