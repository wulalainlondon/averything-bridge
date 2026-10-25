package remotedesktop

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeDesktop struct {
	availability Availability
	clicks       int
	buttons      []string
	pointers     []string
	texts        []string
	keys         []string
}

func (f *fakeDesktop) Status(context.Context) (Availability, error) { return f.availability, nil }
func (f *fakeDesktop) Capture(context.Context) ([]byte, error) {
	return []byte{0xff, 0xd8, 0xff, 0xd9}, nil
}
func (f *fakeDesktop) ClickButton(_ context.Context, _, _ float64, button string) error {
	f.clicks++
	f.buttons = append(f.buttons, button)
	return nil
}
func (f *fakeDesktop) Pointer(_ context.Context, action string, _, _ float64) error {
	f.pointers = append(f.pointers, action)
	return nil
}
func (f *fakeDesktop) Text(_ context.Context, value string) error {
	f.texts = append(f.texts, value)
	return nil
}
func (f *fakeDesktop) Key(_ context.Context, value string) error {
	f.keys = append(f.keys, value)
	return nil
}

func newTestHandler(t *testing.T) (*Handler, *fakeDesktop) {
	t.Helper()
	desktop := &fakeDesktop{availability: Availability{ScreenRecording: true, Accessibility: true}}
	h, err := NewHandler("100.113.167.33", func(r *http.Request) bool {
		return r.Header.Get("Authorization") == "Bearer paired-secret"
	}, desktop)
	if err != nil {
		t.Fatal(err)
	}
	return h, desktop
}

func serve(h *Handler, method, path, peer, bearer, session string, body []byte) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	r.RemoteAddr = peer
	if bearer != "" {
		r.Header.Set("Authorization", bearer)
	}
	if session != "" {
		r.Header.Set("X-Remote-Session", session)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func startTestSession(t *testing.T, h *Handler) string {
	t.Helper()
	w := serve(h, "POST", "/v1/start", "100.113.167.33:50000", "Bearer paired-secret", "", nil)
	if w.Code != http.StatusOK {
		t.Fatalf("start: %d %s", w.Code, w.Body.String())
	}
	var result struct{ SessionToken string }
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || len(result.SessionToken) != 64 {
		t.Fatalf("invalid session token: %v", err)
	}
	return result.SessionToken
}

func TestRemoteDesktopRequiresExactPeerAndBearer(t *testing.T) {
	h, _ := newTestHandler(t)
	for _, tc := range []struct {
		peer, bearer string
		want         int
	}{
		{"100.113.167.34:50000", "Bearer paired-secret", 404},
		{"192.168.1.2:50000", "Bearer paired-secret", 404},
		{"100.113.167.33:50000", "", 401},
		{"100.113.167.33:50000", "Bearer ", 401},
		{"100.113.167.33:50000", "Bearer wrong", 401},
		{"100.113.167.33:50000", "Bearer paired-secret", 200},
	} {
		w := serve(h, "GET", "/v1/status", tc.peer, tc.bearer, "", nil)
		if w.Code != tc.want {
			t.Errorf("peer=%q bearer=%q got %d want %d", tc.peer, tc.bearer, w.Code, tc.want)
		}
	}
}

func TestRemoteDesktopNeverFallsBackToQueryCredential(t *testing.T) {
	h, _ := newTestHandler(t)
	h.auth = func(r *http.Request) bool { return r.URL.Query().Get("auth_token") == "paired-secret" }
	w := serve(h, "GET", "/v1/status?auth_token=paired-secret", "100.113.167.33:50000", "Bearer ", "", nil)
	if w.Code != 401 {
		t.Fatalf("query credential accepted: %d", w.Code)
	}
}

func TestRemoteDesktopOriginAndPermissionGate(t *testing.T) {
	h, desktop := newTestHandler(t)
	r := httptest.NewRequest("OPTIONS", "/v1/frame", nil)
	r.RemoteAddr = "100.113.167.33:50000"
	r.Header.Set("Origin", "https://attacker.example")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("foreign origin got %d", w.Code)
	}
	r.Header.Set("Origin", "http://localhost")
	r.Header.Set("Access-Control-Request-Private-Network", "true")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 204 || w.Header().Get("Access-Control-Allow-Origin") != "http://localhost" || w.Header().Get("Access-Control-Allow-Private-Network") != "true" {
		t.Fatalf("Capacitor preflight denied: code=%d headers=%v", w.Code, w.Header())
	}
	desktop.availability.Accessibility = false
	w = serve(h, "POST", "/v1/start", "100.113.167.33:50000", "Bearer paired-secret", "", nil)
	if w.Code != 409 {
		t.Fatalf("missing permission got %d", w.Code)
	}
}

func TestRemoteDesktopSessionClickFrameAndExpiry(t *testing.T) {
	h, desktop := newTestHandler(t)
	clock := time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)
	h.now = func() time.Time { return clock }
	token := startTestSession(t, h)
	if w := serve(h, "GET", "/v1/frame", "100.113.167.33:50000", "Bearer paired-secret", token, nil); w.Code != 200 || w.Header().Get("Content-Type") != "image/jpeg" {
		t.Fatalf("first frame: %d", w.Code)
	}
	if w := serve(h, "GET", "/v1/frame", "100.113.167.33:50000", "Bearer paired-secret", token, nil); w.Code != 429 {
		t.Fatalf("frame rate limit: %d", w.Code)
	}
	for _, body := range []string{`{"x":-1,"y":0.5}`, `{"x":0.5,"y":2}`, `{"x":0.5}`, `{"x":0.5,"y":0.5,"unexpected":1}`} {
		w := serve(h, "POST", "/v1/click", "100.113.167.33:50000", "Bearer paired-secret", token, []byte(body))
		if w.Code != 400 {
			t.Fatalf("invalid click %s got %d", body, w.Code)
		}
	}
	w := serve(h, "POST", "/v1/click", "100.113.167.33:50000", "Bearer paired-secret", token, []byte(`{"x":0.5,"y":0.5}`))
	if w.Code != 204 || desktop.clicks != 1 {
		t.Fatalf("click: code=%d clicks=%d", w.Code, desktop.clicks)
	}
	clock = clock.Add(5 * time.Minute)
	if w := serve(h, "GET", "/v1/frame", "100.113.167.33:50000", "Bearer paired-secret", token, nil); w.Code != 401 {
		t.Fatalf("expired session got %d", w.Code)
	}
}

func TestRemoteDesktopStopInvalidatesSession(t *testing.T) {
	h, _ := newTestHandler(t)
	token := startTestSession(t, h)
	streamContext := h.sessionCtx
	if w := serve(h, "POST", "/v1/stop", "100.113.167.33:50000", "Bearer paired-secret", token, nil); w.Code != 204 {
		t.Fatalf("stop got %d", w.Code)
	}
	select {
	case <-streamContext.Done():
	default:
		t.Fatal("stop did not cancel the video session context")
	}
	if w := serve(h, "GET", "/v1/frame", "100.113.167.33:50000", "Bearer paired-secret", token, nil); w.Code != 401 {
		t.Fatalf("stopped session got %d", w.Code)
	}
}

func TestRemoteDesktopNewSessionCancelsPreviousStream(t *testing.T) {
	h, _ := newTestHandler(t)
	first := startTestSession(t, h)
	previousContext := h.sessionCtx
	second := startTestSession(t, h)
	if first == second {
		t.Fatal("session token was reused")
	}
	select {
	case <-previousContext.Done():
	default:
		t.Fatal("new session did not cancel previous video context")
	}
	if w := serve(h, "POST", "/v1/click", "100.113.167.33:50000", "Bearer paired-secret", first, []byte(`{"x":0.5,"y":0.5}`)); w.Code != 401 {
		t.Fatalf("old session accepted click: %d", w.Code)
	}
}

func TestRemoteDesktopRightClickAndInputValidation(t *testing.T) {
	h, desktop := newTestHandler(t)
	token := startTestSession(t, h)
	for _, body := range []string{`{"x":0.5,"y":0.5,"button":"middle"}`, `{"x":0.5,"y":0.5,"button":"RIGHT"}`} {
		if w := serve(h, "POST", "/v1/click", "100.113.167.33:50000", "Bearer paired-secret", token, []byte(body)); w.Code != 400 {
			t.Fatalf("invalid button %s: %d", body, w.Code)
		}
	}
	if w := serve(h, "POST", "/v1/click", "100.113.167.33:50000", "Bearer paired-secret", token, []byte(`{"x":0.5,"y":0.5,"button":"right"}`)); w.Code != 204 || len(desktop.buttons) != 1 || desktop.buttons[0] != "right" {
		t.Fatalf("right click: %d buttons=%v", w.Code, desktop.buttons)
	}
	for _, body := range []string{`{"text":""}`, `{"text":"a\nb"}`, `{"text":"x","extra":1}`} {
		if w := serve(h, "POST", "/v1/text", "100.113.167.33:50000", "Bearer paired-secret", token, []byte(body)); w.Code != 400 {
			t.Fatalf("invalid text %s: %d", body, w.Code)
		}
	}
	if w := serve(h, "POST", "/v1/text", "100.113.167.33:50000", "Bearer paired-secret", token, []byte(`{"text":"中文🙂"}`)); w.Code != 204 || len(desktop.texts) != 1 || desktop.texts[0] != "中文🙂" {
		t.Fatalf("text: %d texts=%v", w.Code, desktop.texts)
	}
	if w := serve(h, "POST", "/v1/key", "100.113.167.33:50000", "Bearer paired-secret", token, []byte(`{"key":"CommandQ"}`)); w.Code != 400 {
		t.Fatalf("arbitrary key accepted: %d", w.Code)
	}
	h.lastKey = time.Time{}
	if w := serve(h, "POST", "/v1/key", "100.113.167.33:50000", "Bearer paired-secret", token, []byte(`{"key":"Enter"}`)); w.Code != 204 || len(desktop.keys) != 1 {
		t.Fatalf("special key: %d keys=%v", w.Code, desktop.keys)
	}
}

func TestRemoteDesktopDragLifecycleAndAuthorization(t *testing.T) {
	h, desktop := newTestHandler(t)
	token := startTestSession(t, h)
	request := func(action string, seq int, peer, candidate string) int {
		body := []byte(fmt.Sprintf(`{"action":%q,"x":0.4,"y":0.5,"seq":%d}`, action, seq))
		return serve(h, "POST", "/v1/pointer", peer, "Bearer paired-secret", candidate, body).Code
	}
	if got := request("dragStart", 1, "100.113.167.34:50000", token); got != 404 {
		t.Fatalf("wrong peer: %d", got)
	}
	if got := request("dragStart", 1, "100.113.167.33:50000", "wrong"); got != 401 {
		t.Fatalf("wrong session: %d", got)
	}
	if got := request("dragMove", 1, "100.113.167.33:50000", token); got != 409 {
		t.Fatalf("drag without down: %d", got)
	}
	if got := request("dragStart", 1, "100.113.167.33:50000", token); got != 204 {
		t.Fatalf("drag start: %d", got)
	}
	if got := request("dragStart", 2, "100.113.167.33:50000", token); got != 409 {
		t.Fatalf("duplicate down: %d", got)
	}
	if got := request("dragMove", 2, "100.113.167.33:50000", token); got != 204 {
		t.Fatalf("drag move: %d", got)
	}
	if got := request("dragMove", 2, "100.113.167.33:50000", token); got != 409 {
		t.Fatalf("repeated sequence: %d", got)
	}
	if w := serve(h, "POST", "/v1/pointer", "100.113.167.33:50000", "Bearer paired-secret", token, []byte(`{"action":"heartbeat","seq":3}`)); w.Code != 204 {
		t.Fatalf("heartbeat: %d", w.Code)
	}
	if w := serve(h, "POST", "/v1/stop", "100.113.167.33:50000", "Bearer paired-secret", token, nil); w.Code != 204 {
		t.Fatalf("stop: %d", w.Code)
	}
	if got := strings.Join(desktop.pointers, ","); got != "dragStart,dragMove,dragEnd" {
		t.Fatalf("stop failed to release left mouse: %s", got)
	}
	if got := request("move", 4, "100.113.167.33:50000", token); got != 401 {
		t.Fatalf("stopped session moved pointer: %d", got)
	}
}

func TestRemoteDesktopDragWatchdogReleasesMouse(t *testing.T) {
	h, desktop := newTestHandler(t)
	token := startTestSession(t, h)
	w := serve(h, "POST", "/v1/pointer", "100.113.167.33:50000", "Bearer paired-secret", token,
		[]byte(`{"action":"dragStart","x":0.4,"y":0.5,"seq":1}`))
	if w.Code != 204 {
		t.Fatalf("drag start: %d", w.Code)
	}
	t.Cleanup(func() {
		h.mu.Lock()
		h.releaseDragLocked()
		h.mu.Unlock()
	})
	deadline := time.Now().Add(dragTimeout + time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		released := !h.dragging
		actions := strings.Join(desktop.pointers, ",")
		h.mu.Unlock()
		if released && actions != "dragStart" {
			if actions != "dragStart,dragEnd" {
				t.Fatalf("watchdog actions: %s", actions)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("drag watchdog left Mac mouse button down")
}
