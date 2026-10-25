// Package remotedesktop exposes a short-lived, Tailscale-only desktop rescue
// surface. It is deliberately separate from the public Bridge listener.
package remotedesktop

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	sessionLifetime = 5 * time.Minute
	frameInterval   = 900 * time.Millisecond
	clickInterval   = 180 * time.Millisecond
	moveInterval    = 35 * time.Millisecond
	keyInterval     = 80 * time.Millisecond
	dragTimeout     = 3 * time.Second
	maxFrameBytes   = 4 << 20
)

type Availability struct {
	ScreenRecording bool `json:"screenRecording"`
	Accessibility   bool `json:"accessibility"`
}

type Desktop interface {
	Status(context.Context) (Availability, error)
	Capture(context.Context) ([]byte, error)
	ClickButton(context.Context, float64, float64, string) error
	Pointer(context.Context, string, float64, float64) error
	Text(context.Context, string) error
	Key(context.Context, string) error
}

type Handler struct {
	peerIP        net.IP
	auth          func(*http.Request) bool
	desktop       Desktop
	now           func() time.Time
	mu            sync.Mutex
	token         string
	expiry        time.Time
	lastFrame     time.Time
	lastClick     time.Time
	lastMove      time.Time
	lastKey       time.Time
	lastInputSeq  uint64
	dragging      bool
	dragX         float64
	dragY         float64
	dragTimer     *time.Timer
	dragSerial    uint64
	sessionCtx    context.Context
	sessionCancel context.CancelFunc
	video         *VideoService
}

func (h *Handler) SetVideoService(video *VideoService) { h.video = video }

func NewHandler(peerIP string, auth func(*http.Request) bool, desktop Desktop) (*Handler, error) {
	parsed := net.ParseIP(strings.TrimSpace(peerIP))
	ipv4 := parsed.To4()
	if ipv4 == nil || ipv4[0] != 100 || ipv4[1] < 64 || ipv4[1] > 127 || auth == nil || desktop == nil {
		return nil, errors.New("remote desktop requires one explicit IPv4 peer, auth, and desktop helper")
	}
	return &Handler{peerIP: ipv4, auth: auth, desktop: desktop, now: time.Now}, nil
}

func allowedOrigin(origin string) bool {
	switch origin {
	case "", "http://localhost", "https://localhost", "capacitor://localhost", "http://localhost:5173":
		return true
	default:
		return false
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || !net.ParseIP(host).Equal(h.peerIP) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	origin := r.Header.Get("Origin")
	if !allowedOrigin(origin) {
		http.Error(w, "origin denied", http.StatusForbidden)
		return
	}
	if origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Remote-Session")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Header.Get("Access-Control-Request-Private-Network") == "true" {
			w.Header().Set("Access-Control-Allow-Private-Network", "true")
		}
	}
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	bearer := r.Header.Get("Authorization")
	if !strings.HasPrefix(bearer, "Bearer ") || strings.TrimSpace(strings.TrimPrefix(bearer, "Bearer ")) == "" || !h.auth(r) {
		http.Error(w, "paired device required", http.StatusUnauthorized)
		return
	}

	switch r.URL.Path {
	case "/v1/status":
		if r.Method != http.MethodGet {
			break
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		status, err := h.desktop.Status(ctx)
		if err != nil {
			http.Error(w, "desktop unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
		return
	case "/v1/start":
		if r.Method != http.MethodPost {
			break
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		status, err := h.desktop.Status(ctx)
		if err != nil {
			http.Error(w, "desktop unavailable", http.StatusServiceUnavailable)
			return
		}
		if !status.ScreenRecording || !status.Accessibility {
			http.Error(w, "Mac screen recording and accessibility permissions required", http.StatusConflict)
			return
		}
		var random [32]byte
		if _, err := rand.Read(random[:]); err != nil {
			http.Error(w, "session unavailable", http.StatusServiceUnavailable)
			return
		}
		h.mu.Lock()
		h.releaseDragLocked()
		if h.sessionCancel != nil {
			h.sessionCancel()
		}
		if h.video != nil {
			h.video.Close()
		}
		h.token = hex.EncodeToString(random[:])
		h.expiry = h.now().Add(sessionLifetime)
		h.sessionCtx, h.sessionCancel = context.WithDeadline(context.Background(), h.expiry)
		h.lastFrame = time.Time{}
		h.lastClick = time.Time{}
		h.lastMove = time.Time{}
		h.lastKey = time.Time{}
		h.lastInputSeq = 0
		token, expiresAt := h.token, h.expiry.UnixMilli()
		h.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"sessionToken": token, "expiresAt": expiresAt})
		return
	case "/v1/video-offer":
		if r.Method != http.MethodPost {
			break
		}
		if h.video == nil {
			http.Error(w, "video unavailable", http.StatusNotImplemented)
			return
		}
		var input struct {
			SDP string `json:"sdp"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&input) != nil || decoder.Decode(&struct{}{}) != io.EOF || input.SDP == "" {
			http.Error(w, "invalid offer", http.StatusBadRequest)
			return
		}
		h.mu.Lock()
		if !h.sessionValidLocked(r.Header.Get("X-Remote-Session")) {
			h.mu.Unlock()
			http.Error(w, "session expired", http.StatusUnauthorized)
			return
		}
		sessionCtx := h.sessionCtx
		h.mu.Unlock()
		answer, err := h.video.Offer(sessionCtx, input.SDP)
		if err != nil {
			http.Error(w, "video negotiation failed", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"sdp": answer})
		return
	case "/v1/frame":
		if r.Method != http.MethodGet {
			break
		}
		h.mu.Lock()
		if !h.sessionValidLocked(r.Header.Get("X-Remote-Session")) {
			h.mu.Unlock()
			http.Error(w, "session expired", http.StatusUnauthorized)
			return
		}
		if h.now().Sub(h.lastFrame) < frameInterval {
			h.mu.Unlock()
			http.Error(w, "frame rate limited", http.StatusTooManyRequests)
			return
		}
		h.lastFrame = h.now()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		frame, err := h.desktop.Capture(ctx)
		if err != nil || len(frame) == 0 || len(frame) > maxFrameBytes {
			h.mu.Unlock()
			http.Error(w, "capture unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write(frame)
		h.mu.Unlock()
		return
	case "/v1/click":
		if r.Method != http.MethodPost {
			break
		}
		var point struct {
			X      *float64 `json:"x"`
			Y      *float64 `json:"y"`
			Button string   `json:"button"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&point) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
			!validInputPoint(point.X, point.Y) ||
			(point.Button != "" && point.Button != "left" && point.Button != "right") {
			http.Error(w, "invalid point", http.StatusBadRequest)
			return
		}
		h.mu.Lock()
		if !h.sessionValidLocked(r.Header.Get("X-Remote-Session")) {
			h.mu.Unlock()
			http.Error(w, "session expired", http.StatusUnauthorized)
			return
		}
		if h.dragging {
			h.mu.Unlock()
			http.Error(w, "drag in progress", http.StatusConflict)
			return
		}
		if h.now().Sub(h.lastClick) < clickInterval {
			h.mu.Unlock()
			http.Error(w, "click rate limited", http.StatusTooManyRequests)
			return
		}
		h.lastClick = h.now()
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		button := point.Button
		if button == "" {
			button = "left"
		}
		err := h.desktop.ClickButton(ctx, *point.X, *point.Y, button)
		h.mu.Unlock()
		if err != nil {
			http.Error(w, "input unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	case "/v1/pointer":
		h.handlePointer(w, r)
		return
	case "/v1/text":
		h.handleText(w, r)
		return
	case "/v1/key":
		h.handleKey(w, r)
		return
	case "/v1/stop":
		if r.Method != http.MethodPost {
			break
		}
		h.mu.Lock()
		if !h.sessionValidLocked(r.Header.Get("X-Remote-Session")) {
			h.mu.Unlock()
			http.Error(w, "session expired", http.StatusUnauthorized)
			return
		}
		h.token = ""
		h.expiry = time.Time{}
		h.releaseDragLocked()
		if h.sessionCancel != nil {
			h.sessionCancel()
			h.sessionCancel = nil
		}
		h.sessionCtx = nil
		if h.video != nil {
			h.video.Close()
		}
		h.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	default:
		http.NotFound(w, r)
		return
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (h *Handler) sessionValidLocked(candidate string) bool {
	return h.token != "" && h.now().Before(h.expiry) &&
		subtle.ConstantTimeCompare([]byte(candidate), []byte(h.token)) == 1
}
