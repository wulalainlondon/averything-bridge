package remotedesktop

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math"
	"net/http"
	"time"
	"unicode"
	"unicode/utf8"
)

func validInputPoint(x, y *float64) bool {
	return x != nil && y != nil && !math.IsNaN(*x) && !math.IsNaN(*y) &&
		!math.IsInf(*x, 0) && !math.IsInf(*y, 0) &&
		*x >= 0 && *x <= 1 && *y >= 0 && *y <= 1
}

func (h *Handler) handlePointer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input struct {
		Action string   `json:"action"`
		X      *float64 `json:"x"`
		Y      *float64 `json:"y"`
		Seq    uint64   `json:"seq"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(&struct{}{}) != io.EOF || input.Seq == 0 ||
		(input.Action != "heartbeat" && !validInputPoint(input.X, input.Y)) ||
		(input.Action != "move" && input.Action != "dragStart" &&
			input.Action != "dragMove" && input.Action != "dragEnd" && input.Action != "heartbeat") {
		http.Error(w, "invalid pointer input", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.sessionValidLocked(r.Header.Get("X-Remote-Session")) {
		http.Error(w, "session expired", http.StatusUnauthorized)
		return
	}
	if input.Seq <= h.lastInputSeq {
		http.Error(w, "pointer event out of order", http.StatusConflict)
		return
	}
	if input.Action == "move" || input.Action == "dragMove" {
		if h.now().Sub(h.lastMove) < moveInterval {
			http.Error(w, "pointer rate limited", http.StatusTooManyRequests)
			return
		}
	}
	if h.dragging && (input.Action == "move" || input.Action == "dragStart") ||
		!h.dragging && (input.Action == "dragMove" || input.Action == "dragEnd" || input.Action == "heartbeat") {
		http.Error(w, "invalid drag state", http.StatusConflict)
		return
	}
	h.lastInputSeq = input.Seq
	if input.Action == "heartbeat" {
		h.armDragWatchdogLocked()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	x, y := *input.X, *input.Y
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := h.desktop.Pointer(ctx, input.Action, x, y); err != nil {
		if h.dragging {
			h.releaseDragLocked()
		} else if input.Action == "dragStart" {
			h.forceMouseUpLocked(x, y)
		}
		http.Error(w, "pointer unavailable", http.StatusServiceUnavailable)
		return
	}
	h.dragX, h.dragY = x, y
	switch input.Action {
	case "move":
		h.lastMove = h.now()
	case "dragStart":
		h.dragging = true
		h.armDragWatchdogLocked()
	case "dragMove":
		h.lastMove = h.now()
		h.armDragWatchdogLocked()
	case "dragEnd":
		h.dragging = false
		h.stopDragWatchdogLocked()
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleText(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input struct {
		Text string `json:"text"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		!utf8.ValidString(input.Text) || len(input.Text) == 0 || len(input.Text) > 2048 ||
		utf8.RuneCountInString(input.Text) > 512 {
		http.Error(w, "invalid text input", http.StatusBadRequest)
		return
	}
	for _, value := range input.Text {
		if unicode.IsControl(value) {
			http.Error(w, "control characters are not text input", http.StatusBadRequest)
			return
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.sessionValidLocked(r.Header.Get("X-Remote-Session")) {
		http.Error(w, "session expired", http.StatusUnauthorized)
		return
	}
	if h.dragging {
		http.Error(w, "drag in progress", http.StatusConflict)
		return
	}
	if h.now().Sub(h.lastKey) < keyInterval {
		http.Error(w, "input rate limited", http.StatusTooManyRequests)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := h.desktop.Text(ctx, input.Text); err != nil {
		http.Error(w, "text input unavailable", http.StatusServiceUnavailable)
		return
	}
	h.lastKey = h.now()
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleKey(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var input struct {
		Key string `json:"key"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(&struct{}{}) != io.EOF || !validSpecialKey(input.Key) {
		http.Error(w, "invalid key", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.sessionValidLocked(r.Header.Get("X-Remote-Session")) {
		http.Error(w, "session expired", http.StatusUnauthorized)
		return
	}
	if h.dragging {
		http.Error(w, "drag in progress", http.StatusConflict)
		return
	}
	if h.now().Sub(h.lastKey) < keyInterval {
		http.Error(w, "input rate limited", http.StatusTooManyRequests)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := h.desktop.Key(ctx, input.Key); err != nil {
		http.Error(w, "key unavailable", http.StatusServiceUnavailable)
		return
	}
	h.lastKey = h.now()
	w.WriteHeader(http.StatusNoContent)
}

func validSpecialKey(key string) bool {
	switch key {
	case "Enter", "Backspace", "Tab", "Escape", "ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown":
		return true
	default:
		return false
	}
}

func (h *Handler) armDragWatchdogLocked() {
	h.stopDragWatchdogLocked()
	serial, token := h.dragSerial, h.token
	h.dragTimer = time.AfterFunc(dragTimeout, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.dragging && h.dragSerial == serial && h.token == token {
			h.releaseDragLocked()
		}
	})
}

func (h *Handler) stopDragWatchdogLocked() {
	h.dragSerial++
	if h.dragTimer != nil {
		h.dragTimer.Stop()
		h.dragTimer = nil
	}
}

func (h *Handler) forceMouseUpLocked(x, y float64) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := h.desktop.Pointer(ctx, "dragEnd", x, y); err != nil {
		log.Printf("[remote-desktop] failed to release remote mouse button: %v", err)
	}
}

func (h *Handler) releaseDragLocked() {
	h.stopDragWatchdogLocked()
	if !h.dragging {
		return
	}
	h.dragging = false
	h.forceMouseUpLocked(h.dragX, h.dragY)
}
