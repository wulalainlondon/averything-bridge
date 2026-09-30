package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"everything-go/internal/backend"
	"everything-go/internal/notificationreply"
	"everything-go/internal/session"
)

const (
	notificationReplyTTL      = 24 * time.Hour
	notificationReplyMaxRunes = 500
	notificationReplyMaxBody  = 8 << 10
)

type notificationReplyRequest struct {
	ReplyID   string `json:"reply_id"`
	SessionID string `json:"session_id"`
	Content   string `json:"content"`
}

type notificationReplyResponse struct {
	ReplyID       string `json:"reply_id"`
	SessionID     string `json:"session_id"`
	Status        string `json:"status"`
	QueuePosition int    `json:"queue_position,omitempty"`
	Error         string `json:"error,omitempty"`
}

func (h *Hub) notificationReplyAction(sessionID string) (replyURL, fallbackURL, capability string, expiresAt int64) {
	if h.replyCaps == nil || sessionID == "" {
		return "", "", "", 0
	}
	host := strings.TrimSpace(h.cfg.TailscaleIP)
	if host == "" {
		host = strings.TrimSpace(h.cfg.LanIP)
	} else if lanHost := strings.TrimSpace(h.cfg.LanIP); lanHost != "" && lanHost != host && h.cfg.Port > 0 {
		fallbackURL = fmt.Sprintf("http://%s:%d/api/notification/v1/replies", lanHost, h.cfg.Port)
	}
	if host == "" || h.cfg.Port <= 0 {
		return "", "", "", 0
	}
	replyURL = fmt.Sprintf("http://%s:%d/api/notification/v1/replies", host, h.cfg.Port)
	capability, expiresAt = h.replyCaps.Issue(sessionID, notificationReplyTTL)
	return
}

func (h *Hub) ServeNotificationReplyAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeNotificationReply(w, http.StatusMethodNotAllowed, notificationReplyResponse{Status: "rejected", Error: "method_not_allowed"})
		return
	}
	var input notificationReplyRequest
	decoder := json.NewDecoder(io.LimitReader(r.Body, notificationReplyMaxBody))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil {
		writeNotificationReply(w, http.StatusBadRequest, notificationReplyResponse{Status: "rejected", Error: "invalid_json"})
		return
	}
	input.ReplyID = strings.TrimSpace(input.ReplyID)
	input.SessionID = strings.TrimSpace(input.SessionID)
	input.Content = strings.TrimSpace(input.Content)
	if input.ReplyID == "" || len(input.ReplyID) > 160 || input.SessionID == "" || input.Content == "" ||
		utf8.RuneCountInString(input.Content) > notificationReplyMaxRunes {
		writeNotificationReply(w, http.StatusBadRequest, notificationReplyResponse{ReplyID: input.ReplyID, SessionID: input.SessionID, Status: "rejected", Error: "invalid_reply"})
		return
	}
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	capability := strings.TrimSpace(strings.TrimPrefix(authorization, "Reply "))
	if capability == authorization || h.replyCaps == nil {
		writeNotificationReply(w, http.StatusUnauthorized, notificationReplyResponse{ReplyID: input.ReplyID, SessionID: input.SessionID, Status: "rejected", Error: "missing_capability"})
		return
	}
	if err := h.replyCaps.Validate(capability, input.SessionID); err != nil {
		code := "invalid_capability"
		if errors.Is(err, notificationreply.ErrExpiredCapability) {
			code = "expired_capability"
		}
		writeNotificationReply(w, http.StatusUnauthorized, notificationReplyResponse{ReplyID: input.ReplyID, SessionID: input.SessionID, Status: "rejected", Error: code})
		return
	}
	if _, ok := h.registry.Get(input.SessionID); !ok {
		writeNotificationReply(w, http.StatusNotFound, notificationReplyResponse{ReplyID: input.ReplyID, SessionID: input.SessionID, Status: "rejected", Error: "unknown_session"})
		return
	}
	if !h.controls.MobileMayWrite(input.SessionID) {
		writeNotificationReply(w, http.StatusConflict, notificationReplyResponse{ReplyID: input.ReplyID, SessionID: input.SessionID, Status: "rejected", Error: "session_controlled_by_desktop"})
		return
	}
	record, created, err := h.notificationReplies.Enqueue(input.ReplyID, input.SessionID, input.Content)
	if errors.Is(err, notificationreply.ErrReplyConflict) {
		writeNotificationReply(w, http.StatusConflict, notificationReplyResponse{ReplyID: input.ReplyID, SessionID: input.SessionID, Status: "rejected", Error: "reply_id_conflict"})
		return
	}
	if err != nil {
		writeNotificationReply(w, http.StatusInternalServerError, notificationReplyResponse{ReplyID: input.ReplyID, SessionID: input.SessionID, Status: "rejected", Error: "persistence_failed"})
		return
	}
	queuePosition := 0
	if created || record.Status == notificationreply.StatusPending {
		queuePosition, err = h.dispatchNotificationReply(input.ReplyID)
		if err != nil {
			writeNotificationReply(w, http.StatusConflict, notificationReplyResponse{ReplyID: input.ReplyID, SessionID: input.SessionID, Status: "rejected", Error: err.Error()})
			return
		}
	}
	writeNotificationReply(w, http.StatusAccepted, notificationReplyResponse{
		ReplyID: input.ReplyID, SessionID: input.SessionID, Status: "queued", QueuePosition: queuePosition,
	})
}

func writeNotificationReply(w http.ResponseWriter, status int, value notificationReplyResponse) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (h *Hub) dispatchNotificationReply(id string) (int, error) {
	record, claimed := h.notificationReplies.Claim(id)
	if !claimed {
		if record.Status == notificationreply.StatusFailed {
			return 0, errors.New(record.Error)
		}
		return 0, nil
	}
	s, ok := h.registry.Get(record.SessionID)
	if !ok {
		h.notificationReplies.MarkFailed(id, "unknown_session")
		return 0, errors.New("unknown_session")
	}
	if !h.controls.MobileMayWrite(record.SessionID) {
		h.notificationReplies.MarkFailed(id, "session_controlled_by_desktop")
		return 0, errors.New("session_controlled_by_desktop")
	}
	queuedBehindActive := s.State() != session.Idle || s.QueueLen() > 0
	queuePosition := 0
	requestID := "notification-reply-" + record.ID
	if !queuedBehindActive {
		h.updateRuntime(record.SessionID, "queued", requestID, 0, "", "")
	}
	accepted := s.Submit(func() {
		h.updateRuntime(record.SessionID, "running", requestID, s.QueueLen(), "", "")
		if err := h.exec.Send(context.Background(), s, requestID, record.Content, nil, nil); err != nil {
			if errors.Is(err, backend.ErrThreadActiveWriter) {
				h.markDesktopWriter(s)
			}
			h.notificationReplies.MarkFailed(id, err.Error())
			h.updateRuntime(record.SessionID, "failed", requestID, 0, "failed", err.Error())
			s.EndTurn()
			return
		}
		h.notificationReplies.MarkSent(id)
	})
	if !accepted {
		h.notificationReplies.MarkFailed(id, "session_closed")
		h.updateRuntime(record.SessionID, "failed", requestID, 0, "failed", "session is closed")
		return 0, errors.New("session_closed")
	}
	if queuedBehindActive {
		queuePosition = s.QueueLen()
		h.updateRuntimeQueueLength(record.SessionID, queuePosition)
	}
	return queuePosition, nil
}

func (h *Hub) resumeNotificationReplies() {
	if h.notificationReplies == nil || h.exec == nil {
		return
	}
	for _, record := range h.notificationReplies.Pending() {
		_, _ = h.dispatchNotificationReply(record.ID)
	}
}
