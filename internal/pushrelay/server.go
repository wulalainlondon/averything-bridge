package pushrelay

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Sender interface {
	Send(context.Context, string, Message) ProviderResult
}

type Server struct {
	store  *Store
	sender Sender
	now    func() time.Time
}

func NewServer(store *Store, sender Sender) *Server {
	return &Server{store: store, sender: sender, now: time.Now}
}

func reply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func reject(w http.ResponseWriter, status int, code string) { reply(w, status, Response{Error: code}) }

func (s *Server) limited(w http.ResponseWriter, r *http.Request, bucket string, limit int, window time.Duration) bool {
	allowed, err := s.store.rate(r.Context(), bucket, limit, window, s.now())
	if err != nil {
		reject(w, 503, "unavailable")
		return true
	}
	if !allowed {
		w.Header().Set("Retry-After", strconv.Itoa(int(window/time.Second)))
		reject(w, 429, "rate_limited")
		return true
	}
	return false
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" && r.Method == "GET" {
		reply(w, 200, Response{Status: "ok"})
		return
	}
	// This endpoint must be served behind HTTPS. Requests carry a Bridge-scoped
	// credential, not a Firebase key. No CORS, redirects, or arbitrary proxy APIs.
	if r.Method != "POST" {
		reject(w, 405, "method_not_allowed")
		return
	}
	if r.Header.Get("Content-Type") != "application/json" {
		reject(w, 415, "json_required")
		return
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	// Ignore caller-supplied forwarded IP headers. Behind a proxy this is a
	// conservative aggregate safety limit, not a per-end-user identity claim.
	if s.limited(w, r, "ip:"+digest(ip), 300, time.Minute) {
		return
	}
	credential := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	bridge, valid := credentialID(credential)
	if !valid || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		reject(w, 401, "unauthorized")
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		reject(w, 413, "request_too_large")
		return
	}
	if r.URL.Path == "/v1/enroll" {
		var input struct {
			Authority string `json:"authority_instance_id"`
		}
		if strictJSON(data, &input) != nil || !identifier.MatchString(input.Authority) {
			reject(w, 400, "invalid_request")
			return
		}
		if s.limited(w, r, "enrollment-global", 100, time.Hour) {
			return
		}
		if err = s.store.enroll(r.Context(), bridge, input.Authority); err != nil {
			reject(w, 403, "not_authorized")
			return
		}
		reply(w, 200, struct {
			BridgeID string `json:"bridge_id"`
		}{bridge})
		return
	}
	authority, err := s.store.authority(r.Context(), bridge)
	if err != nil {
		if errors.Is(err, ErrDenied) {
			reject(w, 401, "unauthorized")
		} else {
			reject(w, 503, "unavailable")
		}
		return
	}
	switch r.URL.Path {
	case "/v1/live-activities":
		s.handleLiveEnrollment(w, r, bridge, data)
	case "/v1/devices/challenge":
		s.beginChallenge(w, r, bridge, data)
	case "/v1/devices/confirm":
		var input ConfirmRequest
		if strictJSON(data, &input) != nil || len(input.Code) != 6 {
			reject(w, 400, "invalid_request")
			return
		}
		if s.limited(w, r, "confirm:"+bridge, 30, time.Minute) {
			return
		}
		if err = s.store.confirm(r.Context(), bridge, input, s.now().Unix()); err != nil {
			reject(w, 403, "verification_failed")
			return
		}
		reply(w, 200, Response{Status: "approved"})
	case "/v1/devices/list":
		if strictJSON(data, &struct{}{}) != nil {
			reject(w, 400, "invalid_request")
			return
		}
		devices, err := s.store.devices(r.Context(), bridge)
		if err != nil {
			reject(w, 503, "unavailable")
			return
		}
		reply(w, 200, devices)
	case "/v1/devices/revoke":
		var input struct {
			DeviceID  string `json:"device_id"`
			TokenHash string `json:"token_sha256"`
		}
		if strictJSON(data, &input) != nil || !identifier.MatchString(input.DeviceID) || len(input.TokenHash) != 64 {
			reject(w, 400, "invalid_request")
			return
		}
		if err = s.store.revokeDevice(r.Context(), bridge, input.DeviceID, input.TokenHash); err != nil {
			reject(w, 503, "unavailable")
			return
		}
		reply(w, 200, Response{Status: "revoked"})
	case "/v1/notifications":
		s.send(w, r, bridge, authority, data)
	default:
		reject(w, 404, "not_found")
	}
}

func (s *Server) beginChallenge(w http.ResponseWriter, r *http.Request, bridge string, data []byte) {
	var input ChallengeRequest
	if strictJSON(data, &input) != nil || !identifier.MatchString(input.DeviceID) || len(input.Token) < 16 || len(input.Token) > 4096 || (input.Platform != "" && input.Platform != "ios" && input.Platform != "android") {
		reject(w, 400, "invalid_request")
		return
	}
	// Unverified phones can receive only this fixed verification message. No
	// client-selected title/body/deep-link/control payload is accepted here.
	for _, rule := range []struct {
		bucket string
		limit  int
		window time.Duration
	}{
		{"challenge-bridge:" + bridge, 5, 10 * time.Minute},
		{"challenge-token:" + digest(input.Token), 1, time.Minute},
		{"challenge-token-day:" + digest(input.Token), 8, 24 * time.Hour},
		{"challenge-global", 1000, time.Hour},
	} {
		if s.limited(w, r, rule.bucket, rule.limit, rule.window) {
			return
		}
	}
	codeNumber, err := rand.Int(rand.Reader, big.NewInt(1000000))
	if err != nil {
		reject(w, 503, "unavailable")
		return
	}
	code := fmt.Sprintf("%06d", codeNumber.Int64())
	id, err := randomID("pc_", 24)
	if err != nil {
		reject(w, 503, "unavailable")
		return
	}
	expires := s.now().Add(5 * time.Minute).Unix()
	if err = s.store.challenge(r.Context(), id, bridge, input, code, expires); err != nil {
		reject(w, 503, "unavailable")
		return
	}
	alert := &Notification{Title: "Averything 推播配對", Body: "驗證碼 " + code + "。僅在你剛開始配對的電腦輸入；未要求配對請忽略。"}
	message := Message{Notification: alert, Data: map[string]string{"type": "push_pairing"}, Android: &Android{Priority: "high", TTL: "300s"}, APNS: &APNS{Headers: map[string]string{"apns-push-type": "alert", "apns-priority": "10", "apns-expiration": strconv.FormatInt(expires, 10)}}}
	message.APNS.Payload.APS.Alert = alert
	result := s.sender.Send(r.Context(), input.Token, message)
	if result.Status != Sent {
		reject(w, 502, "verification_delivery_failed")
		return
	}
	// The code is never returned to the requesting Bridge, even for debugging.
	reply(w, 200, ChallengeResponse{ChallengeID: id, ExpiresAt: expires})
}

func (s *Server) send(w http.ResponseWriter, r *http.Request, bridge, authority string, data []byte) {
	var input SendRequest
	if strictJSON(data, &input) != nil || !identifier.MatchString(input.DeviceID) || !input.Message.validate(authority) || !validRequestID(input.RequestID, s.now()) {
		reject(w, 400, "invalid_request")
		return
	}
	token, err := s.store.token(r.Context(), bridge, input.DeviceID, input.TokenHash)
	if err != nil {
		if errors.Is(err, ErrDenied) {
			reject(w, 403, "device_not_approved")
		} else {
			reject(w, 503, "unavailable")
		}
		return
	}
	for _, rule := range []struct {
		bucket string
		limit  int
	}{{"send:" + bridge, 240}, {"send-global", 2000}} {
		if s.limited(w, r, rule.bucket, rule.limit, time.Minute) {
			return
		}
	}
	if delivery := input.Message.LiveActivity; delivery != nil {
		if delivery.Timestamp > s.now().Unix()+5 || delivery.Timestamp < s.now().Add(-time.Hour).Unix() {
			reject(w, 400, "invalid_timestamp")
			return
		}
		if _, failure := s.store.liveToken(r.Context(), bridge, input.DeviceID, input.TokenHash, delivery.ActivityID, s.now().Unix()); failure != nil {
			reject(w, 403, "activity_not_registered")
			return
		}
	}
	submit, result, err := s.store.reserve(r.Context(), bridge, input, s.now().Unix())
	if err != nil {
		if err.Error() == "idempotency_conflict" {
			reject(w, 409, "idempotency_conflict")
		} else {
			reject(w, 503, "unavailable")
		}
		return
	}
	if submit {
		// Revocation is checked again immediately before contacting Google.
		if _, err = s.store.authority(r.Context(), bridge); err != nil {
			reject(w, 401, "unauthorized")
			return
		}
		if _, err = s.store.token(r.Context(), bridge, input.DeviceID, input.TokenHash); err != nil {
			reject(w, 403, "device_not_approved")
			return
		}
		if delivery := input.Message.LiveActivity; delivery != nil {
			if delivery.Timestamp > s.now().Unix()+5 || delivery.Timestamp < s.now().Add(-time.Hour).Unix() {
				reject(w, 400, "invalid_timestamp")
				return
			}
			liveToken, failure := s.store.liveToken(r.Context(), bridge, input.DeviceID, input.TokenHash, delivery.ActivityID, s.now().Unix())
			if failure != nil {
				reject(w, 403, "activity_not_registered")
				return
			}
			input.Message.activityToken = liveToken
		}
		result = s.sender.Send(r.Context(), token, input.Message)
		// Persist independently of an HTTP client disconnect. Do not undo an
		// accepted send because the caller stopped waiting for the response.
		persistCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = s.store.finish(persistCtx, bridge, input.RequestID, result, s.now().Unix())
		if result.Error == "UNREGISTERED" {
			_ = s.store.invalidateDevice(persistCtx, bridge, input.DeviceID, token)
		}
		cancel()
		if err != nil {
			reject(w, 503, "delivery_uncertain")
			return
		}
	}
	switch result.Status {
	case Sent:
		reply(w, 200, Response{Status: Sent})
	case Retryable:
		w.Header().Set("Retry-After", "2")
		reject(w, 503, "provider_retryable")
	case Failed:
		reject(w, 422, result.Error)
	default:
		reply(w, 202, Response{Status: Uncertain, Error: "delivery_uncertain"})
	}
}

func validRequestID(id string, now time.Time) bool {
	parts := strings.SplitN(id, "_", 2)
	if len(parts) != 2 || len(parts[1]) < 20 || len(parts[1]) > 64 {
		return false
	}
	stamp, err := strconv.ParseInt(parts[0], 10, 64)
	return err == nil && stamp <= now.Unix()+120 && stamp > now.Unix()-24*3600 && identifier.MatchString(parts[1])
}

// Prune is an explicit service maintenance operation; no notification payload
// or credential is retained in receipts, only content digests and outcomes.
func (s *Server) Prune(ctx context.Context) error { return s.store.prune(ctx, s.now().Unix()) }
