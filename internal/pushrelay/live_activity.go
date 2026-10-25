package pushrelay

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"everything-go/internal/liveactivity"
)

type liveEnrollment struct {
	DeviceID      string `json:"device_id"`
	TokenHash     string `json:"token_hash"`
	ActivityID    string `json:"activity_id"`
	ActivityToken string `json:"activity_token"`
	ExpiresAt     int64  `json:"expires_at"`
}

func (s *Server) handleLiveEnrollment(w http.ResponseWriter, r *http.Request, bridge string, data []byte) {
	var input liveEnrollment
	now := s.now().Unix()
	if strictJSON(data, &input) != nil || !identifier.MatchString(input.DeviceID) || !identifier.MatchString(input.ActivityID) ||
		!liveactivity.ValidToken(input.ActivityToken) || input.ExpiresAt <= now || input.ExpiresAt > now+8*3600 {
		reject(w, 400, "invalid_request")
		return
	}
	if s.limited(w, r, "live-enroll:"+bridge+":"+input.DeviceID, 30, time.Minute) {
		return
	}
	tx, err := s.store.db.BeginTx(r.Context(), nil)
	if err != nil {
		reject(w, 503, "unavailable")
		return
	}
	defer tx.Rollback()
	var token string
	if err = tx.QueryRowContext(r.Context(), `SELECT token FROM devices WHERE bridge=? AND device=?`, bridge, input.DeviceID).Scan(&token); err != nil || digest(token) != input.TokenHash {
		reject(w, 403, "device_not_verified")
		return
	}
	if _, err = tx.ExecContext(r.Context(), `DELETE FROM live_activities WHERE expires<=?`, now); err != nil {
		reject(w, 503, "unavailable")
		return
	}
	// Only one activity per verified device. Rotation replaces the old token;
	// callers cannot select a destination token in subsequent send requests.
	_, err = tx.ExecContext(r.Context(), `INSERT INTO live_activities(bridge,device,activity,token_hash,live_token,expires) VALUES(?,?,?,?,?,?)
        ON CONFLICT(bridge,device) DO UPDATE SET activity=excluded.activity,token_hash=excluded.token_hash,live_token=excluded.live_token,expires=excluded.expires`,
		bridge, input.DeviceID, input.ActivityID, input.TokenHash, input.ActivityToken, input.ExpiresAt)
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		reject(w, 503, "unavailable")
		return
	}
	reply(w, 200, Response{Status: "registered"})
}
func (s *Store) liveToken(ctx context.Context, bridge, device, tokenHash, activity string, now int64) (string, error) {
	var token string
	err := s.db.QueryRowContext(ctx, `SELECT a.live_token FROM live_activities a JOIN devices d ON d.bridge=a.bridge AND d.device=a.device
        JOIN bridges b ON b.id=a.bridge WHERE a.bridge=? AND a.device=? AND a.activity=? AND a.token_hash=? AND a.expires>? AND b.disabled=0`,
		bridge, device, activity, tokenHash, now).Scan(&token)
	if err != nil || !liveactivity.ValidToken(token) {
		return "", ErrDenied
	}
	// Token rotation at the verified device registry invalidates the lease.
	var deviceToken string
	if err = s.db.QueryRowContext(ctx, `SELECT token FROM devices WHERE bridge=? AND device=?`, bridge, device).Scan(&deviceToken); err != nil || digest(deviceToken) != tokenHash {
		return "", ErrDenied
	}
	return token, nil
}
func (c *Client) RegisterLiveActivity(ctx context.Context, device, deviceToken, activity, activityToken string, expires int64) error {
	var response Response
	input := liveEnrollment{device, digest(deviceToken), activity, activityToken, expires}
	if err := c.request(ctx, "/v1/live-activities", input, &response); err != nil {
		return err
	}
	if response.Status != "registered" {
		return &APIError{Code: "invalid_response"}
	}
	return nil
}
func (c *Client) SendLiveActivity(ctx context.Context, device, deviceToken string, delivery liveactivity.Delivery, guard func() bool) error {
	if !delivery.Valid() {
		return &APIError{Code: "invalid_request"}
	}
	nonce, err := randomID("", 18)
	if err != nil {
		return err
	}
	input := SendRequest{RequestID: strconv.FormatInt(time.Now().Unix(), 10) + "_" + nonce, DeviceID: device, TokenHash: digest(deviceToken),
		Message: Message{Data: map[string]string{"type": "live_activity", "authority_instance_id": c.identity.Authority}, LiveActivity: &delivery}}
	for attempt := 0; attempt < 3; attempt++ {
		if !guard() {
			return &APIError{Code: "notification_no_longer_allowed"}
		}
		var response Response
		err = c.request(ctx, "/v1/notifications", input, &response)
		if err == nil {
			if response.Status == Sent {
				return nil
			}
			return &APIError{Code: "invalid_response"}
		}
		var failure *APIError
		if !errors.As(err, &failure) || (failure.Code != "provider_retryable" && failure.Code != "unavailable") {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

// Enriched transport fields are server-internal and never decoded from input.
func liveAPNS(delivery liveactivity.Delivery, token string) map[string]any {
	aps := map[string]any{"timestamp": delivery.Timestamp, "event": delivery.Event, "content-state": delivery.State}
	if delivery.StaleDate != 0 {
		aps["stale-date"] = delivery.StaleDate
	}
	if delivery.DismissalDate != 0 {
		aps["dismissal-date"] = delivery.DismissalDate
	}
	return map[string]any{"live_activity_token": token, "headers": map[string]string{"apns-push-type": "liveactivity", "apns-priority": "5", "apns-topic": "com.morrie.text.push-type.liveactivity"}, "payload": map[string]any{"aps": aps}}
}
