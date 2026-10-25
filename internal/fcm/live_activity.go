package fcm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"everything-go/internal/liveactivity"
	"everything-go/internal/pushrelay"
)

type liveLease struct {
	DeviceID         string             `json:"deviceId"`
	ActivityID       string             `json:"activityId"`
	Authority        string             `json:"authority"`
	SessionID        string             `json:"sessionId"`
	RequestID        string             `json:"requestId"`
	Token            string             `json:"token"`
	DeviceTokenHash  string             `json:"deviceTokenHash"`
	CredentialDigest string             `json:"credentialDigest"`
	ExpiresAt        int64              `json:"expiresAt"`
	Pending          liveactivity.State `json:"pending"`
	SentRevision     uint64             `json:"sentRevision"`
	SentAt           int64              `json:"sentAt"`
	Terminal         bool               `json:"terminal"`
}
type liveRegistry struct {
	mu        sync.Mutex
	leases    map[string]liveLease
	timers    map[string]*time.Timer
	authorize func(string, string) bool
	closed    bool
}

func (n *Notifier) SetLiveActivityAuthorizer(check func(string, string) bool) {
	n.live.mu.Lock()
	n.live.authorize = check
	n.live.mu.Unlock()
}
func (n *Notifier) liveLoadLocked() {
	if n.live.leases != nil {
		return
	}
	n.live.leases = make(map[string]liveLease)
	n.live.timers = make(map[string]*time.Timer)
	body, err := os.ReadFile(n.registryPath + ".live-activities.json")
	if err != nil || len(body) > 4<<20 {
		return
	}
	var saved []liveLease
	if json.Unmarshal(body, &saved) != nil {
		return
	}
	now := time.Now().Unix()
	for _, lease := range saved {
		if len(n.live.leases) >= 2000 {
			break
		}
		if lease.ExpiresAt > now && lease.ExpiresAt <= now+8*3600 && liveactivity.ValidToken(lease.Token) {
			n.live.leases[lease.DeviceID] = lease
		}
	}
}
func (n *Notifier) liveSaveLocked() error {
	if n.registryPath == "" {
		return errors.New("storage_unavailable")
	}
	values := make([]liveLease, 0, len(n.live.leases))
	for _, value := range n.live.leases {
		if value.ExpiresAt > time.Now().Unix() {
			values = append(values, value)
		}
	}
	body, err := json.Marshal(values)
	if err != nil {
		return err
	}
	file := n.registryPath + ".live-activities.json"
	if err = os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(file), ".live-activities-")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err = tmp.Chmod(0600); err == nil {
		_, err = tmp.Write(body)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, file)
	}
	return err
}
func (n *Notifier) RegisterLiveActivity(ctx context.Context, deviceID, activityID, authority, sessionID, requestID, token, credentialDigest string, initial liveactivity.State) (int64, error) {
	if !liveactivity.ValidToken(token) || !initial.Valid() || initial.Terminal() || len(activityID) == 0 || len(activityID) > 160 || requestID == "" {
		return 0, errors.New("invalid_registration")
	}
	n.mu.RLock()
	device, exists := n.devices[deviceID]
	n.mu.RUnlock()
	if !exists || device.Platform != "ios" || device.Token == "" || (n.relay == nil && (n.tokenSource == nil || n.endpoint == "")) {
		return 0, errors.New("push_unavailable")
	}
	n.live.mu.Lock()
	n.liveLoadLocked()
	check := n.live.authorize
	n.live.mu.Unlock()
	if check == nil || !check(deviceID, credentialDigest) {
		return 0, errors.New("device_auth_required")
	}
	expires := time.Now().Add(8 * time.Hour).Unix()
	if n.relay != nil {
		if err := n.relay.RegisterLiveActivity(ctx, deviceID, device.Token, activityID, token, expires); err != nil {
			return 0, errors.New("push_unavailable")
		}
	}
	n.live.mu.Lock()
	defer n.live.mu.Unlock()
	if n.live.closed || !check(deviceID, credentialDigest) {
		return 0, errors.New("device_auth_required")
	}
	if len(n.live.leases) >= 2000 {
		if _, ok := n.live.leases[deviceID]; !ok {
			return 0, errors.New("too_many_activities")
		}
	}
	old, present := n.live.leases[deviceID]
	if present && old.ActivityID == activityID && old.Authority == authority && old.SessionID == sessionID && old.RequestID == requestID && old.Token == token && old.ExpiresAt > time.Now().Unix() {
		return old.ExpiresAt, nil
	}
	if timer := n.live.timers[deviceID]; timer != nil {
		timer.Stop()
		delete(n.live.timers, deviceID)
	}
	n.live.leases[deviceID] = liveLease{DeviceID: deviceID, ActivityID: activityID, Authority: authority, SessionID: sessionID, RequestID: requestID,
		Token: token, DeviceTokenHash: pushrelay.TokenHash(device.Token), CredentialDigest: credentialDigest, ExpiresAt: expires, Pending: initial}
	if err := n.liveSaveLocked(); err != nil {
		if present {
			n.live.leases[deviceID] = old
		} else {
			delete(n.live.leases, deviceID)
		}
		return 0, errors.New("storage_unavailable")
	}
	n.live.timers[deviceID] = time.AfterFunc(time.Millisecond, func() { n.flushLiveActivity(deviceID) })
	return expires, nil
}
func (n *Notifier) UnregisterLiveActivity(deviceID, activityID string) {
	n.live.mu.Lock()
	defer n.live.mu.Unlock()
	n.liveLoadLocked()
	if lease, ok := n.live.leases[deviceID]; ok && lease.ActivityID == activityID {
		delete(n.live.leases, deviceID)
		if timer := n.live.timers[deviceID]; timer != nil {
			timer.Stop()
			delete(n.live.timers, deviceID)
		}
		_ = n.liveSaveLocked()
	}
}
func (n *Notifier) NotifyLiveActivity(authority, sessionID, requestID string, state liveactivity.State) {
	if !state.Valid() || (requestID == "" && !((state.Phase == "interrupted" || state.Phase == "closed") && state.StartedAt == 0)) {
		return
	}
	n.live.mu.Lock()
	defer n.live.mu.Unlock()
	n.liveLoadLocked()
	if n.live.closed {
		return
	}
	now := time.Now().Unix()
	for deviceID, lease := range n.live.leases {
		if lease.ExpiresAt <= now {
			delete(n.live.leases, deviceID)
			continue
		}
		if lease.Authority != authority || lease.SessionID != sessionID || (requestID != "" && lease.RequestID != requestID) || !lease.Pending.Accepts(state) || (lease.Terminal && !state.Terminal()) {
			continue
		}
		lease.Pending = state
		n.live.leases[deviceID] = lease
		delay := time.Duration(15-(now-lease.SentAt)) * time.Second
		if state.Terminal() || state.Phase == "waiting" || delay < 0 {
			delay = 0
		}
		if lease.SentAt >= now {
			delay = time.Second
		} // APNs ordering timestamps are seconds, not revision integers.
		if timer := n.live.timers[deviceID]; timer != nil {
			timer.Stop()
		}
		n.live.timers[deviceID] = time.AfterFunc(delay, func() { n.flushLiveActivity(deviceID) })
	}
	_ = n.liveSaveLocked()
}
func (n *Notifier) flushLiveActivity(deviceID string) {
	n.live.mu.Lock()
	n.liveLoadLocked()
	delete(n.live.timers, deviceID)
	lease, exists := n.live.leases[deviceID]
	check := n.live.authorize
	if !exists || n.live.closed || lease.ExpiresAt <= time.Now().Unix() || lease.Pending.Revision <= lease.SentRevision {
		n.live.mu.Unlock()
		return
	}
	delivery := liveactivity.Delivery{ActivityID: lease.ActivityID, Timestamp: time.Now().Unix(), Event: "update", State: lease.Pending, StaleDate: time.Now().Add(3 * time.Minute).Unix()}
	if lease.Pending.Terminal() {
		delivery.Event = "end"
		delivery.StaleDate = 0
		delivery.DismissalDate = time.Now().Add(15 * time.Minute).Unix()
	}
	// Reserve before releasing the lock. A concurrent flush never sends a
	// second revision with the same timestamp or resurrects an ended run.
	lease.SentRevision = lease.Pending.Revision
	lease.SentAt = delivery.Timestamp
	lease.Terminal = delivery.Event == "end"
	n.live.leases[deviceID] = lease
	_ = n.liveSaveLocked()
	n.live.mu.Unlock()
	guard := func() bool {
		if check == nil || !check(deviceID, lease.CredentialDigest) {
			return false
		}
		n.live.mu.Lock()
		current, ok := n.live.leases[deviceID]
		closed := n.live.closed
		n.live.mu.Unlock()
		n.mu.RLock()
		device, registered := n.devices[deviceID]
		n.mu.RUnlock()
		return !closed && ok && registered && device.Platform == "ios" && current.ActivityID == lease.ActivityID && current.Token == lease.Token && current.RequestID == lease.RequestID &&
			pushrelay.TokenHash(device.Token) == lease.DeviceTokenHash && current.ExpiresAt > time.Now().Unix() && current.Pending.Revision == delivery.State.Revision
	}
	if !guard() {
		return
	}
	n.mu.RLock()
	device := n.devices[deviceID]
	n.mu.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	if n.relay != nil {
		_ = n.relay.SendLiveActivity(ctx, deviceID, device.Token, delivery, guard)
		return
	}
	for attempt := 0; attempt < 3 && guard(); attempt++ {
		err := n.sendDirectLiveActivity(ctx, device.Token, lease.Token, delivery, guard)
		if err == nil || (err.Error() != "provider_retryable" && err.Error() != "provider_auth_unavailable") {
			break
		}
		timer := time.NewTimer(time.Duration(attempt+1) * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}
func (n *Notifier) sendDirectLiveActivity(ctx context.Context, fcmToken, activityToken string, delivery liveactivity.Delivery, guard func() bool) error {
	if !delivery.Valid() || !guard() {
		return errors.New("not_allowed")
	}
	aps := map[string]any{"timestamp": delivery.Timestamp, "event": delivery.Event, "content-state": delivery.State}
	if delivery.StaleDate > 0 {
		aps["stale-date"] = delivery.StaleDate
	}
	if delivery.DismissalDate > 0 {
		aps["dismissal-date"] = delivery.DismissalDate
	}
	envelope := map[string]any{"message": map[string]any{"token": fcmToken, "apns": map[string]any{"live_activity_token": activityToken,
		"headers": map[string]string{"apns-push-type": "liveactivity", "apns-priority": "5", "apns-topic": "com.morrie.text.push-type.liveactivity"}, "payload": map[string]any{"aps": aps}}}}
	body, err := json.Marshal(envelope)
	if err != nil || len(body) > 4096 {
		return errors.New("invalid_payload")
	}
	auth, err := n.tokenSource.Token()
	if err != nil {
		return errors.New("provider_auth_unavailable")
	}
	if !guard() {
		return errors.New("not_allowed")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, n.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+auth.AccessToken)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Transport: n.http.Transport, Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("delivery_uncertain")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode != 200 {
		if response.StatusCode == 429 || response.StatusCode >= 500 {
			return errors.New("provider_retryable")
		}
		return errors.New("provider_rejected")
	}
	return nil
}
func (n *Notifier) closeLiveActivities() {
	n.live.mu.Lock()
	defer n.live.mu.Unlock()
	n.live.closed = true
	for _, timer := range n.live.timers {
		timer.Stop()
	}
}
func (n *Notifier) removeLiveDevice(deviceID string) {
	n.live.mu.Lock()
	defer n.live.mu.Unlock()
	n.liveLoadLocked()
	delete(n.live.leases, deviceID)
	if timer := n.live.timers[deviceID]; timer != nil {
		timer.Stop()
		delete(n.live.timers, deviceID)
	}
	_ = n.liveSaveLocked()
}
