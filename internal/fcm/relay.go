package fcm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"everything-go/internal/pushrelay"
)

func NewRelay(address, authority, identityPath, registryPath string) (*Notifier, error) {
	client, err := pushrelay.NewClient(address, authority, identityPath)
	if err != nil {
		return nil, err
	}
	n := &Notifier{relay: client, registryPath: registryPath, http: http.DefaultClient, devices: make(map[string]deviceRegistration), statusLastSent: make(map[string]time.Time), statusLastPhase: make(map[string]string), statusPending: make(map[string]v1message), statusTimers: make(map[string]*time.Timer), statusMinInterval: 15 * time.Second}
	n.loadRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	n.relayCancel = cancel
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			flush, stop := context.WithTimeout(ctx, 20*time.Second)
			_ = client.FlushRevocations(flush)
			stop()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return n, nil
}

func (n *Notifier) Close() {
	if n == nil {
		return
	}
	n.closeLiveActivities()
	if n.relayCancel != nil {
		n.relayCancel()
	}
	n.statusMu.Lock()
	defer n.statusMu.Unlock()
	for _, timer := range n.statusTimers {
		timer.Stop()
	}
}

func (n *Notifier) RelayEnabled() bool { return n != nil && n.relay != nil }

// SetDeviceFilter must consult the current pairing state. A cached, previously
// approved token cannot keep receiving events after local unpairing, even if
// broker revocation temporarily fails or an old socket submits another token.
func (n *Notifier) SetDeviceFilter(allowed func(string) bool) {
	n.mu.Lock()
	n.deviceAllowed = allowed
	n.mu.Unlock()
}

type RelayDevice struct {
	DeviceID string `json:"device_id"`
	Platform string `json:"platform"`
	Approved bool   `json:"approved"`
}

func (n *Notifier) RelayDevices(ctx context.Context, allowed map[string]bool) ([]RelayDevice, error) {
	if !n.RelayEnabled() {
		return nil, errors.New("push relay not configured")
	}
	registered, err := n.relay.Devices(ctx)
	if err != nil {
		var failure *pushrelay.APIError
		// A fresh identity has not enrolled yet. Display local paired phones,
		// but do not enroll/network-send verification without a user action.
		if !errors.As(err, &failure) || failure.Code != "unauthorized" {
			return nil, err
		}
	}
	approved := make(map[string]string)
	for _, device := range registered {
		approved[device.DeviceID] = device.TokenHash
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	result := make([]RelayDevice, 0)
	for id, device := range n.devices {
		if allowed[id] && device.Token != "" {
			result = append(result, RelayDevice{DeviceID: id, Platform: device.Platform, Approved: approved[id] == pushrelay.TokenHash(device.Token)})
		}
	}
	return result, nil
}

func (n *Notifier) BeginRelayPairing(ctx context.Context, id string) (pushrelay.ChallengeResponse, error) {
	if !n.RelayEnabled() {
		return pushrelay.ChallengeResponse{}, errors.New("push relay not configured")
	}
	n.mu.RLock()
	registration, ok := n.devices[id]
	n.mu.RUnlock()
	if !ok || registration.Token == "" {
		return pushrelay.ChallengeResponse{}, errors.New("device registration unavailable")
	}
	return n.relay.BeginPairing(ctx, pushrelay.ChallengeRequest{DeviceID: id, Token: registration.Token, Platform: registration.Platform})
}
func (n *Notifier) ConfirmRelayPairing(ctx context.Context, id, code string) error {
	if !n.RelayEnabled() {
		return errors.New("push relay not configured")
	}
	return n.relay.ConfirmPairing(ctx, pushrelay.ConfirmRequest{ChallengeID: id, Code: code})
}

func (n *Notifier) sendRelay(msg v1message, kind string, dst target) {
	filtered, allowed := n.messageForDevice(msg, kind, dst)
	if !allowed {
		return
	}
	body, err := json.Marshal(filtered)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
	defer cancel()
	if err = n.relay.SendFCM(ctx, dst.deviceID, dst.token, body, func() bool {
		current, allowed := n.messageForDevice(msg, kind, dst)
		if !allowed {
			return false
		}
		currentBody, err := json.Marshal(current)
		return err == nil && bytes.Equal(body, currentBody)
	}); err != nil {
		var failure *pushrelay.APIError
		if errors.As(err, &failure) && failure.Code == "UNREGISTERED" {
			n.invalidate(dst)
		}
		// APIError contains a validated machine code only; never payloads,
		// Google credentials or arbitrary HTTP response bodies.
		log.Printf("[fcm] relay notification not accepted: %v", err)
		return
	}
	log.Printf("[fcm] %s relay notification accepted", kind)
}

// RemoveDevice stops local delivery immediately. Revocation is best-effort at
// the broker; it must not block the authenticated unpair acknowledgement.
func (n *Notifier) RemoveDevice(id string) {
	if n == nil {
		return
	}
	n.removeLiveDevice(id)
	n.mu.Lock()
	previous := n.devices[id]
	delete(n.devices, id)
	n.persistLocked()
	n.mu.Unlock()
	if n.relay != nil && previous.Token != "" {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := n.relay.RevokeDevice(ctx, id, pushrelay.TokenHash(previous.Token)); err != nil {
				log.Printf("[fcm] relay device revocation pending: %v", err)
			}
		}()
	}
}
