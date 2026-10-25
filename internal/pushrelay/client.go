package pushrelay

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type clientIdentity struct {
	URL                string            `json:"url"`
	Authority          string            `json:"authority_instance_id"`
	Credential         string            `json:"credential"`
	PendingRevocations map[string]string `json:"pending_revocations,omitempty"`
}

type Client struct {
	identity clientIdentity
	http     *http.Client
	mu       sync.Mutex
	path     string
}
type APIError struct {
	Code       string
	HTTPStatus int
}

func (e *APIError) Error() string { return "push relay: " + e.Code }

// NewClient retains a private per-installation broker credential. Its file must
// never be published. Changing the service URL cannot forward an existing
// credential to a different host; explicit re-enrollment needs a new file.
func NewClient(address, authority, identityPath string) (*Client, error) {
	if len(address) > 2048 {
		return nil, errors.New("invalid push relay URL")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("invalid push relay URL")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "https" && !(u.Scheme == "http" && ip != nil && ip.IsLoopback()) {
		return nil, errors.New("push relay requires HTTPS (HTTP only for literal loopback)")
	}
	if !identifier.MatchString(authority) {
		return nil, errors.New("invalid bridge authority")
	}
	address = strings.TrimSuffix(u.String(), "/")
	identity := clientIdentity{URL: address, Authority: authority}
	info, err := os.Lstat(identityPath)
	if err == nil {
		if !info.Mode().IsRegular() || info.Size() > 64<<10 || (runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
			return nil, errors.New("push relay credential must be a private regular file (0600)")
		}
		data, readErr := os.ReadFile(identityPath)
		if readErr != nil || strictJSONSize(data, &identity, 64<<10) != nil {
			return nil, errors.New("push relay credential unavailable")
		}
		if identity.URL != address || identity.Authority != authority {
			return nil, errors.New("push relay credential belongs to another URL or Bridge; explicit re-enrollment required")
		}
		if _, ok := credentialID(identity.Credential); !ok {
			return nil, errors.New("invalid push relay credential")
		}
		if len(identity.PendingRevocations) > 128 {
			return nil, errors.New("invalid push relay revocation state")
		}
		for id, hash := range identity.PendingRevocations {
			if !identifier.MatchString(id) || len(hash) != 64 {
				return nil, errors.New("invalid push relay revocation state")
			}
		}
	} else if errors.Is(err, os.ErrNotExist) {
		identity.Credential, err = randomID("prc_", 32)
		if err != nil {
			return nil, errors.New("push relay credential creation failed")
		}
		if err = os.MkdirAll(filepath.Dir(identityPath), 0700); err != nil {
			return nil, errors.New("push relay credential directory unavailable")
		}
		data, _ := jsonBytes(identity)
		// O_EXCL prevents two Bridge processes from silently replacing one
		// another's broker identity. A failed write is not a usable identity.
		file, openErr := os.OpenFile(identityPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if openErr != nil {
			return nil, errors.New("push relay credential creation failed")
		}
		_, writeErr := file.Write(data)
		syncErr := file.Sync()
		closeErr := file.Close()
		if writeErr != nil || syncErr != nil || closeErr != nil {
			return nil, errors.New("push relay credential could not be saved")
		}
	} else {
		return nil, errors.New("push relay credential unavailable")
	}
	return &Client{identity: identity, path: identityPath, http: &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (c *Client) request(ctx context.Context, path string, input, output any) error {
	data, err := jsonBytes(input)
	if err != nil || len(data) > maxBody {
		return &APIError{Code: "invalid_request"}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.identity.URL+path, bytes.NewReader(data))
	if err != nil {
		return &APIError{Code: "unavailable"}
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.identity.Credential)
	response, err := c.http.Do(request)
	if err != nil {
		return &APIError{Code: "unavailable"}
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBody+1))
	if err != nil || len(body) > maxBody {
		return &APIError{Code: "invalid_response", HTTPStatus: response.StatusCode}
	}
	if response.StatusCode != 200 {
		var result Response
		_ = strictJSON(body, &result)
		// Accept only machine codes, never an arbitrary relay response body.
		code := result.Error
		if !regexpCode(code) {
			code = "request_rejected"
		}
		return &APIError{Code: code, HTTPStatus: response.StatusCode}
	}
	if output != nil && strictJSON(body, output) != nil {
		return &APIError{Code: "invalid_response", HTTPStatus: response.StatusCode}
	}
	return nil
}
func regexpCode(code string) bool {
	switch code {
	case "unauthorized", "not_authorized", "invalid_request", "request_too_large", "json_required", "method_not_allowed", "not_found", "unavailable", "rate_limited", "verification_failed", "verification_delivery_failed", "device_not_approved", "idempotency_conflict", "delivery_uncertain", "provider_retryable", "provider_rejected", "invalid_message", "UNREGISTERED":
		return true
	case "device_not_verified", "activity_not_registered", "invalid_timestamp":
		return true
	default:
		return false
	}
}

func (c *Client) Enroll(ctx context.Context) error {
	var result struct {
		BridgeID string `json:"bridge_id"`
	}
	return c.request(ctx, "/v1/enroll", map[string]string{"authority_instance_id": c.identity.Authority}, &result)
}
func (c *Client) BeginPairing(ctx context.Context, input ChallengeRequest) (ChallengeResponse, error) {
	if err := c.Enroll(ctx); err != nil {
		return ChallengeResponse{}, err
	}
	var result ChallengeResponse
	err := c.request(ctx, "/v1/devices/challenge", input, &result)
	return result, err
}
func (c *Client) ConfirmPairing(ctx context.Context, input ConfirmRequest) error {
	var response Response
	return c.request(ctx, "/v1/devices/confirm", input, &response)
}
func (c *Client) Devices(ctx context.Context) ([]DeviceStatus, error) {
	var result []DeviceStatus
	err := c.request(ctx, "/v1/devices/list", struct{}{}, &result)
	return result, err
}
func (c *Client) RevokeDevice(ctx context.Context, id string, expectedHash ...string) error {
	hash := ""
	if len(expectedHash) > 0 {
		hash = expectedHash[0]
	} else {
		devices, err := c.Devices(ctx)
		if err != nil {
			return err
		}
		for _, device := range devices {
			if device.DeviceID == id {
				hash = device.TokenHash
				break
			}
		}
	}
	if hash == "" {
		return nil
	}
	c.mu.Lock()
	if c.identity.PendingRevocations == nil {
		c.identity.PendingRevocations = make(map[string]string)
	}
	if len(c.identity.PendingRevocations) >= 128 && c.identity.PendingRevocations[id] == "" {
		c.mu.Unlock()
		return &APIError{Code: "revocation_queue_full"}
	}
	c.identity.PendingRevocations[id] = hash
	err := c.persistLocked()
	c.mu.Unlock()
	if err != nil {
		return err
	}
	return c.FlushRevocations(ctx)
}

func (c *Client) persistLocked() error {
	data, err := jsonBytes(c.identity)
	if err != nil {
		return &APIError{Code: "credential_save_failed"}
	}
	file, err := os.CreateTemp(filepath.Dir(c.path), ".push-relay-client-*")
	if err != nil {
		return &APIError{Code: "credential_save_failed"}
	}
	name := file.Name()
	defer os.Remove(name)
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return &APIError{Code: "credential_save_failed"}
	}
	if err = os.Rename(name, c.path); err != nil {
		return &APIError{Code: "credential_save_failed"}
	}
	return nil
}

// FlushRevocations survives Bridge restart and service outages. Each entry
// names the original token fingerprint: a stale queued revoke cannot remove a
// new phone registration verified under the same stable device ID.
func (c *Client) FlushRevocations(ctx context.Context) error {
	c.mu.Lock()
	pending := make(map[string]string, len(c.identity.PendingRevocations))
	for id, hash := range c.identity.PendingRevocations {
		pending[id] = hash
	}
	c.mu.Unlock()
	for id, hash := range pending {
		var response Response
		err := c.request(ctx, "/v1/devices/revoke", map[string]string{"device_id": id, "token_sha256": hash}, &response)
		var failure *APIError
		// A revoked/unrecognized Bridge has no usable delivery capability.
		if err != nil && !(errors.As(err, &failure) && failure.Code == "unauthorized") {
			return err
		}
		c.mu.Lock()
		if c.identity.PendingRevocations[id] == hash {
			delete(c.identity.PendingRevocations, id)
		}
		err = c.persistLocked()
		c.mu.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// SendFCM preserves existing iOS/Android payloads. Raw device tokens are removed
// before submission; the service chooses the verified destination. One ID is
// used across transport/retryable-provider retries, never an ambiguous resend.
func (c *Client) SendFCM(ctx context.Context, device, token string, envelope []byte, guards ...func() bool) error {
	var input struct {
		Message struct {
			Token        string            `json:"token"`
			Notification *Notification     `json:"notification,omitempty"`
			Data         map[string]string `json:"data,omitempty"`
			Android      *Android          `json:"android,omitempty"`
			APNS         *APNS             `json:"apns,omitempty"`
		} `json:"message"`
	}
	if strictJSON(envelope, &input) != nil || input.Message.Token != token {
		return &APIError{Code: "invalid_request"}
	}
	nonce, err := randomID("", 18)
	if err != nil {
		return &APIError{Code: "unavailable"}
	}
	message := Message{Notification: input.Message.Notification, Data: input.Message.Data, Android: input.Message.Android, APNS: input.Message.APNS}
	request := SendRequest{RequestID: strconv.FormatInt(time.Now().Unix(), 10) + "_" + nonce, DeviceID: device, TokenHash: digest(token), Message: message}
	for attempt := 0; attempt < 3; attempt++ {
		if len(guards) > 0 && !guards[0]() {
			return &APIError{Code: "notification_no_longer_allowed"}
		}
		var response Response
		err = c.request(ctx, "/v1/notifications", request, &response)
		if err == nil {
			if response.Status != Sent {
				return &APIError{Code: "invalid_response"}
			}
			return nil
		}
		var failure *APIError
		if !errors.As(err, &failure) || failure.Code == "delivery_uncertain" || !(failure.Code == "unavailable" || failure.Code == "provider_retryable") {
			return err
		}
		if attempt < 2 {
			timer := time.NewTimer(time.Duration(attempt+1) * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return &APIError{Code: "unavailable"}
			case <-timer.C:
			}
		}
	}
	return err
}

func TokenHash(token string) string { return digest(token) }
func (c *Client) BridgeID() string  { id, _ := credentialID(c.identity.Credential); return id }
func (c *Client) String() string    { return fmt.Sprintf("push relay (%s)", c.identity.URL) }
