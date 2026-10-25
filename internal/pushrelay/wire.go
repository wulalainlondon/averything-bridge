// Package pushrelay implements a device-scoped push broker. A Bridge credential
// authenticates only to this broker, never to Google; device ownership must be
// proved by a one-time code delivered to the existing mobile app.
package pushrelay

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"

	"everything-go/internal/identity"
	"everything-go/internal/liveactivity"
)

const maxBody = 16 << 10

var identifier = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,200}$`)

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func randomID(prefix string, size int) (string, error) {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(b), nil
}

func credentialID(credential string) (string, bool) {
	if !strings.HasPrefix(credential, "prc_") {
		return "", false
	}
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(credential, "prc_"))
	if err != nil || len(b) != 32 {
		return "", false
	}
	return "pr_" + digest(credential), true
}

func strictJSON(data []byte, value any) error {
	return strictJSONSize(data, value, maxBody)
}

func strictJSONSize(data []byte, value any, limit int) error {
	if len(data) > limit {
		return errors.New("request_too_large")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return errors.New("invalid_request")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("invalid_request")
	}
	return nil
}

func jsonBytes(value any) ([]byte, error) { return json.Marshal(value) }

// Message deliberately has no token, topic, condition, project or arbitrary
// transport options. Only the server inserts the verified device destination.
type Message struct {
	LiveActivity  *liveactivity.Delivery `json:"live_activity,omitempty"`
	activityToken string
	Notification  *Notification     `json:"notification,omitempty"`
	Data          map[string]string `json:"data,omitempty"`
	Android       *Android          `json:"android,omitempty"`
	APNS          *APNS             `json:"apns,omitempty"`
}
type Notification struct {
	Title string `json:"title"`
	Body  string `json:"body"`
}
type Android struct {
	Priority    string `json:"priority,omitempty"`
	TTL         string `json:"ttl,omitempty"`
	CollapseKey string `json:"collapse_key,omitempty"`
}
type APNS struct {
	Headers map[string]string `json:"headers,omitempty"`
	Payload struct {
		APS APS `json:"aps"`
	} `json:"payload"`
}
type APS struct {
	Alert             *Notification `json:"alert,omitempty"`
	Category          string        `json:"category,omitempty"`
	ThreadID          string        `json:"thread-id,omitempty"`
	ContentAvailable  int           `json:"content-available,omitempty"`
	InterruptionLevel string        `json:"interruption-level,omitempty"`
}

func (m Message) validate(authority string) bool {
	if m.LiveActivity != nil && (!m.LiveActivity.Valid() || m.Notification != nil || m.APNS != nil || m.Android != nil || m.Data["type"] != "live_activity") {
		return false
	}
	if len(m.Data) == 0 || m.Data["type"] == "" {
		return false
	}
	for _, key := range []string{"instance_id", "authority_instance_id"} {
		if value := m.Data[key]; value != "" && value != authority {
			return false
		}
	}
	if key := m.Data["session_key"]; key != "" {
		parsed, ok := identity.ParseSessionKey(key)
		if !ok || parsed.AuthorityInstanceID != authority {
			return false
		}
	}
	if m.APNS != nil {
		for key := range m.APNS.Headers {
			switch key {
			case "apns-priority", "apns-push-type", "apns-expiration", "apns-collapse-id":
			default:
				return false
			}
		}
	}
	data, err := json.Marshal(m)
	return err == nil && len(data) <= 8<<10
}

type ChallengeRequest struct {
	DeviceID string `json:"device_id"`
	Token    string `json:"token"`
	Platform string `json:"platform"`
}
type ChallengeResponse struct {
	ChallengeID string `json:"challenge_id"`
	ExpiresAt   int64  `json:"expires_at"`
}
type ConfirmRequest struct {
	ChallengeID string `json:"challenge_id"`
	Code        string `json:"code"`
}
type SendRequest struct {
	RequestID string  `json:"request_id"`
	DeviceID  string  `json:"device_id"`
	TokenHash string  `json:"token_sha256"`
	Message   Message `json:"message"`
}
type DeviceStatus struct {
	DeviceID  string `json:"device_id"`
	TokenHash string `json:"token_sha256"`
}
type Response struct {
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
}

// ProviderResult is sanitized. Google response bodies, bearer tokens and
// registration tokens never reach the Bridge or application logs.
type ProviderResult struct {
	Status string
	Error  string
}

const (
	Sent      = "sent"
	Retryable = "retryable"
	Uncertain = "uncertain"
	Failed    = "failed"
)
