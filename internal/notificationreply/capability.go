package notificationreply

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidCapability = errors.New("invalid reply capability")
	ErrExpiredCapability = errors.New("reply capability expired")
)

type claims struct {
	InstanceID string `json:"instance_id"`
	SessionID  string `json:"session_id"`
	ExpiresAt  int64  `json:"expires_at"`
}

// Capabilities signs short-lived, Session-bound tokens. Android never needs
// the Bridge's durable pairing credential in a notification or WorkManager DB.
type Capabilities struct {
	mu         sync.Mutex
	secret     []byte
	instanceID string
	now        func() time.Time
}

func NewCapabilities(dataDir, instanceID string) (*Capabilities, error) {
	secret, err := loadOrCreateSecret(filepath.Join(dataDir, "notification_reply_secret"))
	if err != nil {
		return nil, err
	}
	return &Capabilities{secret: secret, instanceID: instanceID, now: time.Now}, nil
}

func (c *Capabilities) Issue(sessionID string, ttl time.Duration) (string, int64) {
	if sessionID == "" || ttl <= 0 {
		return "", 0
	}
	expiresAt := c.now().Add(ttl).UnixMilli()
	body, _ := json.Marshal(claims{InstanceID: c.instanceID, SessionID: sessionID, ExpiresAt: expiresAt})
	encoded := base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, c.secret)
	_, _ = mac.Write([]byte(encoded))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encoded + "." + signature, expiresAt
}

func (c *Capabilities) Validate(token, sessionID string) error {
	parts := strings.Split(token, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return ErrInvalidCapability
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ErrInvalidCapability
	}
	mac := hmac.New(sha256.New, c.secret)
	_, _ = mac.Write([]byte(parts[0]))
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return ErrInvalidCapability
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return ErrInvalidCapability
	}
	var value claims
	if json.Unmarshal(body, &value) != nil || value.InstanceID != c.instanceID || value.SessionID != sessionID || value.ExpiresAt <= 0 {
		return ErrInvalidCapability
	}
	if c.now().UnixMilli() > value.ExpiresAt {
		return ErrExpiredCapability
	}
	return nil
}

func loadOrCreateSecret(path string) ([]byte, error) {
	if path != "" {
		if value, err := os.ReadFile(path); err == nil && len(value) >= 32 {
			return value, nil
		}
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	if path == "" {
		return secret, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, secret, 0o600); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	return secret, nil
}
