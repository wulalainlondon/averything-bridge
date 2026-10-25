// Package widgetaccess issues revocable, read-only widget snapshot grants.
package widgetaccess

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var ErrInvalid = errors.New("invalid widget capability")

type Claims struct {
	Scope            string `json:"scope"`
	Authority        string `json:"authority"`
	DeviceID         string `json:"device_id"`
	CredentialDigest string `json:"credential_digest"`
	Since            int64  `json:"since"`
	ExpiresAt        int64  `json:"expires_at"`
}

type Signer struct {
	key       []byte
	authority string
}

func CredentialDigest(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func New(dataDir, authority string) (*Signer, error) {
	if dataDir == "" || authority == "" {
		return nil, ErrInvalid
	}
	path := filepath.Join(dataDir, "widget_read_secret")
	key, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dataDir, 0700); err != nil {
			return nil, err
		}
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return nil, err
		}
		_, writeErr := f.Write(key)
		if writeErr == nil {
			writeErr = f.Sync()
		}
		closeErr := f.Close()
		if writeErr != nil {
			return nil, writeErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
	} else if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, ErrInvalid
	}
	return &Signer{key: key, authority: authority}, nil
}

func (s *Signer) Issue(deviceID, credential, previous string, now time.Time) (string, Claims) {
	since := now.UnixMilli()
	digest := CredentialDigest(credential)
	// Full device authentication is required by the caller for renewal. Keep
	// the original observation horizon, including after a previous grant expires.
	if old, err := s.Parse(previous, time.Time{}); err == nil && old.DeviceID == deviceID && old.CredentialDigest == digest {
		since = old.Since
	}
	claims := Claims{Scope: "widget:snapshot", Authority: s.authority, DeviceID: deviceID,
		CredentialDigest: digest, Since: since, ExpiresAt: now.Add(7 * 24 * time.Hour).UnixMilli()}
	body, _ := json.Marshal(claims)
	aead, err := s.aead()
	if err != nil {
		return "", Claims{}
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", Claims{}
	}
	// Some legacy installs use their device ID as the pairing credential.
	// Claims must therefore be opaque, not a readable JWT containing device_id.
	encrypted := aead.Seal(nonce, nonce, body, []byte("widget:snapshot:v1"))
	return "wr1." + base64.RawURLEncoding.EncodeToString(encrypted), claims
}

func (s *Signer) aead() (cipher.AEAD, error) {
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func (s *Signer) Parse(token string, now time.Time) (Claims, error) {
	var claims Claims
	parts := strings.Split(token, ".")
	if len(token) > 4096 || len(parts) != 2 || parts[0] != "wr1" {
		return claims, ErrInvalid
	}
	encrypted, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return claims, ErrInvalid
	}
	aead, err := s.aead()
	if err != nil || len(encrypted) < aead.NonceSize()+aead.Overhead() {
		return claims, ErrInvalid
	}
	body, err := aead.Open(nil, encrypted[:aead.NonceSize()], encrypted[aead.NonceSize():], []byte("widget:snapshot:v1"))
	if err != nil || json.Unmarshal(body, &claims) != nil || claims.Authority != s.authority || claims.Scope != "widget:snapshot" || claims.DeviceID == "" || claims.Since <= 0 || claims.ExpiresAt <= claims.Since {
		return Claims{}, ErrInvalid
	}
	if !now.IsZero() && now.UnixMilli() >= claims.ExpiresAt {
		return Claims{}, ErrInvalid
	}
	return claims, nil
}
