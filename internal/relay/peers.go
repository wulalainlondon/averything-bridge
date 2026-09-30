package relay

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Peer struct {
	InstanceID string `json:"instance_id"`
	BaseURL    string `json:"base_url"`
	SecretRef  string `json:"secret_ref"`
}

type Peers map[string]Peer

func LoadPeers() (Peers, error) {
	raw := strings.TrimSpace(os.Getenv("BRIDGE_RELAY_PEERS_JSON"))
	if raw == "" {
		return Peers{}, nil
	}
	var list []Peer
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		return nil, fmt.Errorf("parse BRIDGE_RELAY_PEERS_JSON: %w", err)
	}
	out := Peers{}
	for _, peer := range list {
		peer.InstanceID, peer.BaseURL, peer.SecretRef = strings.TrimSpace(peer.InstanceID), strings.TrimRight(strings.TrimSpace(peer.BaseURL), "/"), strings.TrimSpace(peer.SecretRef)
		parsed, err := url.Parse(peer.BaseURL)
		if peer.InstanceID == "" || err != nil || parsed.Scheme != "http" || parsed.Host == "" || !tailscaleRelayHost(parsed.Hostname()) || !strings.HasPrefix(peer.SecretRef, "env:") {
			return nil, errors.New("relay peer requires instance_id, Tailscale http base_url and env: secret_ref")
		}
		out[peer.InstanceID] = peer
	}
	return out, nil
}

func tailscaleRelayHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if strings.HasSuffix(host, ".ts.net") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		return v4[0] == 100 && v4[1]&0xc0 == 0x40
	}
	return strings.HasPrefix(ip.String(), "fd7a:115c:a1e0:")
}

func (p Peer) Secret() (string, error) {
	name := strings.TrimPrefix(p.SecretRef, "env:")
	value := strings.TrimSpace(os.Getenv(name))
	if name == "" || value == "" {
		return "", errors.New("relay peer secret unavailable")
	}
	return value, nil
}

func Sign(secret, instanceID, method, path string, body []byte, now time.Time) map[string]string {
	timestamp := strconv.FormatInt(now.Unix(), 10)
	nonceBytes := make([]byte, 16)
	_, _ = rand.Read(nonceBytes)
	nonce := hex.EncodeToString(nonceBytes)
	digest := sha256.Sum256(body)
	canonical := method + "\n" + path + "\n" + timestamp + "\n" + nonce + "\n" + hex.EncodeToString(digest[:])
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(canonical))
	return map[string]string{
		"X-Bridge-Relay-Instance":  instanceID,
		"X-Bridge-Relay-Timestamp": timestamp,
		"X-Bridge-Relay-Nonce":     nonce,
		"X-Bridge-Relay-Signature": hex.EncodeToString(mac.Sum(nil)),
	}
}

func Verify(secret, method, path string, body []byte, headers map[string]string, now time.Time) error {
	timestamp := headers["timestamp"]
	parsed, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || now.Sub(time.Unix(parsed, 0)) > 5*time.Minute || time.Unix(parsed, 0).Sub(now) > 5*time.Minute {
		return errors.New("relay timestamp outside replay window")
	}
	nonce := headers["nonce"]
	if len(nonce) != 32 {
		return errors.New("invalid relay nonce")
	}
	digest := sha256.Sum256(body)
	canonical := method + "\n" + path + "\n" + timestamp + "\n" + nonce + "\n" + hex.EncodeToString(digest[:])
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(canonical))
	provided, err := hex.DecodeString(headers["signature"])
	if err != nil || !hmac.Equal(provided, mac.Sum(nil)) {
		return errors.New("invalid relay signature")
	}
	return nil
}
