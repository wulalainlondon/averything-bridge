package core

import (
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

//go:embed push_setup.html
var pushSetupHTML string

var pushSetupTemplate = template.Must(template.New("push-setup").Parse(pushSetupHTML))

type pushSetupPending struct {
	DeviceID string
	Expires  int64
}

func localPushSetupRequest(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		return false
	}
	u, err := url.Parse("http://" + r.Host)
	if err != nil || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	host = u.Hostname()
	if host != "localhost" && !net.ParseIP(host).IsLoopback() {
		return false
	}
	for _, header := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "CF-Connecting-IP"} {
		if r.Header.Get(header) != "" {
			return false
		}
	}
	return r.Header.Get("Origin") == "" || r.Header.Get("Origin") == "http://"+r.Host
}

func (h *Hub) pushSetupToken() (string, error) {
	h.pushSetupMu.Lock()
	defer h.pushSetupMu.Unlock()
	if h.pushSetupCSRF == "" {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			return "", err
		}
		h.pushSetupCSRF = hex.EncodeToString(b)
	}
	return h.pushSetupCSRF, nil
}

func (h *Hub) pairedPushDevices() map[string]bool {
	allowed := make(map[string]bool)
	for _, binding := range h.pairing.DeviceBindings() {
		if binding.DeviceID != "" {
			allowed[binding.DeviceID] = true
		}
	}
	return allowed
}

// ServePushSetup is deliberately loopback-only, not a public/tunnel pairing
// authority. Browser requests require a per-boot CSRF token and same origin;
// neither Google credentials nor broker credentials appear in page/API output.
func (h *Hub) ServePushSetup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Frame-Options", "DENY")
	respond := func(status int, value any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(value)
	}
	fail := func(status int, code string) { respond(status, map[string]string{"error": code}) }
	if !localPushSetupRequest(r) {
		fail(403, "local_setup_only")
		return
	}
	if h.fcm == nil || !h.fcm.RelayEnabled() {
		fail(409, "relay_not_configured")
		return
	}
	csrf, err := h.pushSetupToken()
	if err != nil {
		fail(503, "unavailable")
		return
	}
	if r.URL.Path == "/push/setup" && r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'nonce-"+csrf+"'; style-src 'nonce-"+csrf+"'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
		_ = pushSetupTemplate.Execute(w, struct{ CSRF string }{csrf})
		return
	}
	if r.URL.Path != "/push/setup/api" || r.Method != http.MethodPost {
		fail(405, "method_not_allowed")
		return
	}
	if r.Header.Get("X-Push-Setup-CSRF") != csrf || r.Header.Get("Content-Type") != "application/json" {
		fail(403, "invalid_setup_request")
		return
	}
	var input struct {
		Action      string `json:"action"`
		DeviceID    string `json:"device_id,omitempty"`
		ChallengeID string `json:"challenge_id,omitempty"`
		Code        string `json:"code,omitempty"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
		fail(400, "invalid_request")
		return
	}
	allowed := h.pairedPushDevices()
	switch input.Action {
	case "list":
		devices, err := h.fcm.RelayDevices(r.Context(), allowed)
		if err != nil {
			fail(502, "relay_unavailable")
			return
		}
		sort.Slice(devices, func(i, j int) bool { return devices[i].DeviceID < devices[j].DeviceID })
		respond(200, devices)
	case "challenge":
		if !allowed[input.DeviceID] {
			fail(403, "device_not_paired")
			return
		}
		challenge, err := h.fcm.BeginRelayPairing(r.Context(), input.DeviceID)
		if err != nil {
			fail(502, "verification_not_sent")
			return
		}
		h.pushSetupMu.Lock()
		if h.pushSetupChallenges == nil {
			h.pushSetupChallenges = make(map[string]pushSetupPending)
		}
		for id, record := range h.pushSetupChallenges {
			if record.Expires <= time.Now().Unix() || record.DeviceID == input.DeviceID {
				delete(h.pushSetupChallenges, id)
			}
		}
		h.pushSetupChallenges[challenge.ChallengeID] = pushSetupPending{DeviceID: input.DeviceID, Expires: challenge.ExpiresAt}
		h.pushSetupMu.Unlock()
		respond(200, challenge)
	case "confirm":
		h.pushSetupMu.Lock()
		pending, ok := h.pushSetupChallenges[input.ChallengeID]
		h.pushSetupMu.Unlock()
		if !ok || pending.Expires <= time.Now().Unix() || !allowed[pending.DeviceID] {
			fail(403, "verification_expired")
			return
		}
		if err := h.fcm.ConfirmRelayPairing(r.Context(), input.ChallengeID, strings.TrimSpace(input.Code)); err != nil {
			fail(403, "verification_failed")
			return
		}
		h.pushSetupMu.Lock()
		delete(h.pushSetupChallenges, input.ChallengeID)
		h.pushSetupMu.Unlock()
		respond(200, map[string]string{"status": "approved"})
	default:
		fail(400, "invalid_request")
	}
}

func (h *Hub) removeUnpairedPushDevice(id string) {
	if id != "" && h.fcm != nil && !h.pairedPushDevices()[id] {
		h.fcm.RemoveDevice(id)
	}
}
