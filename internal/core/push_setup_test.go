package core

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"everything-go/internal/fcm"
	"everything-go/internal/pushrelay"
	"github.com/coder/websocket"
)

type setupPhonePush struct {
	mu       sync.Mutex
	messages []pushrelay.Message
	tokens   []string
}

func (p *setupPhonePush) Send(_ context.Context, token string, message pushrelay.Message) pushrelay.ProviderResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, message)
	p.tokens = append(p.tokens, token)
	return pushrelay.ProviderResult{Status: pushrelay.Sent}
}
func (p *setupPhonePush) code() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := len(p.messages) - 1; i >= 0; i-- {
		message := p.messages[i]
		if message.Data["type"] == "push_pairing" {
			return regexp.MustCompile(`[0-9]{6}`).FindString(message.Notification.Body)
		}
	}
	return ""
}
func (p *setupPhonePush) last() (pushrelay.Message, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.messages) == 0 {
		return pushrelay.Message{}, 0
	}
	return p.messages[len(p.messages)-1], len(p.messages)
}

type pushSetupFixture struct {
	hub      *Hub
	phone    *setupPhonePush
	server   *httptest.Server
	notifier *fcm.Notifier
	csrf     string
}

func newPushSetupFixture(t *testing.T) *pushSetupFixture {
	t.Helper()
	h, _ := newTestHub(t)
	state := t.TempDir()
	store, err := pushrelay.OpenStore(filepath.Join(state, "private-relay", "relay.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	phone := &setupPhonePush{}
	relay := httptest.NewServer(pushrelay.NewServer(store, phone))
	n, err := fcm.NewRelay(relay.URL, h.cfg.InstanceID, filepath.Join(state, "client.json"), filepath.Join(state, "tokens.json"))
	if err != nil {
		t.Fatal(err)
	}
	h.SetFCM(n)
	t.Cleanup(n.Close)
	mux := http.NewServeMux()
	mux.HandleFunc("/push/setup", h.ServePushSetup)
	mux.HandleFunc("/push/setup/api", h.ServePushSetup)
	mux.HandleFunc("/", h.ServeWS)
	server := httptest.NewServer(mux)
	t.Cleanup(func() { server.Close(); relay.Close(); _ = store.Close() })
	return &pushSetupFixture{hub: h, phone: phone, server: server, notifier: n}
}

func (f *pushSetupFixture) connectPhone(t *testing.T) *websocket.Conn {
	t.Helper()
	if err := f.hub.pairing.Claim("fixture-phone-auth-only", "iphone-qa"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(f.server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"hello","auth_token":"fixture-phone-auth-only","device_id":"iphone-qa","protocol_version":3}`)); err != nil {
		t.Fatal(err)
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]any
		_ = json.Unmarshal(data, &value)
		if value["type"] == "hello_ack" {
			break
		}
	}
	// This is the unchanged released mobile token/preference wire frame. An
	// authenticated connection's device ID wins over any payload device ID.
	if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"fcm_token","device_id":"another-phone","token":"fixture-real-ws-phone-token-0001","platform":"ios","notification_preferences":{"task_done_enabled":true,"error_enabled":true,"alert_when_waiting":true,"show_lockscreen_details":false}}`)); err != nil {
		t.Fatal(err)
	}
	if err = conn.Write(ctx, websocket.MessageText, []byte(`{"type":"ping"}`)); err != nil {
		t.Fatal(err)
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]any
		_ = json.Unmarshal(data, &value)
		if value["type"] == "pong" {
			break
		}
	}
	return conn
}

func (f *pushSetupFixture) page(t *testing.T) string {
	t.Helper()
	response, err := http.Get(f.server.URL + "/push/setup")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	if response.StatusCode != 200 {
		t.Fatalf("setup page returned %d", response.StatusCode)
	}
	f.csrf = f.hub.pushSetupCSRF
	if !strings.Contains(response.Header.Get("Content-Security-Policy"), "nonce-"+f.csrf) {
		t.Fatal("CSP nonce missing")
	}
	if strings.Contains(string(data), "prc_") || strings.Contains(string(data), "fixture-real-ws-phone-token") {
		t.Fatal("setup page leaked transport credential")
	}
	return string(data)
}
func (f *pushSetupFixture) api(t *testing.T, input any) (int, []byte) {
	t.Helper()
	body, _ := json.Marshal(input)
	request, _ := http.NewRequest("POST", f.server.URL+"/push/setup/api", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Push-Setup-CSRF", f.csrf)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, data
}

func TestReleasedPhoneWireToLocalSetupToRelayToIOSPayload(t *testing.T) {
	f := newPushSetupFixture(t)
	f.connectPhone(t)
	f.page(t)
	status, data := f.api(t, map[string]string{"action": "list"})
	if status != 200 || !strings.Contains(string(data), "iphone-qa") || strings.Contains(string(data), "another-phone") || strings.Contains(string(data), "fixture-real-ws-phone-token") {
		t.Fatal("authenticated phone list wrong or leaked token")
	}
	status, data = f.api(t, map[string]string{"action": "challenge", "device_id": "iphone-qa"})
	if status != 200 {
		t.Fatal("challenge failed", status)
	}
	var challenge pushrelay.ChallengeResponse
	if json.Unmarshal(data, &challenge) != nil {
		t.Fatal("challenge response invalid")
	}
	status, _ = f.api(t, map[string]string{"action": "confirm", "challenge_id": challenge.ChallengeID, "code": f.phone.code()})
	if status != 200 {
		t.Fatal("phone proof failed", status)
	}
	status, data = f.api(t, map[string]string{"action": "list"})
	if status != 200 || !strings.Contains(string(data), `"approved":true`) {
		t.Fatal("approved state did not persist")
	}
	f.notifier.NotifyTaskDoneWithAuthority(f.hub.cfg.InstanceID, "private-authority-name", "private-session-title", "private-result-text", "session-1", "request-1", fcm.ReplyAction{})
	message, count := f.phone.last()
	if count != 2 || message.Data["type"] != "task_done" || message.Data["authority_instance_id"] != f.hub.cfg.InstanceID || message.Data["session_key"] != "sk1:i1:session-1" {
		t.Fatal("legacy notification identity was not preserved")
	}
	if message.APNS == nil || message.APNS.Payload.APS.Category != "BRIDGE_SESSION_REPLY" || message.APNS.Headers["apns-collapse-id"] == "" {
		t.Fatal("released iOS/APNs notification format changed")
	}
	encoded, _ := json.Marshal(message)
	for _, value := range []string{"private-session-title", "private-result-text", "private-authority-name"} {
		if strings.Contains(string(encoded), value) {
			t.Fatal("lockscreen privacy was lost at relay boundary")
		}
	}
}

func TestPushSetupRejectsRemoteTunnelDNSRebindingAndCSRF(t *testing.T) {
	f := newPushSetupFixture(t)
	f.page(t)
	for _, test := range []struct{ name, remote, host, origin, forwarded, csrf string }{
		{"remote", "198.51.100.1:1234", "127.0.0.1:8766", "", "", f.csrf},
		{"rebinding", "127.0.0.1:1234", "evil.example", "", "", f.csrf},
		{"cross-origin", "127.0.0.1:1234", "127.0.0.1:8766", "https://evil.example", "", f.csrf},
		{"tunnel", "127.0.0.1:1234", "127.0.0.1:8766", "", "203.0.113.1", f.csrf},
		{"csrf", "127.0.0.1:1234", "127.0.0.1:8766", "", "", "wrong"},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest("POST", "http://"+test.host+"/push/setup/api", strings.NewReader(`{"action":"list"}`))
			request.RemoteAddr = test.remote
			request.Host = test.host
			request.Header.Set("Origin", test.origin)
			request.Header.Set("X-Forwarded-For", test.forwarded)
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-Push-Setup-CSRF", test.csrf)
			response := httptest.NewRecorder()
			f.hub.ServePushSetup(response, request)
			if response.Code != 403 {
				t.Fatal("setup boundary bypassed")
			}
		})
	}
}

func TestSetupCannotApproveUnpairedOrRemovedPhone(t *testing.T) {
	f := newPushSetupFixture(t)
	f.connectPhone(t)
	f.page(t)
	f.notifier.SetToken("unpaired", "fixture-unpaired-phone-token-0002", "ios")
	status, _ := f.api(t, map[string]string{"action": "challenge", "device_id": "unpaired"})
	if status != 403 {
		t.Fatal("unpaired local registration could request approval")
	}
	status, data := f.api(t, map[string]string{"action": "challenge", "device_id": "iphone-qa"})
	if status != 200 {
		t.Fatal("challenge failed")
	}
	var challenge pushrelay.ChallengeResponse
	_ = json.Unmarshal(data, &challenge)
	if err := f.hub.pairing.Unclaim("fixture-phone-auth-only"); err != nil {
		t.Fatal(err)
	}
	status, _ = f.api(t, map[string]string{"action": "confirm", "challenge_id": challenge.ChallengeID, "code": f.phone.code()})
	if status != 403 {
		t.Fatal("removed phone could complete approval")
	}
}

func TestUnpairStopsLocalPushImmediately(t *testing.T) {
	f := newPushSetupFixture(t)
	f.connectPhone(t)
	f.page(t)
	status, data := f.api(t, map[string]string{"action": "challenge", "device_id": "iphone-qa"})
	if status != 200 {
		t.Fatal("challenge failed")
	}
	var challenge pushrelay.ChallengeResponse
	_ = json.Unmarshal(data, &challenge)
	status, _ = f.api(t, map[string]string{"action": "confirm", "challenge_id": challenge.ChallengeID, "code": f.phone.code()})
	if status != 200 {
		t.Fatal("confirmation failed")
	}
	if err := f.hub.pairing.Unclaim("fixture-phone-auth-only"); err != nil {
		t.Fatal(err)
	}
	f.hub.removeUnpairedPushDevice("iphone-qa")
	// An already-open old socket must not restore its registration after
	// unpairing while broker revocation is in flight.
	f.notifier.SetToken("iphone-qa", "fixture-real-ws-phone-token-0001", "ios")
	f.notifier.NotifyTaskDoneWithAuthority("i1", "Bridge", "task", "completed", "s1", "r1", fcm.ReplyAction{})
	_, count := f.phone.last()
	if count != 1 {
		t.Fatal("unpaired phone still received task notification")
	}
}

// Browser harness exists only in tests and uses a fake phone/FCM provider. The
// code endpoint cannot be built into the production Bridge or broker binary.
func TestPushSetupBrowserHarness(t *testing.T) {
	if os.Getenv("BRIDGE_PUSH_BROWSER_HARNESS") != "1" {
		t.Skip("opt-in local browser fixture")
	}
	f := newPushSetupFixture(t)
	f.connectPhone(t)
	// Swap the httptest server handler before any browser request. WS has its
	// own already-selected handler and no model/production services are used.
	original := f.server.Config.Handler
	finish := make(chan struct{})
	var once sync.Once
	f.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/qa/phone":
			_ = json.NewEncoder(w).Encode(map[string]string{"code": f.phone.code()})
		case "/qa/notify":
			f.notifier.NotifyTaskDoneWithAuthority("i1", "Fixture Bridge", "Fixture task", "Fixture completion", "fixture-session", "fixture-request", fcm.ReplyAction{})
			message, count := f.phone.last()
			_ = json.NewEncoder(w).Encode(map[string]any{"messages": count, "type": message.Data["type"]})
		case "/qa/finish":
			once.Do(func() { close(finish) })
			w.WriteHeader(204)
		default:
			original.ServeHTTP(w, r)
		}
	})
	manifest := os.Getenv("BRIDGE_PUSH_QA_MANIFEST")
	if manifest == "" {
		t.Fatal("private browser manifest path required")
	}
	data, _ := json.Marshal(map[string]string{"url": f.server.URL + "/push/setup"})
	if err := os.WriteFile(manifest, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("PUSH_SETUP_BROWSER_READY (fake phone; no Google calls)")
	select {
	case <-finish:
	case <-time.After(10 * time.Minute):
		t.Fatal("browser harness deadline")
	}
}
