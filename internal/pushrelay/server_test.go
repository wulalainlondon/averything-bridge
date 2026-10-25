package pushrelay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordedPush struct {
	token   string
	message Message
}
type fakeSender struct {
	mu     sync.Mutex
	pushes []recordedPush
	next   []ProviderResult
}

func (s *fakeSender) Send(_ context.Context, token string, message Message) ProviderResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushes = append(s.pushes, recordedPush{token, message})
	if len(s.next) > 0 {
		result := s.next[0]
		s.next = s.next[1:]
		return result
	}
	return ProviderResult{Status: Sent}
}
func (s *fakeSender) code() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.pushes) - 1; i >= 0; i-- {
		if s.pushes[i].message.Data["type"] == "push_pairing" {
			return regexp.MustCompile(`[0-9]{6}`).FindString(s.pushes[i].message.Notification.Body)
		}
	}
	return ""
}
func (s *fakeSender) count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.pushes) }
func (s *fakeSender) results(values ...ProviderResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next = values
}

type fixture struct {
	store   *Store
	handler *Server
	http    *httptest.Server
	sender  *fakeSender
	root    string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := filepath.Join(t.TempDir(), "private-state")
	store, err := OpenStore(filepath.Join(root, "relay.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	sender := &fakeSender{}
	handler := NewServer(store, sender)
	server := httptest.NewServer(handler)
	f := &fixture{store, handler, server, sender, root}
	t.Cleanup(func() { server.Close(); _ = store.Close() })
	return f
}
func (f *fixture) client(t *testing.T, authority string) *Client {
	t.Helper()
	client, err := NewClient(f.http.URL, authority, filepath.Join(t.TempDir(), "client.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Enroll(context.Background()); err != nil {
		t.Fatal(err)
	}
	return client
}

const testPhoneToken = "private-fixture-phone-token-000001"

func approve(t *testing.T, f *fixture, c *Client, device, token string) ChallengeResponse {
	t.Helper()
	challenge, err := c.BeginPairing(context.Background(), ChallengeRequest{DeviceID: device, Token: token, Platform: "ios"})
	if err != nil {
		t.Fatal(err)
	}
	// Simulates the phone reading the notification; not a code returned by the
	// relay to the Bridge. No real Firebase credential or push is used.
	if err = c.ConfirmPairing(context.Background(), ConfirmRequest{ChallengeID: challenge.ChallengeID, Code: f.sender.code()}); err != nil {
		t.Fatal(err)
	}
	return challenge
}
func testSend(device, token, authority string) SendRequest {
	nonce, _ := randomID("", 18)
	return SendRequest{RequestID: strconv.FormatInt(time.Now().Unix(), 10) + "_" + nonce, DeviceID: device, TokenHash: TokenHash(token), Message: Message{Data: map[string]string{"type": "task_done", "authority_instance_id": authority, "event_id": "turn-1"}, Notification: &Notification{Title: "Averything", Body: "任務已完成"}}}
}
func call(t *testing.T, c *Client, path string, input any) error {
	t.Helper()
	return c.request(context.Background(), path, input, &Response{})
}
func codeIs(t *testing.T, err error, code string) {
	t.Helper()
	var failure *APIError
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("wanted code %s, got sanitized error %v", code, err)
	}
}

func TestPhoneProofAndCrossBridgeIsolation(t *testing.T) {
	f := newFixture(t)
	a := f.client(t, "bridge-a")
	b := f.client(t, "bridge-b")
	codeIs(t, call(t, a, "/v1/notifications", testSend("phone", testPhoneToken, "bridge-a")), "device_not_approved")
	if f.sender.count() != 0 {
		t.Fatal("unapproved destination contacted provider")
	}
	challenge, err := a.BeginPairing(context.Background(), ChallengeRequest{DeviceID: "phone", Token: testPhoneToken, Platform: "ios"})
	if err != nil {
		t.Fatal(err)
	}
	code := f.sender.code()
	if len(code) != 6 {
		t.Fatal("verification push did not carry a code")
	}
	response, _ := json.Marshal(challenge)
	var fields map[string]any
	_ = json.Unmarshal(response, &fields)
	if len(fields) != 2 || fields["code"] != nil {
		t.Fatal("Bridge received verification secret")
	}
	codeIs(t, b.ConfirmPairing(context.Background(), ConfirmRequest{ChallengeID: challenge.ChallengeID, Code: code}), "verification_failed")
	if err = a.ConfirmPairing(context.Background(), ConfirmRequest{ChallengeID: challenge.ChallengeID, Code: code}); err != nil {
		t.Fatal(err)
	}
	if err = call(t, a, "/v1/notifications", testSend("phone", testPhoneToken, "bridge-a")); err != nil {
		t.Fatal(err)
	}
	codeIs(t, call(t, b, "/v1/notifications", testSend("phone", testPhoneToken, "bridge-b")), "device_not_approved")
	codeIs(t, call(t, a, "/v1/notifications", testSend("phone", testPhoneToken, "bridge-b")), "invalid_request")
	codeIs(t, a.ConfirmPairing(context.Background(), ConfirmRequest{ChallengeID: challenge.ChallengeID, Code: code}), "verification_failed")
	devices, err := b.Devices(context.Background())
	if err != nil || len(devices) != 0 {
		t.Fatal("cross-Bridge device list leaked")
	}
	if f.sender.count() != 2 {
		t.Fatal("provider saw unapproved or duplicate requests")
	}
}

func TestWrongCodesLockOutAndExpiredCodesCannotBind(t *testing.T) {
	t.Run("lockout", func(t *testing.T) {
		f := newFixture(t)
		c := f.client(t, "bridge-a")
		challenge, err := c.BeginPairing(context.Background(), ChallengeRequest{DeviceID: "phone", Token: testPhoneToken, Platform: "ios"})
		if err != nil {
			t.Fatal(err)
		}
		code := f.sender.code()
		wrong := "000000"
		if code == wrong {
			wrong = "000001"
		}
		for i := 0; i < 5; i++ {
			codeIs(t, c.ConfirmPairing(context.Background(), ConfirmRequest{ChallengeID: challenge.ChallengeID, Code: wrong}), "verification_failed")
		}
		codeIs(t, c.ConfirmPairing(context.Background(), ConfirmRequest{ChallengeID: challenge.ChallengeID, Code: code}), "verification_failed")
	})
	t.Run("expiry", func(t *testing.T) {
		f := newFixture(t)
		c := f.client(t, "bridge-a")
		challenge, err := c.BeginPairing(context.Background(), ChallengeRequest{DeviceID: "phone", Token: testPhoneToken, Platform: "ios"})
		if err != nil {
			t.Fatal(err)
		}
		future := time.Unix(challenge.ExpiresAt, 0)
		f.handler.now = func() time.Time { return future }
		codeIs(t, c.ConfirmPairing(context.Background(), ConfirmRequest{ChallengeID: challenge.ChallengeID, Code: f.sender.code()}), "verification_failed")
	})
}

func TestClientCannotSelectRawTokenTopicOrAPNSApp(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	for _, field := range []string{"token", "topic", "condition"} {
		input := testSend("phone", testPhoneToken, "bridge-a")
		body, _ := json.Marshal(input)
		var value map[string]any
		_ = json.Unmarshal(body, &value)
		value["message"].(map[string]any)[field] = "other-recipient"
		codeIs(t, call(t, c, "/v1/notifications", value), "invalid_request")
	}
	input := testSend("phone", testPhoneToken, "bridge-a")
	input.Message.APNS = &APNS{Headers: map[string]string{"apns-topic": "other.app"}}
	codeIs(t, call(t, c, "/v1/notifications", input), "invalid_request")
	input = testSend("phone", testPhoneToken, "bridge-a")
	input.Message.Data["session_key"] = "sk1:other-bridge:session"
	codeIs(t, call(t, c, "/v1/notifications", input), "invalid_request")
	if f.sender.count() != 1 {
		t.Fatal("injected destination contacted provider")
	}
}

func TestTokenRotationRequiresAnotherPhoneProof(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	newToken := "new-registration-token-000002"
	codeIs(t, call(t, c, "/v1/notifications", testSend("phone", newToken, "bridge-a")), "device_not_approved")
	approve(t, f, c, "phone", newToken)
	if err := call(t, c, "/v1/notifications", testSend("phone", newToken, "bridge-a")); err != nil {
		t.Fatal(err)
	}
	codeIs(t, call(t, c, "/v1/notifications", testSend("phone", testPhoneToken, "bridge-a")), "device_not_approved")
}

func TestRevokeDeviceAndBridgeCannotBeReenrolled(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	if err := c.RevokeDevice(context.Background(), "phone"); err != nil {
		t.Fatal(err)
	}
	codeIs(t, call(t, c, "/v1/notifications", testSend("phone", testPhoneToken, "bridge-a")), "device_not_approved")
	if err := f.store.RevokeBridge(context.Background(), c.BridgeID()); err != nil {
		t.Fatal(err)
	}
	codeIs(t, c.Enroll(context.Background()), "not_authorized")
	codeIs(t, call(t, c, "/v1/notifications", testSend("phone", testPhoneToken, "bridge-a")), "unauthorized")
}

func TestDuplicateRequestsAndChangedContent(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	input := testSend("phone", testPhoneToken, "bridge-a")
	for i := 0; i < 3; i++ {
		if err := call(t, c, "/v1/notifications", input); err != nil {
			t.Fatal(err)
		}
	}
	input.Message.Data["event_id"] = "different-turn"
	codeIs(t, call(t, c, "/v1/notifications", input), "idempotency_conflict")
	if f.sender.count() != 2 {
		t.Fatal("duplicate receipt resubmitted to provider")
	}
}

func TestConcurrentDuplicateRequestsSubmitOnce(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	input := testSend("phone", testPhoneToken, "bridge-a")
	var wg sync.WaitGroup
	failures := make(chan error, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var result Response
			err := c.request(context.Background(), "/v1/notifications", input, &result)
			if err != nil {
				var failure *APIError
				if !errors.As(err, &failure) || failure.Code != "delivery_uncertain" {
					failures <- err
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if f.sender.count() != 2 {
		t.Fatal("parallel replay bypassed durable reservation")
	}
}

func TestProviderUncertaintyIsNotRetried(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	f.sender.results(ProviderResult{Status: Uncertain, Error: "provider_delivery_uncertain"})
	input := testSend("phone", testPhoneToken, "bridge-a")
	codeIs(t, call(t, c, "/v1/notifications", input), "delivery_uncertain")
	codeIs(t, call(t, c, "/v1/notifications", input), "delivery_uncertain")
	if f.sender.count() != 2 {
		t.Fatal("uncertain provider delivery was resubmitted")
	}
}

type dropResponse struct {
	once atomic.Bool
	base http.RoundTripper
}

func (d *dropResponse) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := d.base.RoundTrip(r)
	if err == nil && r.URL.Path == "/v1/notifications" && response.StatusCode == 200 && !d.once.Swap(true) {
		response.Body.Close()
		return nil, errors.New("simulated response loss after provider success")
	}
	return response, err
}

func fcmEnvelope(input SendRequest, token string) []byte {
	body, _ := json.Marshal(input.Message)
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	m["token"] = token
	result, _ := json.Marshal(map[string]any{"message": m})
	return result
}

func TestClientLostResponseRetriesSameReceiptWithoutDuplicate(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	c.http.Transport = &dropResponse{base: http.DefaultTransport}
	if err := c.SendFCM(context.Background(), "phone", testPhoneToken, fcmEnvelope(testSend("phone", testPhoneToken, "bridge-a"), testPhoneToken)); err != nil {
		t.Fatal(err)
	}
	if f.sender.count() != 2 {
		t.Fatal("lost HTTP response duplicated the push")
	}
}

func TestClientExplicitProviderRejectionMayRetry(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	f.sender.results(ProviderResult{Status: Retryable, Error: "provider_retryable"}, ProviderResult{Status: Sent})
	if err := c.SendFCM(context.Background(), "phone", testPhoneToken, fcmEnvelope(testSend("phone", testPhoneToken, "bridge-a"), testPhoneToken)); err != nil {
		t.Fatal(err)
	}
	if f.sender.count() != 3 {
		t.Fatal("retryable provider rejection did not retry exactly once")
	}
}

func TestPermanentInvalidTokenDoesNotRemoveOtherPhone(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	approve(t, f, c, "tablet", "private-fixture-tablet-token-000002")
	f.sender.results(ProviderResult{Status: Failed, Error: "UNREGISTERED"})
	codeIs(t, call(t, c, "/v1/notifications", testSend("phone", testPhoneToken, "bridge-a")), "UNREGISTERED")
	devices, err := c.Devices(context.Background())
	if err != nil || len(devices) != 1 || devices[0].DeviceID != "tablet" {
		t.Fatal("wrong device revoked")
	}
}

func TestVerificationSpamAndRequestSizeAreBounded(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	_, err := c.BeginPairing(context.Background(), ChallengeRequest{DeviceID: "phone", Token: testPhoneToken, Platform: "ios"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.BeginPairing(context.Background(), ChallengeRequest{DeviceID: "phone", Token: testPhoneToken, Platform: "ios"})
	codeIs(t, err, "rate_limited")
	body := bytes.NewBufferString(strings.Repeat("x", maxBody+1))
	request, _ := http.NewRequest("POST", f.http.URL+"/v1/notifications", body)
	request.Header.Set("Authorization", "Bearer "+c.identity.Credential)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 413 {
		t.Fatal("oversized request accepted")
	}
	if f.sender.count() != 1 {
		t.Fatal("verification spam reached provider")
	}
}

func TestRateReservationsAreAtomic(t *testing.T) {
	f := newFixture(t)
	var allowed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := f.store.rate(context.Background(), "test", 10, time.Hour, time.Now())
			if err != nil {
				t.Error("rate store failed")
			}
			if ok {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if allowed.Load() != 10 {
		t.Fatal("rate counter lost an increment")
	}
}

func TestClientIdentityStaysPrivateAndCannotMoveToAnotherService(t *testing.T) {
	f := newFixture(t)
	path := filepath.Join(t.TempDir(), "client.json")
	c, err := NewClient(f.http.URL, "bridge-a", path)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := NewClient(f.http.URL, "bridge-a", path)
	if err != nil || c.BridgeID() != reloaded.BridgeID() {
		t.Fatal("stable client identity was replaced")
	}
	for _, address := range []string{"http://remote.example", "https://user:secret@example.com", "https://example.com?secret=value", "https://other.example"} {
		if _, err = NewClient(address, "bridge-a", path); err == nil {
			t.Fatal("credential forwarded to different/unsafe URL")
		}
	}
	if _, err = NewClient(f.http.URL, "bridge-b", path); err == nil {
		t.Fatal("identity moved to another Bridge")
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = NewClient(f.http.URL, "bridge-a", path); err == nil {
		t.Fatal("public credential accepted")
	}
}

func TestReceiptsAndRevocationSurviveStoreRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private-state", "relay.sqlite")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	credential, _ := randomID("prc_", 32)
	id, _ := credentialID(credential)
	ctx := context.Background()
	if err = s.enroll(ctx, id, "bridge-a"); err != nil {
		t.Fatal(err)
	}
	input := testSend("phone", testPhoneToken, "bridge-a")
	submit, _, err := s.reserve(ctx, id, input, time.Now().Unix())
	if err != nil || !submit {
		t.Fatal("first reservation failed")
	}
	if err = s.finish(ctx, id, input.RequestID, ProviderResult{Status: Sent}, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	if err = s.RevokeBridge(ctx, id); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.authority(ctx, id); !errors.Is(err, ErrDenied) {
		t.Fatal("revocation lost on restart")
	}
	submit, result, err := s.reserve(ctx, id, input, time.Now().Unix())
	if err != nil || submit || result.Status != Sent {
		t.Fatal("receipt lost on restart")
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal("database mode not private")
	}
	// Server persists only the credential digest, never the bearer credential.
	data, _ := os.ReadFile(path)
	if bytes.Contains(data, []byte(credential)) {
		t.Fatal("server database stored raw broker credential")
	}
}

func TestOldRequestCannotResurrectAfterTombstoneExpiry(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	input := testSend("phone", testPhoneToken, "bridge-a")
	input.RequestID = fmt.Sprintf("%d_%s", time.Now().Add(-25*time.Hour).Unix(), strings.Repeat("a", 24))
	codeIs(t, call(t, c, "/v1/notifications", input), "invalid_request")
	if f.sender.count() != 1 {
		t.Fatal("expired request sent")
	}
}

func TestPruneAndCrashAmbiguity(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	input := testSend("phone", testPhoneToken, "bridge-a")
	bridge := "test"
	submit, _, err := f.store.reserve(ctx, bridge, input, time.Now().Unix())
	if err != nil || !submit {
		t.Fatal(err)
	}
	submit, result, err := f.store.reserve(ctx, bridge, input, time.Now().Unix())
	if err != nil || submit || result.Status != "sending" {
		t.Fatal("unfinished crash reservation resent")
	}
	if err = f.handler.Prune(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNoArbitraryResponseLeaksIntoClientErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		_, _ = io.WriteString(w, `{"error":"PRIVATE_DATA_MUST_NOT_APPEAR"}`)
	}))
	defer server.Close()
	c, err := NewClient(server.URL, "bridge-a", filepath.Join(t.TempDir(), "client.json"))
	if err != nil {
		t.Fatal(err)
	}
	err = c.Enroll(context.Background())
	if err == nil || strings.Contains(err.Error(), "MUST_NOT_APPEAR") {
		t.Fatal("untrusted response leaked")
	}
}

func TestOldInvalidTokenCannotRevokeNewPhoneProof(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	newToken := "replacement-fixture-token-0002"
	approve(t, f, c, "phone", newToken)
	if err := f.store.invalidateDevice(context.Background(), c.BridgeID(), "phone", testPhoneToken); err != nil {
		t.Fatal(err)
	}
	if err := call(t, c, "/v1/notifications", testSend("phone", newToken, "bridge-a")); err != nil {
		t.Fatal("old response revoked newly verified token", err)
	}
}

type failRevoke struct{ base http.RoundTripper }

func (f failRevoke) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == "/v1/devices/revoke" {
		return nil, errors.New("simulated service outage")
	}
	return f.base.RoundTrip(r)
}

func TestDeviceRevocationSurvivesOutageAndClientRestart(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	c.http.Transport = failRevoke{base: http.DefaultTransport}
	if err := c.RevokeDevice(context.Background(), "phone", TokenHash(testPhoneToken)); err == nil {
		t.Fatal("outage was not reported")
	}
	restarted, err := NewClient(f.http.URL, "bridge-a", c.path)
	if err != nil {
		t.Fatal(err)
	}
	if len(restarted.identity.PendingRevocations) != 1 {
		t.Fatal("pending revoke lost on restart")
	}
	if err = restarted.FlushRevocations(context.Background()); err != nil {
		t.Fatal(err)
	}
	codeIs(t, call(t, restarted, "/v1/notifications", testSend("phone", testPhoneToken, "bridge-a")), "device_not_approved")
	again, err := NewClient(f.http.URL, "bridge-a", c.path)
	if err != nil || len(again.identity.PendingRevocations) != 0 {
		t.Fatal("completed revoke not persisted")
	}
}

func TestStaleQueuedRevokeCannotRemoveNewPhoneProof(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	c.http.Transport = failRevoke{base: http.DefaultTransport}
	_ = c.RevokeDevice(context.Background(), "phone", TokenHash(testPhoneToken))
	c.http.Transport = http.DefaultTransport
	newToken := "replacement-token-after-outage-0003"
	approve(t, f, c, "phone", newToken)
	if err := c.FlushRevocations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := call(t, c, "/v1/notifications", testSend("phone", newToken, "bridge-a")); err != nil {
		t.Fatal("stale revoke removed newly approved device", err)
	}
}

func TestClientRetryRechecksLocalRevocationOrPrivacy(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	f.sender.results(ProviderResult{Status: Retryable, Error: "provider_retryable"})
	var calls atomic.Int32
	err := c.SendFCM(context.Background(), "phone", testPhoneToken, fcmEnvelope(testSend("phone", testPhoneToken, "bridge-a"), testPhoneToken), func() bool { return calls.Add(1) == 1 })
	codeIs(t, err, "notification_no_longer_allowed")
	if f.sender.count() != 2 {
		t.Fatal("retry ignored local revocation/privacy change")
	}
}

func TestLegacyRegistrationWithoutPlatformCanVerify(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	challenge, err := c.BeginPairing(context.Background(), ChallengeRequest{DeviceID: "phone", Token: testPhoneToken})
	if err != nil {
		t.Fatal("legacy registration required an App update", err)
	}
	if err = c.ConfirmPairing(context.Background(), ConfirmRequest{ChallengeID: challenge.ChallengeID, Code: f.sender.code()}); err != nil {
		t.Fatal(err)
	}
	if err = call(t, c, "/v1/notifications", testSend("phone", testPhoneToken, "bridge-a")); err != nil {
		t.Fatal(err)
	}
}
