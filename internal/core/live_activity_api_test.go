package core

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLiveActivityAPIRequiresPairedDeviceAndRejectsWidgetGrant(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.DataDir = t.TempDir()
	if err := h.pairing.Claim("paired-secret", "phone"); err != nil {
		t.Fatal(err)
	}
	call := func(auth, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/api/live-activities/v1/register", bytes.NewBufferString(body))
		r.Header.Set("Authorization", auth)
		w := httptest.NewRecorder()
		h.ServeLiveActivityAPI(w, r)
		return w
	}
	body := `{"deviceId":"phone","activityId":"activity","authority":"test","sessionId":"s","requestId":"r","token":"` + strings.Repeat("ab", 32) + `"}`
	if w := call("", body); w.Code != 401 {
		t.Fatal("unauthenticated enrollment")
	}
	if w := call("Bearer paired-secret", strings.Replace(body, "phone", "not-paired", 1)); w.Code != 401 {
		t.Fatal("credential borrowed for unpaired device")
	}
	r := httptest.NewRequest("POST", "/api/widgets/v1/access", strings.NewReader(`{"deviceId":"phone"}`))
	r.Header.Set("Authorization", "Bearer paired-secret")
	w := httptest.NewRecorder()
	h.ServeWidgetAPI(w, r)
	var access struct {
		ReadToken string `json:"readToken"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &access)
	if w := call("Bearer "+access.ReadToken, body); w.Code != 401 {
		t.Fatal("read grant enrolled activity")
	}
	if w := call("Bearer paired-secret", body+body); w.Code != 400 {
		t.Fatal("trailing JSON accepted")
	}
	if w := call("Bearer paired-secret", body); w.Code != 503 || strings.Contains(w.Body.String(), "paired-secret") {
		t.Fatal("push availability/secret redaction")
	}
}
func TestLiveActivityEnrollmentToManagedSenderUsesPinnedRun(t *testing.T) {
	f := newPushSetupFixture(t)
	f.connectPhone(t)
	challenge, err := f.notifier.BeginRelayPairing(context.Background(), "iphone-qa")
	if err != nil {
		t.Fatal(err)
	}
	if err = f.notifier.ConfirmRelayPairing(context.Background(), challenge.ChallengeID, f.phone.code()); err != nil {
		t.Fatal(err)
	}
	f.hub.registry.Create("live-s", "Live QA", t.TempDir(), "codex", "", "", "")
	f.hub.runtimes.Update("live-s", "running", "live-r", 0, "", "")
	body, _ := json.Marshal(map[string]any{"deviceId": "iphone-qa", "activityId": "live-activity-qa", "authority": f.hub.cfg.InstanceID, "sessionId": "live-s", "requestId": "live-r", "token": strings.Repeat("ab", 32)})
	r := httptest.NewRequest("POST", "/api/live-activities/v1/register", bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer fixture-phone-auth-only")
	w := httptest.NewRecorder()
	f.hub.ServeLiveActivityAPI(w, r)
	if w.Code != 200 {
		t.Fatalf("native enrollment failed: %d %s", w.Code, w.Body.String())
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		message, _ := f.phone.last()
		if message.LiveActivity != nil {
			if message.LiveActivity.ActivityID != "live-activity-qa" || message.LiveActivity.State.Phase != "running" {
				t.Fatal("pinned state changed")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("registered native activity did not reach managed sender")
}
