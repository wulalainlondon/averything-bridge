package pushrelay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"everything-go/internal/liveactivity"
	"golang.org/x/oauth2"
)

func liveDelivery() liveactivity.Delivery {
	now := time.Now().Unix()
	return liveactivity.Delivery{ActivityID: "activity-a", Timestamp: now, Event: "update", StaleDate: now + 180, State: liveactivity.State{Phase: "running", Revision: 1, UpdatedAt: now * 1000}}
}
func TestRelayLiveActivityEnrollmentScopeRotationAndRevocation(t *testing.T) {
	f := newFixture(t)
	a := f.client(t, "bridge-a")
	b := f.client(t, "bridge-b")
	approve(t, f, a, "phone-a", testPhoneToken)
	approve(t, f, b, "phone-b", testPhoneToken+"other")
	ctx := context.Background()
	token := strings.Repeat("ab", 32)
	expires := time.Now().Add(time.Hour).Unix()
	if err := a.RegisterLiveActivity(ctx, "phone-a", testPhoneToken, "activity-a", token, expires); err != nil {
		t.Fatal(err)
	}
	codeIs(t, b.RegisterLiveActivity(ctx, "phone-a", testPhoneToken, "activity-a", token, expires), "device_not_verified")
	codeIs(t, a.RegisterLiveActivity(ctx, "phone-a", "wrong-fcm-token", "activity-a", token, expires), "device_not_verified")
	before := f.sender.count()
	delivery := liveDelivery()
	if err := a.SendLiveActivity(ctx, "phone-a", testPhoneToken, delivery, func() bool { return true }); err != nil {
		t.Fatal(err)
	}
	f.sender.mu.Lock()
	sent := f.sender.pushes[len(f.sender.pushes)-1]
	f.sender.mu.Unlock()
	if f.sender.count() != before+1 || sent.token != testPhoneToken || sent.message.activityToken != token || sent.message.LiveActivity == nil {
		t.Fatal("sender did not retrieve own enrolled token")
	}
	if err := a.RegisterLiveActivity(ctx, "phone-a", testPhoneToken, "activity-b", strings.Repeat("cd", 32), expires); err != nil {
		t.Fatal(err)
	}
	codeIs(t, a.SendLiveActivity(ctx, "phone-a", testPhoneToken, delivery, func() bool { return true }), "activity_not_registered")
	delivery.ActivityID = "activity-b"
	if err := a.SendLiveActivity(ctx, "phone-a", testPhoneToken, delivery, func() bool { return true }); err != nil {
		t.Fatal(err)
	}
	if err := a.RevokeDevice(ctx, "phone-a", TokenHash(testPhoneToken)); err != nil {
		t.Fatal(err)
	}
	codeIs(t, a.SendLiveActivity(ctx, "phone-a", testPhoneToken, delivery, func() bool { return true }), "device_not_approved")
}
func TestRelayLiveActivityRejectsRawAPNSInjectionAndUnenrolledActivity(t *testing.T) {
	f := newFixture(t)
	c := f.client(t, "bridge-a")
	approve(t, f, c, "phone", testPhoneToken)
	delivery := liveDelivery()
	codeIs(t, c.SendLiveActivity(context.Background(), "phone", testPhoneToken, delivery, func() bool { return true }), "activity_not_registered")
	raw := testSend("phone", testPhoneToken, "bridge-a")
	raw.Message.Notification = nil
	raw.Message.LiveActivity = &delivery
	raw.Message.APNS = &APNS{Headers: map[string]string{"apns-topic": "other.app"}}
	codeIs(t, call(t, c, "/v1/notifications", raw), "invalid_request")
	codeIs(t, c.RegisterLiveActivity(context.Background(), "phone", testPhoneToken, "activity-a", "../not-a-token", time.Now().Add(time.Hour).Unix()), "invalid_request")
}
func TestLiveSenderConstructsOnlyFixedBoundedAPNSContract(t *testing.T) {
	var captured map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&captured)
		w.WriteHeader(200)
	}))
	defer server.Close()
	sender := &FCMSender{tokens: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "server-oauth"}), endpoint: server.URL, http: server.Client()}
	delivery := liveDelivery()
	result := sender.Send(context.Background(), testPhoneToken, Message{LiveActivity: &delivery, activityToken: strings.Repeat("ab", 32), Data: map[string]string{"type": "live_activity"}})
	if result.Status != Sent {
		t.Fatal(result.Error)
	}
	message := captured["message"].(map[string]any)
	if message["token"] != testPhoneToken || message["live_activity"] != nil {
		t.Fatal("internal contract leaked into FCM envelope")
	}
	apns := message["apns"].(map[string]any)
	headers := apns["headers"].(map[string]any)
	if headers["apns-topic"] != "com.morrie.text.push-type.liveactivity" || headers["apns-push-type"] != "liveactivity" {
		t.Fatal("caller controlled APNs scope")
	}
}
