package core

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"everything-go/internal/relay"
)

func TestRelayAPIAuthenticatesTargetAndDeduplicates(t *testing.T) {
	h, _ := newTestHub(t)
	h.cfg.InstanceID = "morrie"
	attachEventStore(t, h)
	store, err := relay.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	t.Setenv("RELAY_TEST_SECRET", "secret")
	t.Setenv("BRIDGE_RELAY_ALLOW_LOOPBACK", "1")
	h.SetRelay(store, relay.Peers{"wulala": {InstanceID: "wulala", BaseURL: "http://100.64.0.1:8766", SecretRef: "env:RELAY_TEST_SECRET"}})
	job := relay.Job{SchemaVersion: 1, ID: "job-1", OriginInstanceID: "wulala", TargetInstanceID: "morrie",
		EventID: "event-1", TargetWorkItemID: "wi", TargetSessionID: "session", Instruction: "review", ReviewOnly: true}
	body, _ := json.Marshal(job)
	post := func(payload []byte, signedBody []byte) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/relay/v1/jobs", bytes.NewReader(payload))
		req.RemoteAddr = "127.0.0.1:1234"
		for key, value := range relay.Sign("secret", "wulala", http.MethodPost, req.URL.Path, signedBody, time.Now()) {
			req.Header.Set(key, value)
		}
		response := httptest.NewRecorder()
		h.ServeRelayAPI(response, req)
		return response
	}
	first := post(body, body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	second := post(body, body)
	if second.Code != http.StatusAccepted || !bytes.Contains(second.Body.Bytes(), []byte(`"deduplicated":true`)) {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
	tampered := post(append([]byte(nil), body[:len(body)-1]...), body)
	if tampered.Code != http.StatusUnauthorized {
		t.Fatalf("tampered status=%d", tampered.Code)
	}
}
