package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"everything-go/internal/eventinbox"
)

func attachEventStore(t *testing.T, h *Hub) *eventinbox.Store {
	t.Helper()
	store, err := eventinbox.Open(t.TempDir(), "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	h.SetEventInbox(store)
	return store
}

func TestEventAPILoopbackPersistsAndDeduplicates(t *testing.T) {
	h, _ := newTestHub(t)
	store := attachEventStore(t, h)
	body := `{"schema_version":1,"source":"github","kind":"ci_failed","severity":"error","title":"CI failed","body":"race test failed","data":{"run_id":42}}`

	post := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/events/v1/events", strings.NewReader(body))
		req.RemoteAddr = "127.0.0.1:42000"
		req.Header.Set("Idempotency-Key", "run-42")
		response := httptest.NewRecorder()
		h.ServeEventAPI(response, req)
		return response
	}
	first := post()
	if first.Code != http.StatusAccepted || !strings.Contains(first.Body.String(), `"deduplicated":false`) {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	if h.offline.Len() != 0 {
		t.Fatalf("canonical event leaked into global offline journal: %d", h.offline.Len())
	}
	second := post()
	if second.Code != http.StatusAccepted || !strings.Contains(second.Body.String(), `"deduplicated":true`) {
		t.Fatalf("second status=%d body=%s", second.Code, second.Body.String())
	}
	snapshot, err := store.Snapshot(context.Background(), "phone", 0)
	if err != nil || len(snapshot.Items) != 1 || snapshot.Items[0].Source != "github" {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
}

func TestEventAPIDedicatedTokenAndValidation(t *testing.T) {
	t.Setenv("EVENT_INGRESS_TOKEN", "event-secret")
	h, _ := newTestHub(t)
	attachEventStore(t, h)
	body := `{"source":"monitor","event_key":"alert-1","kind":"alert","title":"Alert"}`
	req := httptest.NewRequest(http.MethodPost, "/api/events/v1/events", strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:42000"
	response := httptest.NewRecorder()
	h.ServeEventAPI(response, req)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status=%d", response.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/events/v1/events", strings.NewReader(body))
	req.RemoteAddr = "203.0.113.8:42000"
	req.Header.Set("Authorization", "Bearer event-secret")
	response = httptest.NewRecorder()
	h.ServeEventAPI(response, req)
	if response.Code != http.StatusAccepted {
		t.Fatalf("authorized status=%d body=%s", response.Code, response.Body.String())
	}

	invalid := `{"source":"monitor","event_key":"bad","kind":"alert","severity":"execute","title":"No"}`
	req = httptest.NewRequest(http.MethodPost, "/api/events/v1/events", strings.NewReader(invalid))
	req.RemoteAddr = "203.0.113.8:42000"
	req.Header.Set("Authorization", "Bearer event-secret")
	response = httptest.NewRecorder()
	h.ServeEventAPI(response, req)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid severity status=%d", response.Code)
	}
}

func TestEventAPIRejectsAmbiguousOrIncompatiblePayloads(t *testing.T) {
	h, _ := newTestHub(t)
	attachEventStore(t, h)
	tests := []string{
		`{"source":"monitor","event_key":"trailing","kind":"alert","title":"Alert"}{}`,
		`{"source":"monitor","event_key":"array","kind":"alert","title":"Alert","data":[]}`,
		`{"source":"monitor","event_key":"time","kind":"alert","title":"Alert","occurred_at":"2026-08-29T12:00:00Z","expires_at":"2026-08-29T11:00:00Z"}`,
	}
	for _, body := range tests {
		req := httptest.NewRequest(http.MethodPost, "/api/events/v1/events", strings.NewReader(body))
		req.RemoteAddr = "127.0.0.1:42000"
		response := httptest.NewRecorder()
		h.ServeEventAPI(response, req)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body=%s status=%d response=%s", body, response.Code, response.Body.String())
		}
	}
}
