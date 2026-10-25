package core

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAutomationNativeAppPreflightDoesNotNeedCredentialsOrReadData(t *testing.T) {
	h, _ := newTestHub(t)
	for _, origin := range []string{"http://localhost", "https://localhost", "capacitor://localhost", "tauri://localhost", "http://tauri.localhost", "https://tauri.localhost"} {
		t.Run(origin, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodOptions, "/api/automation/v1/inbox/reprocess", nil)
			r.RemoteAddr = "100.64.0.10:1234"
			r.Header.Set("Origin", origin)
			r.Header.Set("Access-Control-Request-Method", "POST")
			r.Header.Set("Access-Control-Request-Headers", "authorization,content-type,x-bridge-device-id")
			w := httptest.NewRecorder()
			h.ServeAutomationAPI(w, r) // No automation store or credentials.
			if w.Code != http.StatusNoContent || w.Body.Len() != 0 || w.Header().Get("Access-Control-Allow-Origin") != origin {
				t.Fatalf("status=%d headers=%v body=%s", w.Code, w.Header(), w.Body.String())
			}
			if w.Header().Get("Access-Control-Allow-Credentials") != "" || !strings.Contains(strings.Join(w.Header().Values("Vary"), ","), "Origin") {
				t.Fatalf("unexpected credential/cache policy: %v", w.Header())
			}
		})
	}
}

func TestAutomationCORSDoesNotGrantAuthentication(t *testing.T) {
	h, _ := newTestHub(t)
	attachAutomationStore(t, h, t.TempDir())
	t.Setenv("BRIDGE_AUTH_TOKEN", "paired-token")
	for _, tc := range []struct {
		origin, token string
		status        int
		cors          string
	}{
		{"http://localhost", "", 401, "http://localhost"},
		{"http://localhost", "wrong", 401, "http://localhost"},
		{"http://localhost", "paired-token", 200, "http://localhost"},
		{"https://untrusted.example", "paired-token", 200, ""},
		{"", "paired-token", 200, ""},
	} {
		r := httptest.NewRequest(http.MethodGet, "/api/automation/v1/snapshot", nil)
		r.RemoteAddr = "100.64.0.10:1234"
		r.Header.Set("Origin", tc.origin)
		if tc.token != "" {
			r.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		h.ServeAutomationAPI(w, r)
		if w.Code != tc.status || w.Header().Get("Access-Control-Allow-Origin") != tc.cors {
			t.Fatalf("origin=%q tokenPresent=%t status=%d headers=%v", tc.origin, tc.token != "", w.Code, w.Header())
		}
		if !strings.Contains(strings.Join(w.Header().Values("Vary"), ","), "Origin") {
			t.Fatalf("response cannot be cached across application origins: %v", w.Header())
		}
	}
}

func TestAutomationPreflightRejectsOtherOriginsMethodsAndHeaders(t *testing.T) {
	h, _ := newTestHub(t)
	for _, tc := range []struct{ origin, method, headers string }{
		{"https://untrusted.example", "POST", "authorization"},
		{"http://localhost.evil.example", "POST", "authorization"},
		{"null", "POST", "authorization"},
		{"http://localhost:8181", "POST", "authorization"},
		{"http://localhost", "DELETE", "authorization"},
		{"http://localhost", "", "authorization"},
		{"http://localhost", "POST", "authorization,x-unexpected-header"},
	} {
		r := httptest.NewRequest(http.MethodOptions, "/api/automation/v1/accounts", nil)
		r.RemoteAddr = "100.64.0.10:1234"
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("Access-Control-Request-Method", tc.method)
		r.Header.Set("Access-Control-Request-Headers", tc.headers)
		w := httptest.NewRecorder()
		h.ServeAutomationAPI(w, r)
		if w.Code != http.StatusForbidden || w.Header().Get("Access-Control-Allow-Methods") != "" {
			t.Fatalf("preflight unexpectedly allowed: %+v status=%d", tc, w.Code)
		}
	}
}
