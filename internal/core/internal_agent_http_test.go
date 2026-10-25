package core

import (
	"encoding/json"
	"everything-go/internal/agentvisibility"
	"net/http/httptest"
	"testing"
)

func TestVisibilityHTTPRejectsUntrustedScopeAndWrites(t *testing.T) {
	h, _ := newTestHub(t)
	t.Setenv("BRIDGE_AUTH_TOKEN", "fixture-auth-only")
	h.registry.Create("fixture-session", "fixture", t.TempDir(), "codex", "", "", "fixture-root")
	for _, x := range []struct {
		method, path, token string
		status              int
	}{
		{"GET", "/?session_id=fixture-session", "", 401},
		{"POST", "/?session_id=fixture-session", "fixture-auth-only", 405},
		{"GET", "/?session_id=fixture-session&authority=FAKE", "fixture-auth-only", 400},
		{"GET", "/?session_id=fixture-session&session_id=other", "fixture-auth-only", 400},
		{"GET", "/?session_id=missing", "fixture-auth-only", 404},
		{"GET", "/?session_id=fixture-session", "fixture-auth-only", 200},
	} {
		q := httptest.NewRequest(x.method, x.path, nil)
		q.Header.Set("Authorization", "Bearer "+x.token)
		w := httptest.NewRecorder()
		h.ServeInternalAgentObservation(w, q)
		if w.Code != x.status {
			t.Fatalf("method=%s status=%d wanted=%d", x.method, w.Code, x.status)
		}
		if x.status == 200 {
			var s agentvisibility.Snapshot
			if json.Unmarshal(w.Body.Bytes(), &s) != nil || s.Availability != "unsupported" || s.Authority != "i1" || s.ObservedAt != "" {
				t.Fatal("unsupported scope became live")
			}
		}
	}
}
