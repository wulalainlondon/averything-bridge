package core

import (
	"context"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"net/http/httptest"
	"testing"
)

func TestPluginMetadataRevalidatesOriginalPairingAfterProviderIO(t *testing.T) {
	for _, transport := range []string{"http", "socket"} {
		t.Run(transport, func(t *testing.T) {
			h, _ := newTestHub(t)
			t.Setenv("BRIDGE_AUTH_TOKEN", "")
			h.registry.Create("s1", "S", t.TempDir(), backend.Codex, "", "", "root")
			c := sharedReadClient(t, h, "fixture-reader")
			p := &driftVisibilityProvider{coverage: "partial_provider_child_cache", mutate: func(_ *session.Session) { h.pairing.Unclaim("qa-read-fixture-reader") }}
			h.SetExecutor(&driftVisibilityExecutor{fakeExec: fakeExec{sink: h}, p: p})
			if transport == "http" {
				r := httptest.NewRequest("GET", "/?session_id=s1", nil)
				r.Header.Set("Authorization", "Bearer qa-read-fixture-reader")
				w := httptest.NewRecorder()
				h.ServeInternalAgentObservation(w, r)
				if w.Code != 401 || w.Body.Len() != 0 {
					t.Fatal("revoked HTTP principal received metadata")
				}
			} else {
				h.handleInternalAgentObservation(context.Background(), c, "s1", "obs_fixture")
				ev := waitForType(t, c, "error")
				if ev["code"] != "observation_forbidden" {
					t.Fatal("revoked socket principal received metadata", ev)
				}
				select {
				case raw := <-c.send:
					t.Fatal("extra metadata after refusal", string(raw))
				default:
				}
			}
		})
	}
}

func TestPluginMetadataRejectsUnboundAndEnrollmentSocketReaders(t *testing.T) {
	for _, mode := range []string{"unbound", "enrollment", "inventory"} {
		t.Run(mode, func(t *testing.T) {
			h, _ := newTestHub(t)
			h.registry.Create("s1", "S", t.TempDir(), backend.Codex, "", "", "root")
			c := newTestClient(h)
			if mode != "unbound" {
				c = sharedReadClient(t, h, "fixture-reader")
			}
			c.enrollmentOnly = mode == "enrollment"
			c.inventoryProbe = mode == "inventory"
			h.handleInternalAgentObservation(context.Background(), c, "s1", "obs_fixture")
			if waitForType(t, c, "error")["code"] != "observation_forbidden" {
				t.Fatal("reader allowed")
			}
		})
	}
}
