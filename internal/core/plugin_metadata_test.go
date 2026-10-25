package core

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	observationcontract "everything-go/contracts/internal-agent-observation/v1"
	metadatacontract "everything-go/contracts/plugin-metadata/v1"
	taskcontract "everything-go/contracts/task-api/v1"
	"everything-go/internal/agentvisibility"
	"everything-go/internal/backend"
	"everything-go/internal/session"
)

func TestPluginMetadataStatusAxesAndOriginalContract(t *testing.T) {
	h, _ := newTestHub(t)
	t.Setenv("BRIDGE_AUTH_TOKEN", "fixture-auth")
	for _, tc := range []struct {
		method, path, token string
		code                int
	}{
		{"GET", "/", "", 401}, {"POST", "/", "fixture-auth", 405},
		{"GET", "/?installed=true", "fixture-auth", 400}, {"GET", "/", "fixture-auth", 200},
	} {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		h.ServePluginMetadataStatus(w, r)
		if w.Code != tc.code {
			t.Fatalf("status %d expected %d", w.Code, tc.code)
		}
		if tc.code == 200 {
			if e := taskcontract.Validate2020(metadatacontract.Schema, w.Body.Bytes()); e != nil {
				t.Fatal(e)
			}
			var v map[string]any
			if json.Unmarshal(w.Body.Bytes(), &v) != nil {
				t.Fatal("invalid projection")
			}
			host := v["host"].(map[string]any)
			host["loaded"] = "true"
			forged, _ := json.Marshal(v)
			if taskcontract.Validate2020(metadatacontract.Schema, forged) == nil {
				t.Fatal("host load proof forged")
			}
			host["loaded"] = "unknown"
			for _, axis := range []string{"installed", "enabled", "loaded", "discovery"} {
				if host[axis] != "unknown" {
					t.Fatal("Hub read promoted host axis", axis)
				}
			}
			contracts := v["contracts"].(map[string]any)
			task := contracts["task_api"].(map[string]any)
			if task["version"] != "1.0.0-rc.1" || task["sha256"] != "674aada21ce809c5bec8324b192163c4f3c4acf7f5b57f39042eacd9982d23fa" {
				t.Fatal("original contract changed")
			}
			mode := v["supported_mode"].(map[string]any)
			if mode["root_inventory_callback"] != "unsupported" || mode["canonical_qa"] != "unsupported_original_relation_required" {
				t.Fatal("unavailable owner promoted")
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("metadata cached")
			}
		}
	}
	c := sharedReadClient(t, h, "fixture-reader")
	route(h, c, `{"type":"request_plugin_metadata_status","installed":true,"loaded":true}`)
	v := waitForType(t, c, "plugin_metadata_status")
	if v["host"].(map[string]any)["loaded"] != "unknown" {
		t.Fatal("client claimed host status")
	}
}

type driftVisibilityProvider struct {
	plainProv
	mutate              func(*session.Session)
	authority, coverage string
}

func (p *driftVisibilityProvider) ObserveInternalAgents(_ context.Context, s *session.Session, a string) agentvisibility.Snapshot {
	if p.mutate != nil {
		p.mutate(s)
	}
	if p.authority != "" {
		a = p.authority
	}
	return agentvisibility.Snapshot{Type: "internal_agent_observation", Schema: agentvisibility.Schema, Authority: a, SourceEpoch: "fixture-epoch", SessionID: s.ID, RootActorID: "root", Availability: "live", Coverage: p.coverage, ObservedAt: "2026-10-10T00:00:00Z", Children: []agentvisibility.Child{}}
}

type driftVisibilityExecutor struct {
	fakeExec
	p *driftVisibilityProvider
}

func (e *driftVisibilityExecutor) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.p, true
}
func (e *driftVisibilityExecutor) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.p}
}

func TestPluginMetadataRejectsOwnerDriftAndInventoryPromotion(t *testing.T) {
	for _, tc := range []struct {
		name, authority, coverage string
		mutate                    func(*session.Session)
	}{
		{name: "wrong_authority", authority: "forged", coverage: "partial_provider_child_cache"},
		{name: "whole_root_inventory", coverage: "all_root_qa"},
		{name: "missing_coverage"},
		{name: "config_drift", coverage: "partial_provider_child_cache", mutate: func(s *session.Session) { s.SetConfigRevision(2) }},
		{name: "hidden_during_read", coverage: "partial_provider_child_cache", mutate: func(s *session.Session) { b := true; s.SetMeta(nil, &b) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newTestHub(t)
			h.registry.Create("s1", "S", t.TempDir(), backend.Codex, "", "", "root")
			p := &driftVisibilityProvider{authority: tc.authority, coverage: tc.coverage, mutate: tc.mutate}
			h.SetExecutor(&driftVisibilityExecutor{fakeExec: fakeExec{sink: h}, p: p})
			c := sharedReadClient(t, h, "fixture-reader")
			h.handleInternalAgentObservation(context.Background(), c, "s1", "obs_fixture")
			binding := waitForType(t, c, "internal_agent_owner_binding")
			if binding["supported"] != false || binding["root_inventory_callback"] != "unsupported" {
				t.Fatal("stale/full inventory binding accepted")
			}
			ev := waitForType(t, c, "internal_agent_observation")
			if ev["availability"] != "unknownstale" || ev["observed_at"] != "" {
				t.Fatal("drift refreshed source")
			}
		})
	}
}

func TestPluginMetadataScopeUnavailableAndReadonlyRoute(t *testing.T) {
	h, _ := newTestHub(t)
	h.registry.Create("hidden", "S", t.TempDir(), backend.Codex, "", "", "root")
	s, _ := h.registry.Get("hidden")
	b := true
	s.SetMeta(nil, &b)
	if _, ok := h.observeInternalAgents(context.Background(), "hidden"); ok {
		t.Fatal("hidden session observed")
	}
	h.cfg.RootDir = t.TempDir()
	h.registry.Create("outside", "S", t.TempDir(), backend.Codex, "", "", "root")
	if _, ok := h.observeInternalAgents(context.Background(), "outside"); ok {
		t.Fatal("out-of-root observed")
	}
	h.cfg.RootDir = ""
	s = h.registry.Create("unsupported", "S", t.TempDir(), backend.Claude, "", "", "root")
	before := s.Snapshot()
	c := sharedReadClient(t, h, "fixture-reader")
	route(h, c, `{"type":"request_internal_agent_observation","session_id":"unsupported","request_id":"obs_fixture","authority":"forged","source_epoch":"forged","children":[{"state":"done"}]}`)
	binding := waitForType(t, c, "internal_agent_owner_binding")
	if binding["supported"] != false || binding["authority"] != "i1" {
		t.Fatal("unsupported owner promoted")
	}
	ev := waitForType(t, c, "internal_agent_observation")
	raw, _ := json.Marshal(ev)
	if e := taskcontract.Validate2020(observationcontract.Schema, raw); e != nil {
		t.Fatal(e)
	}
	ev["schema"] = "1.0.0-rc.2"
	raw, _ = json.Marshal(ev)
	if taskcontract.Validate2020(observationcontract.Schema, raw) == nil {
		t.Fatal("TaskAPI alias accepted")
	}
	after := s.Snapshot()
	if before.ResumeID != after.ResumeID || before.ConfigRevision != after.ConfigRevision || before.State != after.State {
		t.Fatal("read changed original session")
	}
}
