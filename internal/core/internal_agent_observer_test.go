package core

import (
	"context"
	"everything-go/internal/agentvisibility"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"testing"
)

type visibilityProvider struct {
	plainProv
	gotAuthority, gotSession string
	switchRoot               bool
}

func (p *visibilityProvider) ObserveInternalAgents(ctx context.Context, s *session.Session, authority string) agentvisibility.Snapshot {
	p.gotAuthority = authority
	p.gotSession = s.ID
	if p.switchRoot {
		s.SetResumeID("new-root")
	}
	return agentvisibility.Snapshot{Type: "internal_agent_observation", Schema: agentvisibility.Schema, Authority: authority, SourceEpoch: "native-epoch", SessionID: s.ID, RootActorID: "root", Availability: "live", Coverage: "partial_provider_child_cache", ObservedAt: "2026-10-09T00:00:00Z", Children: []agentvisibility.Child{}}
}

type visibilityExecutor struct {
	fakeExec
	p *visibilityProvider
}

func (e *visibilityExecutor) ProviderFor(*session.Session) (backend.HistoryProvider, bool) {
	return e.p, true
}
func (e *visibilityExecutor) AllProviders() []backend.HistoryProvider {
	return []backend.HistoryProvider{e.p}
}

func TestVisibilityAuthenticatedRoutePreservesSessionScope(t *testing.T) {
	h, _ := newTestHub(t)
	p := &visibilityProvider{}
	h.SetExecutor(&visibilityExecutor{fakeExec: fakeExec{sink: h}, p: p})
	h.registry.Create("s1", "S", t.TempDir(), backend.Codex, "", "", "root")
	c := sharedReadClient(t, h, "fixture-reader")
	route(h, c, `{"type":"request_internal_agent_observation","session_id":"s1","authority":"FAKE","root_actor_id":"FAKE","binding":{"parent_task_id":"FAKE"}}`)
	ev := waitForType(t, c, "internal_agent_observation")
	if p.gotAuthority != "i1" || p.gotSession != "s1" || ev["root_actor_id"] != "root" {
		t.Fatalf("client controlled original scope: %+v", ev)
	}
}
func TestVisibilityRootSwitchDuringReadInvalidates(t *testing.T) {
	h, _ := newTestHub(t)
	p := &visibilityProvider{switchRoot: true}
	h.SetExecutor(&visibilityExecutor{fakeExec: fakeExec{sink: h}, p: p})
	h.registry.Create("s1", "S", t.TempDir(), backend.Codex, "", "", "root")
	c := sharedReadClient(t, h, "fixture-reader")
	h.handleInternalAgentObservation(context.Background(), c, "s1")
	ev := waitForType(t, c, "internal_agent_observation")
	if ev["availability"] != "unknownstale" || ev["observed_at"] != "" {
		t.Fatalf("old root promoted: %+v", ev)
	}
}
func TestVisibilityMissingNativeProviderUnsupported(t *testing.T) {
	h, _ := newTestHub(t)
	h.SetExecutor(&plainExec{fakeExec: fakeExec{sink: h}})
	h.registry.Create("s1", "S", t.TempDir(), backend.Claude, "", "", "root")
	c := sharedReadClient(t, h, "fixture-reader")
	h.handleInternalAgentObservation(context.Background(), c, "s1")
	if ev := waitForType(t, c, "internal_agent_observation"); ev["availability"] != "unsupported" {
		t.Fatalf("root wire promoted: %+v", ev)
	}
}
