package agentvisibility

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var at = time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
var scope = Scope{"authority", "epoch1", "session", "root"}
var binding = Binding{ParentTaskID: "p", ChildTaskID: "c", TaskGroupID: "g", ResultVersionID: "v1", SourceHash: strings.Repeat("a", 64), ReviewScope: "candidate only", DefinitionRevision: 1}

type fakeReader struct {
	s    Scope
	p    Probe
	hook func()
	err  error
}

func (f *fakeReader) CurrentScope() Scope { return f.s }
func (f *fakeReader) Probe(context.Context, Scope) (Probe, error) {
	if f.hook != nil {
		f.hook()
	}
	return f.p, f.err
}
func reader() *fakeReader {
	return &fakeReader{s: scope, p: Probe{Scope: scope, Root: Actor{ID: "root", SessionTreeID: "native-session", ObservedAt: at}, Actors: []Actor{{ID: "qa", ParentID: "root", SessionTreeID: "native-session", Status: "active", Source: "native_probe", ObservedAt: at}}, ObservedAt: at}}
}

type fakeResolver struct {
	r        Resolution
	b        Binding
	calls    int
	changeAt int
}

func (f *fakeResolver) CanonicalEpoch() string                                    { return "canonical1" }
func (f *fakeResolver) Resolve(context.Context, Scope, Actor) (Resolution, error) { return f.r, nil }
func (f *fakeResolver) CurrentBinding(context.Context, Scope, string) (Binding, error) {
	f.calls++
	b := f.b
	if f.changeAt > 0 && f.calls >= f.changeAt {
		b.DefinitionRevision++
	}
	return b, nil
}
func resolve() *fakeResolver {
	return &fakeResolver{b: binding, r: Resolution{Binding: binding, Verified: true, Role: "review", Title: "驗收", Axes: Axes{NativeFinal: "sealed", Review: "pass", Delivery: "unverified", Cancellation: "unknown", Deployment: "unknown"}, Report: &Report{Availability: "available", TaskID: "c", ResultVersionID: "v1", SourceHash: binding.SourceHash, ReportID: "report", WindowID: "window", ReadingAnchor: "anchor"}}}
}
func observe(r Reader, v Resolver) Snapshot {
	return Observe(context.Background(), scope, r, v, func() time.Time { return at }, time.Minute)
}
func TestNativeMetadataIsNotCanonicalQA(t *testing.T) {
	s := observe(reader(), nil)
	c := s.Children[0]
	if s.Availability != "live" || c.State != "active" || c.Association != "unmatched" || c.Axes.Review != "unverified" || c.Binding != nil {
		t.Fatalf("unbound probe promoted: %+v", s)
	}
}
func TestCompletedAndClosedDoNotBorrowFinal(t *testing.T) {
	for _, status := range []string{"completed", "idle", "shutdown", "notLoaded", "systemError"} {
		r := reader()
		r.p.Actors[0].Status = status
		c := observe(r, nil).Children[0]
		if c.State == "done" || c.State == "needsfix" || c.Axes.NativeFinal == "sealed" {
			t.Fatalf("status %s promoted", status)
		}
	}
}
func TestCurrentScopedQAPassAndUnavailableReportIndependent(t *testing.T) {
	v := resolve()
	v.r.Report.Availability = "unavailable"
	s := observe(reader(), v)
	c := s.Children[0]
	if c.State != "done" || c.Axes.Review != "pass" || c.Report.Availability != "unavailable" || c.Axes.Deployment != "unknown" || c.Axes.Delivery != "unverified" {
		t.Fatalf("independent axes lost %+v", c)
	}
}
func TestStaleFutureMissingSourceAndWrongParentNeverActive(t *testing.T) {
	for _, mutate := range []func(*fakeReader){func(r *fakeReader) { r.p.Actors[0].ObservedAt = at.Add(-2 * time.Minute) }, func(r *fakeReader) { r.p.Actors[0].ObservedAt = at.Add(time.Second) }, func(r *fakeReader) { r.p.Actors[0].Source = "cache" }, func(r *fakeReader) { r.p.Actors[0].ParentID = "wrong" }, func(r *fakeReader) { r.p.Actors[0].SessionTreeID = "other" }} {
		r := reader()
		mutate(r)
		c := observe(r, resolve()).Children[0]
		if c.State != "unknownstale" || c.LastObserved != "" || c.Binding != nil {
			t.Fatalf("untrusted child promoted %+v", c)
		}
	}
}
func TestLateEpochRootAndCurrentRequirementInvalidate(t *testing.T) {
	r := reader()
	r.hook = func() { r.s.Epoch = "epoch2" }
	if observe(r, resolve()).Availability != "unknownstale" {
		t.Fatal("late epoch accepted")
	}
	v := resolve()
	v.changeAt = 2
	if observe(reader(), v).Availability != "unknownstale" {
		t.Fatal("late new requirement accepted")
	}
	r = reader()
	r.p.Root.ID = "other"
	if observe(r, nil).Availability != "unknownstale" {
		t.Fatal("wrong root accepted")
	}
}
func TestVersionMismatchCannotBorrowPassOrReport(t *testing.T) {
	v := resolve()
	v.b.ResultVersionID = "new"
	c := observe(reader(), v).Children[0]
	if c.Association != "unmatched" || c.State == "done" || c.Report != nil {
		t.Fatalf("old QA promoted %+v", c)
	}
}
func TestDuplicateAndCycleBounded(t *testing.T) {
	r := reader()
	r.p.Actors = append(r.p.Actors, r.p.Actors[0])
	if observe(r, nil).Availability != "unknownstale" {
		t.Fatal("duplicate IDs accepted")
	}
	r = reader()
	r.p.Actors[0].ParentID = "qa"
	if observe(r, nil).Children[0].State != "unknownstale" {
		t.Fatal("cycle accepted")
	}
}
func TestFreshRootDoesNotFreshenMissingChild(t *testing.T) {
	r := reader()
	r.p.Actors[0] = Actor{ID: "qa", Status: "unknown"}
	s := observe(r, nil)
	if s.Availability != "live" || s.Children[0].Source != "none" || s.Children[0].LastObserved != "" {
		t.Fatal("fresh root impersonates child heartbeat")
	}
}
func TestNestedSameTreeActorAncestry(t *testing.T) {
	r := reader()
	a := r.p.Actors[0]
	a.ID = "impl"
	r.p.Actors = append(r.p.Actors, a)
	r.p.Actors[0].ParentID = "impl"
	if observe(r, nil).Children[1].State != "active" {
		t.Fatal("genuine descendant missing")
	}
}
func TestCanceledProbeAndUnavailableReader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := Observe(ctx, scope, reader(), nil, func() time.Time { return at }, time.Minute)
	if s.Availability != "unknownstale" || len(s.Children) > 0 {
		t.Fatal("canceled read used")
	}
	if observe(nil, nil).Availability != "unsupported" {
		t.Fatal("missing wire claims live")
	}
}
func TestNativeWhitelistPrivacyParentConflictAndNoHistory(t *testing.T) {
	raw := `{"thread":{"id":"qa","parentThreadId":"root","sessionId":"native-session","status":{"type":"active"},"source":{"subAgent":{"thread_spawn":{"parent_thread_id":"root","depth":1}}},"turns":[],"preview":"PRIVATE_PAYLOAD","name":"PRIVATE_PAYLOAD","path":"PRIVATE_PAYLOAD","cwd":"PRIVATE_PAYLOAD","model":"PRIVATE_PAYLOAD","gitInfo":{"x":"PRIVATE_PAYLOAD"}}}`
	a, e := DecodeThreadMetadata([]byte(raw), "qa")
	if e != nil {
		t.Fatal(e)
	}
	b, _ := json.Marshal(a)
	if strings.Contains(string(b), "PRIVATE_PAYLOAD") {
		t.Fatal("private metadata retained")
	}
	for _, bad := range []string{strings.Replace(raw, `"parent_thread_id":"root"`, `"parent_thread_id":"other"`, 1), strings.Replace(raw, `"turns":[]`, `"turns":[{"text":"PRIVATE_PAYLOAD"}]`, 1), strings.Repeat("x", 65537)} {
		if _, e := DecodeThreadMetadata([]byte(bad), "qa"); e == nil {
			t.Fatal("invalid provider metadata accepted")
		}
	}
}

// Shared fixture proves actual adapter projection separately from live probes.
func TestExportInteropFixtures(t *testing.T) {
	for _, v := range []Resolver{nil, resolve()} {
		b, e := json.Marshal(observe(reader(), v))
		if e != nil {
			t.Fatal(e)
		}
		t.Log(string(b))
	}
}
