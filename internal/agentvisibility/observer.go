// Package agentvisibility projects trusted metadata. It does not dispatch,
// read history, attest QA, change permissions, or create a second task store.
package agentvisibility

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"
)

const Schema = "internal-agent-observation.v1"
const MaxActors = 16

// Scope is supplied by the authenticated original session owner, never client JSON.
type Scope struct{ Authority, Epoch, SessionID, RootActorID string }

func (s Scope) Valid() bool {
	return s.Authority != "" && s.Epoch != "" && s.SessionID != "" && s.RootActorID != ""
}

// Binding is resolved by the original canonical owner against its current body,
// version and scope. Actor ancestry alone is deliberately insufficient.
type Binding struct {
	ParentTaskID       string `json:"parent_task_id"`
	ChildTaskID        string `json:"child_task_id"`
	TaskGroupID        string `json:"task_group_id"`
	ResultVersionID    string `json:"result_version_id"`
	SourceHash         string `json:"source_hash"`
	ReviewScope        string `json:"review_scope"`
	DefinitionRevision uint64 `json:"definition_revision"`
	CommandReceiptID   string `json:"command_receipt_id,omitempty"`
}

func (b Binding) Valid() bool {
	return b.ParentTaskID != "" && b.ChildTaskID != "" && b.TaskGroupID != "" && b.ResultVersionID != "" && regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(b.SourceHash) && b.ReviewScope != "" && b.DefinitionRevision > 0
}

type Report struct {
	Availability    string `json:"availability"`
	TaskID          string `json:"task_id"`
	ResultVersionID string `json:"result_version_id"`
	SourceHash      string `json:"source_hash"`
	ReportID        string `json:"report_id"`
	WindowID        string `json:"window_id"`
	ReadingAnchor   string `json:"reading_anchor"`
}

func (r Report) Matches(b Binding) bool {
	return r.Availability == "available" && r.TaskID == b.ChildTaskID && r.ResultVersionID == b.ResultVersionID && r.SourceHash == b.SourceHash && r.ReportID != "" && r.WindowID != "" && r.ReadingAnchor != ""
}

type Axes struct {
	Execution    string `json:"execution"`
	NativeFinal  string `json:"native_final"`
	Review       string `json:"review"`
	Delivery     string `json:"delivery"`
	Cancellation string `json:"cancellation"`
	Deployment   string `json:"deployment"`
}
type Child struct {
	ActorID        string   `json:"actor_id"`
	NativeIDStatus string   `json:"native_id_status,omitempty"`
	RelationProof  string   `json:"relation_proof,omitempty"`
	ProbeError     string   `json:"probe_error,omitempty"`
	ParentActorID  string   `json:"parent_actor_id"`
	ActorKind      string   `json:"actor_kind"`
	Role           string   `json:"role"`
	Title          string   `json:"title"`
	Stage          string   `json:"stage"`
	State          string   `json:"state"`
	ReportedState  string   `json:"reported_state"`
	Source         string   `json:"source"`
	LastObserved   string   `json:"last_observed"`
	Association    string   `json:"association"`
	Binding        *Binding `json:"binding,omitempty"`
	Report         *Report  `json:"report,omitempty"`
	Axes           Axes     `json:"axes"`
}
type Snapshot struct {
	Type         string  `json:"type"`
	Schema       string  `json:"schema"`
	Authority    string  `json:"authority"`
	SourceEpoch  string  `json:"source_epoch"`
	SessionID    string  `json:"session_id"`
	RootActorID  string  `json:"root_actor_id"`
	ObservedAt   string  `json:"observed_at"`
	Availability string  `json:"availability"`
	Children     []Child `json:"children"`
	RootState    string  `json:"root_state,omitempty"`
	Coverage     string  `json:"coverage,omitempty"`
	ProbeError   string  `json:"probe_error,omitempty"`
	AttemptedAt  string  `json:"attempted_at,omitempty"`
}

func Unsupported(s Scope) Snapshot {
	return Snapshot{Type: "internal_agent_observation", Schema: Schema, Authority: s.Authority, SourceEpoch: s.Epoch, SessionID: s.SessionID, RootActorID: s.RootActorID, Availability: "unsupported", Children: []Child{}}
}

// Actor contains only provider/orchestration metadata; no name, prompt, preview,
// tool arguments, output, reasoning, paths, environment, credentials or messages.
type Actor struct {
	ID, ParentID, SessionTreeID, Status, Source string
	NativeIDStatus, RelationProof, ProbeError   string
	ObservedAt                                  time.Time
}
type Probe struct {
	Coverage    string
	AttemptedAt time.Time
	Scope       Scope
	Root        Actor
	Actors      []Actor
	ObservedAt  time.Time
}

// Reader is installed by the original execution owner. An arbitrary parsed JSON
// body or a persisted cache must never implement this trust boundary.
type Reader interface {
	Probe(context.Context, Scope) (Probe, error)
	CurrentScope() Scope
}

// Resolution is a verified scoped read from the existing canonical owner. Only
// that owner may supply report proof. This is not exposed on a writable wire.
type Resolution struct {
	Binding            Binding
	Report             *Report
	Axes               Axes
	Role, Title, Stage string
	Verified           bool
}
type Resolver interface {
	// CanonicalEpoch changes on any binding/result/requirement/authority change.
	CanonicalEpoch() string
	Resolve(context.Context, Scope, Actor) (Resolution, error)
	CurrentBinding(context.Context, Scope, string) (Binding, error)
}

func Observe(ctx context.Context, s Scope, reader Reader, resolver Resolver, clock func() time.Time, maxAge time.Duration) Snapshot {
	out := Unsupported(s)
	canonicalEpoch := ""
	if resolver != nil {
		canonicalEpoch = resolver.CanonicalEpoch()
	}
	if !s.Valid() || reader == nil || clock == nil || reader.CurrentScope() != s {
		return out
	}
	p, err := reader.Probe(ctx, s)
	now := clock()
	out.Coverage = p.Coverage
	if !p.AttemptedAt.IsZero() {
		out.AttemptedAt = p.AttemptedAt.UTC().Format(time.RFC3339Nano)
	}
	if err != nil {
		out.ProbeError = probeError(err)
	}
	if err != nil || p.Scope != s || reader.CurrentScope() != s || p.Root.ID != s.RootActorID || p.Root.SessionTreeID == "" || !fresh(p.ObservedAt, now, maxAge) || !fresh(p.Root.ObservedAt, now, maxAge) || len(p.Actors) > MaxActors {
		out.Availability = "unknownstale"
		return out
	}
	out.Availability = "live"
	out.RootState = p.Root.Status
	out.ObservedAt = p.ObservedAt.UTC().Format(time.RFC3339Nano)
	actors := map[string]Actor{}
	for _, a := range p.Actors {
		if a.ID == "" || a.ID == p.Root.ID {
			continue
		}
		if _, duplicate := actors[a.ID]; duplicate {
			out.Availability = "unknownstale"
			out.ObservedAt = ""
			return out
		}
		actors[a.ID] = a
	}
	ids := make([]string, 0, len(actors))
	for id := range actors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		a := actors[id]
		child := Child{NativeIDStatus: a.NativeIDStatus, RelationProof: a.RelationProof, ProbeError: a.ProbeError, ActorID: a.ID, ParentActorID: a.ParentID, ActorKind: "internal_implementation", Role: "implementation", Title: "實作", State: "unknownstale", ReportedState: "unknown", Source: "none", Association: "unmatched", Axes: Axes{Execution: "unknown", NativeFinal: "unverified", Review: "unverified", Delivery: "unverified", Cancellation: "unknown", Deployment: "unknown"}}
		if !ancestry(a, p.Root, actors) || !fresh(a.ObservedAt, now, maxAge) || (a.Source != "native_probe" && a.Source != "root_tool_observation") {
			out.Children = append(out.Children, child)
			continue
		}
		child.ReportedState = a.Status
		child.Source = a.Source
		child.LastObserved = a.ObservedAt.UTC().Format(time.RFC3339Nano)
		switch a.Status {
		case "active", "running":
			child.State = "active"
			child.Axes.Execution = "active"
		case "idle", "waiting":
			child.State = "waiting"
			child.Axes.Execution = "idle"
		case "queued", "pending":
			child.State = "queued"
			child.Axes.Execution = "queued"
		case "completed":
			child.State = "unverified"
		case "needsfix":
			child.State = "unverified"
		default:
			child.State = "unknownstale"
		}
		if resolver != nil && canonicalEpoch != "" && a.NativeIDStatus != "unavailable" {
			r, e := resolver.Resolve(ctx, s, a)
			current, ce := resolver.CurrentBinding(ctx, s, a.ID)
			if e == nil && ce == nil && r.Verified && r.Binding.Valid() && r.Binding == current {
				b := r.Binding
				child.Binding = &b
				child.Association = "bound"
				if r.Role == "review" {
					child.Role = "review"
					child.ActorKind = "internal_qa"
					child.Title = "驗收"
				}
				// Only explicit public projection fields from trusted resolver are allowed.
				if safePublic(r.Title) {
					child.Title = r.Title
				}
				if safePublic(r.Stage) {
					child.Stage = r.Stage
				}
				// Evidence axes are independent from current report-read availability.
				child.Axes.NativeFinal = allowed(r.Axes.NativeFinal, "sealed", "unverified", "unknown")
				child.Axes.Review = allowed(r.Axes.Review, "pass", "needsfix", "inprogress", "unverified", "unknown")
				child.Axes.Delivery = allowed(r.Axes.Delivery, "verified", "unverified", "unknown")
				if r.Report != nil && r.Report.TaskID == b.ChildTaskID && r.Report.ResultVersionID == b.ResultVersionID && r.Report.SourceHash == b.SourceHash {
					report := *r.Report
					report.Availability = allowed(report.Availability, "available", "unavailable", "unverified")
					if report.Availability == "unknown" || (report.Availability == "available" && !report.Matches(b)) {
						report.Availability = "unverified"
					}
					child.Report = &report
				}
				if child.Axes.NativeFinal == "sealed" && (child.Role != "review" || child.Axes.Review == "pass") {
					child.State = "done"
				}
				if child.Axes.Review == "needsfix" {
					child.State = "needsfix"
				}
				child.Axes.Cancellation = allowed(r.Axes.Cancellation, "requested", "confirmed", "unknown")
				child.Axes.Deployment = allowed(r.Axes.Deployment, "verified", "unknown")
			}
		}
		out.Children = append(out.Children, child)
	}
	// Revalidate after both native and canonical reads. A late callback may not
	// attach to a new authority/session/epoch/root or a newly revised requirement.
	if reader.CurrentScope() != s || ctx.Err() != nil || (resolver != nil && resolver.CanonicalEpoch() != canonicalEpoch) {
		out = Unsupported(s)
		out.Availability = "unknownstale"
		return out
	}
	if resolver != nil {
		for _, c := range out.Children {
			if c.Binding != nil {
				b, e := resolver.CurrentBinding(ctx, s, c.ActorID)
				if e != nil || b != *c.Binding {
					out = Unsupported(s)
					out.Availability = "unknownstale"
					return out
				}
			}
		}
	}
	if reader.CurrentScope() != s || ctx.Err() != nil || (resolver != nil && resolver.CanonicalEpoch() != canonicalEpoch) {
		out = Unsupported(s)
		out.Availability = "unknownstale"
		return out
	}
	return out
}
func ancestry(a, root Actor, actors map[string]Actor) bool {
	seen := map[string]bool{}
	for i := 0; i <= MaxActors; i++ {
		if a.SessionTreeID != root.SessionTreeID || a.ID == root.ID || seen[a.ID] || a.ParentID == "" {
			return false
		}
		seen[a.ID] = true
		if a.ParentID == root.ID {
			return true
		}
		parent, ok := actors[a.ParentID]
		if !ok {
			return false
		}
		a = parent
	}
	return false
}
func fresh(t, now time.Time, maxAge time.Duration) bool {
	return maxAge > 0 && !t.IsZero() && !t.After(now) && now.Sub(t) <= maxAge
}
func safePublic(s string) bool {
	return s != "" && len(s) <= 160 && !strings.ContainsAny(s, "\r\n\x00")
}
func allowed(s string, values ...string) string {
	for _, v := range values {
		if s == v {
			return s
		}
	}
	return "unknown"
}

var ErrUnsupported = errors.New("trusted native metadata observer unavailable")

func probeError(e error) string {
	if errors.Is(e, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(e, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(e, ErrUnsupported) {
		return "unsupported"
	}
	return "probe_failed"
}
