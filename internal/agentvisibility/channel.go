package agentvisibility

import (
	"context"
	"sync"
	"time"
)

// Channel retains last actual facts for failed probes without retaining any
// verification. The scope and source timestamps cannot be renewed by cache reads.
// One Channel is installed per original session. No new task/dispatch store.
type Channel struct {
	mu   sync.Mutex
	last Snapshot
}

func (c *Channel) Offline(s Scope, at time.Time) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := Unsupported(s)
	if c.last.Authority != s.Authority || c.last.SourceEpoch != s.Epoch || c.last.SessionID != s.SessionID || c.last.RootActorID != s.RootActorID {
		c.last = Snapshot{}
		return out
	}
	out = c.last
	out.Children = append([]Child(nil), c.last.Children...)
	out.Availability = "unknownstale"
	out.ProbeError = "offline"
	out.AttemptedAt = at.UTC().Format(time.RFC3339Nano)
	for i := range out.Children {
		downgrade(&out.Children[i])
		out.Children[i].ProbeError = "offline"
	}
	c.last = out
	return out
}
func (c *Channel) Read(ctx context.Context, s Scope, r Reader, v Resolver, now func() time.Time, maxAge time.Duration) Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := Observe(ctx, s, r, v, now, maxAge)
	if r == nil || r.CurrentScope() != s {
		c.last = Snapshot{}
		return next
	}
	same := c.last.Authority == s.Authority && c.last.SourceEpoch == s.Epoch && c.last.SessionID == s.SessionID && c.last.RootActorID == s.RootActorID
	if same && next.Availability == "unknownstale" && len(next.Children) == 0 {
		next.Children = append([]Child(nil), c.last.Children...)
		next.ObservedAt = c.last.ObservedAt
		next.RootState = c.last.RootState
		for i := range next.Children {
			downgrade(&next.Children[i])
			next.Children[i].ProbeError = next.ProbeError
		}
	}
	if same {
		for i := range next.Children {
			n := &next.Children[i]
			if n.ProbeError == "" {
				continue
			}
			for _, old := range c.last.Children {
				if old.ActorID == n.ActorID && n.LastObserved == "" {
					n.ParentActorID = old.ParentActorID
					n.LastObserved = old.LastObserved
					n.ReportedState = old.ReportedState
					n.Source = old.Source
					n.RelationProof = old.RelationProof
					downgrade(n)
				}
			}
		}
	}
	c.last = next
	return next
}
func downgrade(c *Child) {
	c.State = "unknownstale"
	c.Axes = Axes{Execution: "unknown", NativeFinal: "unverified", Review: "unverified", Delivery: "unverified", Cancellation: "unknown", Deployment: "unknown"}
	c.Association = "unmatched"
	c.Binding = nil
	c.Report = nil
}
