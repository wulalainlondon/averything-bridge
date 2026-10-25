package agentvisibility

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// MetadataRPC is supplied only by the already-authenticated original provider.
// It must not start/resume a thread, start a daemon, or discover credentials.
type MetadataRPC interface {
	ReadThread(context.Context, string) (json.RawMessage, error)
	Scope() Scope
	ChildIDs() []string
}
type CodexReader struct {
	RPC MetadataRPC
	Now func() time.Time
}

func (r CodexReader) CurrentScope() Scope {
	if r.RPC == nil {
		return Scope{}
	}
	return r.RPC.Scope()
}
func (r CodexReader) Probe(ctx context.Context, s Scope) (Probe, error) {
	p := Probe{Scope: s, Coverage: "partial_provider_child_cache"}
	if r.RPC == nil || r.Now == nil || r.CurrentScope() != s {
		return p, ErrUnsupported
	}
	ids := r.RPC.ChildIDs()
	if len(ids) > MaxActors {
		return p, errors.New("metadata actor bound exceeded")
	}
	read := func(id string) (Actor, error) {
		raw, e := r.RPC.ReadThread(ctx, id)
		p.AttemptedAt = r.Now()
		if e != nil {
			return Actor{}, e
		}
		a, e := DecodeThreadMetadata(raw, id)
		if e == nil {
			a.ObservedAt = r.Now()
			a.Source = "native_probe"
		}
		return a, e
	}
	root, e := read(s.RootActorID)
	if e != nil {
		return p, e
	}
	p.Root = root
	for _, id := range ids {
		if id == s.RootActorID {
			continue
		}
		a, e := read(id)
		if e != nil {
			a = Actor{ID: id, Status: "unknown", ProbeError: probeError(e)}
		}
		p.Actors = append(p.Actors, a)
	}
	if ctx.Err() != nil || r.CurrentScope() != s {
		return p, ErrUnsupported
	}
	p.ObservedAt = r.Now()
	p.AttemptedAt = p.ObservedAt
	return p, nil
}

// DecodeThreadMetadata targets the installed 0.160.0 ThreadReadResponse schema.
// thread/read includeTurns=false still carries private summary fields from the
// provider. Those fields are intentionally absent from this decode whitelist.
func DecodeThreadMetadata(raw []byte, expectedID string) (Actor, error) {
	var v struct {
		Thread struct {
			ID      string  `json:"id"`
			Parent  *string `json:"parentThreadId"`
			Session string  `json:"sessionId"`
			Status  struct {
				Type string `json:"type"`
			} `json:"status"`
			Source json.RawMessage   `json:"source"`
			Turns  []json.RawMessage `json:"turns"`
		} `json:"thread"`
	}
	if len(raw) > 65536 {
		return Actor{}, errors.New("metadata byte bound exceeded")
	}
	if e := json.Unmarshal(raw, &v); e != nil {
		return Actor{}, e
	}
	t := v.Thread
	if t.ID == "" || t.ID != expectedID || t.Session == "" || len(t.Turns) > 0 {
		return Actor{}, errors.New("unexpected thread metadata identity/history")
	}
	a := Actor{ID: t.ID, SessionTreeID: t.Session, Status: t.Status.Type, NativeIDStatus: "known", RelationProof: "native_ancestry"}
	if t.Parent != nil {
		a.ParentID = *t.Parent
	}
	var source struct {
		SubAgent struct {
			Spawn struct {
				Parent string `json:"parent_thread_id"`
			} `json:"thread_spawn"`
		} `json:"subAgent"`
	}
	_ = json.Unmarshal(t.Source, &source)
	if source.SubAgent.Spawn.Parent != "" {
		if a.ParentID != "" && a.ParentID != source.SubAgent.Spawn.Parent {
			return Actor{}, errors.New("native parent relationship conflict")
		}
		a.ParentID = source.SubAgent.Spawn.Parent
	}
	switch a.Status {
	case "active", "idle", "notLoaded", "systemError":
	default:
		return Actor{}, errors.New("unknown provider lifecycle schema")
	}
	return a, nil
}
