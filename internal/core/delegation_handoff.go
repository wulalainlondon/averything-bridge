package core

import (
	"context"
	"path/filepath"
	"time"

	"everything-go/internal/delegation"
	"everything-go/internal/handoff"
	"everything-go/internal/session"
)

func (h *Hub) delegationReturnContext(ctx context.Context, r delegation.Record, parent *session.Session, thread string, revision uint64) (delegation.ReturnContext, error) {
	if saved, found, err := h.delegations.ReturnContext(ctx, r, thread, revision); err != nil || found {
		return saved, err
	}
	now := time.Now()
	snapshot := handoff.Snapshot{Status: "disabled", CapturedAt: now.UTC().Format(time.RFC3339Nano)}
	// Operator configuration is the only path source. Child text never enters
	// policy selection, filename parsing, command execution or authorization.
	if h.cfg.DelegationHandoff != nil {
		if p, enabled := h.cfg.DelegationHandoff.Parents[r.ParentSessionID]; enabled {
			workspace, workspaceErr := filepath.EvalSymlinks(parent.Cwd())
			allowed := h.cfg.RootDir
			var scopeErr error
			if allowed != "" {
				allowed, scopeErr = filepath.EvalSymlinks(allowed)
			}
			if workspaceErr != nil || scopeErr != nil || workspace != p.Workspace || !h.controllerInScope(parent) {
				snapshot = handoff.Unavailable(p, now, "workspace_scope_mismatch")
			} else {
				snapshot = handoff.Read(p, allowed, now)
			}
		}
	}
	if meta, found, e := h.delegations.Round(ctx, r.ID); e != nil {
		return delegation.ReturnContext{}, e
	} else if found {
		r.LogicalID, r.RoundVersion, r.PreviousID = meta.LogicalID, meta.Version, meta.PreviousID
	}
	return h.delegations.SealReturnContext(ctx, r, thread, revision, snapshot, delegationReturnText(r))
}
