package core

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"everything-go/internal/workspace"
)

// SetWorkspaceToolAdapters is an internal operator/bootstrap port. There is no
// wire mutation for an AI or guest to register an account, command or secret.
func (h *Hub) SetWorkspaceToolAdapters(adapters []workspace.ToolAdapter) error {
	if h.workspaceScheduler.Load() {
		return errors.New("workspace_adapters_immutable_after_start")
	}
	next := map[string]workspace.ToolAdapter{}
	for _, a := range adapters {
		if a == nil || a.Account() == "" || a.Revision() == "" || (a.Kind() != "email" && a.Kind() != "calendar") {
			return errors.New("workspace_adapter_invalid")
		}
		key := a.Kind() + ":" + a.Account()
		if next[key] != nil {
			return errors.New("workspace_account_ambiguous")
		}
		next[key] = a
	}
	h.workspaceAdapters = next
	return nil
}
func (h *Hub) workspaceToolAccount(a workspace.Actor, c workspace.Command) (workspace.Result, error) {
	if !a.Owner {
		return workspace.Result{}, errors.New("original_account_owner_required")
	}
	if c.RoomID != "" {
		if _, _, err := h.workspace.Authorize(context.Background(), a, c.RoomID, "read"); err != nil {
			return workspace.Result{}, err
		}
	}
	out := workspace.Result{Action: c.Action, ToolAccounts: []workspace.ToolAccount{}}
	for _, adapter := range h.workspaceAdapters {
		out.ToolAccounts = append(out.ToolAccounts, workspace.ToolAccount{ID: adapter.Account(), Kind: adapter.Kind(), Revision: adapter.Revision(), State: "configured_health_unverified"})
	}
	sort.Slice(out.ToolAccounts, func(i, j int) bool {
		return out.ToolAccounts[i].Kind+out.ToolAccounts[i].ID < out.ToolAccounts[j].Kind+out.ToolAccounts[j].ID
	})
	return out, nil
}
func (h *Hub) workspaceToolRead(ctx context.Context, a workspace.Actor, c workspace.Command) (workspace.Result, error) {
	if !a.Owner {
		return workspace.Result{}, errors.New("original_account_owner_required")
	}
	if _, _, err := h.workspace.Authorize(ctx, a, c.RoomID, "read"); err != nil {
		return workspace.Result{}, err
	}
	var in struct {
		Kind       string         `json:"kind"`
		Account    string         `json:"account"`
		Action     string         `json:"action"`
		Parameters map[string]any `json:"parameters"`
	}
	d := json.NewDecoder(strings.NewReader(string(c.Data)))
	d.DisallowUnknownFields()
	if d.Decode(&in) != nil {
		return workspace.Result{}, errors.New("tool_read_invalid")
	}
	allowed := map[string]bool{"health": true, "list_messages": true, "read_message": true, "list_events": true, "read_event": true}
	if !allowed[in.Action] {
		return workspace.Result{}, errors.New("tool_read_action_forbidden")
	}
	adapter := h.workspaceAdapters[in.Kind+":"+in.Account]
	if adapter == nil {
		return workspace.Result{}, errors.New("original_account_tool_not_configured")
	}
	readCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := adapter.Ready(readCtx); err != nil {
		return workspace.Result{}, err
	}
	value, err := adapter.Read(readCtx, in.Action, in.Parameters)
	return workspace.Result{Action: c.Action, ToolData: value}, err
}
func (h *Hub) verifyWorkspaceEffect(ctx context.Context, a workspace.Actor, c *workspace.Command) error {
	if !a.Owner {
		return errors.New("original_account_owner_approval_required")
	}
	e, err := h.workspace.Lookup(ctx, a, c.RoomID, c.EntityID)
	if err != nil {
		return err
	}
	if e.Kind != "effect" {
		return errors.New("effect_unavailable")
	}
	var v workspace.Effect
	if json.Unmarshal(e.Body, &v) != nil {
		return errors.New("effect_invalid")
	}
	adapter := h.workspaceAdapters[v.Adapter+":"+v.Account]
	if adapter == nil {
		return errors.New("original_account_tool_not_configured")
	}
	readyCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	if err = adapter.Ready(readyCtx); err != nil {
		return err
	}
	if validator, ok := adapter.(interface {
		Validate(context.Context, workspace.Effect) error
	}); ok {
		if err = validator.Validate(readyCtx, v); err != nil {
			return err
		}
	}
	c.VerifiedAdapterRevision = adapter.Revision()
	return nil
}
func (h *Hub) reconcileWorkspaceEffects(ctx context.Context) {
	if h.workspace == nil {
		return
	}
	effects, err := h.workspace.Effects(ctx)
	if err != nil {
		return
	}
	for _, p := range effects {
		if ctx.Err() != nil {
			return
		}
		adapter := h.workspaceAdapters[p.Effect.Adapter+":"+p.Effect.Account]
		if p.Effect.State == "approved" {
			if !h.workspaceGrantCurrent(p.Grant) {
				_ = h.workspace.RecordEffect(ctx, p.Room.ID, p.Entity.ID, "held", "", "", "original_owner_credential_revoked")
				continue
			}
			if adapter == nil || adapter.Revision() != p.Effect.AdapterRevision {
				_ = h.workspace.RecordEffect(ctx, p.Room.ID, p.Entity.ID, "held", "", "", "original_account_profile_unavailable_or_changed")
				continue
			}
			readyCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
			err = adapter.Ready(readyCtx)
			cancel()
			if err != nil {
				_ = h.workspace.RecordEffect(ctx, p.Room.ID, p.Entity.ID, "held", "", "", "original_account_authentication_unavailable")
				continue
			}
			if validator, ok := adapter.(interface {
				Validate(context.Context, workspace.Effect) error
			}); ok {
				validateCtx, stop := context.WithTimeout(ctx, 10*time.Second)
				err = validator.Validate(validateCtx, p.Effect)
				stop()
				if err != nil {
					_ = h.workspace.RecordEffect(ctx, p.Room.ID, p.Entity.ID, "held", "", "", "original_account_draft_validation_changed")
					continue
				}
			}
			assets, err := h.workspace.EffectAssets(ctx, p)
			if err != nil {
				_ = h.workspace.RecordEffect(ctx, p.Room.ID, p.Entity.ID, "held", "", "", err.Error())
				continue
			}
			claimed, err := h.workspace.ClaimEffect(ctx, p.Room.ID, p.Entity.ID)
			if err != nil {
				_ = h.workspace.RecordEffect(ctx, p.Room.ID, p.Entity.ID, "held", "", "", "original_approval_expired_or_changed")
				continue
			}
			if !h.workspaceGrantCurrent(claimed.Grant) {
				_ = h.workspace.RecordEffect(ctx, p.Room.ID, p.Entity.ID, "held", "", "", "original_owner_credential_revoked_at_dispatch")
				continue
			}
			callCtx, stop := context.WithTimeout(ctx, 30*time.Second)
			receipt, err := adapter.Dispatch(callCtx, p.Entity.ID, claimed.Effect, assets)
			stop()
			if err != nil {
				receipt = workspace.ToolReceipt{State: "unknown", Reason: "original_provider_dispatch_unconfirmed"}
			}
			if receipt.State == "" {
				receipt.State = "unknown"
			}
			_ = h.workspace.RecordEffect(ctx, p.Room.ID, p.Entity.ID, receipt.State, receipt.ProviderID, receipt.ProviderState, receipt.Reason)
			h.workspaceInvalidation()
		} else if (p.Effect.State == "unknown" || p.Effect.State == "dispatching") && adapter != nil && adapter.Revision() == p.Effect.AdapterRevision && h.workspaceGrantCurrent(p.Grant) {
			// Recovery queries the original provider intent only. It never calls
			// Dispatch again, even if lookup finds no related remote object.
			readCtx, stop := context.WithTimeout(ctx, 10*time.Second)
			if err := adapter.Ready(readCtx); err != nil {
				stop()
				continue
			}
			receipt, err := adapter.Lookup(readCtx, p.Entity.ID, p.Effect)
			stop()
			if err == nil && receipt.State != "" {
				_ = h.workspace.RecordEffect(ctx, p.Room.ID, p.Entity.ID, receipt.State, receipt.ProviderID, receipt.ProviderState, receipt.Reason)
				h.workspaceInvalidation()
			}
		}
	}
}
