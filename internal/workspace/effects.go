package workspace

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

func marshal(v any) ([]byte, error) { return json.Marshal(v) }
func effectDigest(e Effect) string {
	b, _ := json.Marshal(struct {
		Adapter, Action, Account, Destination, Title, Content string
		Parameters                                            json.RawMessage
		Attachments                                           []Reference
	}{e.Adapter, e.Action, e.Account, e.Destination, e.Title, e.Content, e.Parameters, e.Attachments})
	return digest(b)
}
func (s *Store) effectMutation(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	// A room review role is not permission to use the host's external account.
	// Drafts are collaborative; approval is restricted to an authenticated
	// original paired human and the exact version/account/destination/hash.
	right := "edit"
	if c.Action == "approve_effect" || c.Action == "reject_effect" || c.Action == "cancel_effect" {
		right = "review"
	}
	if _, _, err := s.access(ctx, tx, a, r.ID, right); err != nil {
		return Result{}, err
	}
	out := Result{}
	switch c.Action {
	case "draft_effect", "revise_effect":
		var in struct {
			Adapter     string          `json:"adapter"`
			Action      string          `json:"action"`
			Account     string          `json:"account"`
			Destination string          `json:"destination"`
			Title       string          `json:"title"`
			Content     string          `json:"content"`
			Parameters  json.RawMessage `json:"parameters"`
			Attachments []Reference     `json:"attachments"`
		}
		if err := decode(c.Data, &in); err != nil {
			return out, err
		}
		if in.Adapter != "email" && in.Adapter != "calendar" {
			return out, errors.New("effect_adapter_invalid")
		}
		if (in.Adapter == "email" && in.Action != "send") || (in.Adapter == "calendar" && in.Action != "create_event") {
			return out, errors.New("effect_action_invalid")
		}
		if in.Account == "" || len(in.Account) > 128 {
			return out, errors.New("effect_account_required")
		}
		var err error
		if in.Destination, err = text(in.Destination, 1, 2000); err != nil {
			return out, err
		}
		if in.Title, err = text(in.Title, 1, 500); err != nil {
			return out, err
		}
		if in.Content, err = text(in.Content, 0, 64000); err != nil {
			return out, err
		}
		if len(in.Parameters) > MaxTextBytes {
			return out, errors.New("effect_parameters_limit")
		}
		if err = references(ctx, tx, r, in.Attachments); err != nil {
			return out, err
		}
		for _, ref := range in.Attachments {
			if ref.Kind != "attachment" {
				return out, errors.New("attachment_reference_required")
			}
		}
		e := Entity{ID: s.entityID("effect", a, c.MutationID), Kind: "effect"}
		if c.Action == "revise_effect" {
			e, err = s.getForUpdate(ctx, tx, c, "effect")
			if err != nil {
				return out, err
			}
			prior, err := bodyOf[Effect](e)
			if err != nil {
				return out, err
			}
			if prior.State == "dispatching" || prior.State == "unknown" || prior.State == "acknowledged" {
				return out, errors.New("effect_already_dispatched")
			}
			if _, err = tx.ExecContext(ctx, "DELETE FROM grants WHERE entity_id=?", e.ID); err != nil {
				return out, err
			}
			if _, err = tx.ExecContext(ctx, "DELETE FROM effect_jobs WHERE effect_id=?", e.ID); err != nil {
				return out, err
			}
		}
		effect := Effect{Adapter: in.Adapter, Action: in.Action, Account: in.Account, Destination: in.Destination, Title: in.Title, Content: in.Content, Parameters: in.Parameters, Attachments: in.Attachments, State: "draft", ExpiresAt: s.now().Add(15 * time.Minute).UnixMilli()}
		effect.ContentHash = effectDigest(effect)
		e, err = s.put(ctx, tx, a, r, e, effect)
		return withEntity(out, e, err)
	case "approve_effect", "reject_effect", "cancel_effect":
		if !a.Owner {
			return out, errors.New("original_account_owner_approval_required")
		}
		e, err := s.getForUpdate(ctx, tx, c, "effect")
		if err != nil {
			return out, err
		}
		v, err := bodyOf[Effect](e)
		if err != nil {
			return out, err
		}
		var in struct {
			Hash string `json:"hash"`
		}
		if err = decode(c.Data, &in); err != nil {
			return out, err
		}
		if in.Hash != v.ContentHash || effectDigest(v) != v.ContentHash {
			return out, errors.New("effect_hash_mismatch")
		}
		if v.State == "dispatching" || v.State == "unknown" || v.State == "acknowledged" {
			return out, errors.New("effect_dispatch_boundary_passed")
		}
		if c.Action == "approve_effect" {
			v.AdapterRevision = c.VerifiedAdapterRevision
			if v.State != "draft" || v.ExpiresAt <= s.now().UnixMilli() {
				return out, errors.New("effect_approval_expired")
			}
			if err = references(ctx, tx, r, v.Attachments); err != nil {
				return out, err
			}
			v.State = "approved"
			v.ApprovedBy = a.ID
			v.ApprovedRev = e.Revision
			v.ExpiresAt = s.now().Add(15 * time.Minute).UnixMilli()
			if err = s.saveGrant(ctx, tx, e.ID, a, r.ACLRevision); err != nil {
				return out, err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO effect_jobs VALUES(?,?,?,?)", e.ID, r.ID, "approved", s.now().UnixMilli()); err != nil {
				return out, err
			}
		} else {
			v.State = "rejected"
			if c.Action == "cancel_effect" {
				v.State = "cancelled"
			}
			if _, err = tx.ExecContext(ctx, "UPDATE effect_jobs SET state=?,updated_at=? WHERE effect_id=?", v.State, s.now().UnixMilli(), e.ID); err != nil {
				return out, err
			}
		}
		e, err = s.put(ctx, tx, a, r, e, v)
		return withEntity(out, e, err)
	}
	return out, errors.New("workspace_action_unsupported")
}

type PreparedEffect struct {
	Entity Entity
	Effect Effect
	Room   Room
	Grant  Grant
}

func (s *Store) Effects(ctx context.Context) ([]PreparedEffect, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT effect_id,room_id FROM effect_jobs WHERE state IN ('approved','dispatching','unknown') ORDER BY updated_at LIMIT 20")
	if err != nil {
		return nil, err
	}
	type key struct{ id, room string }
	keys := []key{}
	for rows.Next() {
		var k key
		if err = rows.Scan(&k.id, &k.room); err != nil {
			rows.Close()
			return nil, err
		}
		keys = append(keys, k)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	out := []PreparedEffect{}
	for _, k := range keys {
		e, err := getEntity(ctx, s.db, k.room, k.id)
		if err != nil {
			return nil, err
		}
		v, err := bodyOf[Effect](e)
		if err != nil {
			return nil, err
		}
		r, err := roomByID(ctx, s.db, k.room)
		if err != nil {
			return nil, err
		}
		g, err := grant(ctx, s.db, e.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, PreparedEffect{e, v, r, g})
	}
	return out, nil
}
func (s *Store) ClaimEffect(ctx context.Context, roomID, id string) (PreparedEffect, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PreparedEffect{}, err
	}
	defer tx.Rollback()
	e, err := getEntity(ctx, tx, roomID, id)
	if err != nil {
		return PreparedEffect{}, err
	}
	v, err := bodyOf[Effect](e)
	if err != nil {
		return PreparedEffect{}, err
	}
	g, err := grant(ctx, tx, id)
	if err != nil {
		return PreparedEffect{}, err
	}
	a := actorForGrant(g)
	r, _, err := s.access(ctx, tx, a, roomID, "review")
	if err != nil {
		return PreparedEffect{}, err
	}
	if !g.Owner || r.ACLRevision != g.ACLRevision || v.State != "approved" || v.ExpiresAt <= s.now().UnixMilli() || effectDigest(v) != v.ContentHash || v.ApprovedRev+1 != e.Revision {
		return PreparedEffect{}, errors.New("effect_approval_stale")
	}
	if err = references(ctx, tx, r, v.Attachments); err != nil {
		return PreparedEffect{}, err
	}
	v.State = "dispatching"
	e, err = s.put(ctx, tx, Actor{ID: "effect-gateway"}, r, e, v)
	if err != nil {
		return PreparedEffect{}, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE effect_jobs SET state='dispatching',updated_at=? WHERE effect_id=? AND state='approved'", s.now().UnixMilli(), id); err != nil {
		return PreparedEffect{}, err
	}
	if err = tx.Commit(); err != nil {
		return PreparedEffect{}, err
	}
	return PreparedEffect{e, v, r, g}, nil
}

// RecordEffect accepts an actual adapter observation. Unknown/dispatching jobs
// may only be queried by their ORIGINAL ID; this method cannot retry or create
// another intent. Acknowledged is not evidence of recipient read or attendance.
func (s *Store) RecordEffect(ctx context.Context, roomID, id, state, providerID, providerState, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	e, err := getEntity(ctx, tx, roomID, id)
	if err != nil {
		return err
	}
	v, err := bodyOf[Effect](e)
	if err != nil {
		return err
	}
	r, err := roomByID(ctx, tx, roomID)
	if err != nil {
		return err
	}
	if v.State == "acknowledged" {
		if state != "acknowledged" || v.ProviderID != providerID {
			return errors.New("effect_receipt_immutable")
		}
		return nil
	}
	switch state {
	case "acknowledged", "failed", "unknown", "held":
	default:
		return errors.New("effect_state_invalid")
	}
	if v.State != "dispatching" && v.State != "unknown" && !(v.State == "approved" && state == "held") {
		return errors.New("effect_not_dispatched")
	}
	if state == "acknowledged" && providerID == "" {
		return errors.New("provider_receipt_required")
	}
	if v.State == state && v.ProviderID == providerID && v.ProviderState == providerState && v.Reason == reason {
		return nil
	}
	v.State = state
	v.ProviderID = providerID
	v.ProviderState = providerState
	v.Reason = reason
	if _, err = s.put(ctx, tx, Actor{ID: "effect-adapter"}, r, e, v); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE effect_jobs SET state=?,updated_at=? WHERE effect_id=?", state, s.now().UnixMilli(), id); err != nil {
		return err
	}
	return tx.Commit()
}
