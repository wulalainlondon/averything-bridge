package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"everything-go/internal/backend"
	"everything-go/internal/clientproto"
	"everything-go/internal/identity"
	"everything-go/internal/partners"
	"everything-go/internal/session"
)

func (h *Hub) partnerSession(id string) (*session.Session, bool) {
	s, ok := h.registry.Get(id)
	return s, ok && h.controllerInScope(s) && s.State() != session.Closed
}
func (h *Hub) partnerActor(c *Client) string {
	if h.pairedTaskDevice(c) == "" || !c.live() || c.inventoryProbe || c.enrollmentOnly {
		return ""
	}
	p := c.readIdentity.Load()
	if p == nil {
		return ""
	}
	hash := sha256.Sum256([]byte(p.token))
	return h.pairedTaskDevice(c) + ":" + hex.EncodeToString(hash[:16])
}
func (h *Hub) handlePartnerProfile(c *Client, cmd clientproto.Command) {
	r := cmd.Partner
	out := map[string]any{"type": "partner_profile_result", "request_id": cmd.RequestID, "authority": h.cfg.InstanceID, "ok": false}
	reply := func(code string) { out["error"] = code; c.enqueueEvent(out) }
	actor := h.partnerActor(c)
	if actor == "" {
		reply("permission_denied")
		return
	}
	if h.partners == nil {
		reply("storage_unavailable")
		return
	}
	if r == nil || r.Authority != h.cfg.InstanceID || cmd.RequestID == "" {
		reply("wrong_authority")
		return
	}
	// Profile fields are nested, so the generic legacy envelope normalizer
	// cannot resolve them. Keep exact authority checks before unwrapping sk1 IDs.
	copyRequest := *r
	r = &copyRequest
	normalize := func(id string) (string, bool) {
		if !strings.HasPrefix(id, "sk1:") {
			return id, true
		}
		bound, ok := identity.ParseSessionKey(id)
		return bound.SessionID, ok && bound.AuthorityInstanceID == h.cfg.InstanceID
	}
	if id, ok := normalize(r.SessionID); ok {
		r.SessionID = id
	} else {
		reply("wrong_authority")
		return
	}
	if r.Profile != nil {
		p := *r.Profile
		id, ok := normalize(p.SessionID)
		if !ok {
			reply("wrong_authority")
			return
		}
		p.SessionID = id
		r.Profile = &p
	}
	out["action"] = r.Action
	out["mutation_id"] = r.MutationID
	switch r.Action {
	case "list":
		revision, profiles := h.partners.Snapshot()
		rows := make([]partners.Profile, 0, len(profiles))
		for _, p := range profiles {
			if _, ok := h.partnerSession(p.SessionID); ok {
				rows = append(rows, p)
			}
		}
		offset := 0
		if r.Digest != "" {
			var rev uint64
			var n int
			if _, e := fmt.Sscanf(r.Digest, "%d:%d", &rev, &n); e != nil || rev != revision || n < 0 || n > len(rows) {
				reply("cursor_changed")
				return
			}
			offset = n
		}
		end := offset + 100
		if end > len(rows) {
			end = len(rows)
		}
		out["profiles"] = rows[offset:end]
		out["revision"] = revision
		out["has_more"] = end < len(rows)
		if end < len(rows) {
			out["next_cursor"] = strconv.FormatUint(revision, 10) + ":" + strconv.Itoa(end)
		}
	case "status":
		p, ok := h.partners.Lookup(actor, r.MutationID)
		if !ok {
			reply("mutation_not_found")
			return
		}
		if _, ok = h.partnerSession(p.SessionID); !ok {
			reply("permission_denied")
			return
		}
		out["profile"] = p
	case "avatar":
		p, ok := h.partners.ForSession(r.SessionID)
		if _, allowed := h.partnerSession(r.SessionID); !allowed || !ok || p.Avatar != r.Digest || r.Digest == "" {
			reply("permission_denied")
			return
		}
		b, e := h.partners.Avatar(r.Digest)
		if e != nil {
			reply("avatar_unavailable")
			return
		}
		out["avatar_png"] = b
		out["digest"] = r.Digest
	case "save", "move":
		s, ok := h.partnerSession(r.SessionID)
		if !ok || r.Profile == nil || r.Profile.SessionID != r.SessionID || r.ExpectedThread == nil || r.ExpectedConfigRevision == nil {
			reply("binding_required")
			return
		}
		// A known receipt is returned only after the current caller/session scope is
		// verified. Store.Save additionally verifies exact intent under the same key.
		if s.ResumeID() != *r.ExpectedThread || s.SettingsSnapshot().ConfigRevision != *r.ExpectedConfigRevision {
			reply("session_binding_changed")
			return
		}
		p := *r.Profile
		p.NativeThreadID = s.ResumeID()
		old, existed := h.partners.ForSession(r.SessionID)
		if r.Action == "save" && p.Description != "" && (!existed || old.Description != p.Description) {
			if e := h.intakeWritePolicy(s); e != nil {
				reply("role_not_supported_for_target")
				return
			}
		}
		if len(r.AvatarPNG) > 0 {
			digest, e := h.partners.PutAvatar(r.AvatarPNG)
			if e != nil {
				reply(e.Error())
				return
			}
			p.Avatar = digest
		}
		if p.Avatar != "" {
			if _, e := h.partners.Avatar(p.Avatar); e != nil {
				reply("avatar_unavailable")
				return
			}
		}
		if h.partnerActor(c) != actor {
			reply("permission_denied")
			return
		}
		var e error
		if r.Action == "move" {
			allowed := map[string]bool{}
			_, candidates := h.partners.Snapshot()
			for _, candidate := range candidates {
				if _, ok := h.partnerSession(candidate.SessionID); ok {
					allowed[candidate.SessionID] = true
				}
			}
			p, e = h.partners.MoveScoped(actor, r.MutationID, r.SessionID, r.ExpectedRevision, r.ExpectedRosterRevision, r.Direction, allowed)
		} else {
			p, e = h.partners.Save(actor, r.MutationID, r.ExpectedRevision, p)
		}
		if e != nil {
			reply(e.Error())
			return
		}
		out["profile"] = p
		// Invalidation carries no private role or work data. Each recipient must
		// independently read its authenticated, scoped roster.
		h.Emit(map[string]any{"type": "partner_profiles_changed", "authority": h.cfg.InstanceID})
	default:
		reply("unsupported_action")
		return
	}
	rev, _ := h.partners.Snapshot()
	out["revision"] = rev
	out["ok"] = true
	c.enqueueEvent(out)
}

func (h *Hub) partnerRoleForMessage(c *Client, cmd clientproto.Command, s *session.Session) (*partners.RoleSnapshot, error) {
	if h.partners == nil || h.partnerActor(c) == "" || cmd.TaskOrigin != nil || (cmd.MessagePurpose != "" && cmd.MessagePurpose != "instruction") || strings.HasPrefix(strings.TrimSpace(cmd.Content), "/") {
		return nil, nil
	}
	// Freeze the role from the original accepted request on a transport retry.
	if prior, ok, e := h.messageQueue.Get(s.ID, cmd.RequestID); e == nil && ok {
		var payload queuedPayload
		if json.Unmarshal(prior.Payload, &payload) == nil {
			return payload.PartnerRole, nil
		}
	}
	p, ok := h.partners.ForSession(s.ID)
	if !ok || p.Description == "" {
		return nil, nil
	}
	if p.NativeThreadID != "" && p.NativeThreadID != s.ResumeID() {
		return nil, fmt.Errorf("partner_thread_changed")
	}
	if e := h.intakeWritePolicy(s); e != nil {
		return nil, fmt.Errorf("partner_role_scope_changed")
	}
	if s.Backend() != backend.Claude && s.Backend() != backend.Codex {
		return nil, fmt.Errorf("partner_role_unsupported")
	}
	return p.Role(), nil
}
