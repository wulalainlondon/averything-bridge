package workspace

import (
	"context"
	"crypto/hmac"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

func (s *Store) read(ctx context.Context, a Actor, c Command) (Result, error) {
	if err := s.actorOK(ctx, s.db, a); err != nil {
		return Result{}, err
	}
	r := Result{Action: c.Action, PrincipalID: a.ID}
	if c.Action == "list" {
		rows, err := s.db.QueryContext(ctx, "SELECT DISTINCT w.id,w.name,w.owner,w.revision FROM workspaces w JOIN rooms r ON r.workspace_id=w.id JOIN members m ON m.room_id=r.id WHERE m.principal_id=? AND m.active=1 UNION SELECT id,name,owner,revision FROM workspaces WHERE owner=? ORDER BY id LIMIT 101", a.ID, a.ID)
		if err != nil {
			return r, err
		}
		for rows.Next() {
			var w Workspace
			if err = rows.Scan(&w.ID, &w.Name, &w.Owner, &w.Revision); err != nil {
				rows.Close()
				return r, err
			}
			r.Workspaces = append(r.Workspaces, w)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return r, err
		}
		// Catalogs are bounded as well. A separate room_list cursor is used for
		// large workspaces rather than implying these first 100 are the total.
		if len(r.Workspaces) > 100 {
			r.Workspaces = r.Workspaces[:100]
			r.HasMore = true
		}
		var before int64 = 9223372036854775807
		if c.Cursor != "" {
			p, err := s.decodeCursor(c.Cursor)
			if err != nil || p.Actor != a.ID || p.Kind != "rooms" || p.Room != c.WorkspaceID {
				return r, errors.New("cursor_scope_changed")
			}
			before = p.Before
		}
		query := "SELECT r.id,r.workspace_id,r.name,r.description,r.owner,r.revision,r.acl_revision,r.archived,r.expires_at,r.created_at,r.project_id FROM rooms r JOIN members m ON m.room_id=r.id WHERE m.principal_id=? AND m.active=1 AND r.rowid<?"
		args := []any{a.ID, before}
		if c.WorkspaceID != "" {
			query += " AND r.workspace_id=?"
			args = append(args, c.WorkspaceID)
		}
		query += " ORDER BY r.rowid DESC LIMIT 101"
		rows, err = s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return r, err
		}
		for rows.Next() {
			var room Room
			if err = rows.Scan(&room.ID, &room.WorkspaceID, &room.Name, &room.Description, &room.Owner, &room.Revision, &room.ACLRevision, &room.Archived, &room.ExpiresAt, &room.CreatedAt, &room.ProjectID); err != nil {
				rows.Close()
				return r, err
			}
			r.Rooms = append(r.Rooms, room)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return r, err
		}
		for i := range r.Rooms {
			if err = s.roomPreference(ctx, s.db, a, &r.Rooms[i]); err != nil {
				return r, err
			}
		}
		if len(r.Rooms) > 100 {
			r.Rooms = r.Rooms[:100]
			r.HasMore = true
			var last int64
			if err = s.db.QueryRowContext(ctx, "SELECT rowid FROM rooms WHERE id=?", r.Rooms[99].ID).Scan(&last); err != nil {
				return r, err
			}
			token, e2 := s.encodeCursor(cursor{Room: c.WorkspaceID, Actor: a.ID, Kind: "rooms", Before: last})
			if e2 != nil {
				return r, e2
			}
			r.Page = &Page{Items: []Entity{}, HasMore: true, Cursor: token}
		}
		return r, nil
	}
	if c.Action == "status" {
		var data []byte
		var roomID string
		if err := s.db.QueryRowContext(ctx, "SELECT room_id,result FROM mutations WHERE principal_id=? AND mutation_id=?", a.ID, c.OriginalMutation).Scan(&roomID, &data); err != nil {
			return r, errors.New("mutation_not_found")
		}
		if roomID != "" {
			if _, _, err := s.Authorize(ctx, a, roomID, "read"); err != nil {
				return r, err
			}
		}
		if err := json.Unmarshal(data, &r); err != nil {
			return r, err
		}
		return s.decorateInvite(a, r), nil
	}
	room, role, err := s.Authorize(ctx, a, c.RoomID, "read")
	if err != nil {
		return r, err
	}
	r.Role = role
	switch c.Action {
	case "snapshot":
		// One read transaction prevents a membership change from combining
		// content from one ACL generation with the header of another.
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return r, err
		}
		defer tx.Rollback()
		room, role, err = s.access(ctx, tx, a, c.RoomID, "read")
		if err != nil {
			return r, err
		}
		if err = s.roomPreference(ctx, tx, a, &room); err != nil {
			return r, err
		}
		snapshot := Snapshot{Room: room, Role: role, Data: map[string]Page{}, Members: []Member{}}
		rows, err := tx.QueryContext(ctx, "SELECT room_id,principal_id,name,role,revision,active FROM members WHERE room_id=? AND active=1 ORDER BY role,principal_id LIMIT 101", room.ID)
		if err != nil {
			return r, err
		}
		for rows.Next() {
			var m Member
			if err = rows.Scan(&m.RoomID, &m.PrincipalID, &m.Name, &m.Role, &m.Revision, &m.Active); err != nil {
				rows.Close()
				return r, err
			}
			snapshot.Members = append(snapshot.Members, m)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return r, err
		}
		if len(snapshot.Members) > 100 {
			return r, errors.New("member_limit")
		}
		for _, kind := range []string{"message", "agent", "document", "document_proposal", "task", "workflow", "review", "knowledge", "decision", "attachment", "media_comment", "schedule", "occurrence", "effect", "notification", "reaction", "research_source", "human_todo"} {
			page, e := s.page(ctx, tx, a, room, Command{Kind: kind, Limit: 30})
			if e != nil {
				return r, e
			}
			snapshot.Data[kind] = page
		}
		if err = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq),0) FROM events WHERE room_id=?", room.ID).Scan(&snapshot.Sequence); err != nil {
			return r, err
		}
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM entities e WHERE e.room_id=? AND e.kind='message' AND json_extract(e.body,'$.deleted')=0 AND NOT EXISTS(SELECT 1 FROM read_messages seen WHERE seen.room_id=e.room_id AND seen.principal_id=? AND seen.message_id=e.id)", room.ID, a.ID).Scan(&snapshot.UnreadMessages); err != nil {
			return r, err
		}
		if err = tx.Commit(); err != nil {
			return r, err
		}
		r.Snapshot = &snapshot
		return r, nil
	case "page":
		p, err := s.page(ctx, s.db, a, room, c)
		r.Page = &p
		return r, err
	case "get":
		if c.ExpectedRevision != nil {
			var data []byte
			if err = s.db.QueryRowContext(ctx, "SELECT metadata FROM entity_versions WHERE room_id=? AND entity_id=? AND revision=?", room.ID, c.EntityID, *c.ExpectedRevision).Scan(&data); err != nil {
				return r, errors.New("entity_version_unavailable")
			}
			var e Entity
			if err = json.Unmarshal(data, &e); err != nil {
				return r, err
			}
			if e.Kind == "notification" {
				n, _ := bodyOf[Notification](e)
				if n.PrincipalID != a.ID {
					return r, errors.New("room_permission_denied")
				}
			}
			e = publicEntity(a, e)
			r.Entity = &e
			return r, nil
		}
		e, err := getEntity(ctx, s.db, room.ID, c.EntityID)
		if err != nil {
			return r, err
		}
		if e.Kind == "notification" {
			n, _ := bodyOf[Notification](e)
			if n.PrincipalID != a.ID {
				return r, errors.New("room_permission_denied")
			}
		}
		e = publicEntity(a, e)
		r.Entity = &e
		return r, nil
	case "events":
		if c.ExpectedACL == nil || *c.ExpectedACL != room.ACLRevision {
			return r, errors.New("acl_revision_conflict")
		}
		limit := c.Limit
		if limit <= 0 || limit > 100 {
			limit = 100
		}
		rows, err := s.db.QueryContext(ctx, "SELECT seq,room_id,kind,entity_id,revision,acl_revision,actor,at FROM events WHERE room_id=? AND seq>? AND (kind!='notification' OR entity_id IN (SELECT id FROM entities WHERE room_id=? AND kind='notification' AND json_extract(body,'$.principal_id')=?)) ORDER BY seq LIMIT ?", room.ID, c.Offset, room.ID, a.ID, limit+1)
		if err != nil {
			return r, err
		}
		defer rows.Close()
		for rows.Next() {
			var e Event
			if err = rows.Scan(&e.Sequence, &e.RoomID, &e.Kind, &e.EntityID, &e.Revision, &e.ACLRevision, &e.Actor, &e.At); err != nil {
				return r, err
			}
			r.Events = append(r.Events, e)
		}
		if err = rows.Err(); err != nil {
			return r, err
		}
		if len(r.Events) > limit {
			r.Events = r.Events[:limit]
			r.HasMore = true
		}
		return r, nil
	case "search":
		q := strings.TrimSpace(c.Query)
		if len(q) < 1 || len(q) > 256 {
			return r, errors.New("search_query_invalid")
		}
		limit := c.Limit
		if limit <= 0 || limit > 100 {
			limit = 50
		}
		// Escaped LIKE searches only explicitly readable room content; neither
		// catalog counts nor search snippets ever query other rooms first.
		q = strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(q)
		rows, err := s.db.QueryContext(ctx, "SELECT "+entityColumns+` FROM entities WHERE room_id=? AND kind IN ('message','document','knowledge','decision','media_comment','human_todo') AND CAST(body AS TEXT) LIKE ? ESCAPE '\' ORDER BY room_seq DESC LIMIT ?`, room.ID, "%"+q+"%", limit+1)
		if err != nil {
			return r, err
		}
		defer rows.Close()
		p := Page{Items: []Entity{}}
		for rows.Next() {
			e, err := scanEntity(rows)
			if err != nil {
				return r, err
			}
			p.Items = append(p.Items, e)
		}
		if err = rows.Err(); err != nil {
			return r, err
		}
		if len(p.Items) > limit {
			p.Items = p.Items[:limit]
			p.HasMore = true
		}
		r.Page = &p
		return r, nil
	case "export_document":
		e, err := getEntity(ctx, s.db, room.ID, c.EntityID)
		if err != nil || e.Kind != "document" {
			return r, errors.New("document_unavailable")
		}
		if c.ExpectedRevision == nil || *c.ExpectedRevision != e.Revision {
			return r, errors.New("revision_conflict")
		}
		doc, err := bodyOf[Document](e)
		if err != nil {
			return r, err
		}
		version, err := getEntity(ctx, s.db, room.ID, doc.VersionID)
		if err != nil || version.Kind != "document_version" {
			return r, errors.New("document_version_unavailable")
		}
		body, err := bodyOf[DocumentVersion](version)
		if err != nil {
			return r, err
		}
		if err = references(ctx, s.db, room, body.References); err != nil {
			return r, err
		}
		r.ToolData = map[string]any{"document_id": e.ID, "revision": e.Revision, "entity_hash": e.Hash, "title": doc.Title, "body": doc.Body, "references": body.References, "attachments_included": false, "scope": "current_authorized_room_document_only"}
		return r, nil
	case "read_attachment":
		return s.readAttachment(ctx, a, room, c)
	}
	return r, errors.New("workspace_action_unsupported")
}

func (s *Store) mutate(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	out := Result{}
	switch c.Action {
	case "create_workspace":
		if !a.Owner {
			return out, errors.New("workspace_owner_required")
		}
		var in struct {
			Name string `json:"name"`
		}
		if err := decode(c.Data, &in); err != nil {
			return out, err
		}
		name, err := text(in.Name, 1, 80)
		if err != nil {
			return out, err
		}
		w := Workspace{ID: s.entityID("workspace", a, c.MutationID), Name: name, Owner: a.ID, Revision: 1}
		_, err = tx.ExecContext(ctx, "INSERT INTO workspaces VALUES(?,?,?,?)", w.ID, w.Name, w.Owner, w.Revision)
		out.Workspace = &w
		return out, err
	case "create_room":
		return s.createRoom(ctx, tx, a, c)
	case "set_push_subscription":
		return s.pushMutation(ctx, tx, a, r, c)
	case "set_room_preference":
		var in struct {
			Pinned bool `json:"pinned"`
		}
		if err := decode(c.Data, &in); err != nil {
			return Result{}, err
		}
		if err := s.roomPreference(ctx, tx, a, &r); err != nil {
			return Result{}, err
		}
		if c.ExpectedRevision == nil || *c.ExpectedRevision != r.PreferenceRevision {
			return Result{}, errors.New("revision_conflict")
		}
		r.Pinned = in.Pinned
		r.PreferenceRevision++
		if _, err := tx.ExecContext(ctx, "INSERT INTO room_preferences VALUES(?,?,?,?) ON CONFLICT(room_id,principal_id) DO UPDATE SET pinned=excluded.pinned,revision=excluded.revision", r.ID, a.ID, r.Pinned, r.PreferenceRevision); err != nil {
			return Result{}, err
		}
		return Result{Room: &r}, nil
	case "update_room", "archive_room", "transfer_owner":
		return s.updateRoom(ctx, tx, a, r, c)
	case "create_invite", "create_device_invite", "revoke_invite", "update_member", "leave_room":
		return s.membership(ctx, tx, a, r, c)
	case "create_human_todo", "update_human_todo", "respond_human_todo", "resolve_human_todo":
		return s.humanTodoMutation(ctx, tx, a, r, c)
	case "post_message", "edit_message", "delete_message", "set_reaction", "mark_read", "read_notification":
		return s.messageMutation(ctx, tx, a, r, c)
	case "save_document", "accept_document", "propose_document", "resolve_proposal", "restore_document", "propose_knowledge", "resolve_knowledge", "record_decision", "media_comment":
		return s.documentMutation(ctx, tx, a, r, c)
	case "upload_attachment":
		return s.uploadAttachment(ctx, tx, a, r, c)
	case "bind_agent", "update_agent", "create_task", "approve_task", "start_workflow", "accept_task", "cancel_task", "cancel_workflow":
		return s.taskMutation(ctx, tx, a, r, c)
	case "save_schedule", "set_schedule_enabled", "resolve_occurrence":
		return s.scheduleMutation(ctx, tx, a, r, c)
	case "draft_effect", "revise_effect", "approve_effect", "reject_effect", "cancel_effect":
		return s.effectMutation(ctx, tx, a, r, c)
	}
	return out, errors.New("workspace_action_unsupported")
}
func (s *Store) createRoom(ctx context.Context, tx *sql.Tx, a Actor, c Command) (Result, error) {
	var in struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		ExpiresAt   int64  `json:"expires_at"`
		ProjectID   string `json:"project_id"`
	}
	if !a.Owner {
		return Result{}, errors.New("workspace_owner_required")
	}
	if err := decode(c.Data, &in); err != nil {
		return Result{}, err
	}
	if in.ProjectID != "" && in.ProjectID != c.VerifiedProjectID {
		return Result{}, errors.New("original_project_verification_required")
	}
	var owner string
	if err := tx.QueryRowContext(ctx, "SELECT owner FROM workspaces WHERE id=?", c.WorkspaceID).Scan(&owner); err != nil || owner != a.ID {
		return Result{}, errors.New("workspace_permission_denied")
	}
	name, err := text(in.Name, 1, 80)
	if err != nil {
		return Result{}, err
	}
	description, err := text(in.Description, 0, 4000)
	if err != nil {
		return Result{}, err
	}
	now := s.now().UnixMilli()
	if in.ExpiresAt != 0 && in.ExpiresAt <= now {
		return Result{}, errors.New("expiry_invalid")
	}
	r := Room{ID: s.entityID("room", a, c.MutationID), WorkspaceID: c.WorkspaceID, Name: name, Description: description, Owner: a.ID, Revision: 1, ACLRevision: 1, ExpiresAt: in.ExpiresAt, CreatedAt: now, ProjectID: in.ProjectID}
	_, err = tx.ExecContext(ctx, "INSERT INTO rooms VALUES(?,?,?,?,?,?,?,?,?,?,?)", r.ID, r.WorkspaceID, r.Name, r.Description, r.Owner, r.Revision, r.ACLRevision, r.Archived, r.ExpiresAt, r.CreatedAt, r.ProjectID)
	if err != nil {
		return Result{}, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO members VALUES(?,?,?,?,?,1)", r.ID, a.ID, a.Name, "owner", 1)
	if err != nil {
		return Result{}, err
	}
	return Result{Room: &r}, nil
}
func (s *Store) updateRoom(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	_, role, err := s.access(ctx, tx, a, r.ID, "manage")
	if err != nil {
		return Result{}, err
	}
	if c.ExpectedRevision == nil || *c.ExpectedRevision != r.Revision {
		return Result{}, errors.New("revision_conflict")
	}
	switch c.Action {
	case "update_room":
		var in struct {
			Name        string `json:"name"`
			Description string `json:"description"`
			ExpiresAt   int64  `json:"expires_at"`
		}
		if err = decode(c.Data, &in); err != nil {
			return Result{}, err
		}
		if r.Name, err = text(in.Name, 1, 80); err != nil {
			return Result{}, err
		}
		if r.Description, err = text(in.Description, 0, 4000); err != nil {
			return Result{}, err
		}
		if in.ExpiresAt != 0 && in.ExpiresAt <= s.now().UnixMilli() {
			return Result{}, errors.New("expiry_invalid")
		}
		r.ExpiresAt = in.ExpiresAt
	case "archive_room":
		var in struct {
			Archived bool `json:"archived"`
		}
		if err = decode(c.Data, &in); err != nil {
			return Result{}, err
		}
		r.Archived = in.Archived
	case "transfer_owner":
		if role != "owner" {
			return Result{}, errors.New("room_owner_required")
		}
		var in struct {
			PrincipalID string `json:"principal_id"`
		}
		if err = decode(c.Data, &in); err != nil {
			return Result{}, err
		}
		if in.PrincipalID == a.ID {
			return Result{}, errors.New("owner_unchanged")
		}
		var active bool
		if err = tx.QueryRowContext(ctx, "SELECT active FROM members WHERE room_id=? AND principal_id=?", r.ID, in.PrincipalID).Scan(&active); err != nil || !active {
			return Result{}, errors.New("member_unavailable")
		}
		if _, err = tx.ExecContext(ctx, "UPDATE members SET role='admin',revision=revision+1 WHERE room_id=? AND principal_id=?", r.ID, a.ID); err != nil {
			return Result{}, err
		}
		if _, err = tx.ExecContext(ctx, "UPDATE members SET role='owner',revision=revision+1 WHERE room_id=? AND principal_id=?", r.ID, in.PrincipalID); err != nil {
			return Result{}, err
		}
		r.Owner = in.PrincipalID
		r.ACLRevision++
	}
	r.Revision++
	_, err = tx.ExecContext(ctx, "UPDATE rooms SET name=?,description=?,owner=?,revision=?,acl_revision=?,archived=?,expires_at=? WHERE id=?", r.Name, r.Description, r.Owner, r.Revision, r.ACLRevision, r.Archived, r.ExpiresAt, r.ID)
	if err != nil {
		return Result{}, err
	}
	err = s.appendEvent(ctx, tx, r, "room", r.ID, r.Revision, a.ID)
	return Result{Room: &r}, err
}
func (s *Store) membership(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	right := "manage"
	if c.Action == "leave_room" || c.Action == "create_device_invite" {
		right = "read"
	}
	_, role, err := s.access(ctx, tx, a, r.ID, right)
	if err != nil {
		return Result{}, err
	}
	switch c.Action {
	case "create_invite", "create_device_invite":
		var in struct {
			Role      string `json:"role"`
			ExpiresAt int64  `json:"expires_at"`
		}
		if err = decode(c.Data, &in); err != nil {
			return Result{}, err
		}
		if c.Action == "create_device_invite" {
			if a.Owner {
				return Result{}, errors.New("paired_devices_use_original_pairing")
			}
			in.Role = role
		}
		if !validRole(in.Role) || (c.Action != "create_device_invite" && (in.Role == "owner" || (role != "owner" && in.Role == "admin"))) {
			return Result{}, errors.New("role_not_assignable")
		}
		if in.ExpiresAt <= s.now().UnixMilli() || in.ExpiresAt > s.now().AddDate(0, 0, 7).UnixMilli() {
			return Result{}, errors.New("invite_expiry_invalid")
		}
		if r.Archived || (r.ExpiresAt > 0 && r.ExpiresAt <= s.now().UnixMilli()) {
			return Result{}, errors.New("room_archived")
		}
		v := Invite{ID: s.entityID("invite", a, c.MutationID), RoomID: r.ID, Role: in.Role, ExpiresAt: in.ExpiresAt}
		if c.Action == "create_device_invite" {
			v.PrincipalID = a.ID
			if v.ExpiresAt > s.now().Add(15*time.Minute).UnixMilli() {
				return Result{}, errors.New("invite_expiry_invalid")
			}
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO invites VALUES(?,?,?,?,?,?,0,?)", v.ID, r.ID, v.Role, v.ExpiresAt, "", "", v.PrincipalID)
		if err != nil {
			return Result{}, err
		}
		var memberRevision uint64
		if err = tx.QueryRowContext(ctx, "SELECT revision FROM members WHERE room_id=? AND principal_id=? AND active=1", r.ID, a.ID).Scan(&memberRevision); err != nil {
			return Result{}, err
		}
		proof, err := json.Marshal(inviteAuthorization{Grant: Grant{PrincipalID: a.ID, DeviceID: a.DeviceID, CredentialHash: a.CredentialHash, Owner: a.Owner}, MemberRevision: memberRevision, Right: right})
		if err != nil {
			return Result{}, err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO invite_authorizations VALUES(?,?)", v.ID, proof)
		return Result{Invite: &v}, err
	case "revoke_invite":
		v, err := inviteByID(ctx, tx, c.EntityID)
		if err != nil || v.RoomID != r.ID {
			return Result{}, errors.New("invite_unavailable")
		}
		v.Revoked = true
		_, err = tx.ExecContext(ctx, "UPDATE invites SET revoked=1 WHERE id=? AND room_id=?", v.ID, r.ID)
		return Result{Invite: &v}, err
	case "update_member", "leave_room":
		var in struct {
			PrincipalID string `json:"principal_id"`
			Role        string `json:"role"`
			Active      bool   `json:"active"`
		}
		if c.Action == "leave_room" {
			in.PrincipalID = a.ID
			in.Active = false
		} else if err = decode(c.Data, &in); err != nil {
			return Result{}, err
		}
		var m Member
		err = tx.QueryRowContext(ctx, "SELECT room_id,principal_id,name,role,revision,active FROM members WHERE room_id=? AND principal_id=?", r.ID, in.PrincipalID).Scan(&m.RoomID, &m.PrincipalID, &m.Name, &m.Role, &m.Revision, &m.Active)
		if err != nil {
			return Result{}, errors.New("member_unavailable")
		}
		if m.Role == "owner" {
			return Result{}, errors.New("transfer_owner_first")
		}
		if c.Action != "leave_room" && (c.ExpectedRevision == nil || *c.ExpectedRevision != m.Revision) {
			return Result{}, errors.New("revision_conflict")
		}
		if c.Action != "leave_room" && role != "owner" && (m.Role == "admin" || in.Role == "admin") {
			return Result{}, errors.New("role_not_assignable")
		}
		if in.Active && (!validRole(in.Role) || in.Role == "owner") {
			return Result{}, errors.New("role_not_assignable")
		}
		if in.Role != "" {
			m.Role = in.Role
		}
		m.Active = in.Active
		m.Revision++
		if _, err = tx.ExecContext(ctx, "UPDATE members SET role=?,revision=?,active=? WHERE room_id=? AND principal_id=?", m.Role, m.Revision, m.Active, r.ID, m.PrincipalID); err != nil {
			return Result{}, err
		}
		return s.bumpACL(ctx, tx, a, r)
	}
	return Result{}, errors.New("workspace_action_unsupported")
}

type inviteAuthorization struct {
	Grant          Grant  `json:"grant"`
	MemberRevision uint64 `json:"member_revision"`
	Right          string `json:"right"`
}

func (s *Store) validateInviteIssuer(ctx context.Context, tx *sql.Tx, v Invite) error {
	var payload []byte
	var auth inviteAuthorization
	if err := tx.QueryRowContext(ctx, "SELECT payload FROM invite_authorizations WHERE invite_id=?", v.ID).Scan(&payload); err != nil {
		return errors.New("invite_issuer_revoked")
	}
	if err := json.Unmarshal(payload, &auth); err != nil || (auth.Right != "manage" && auth.Right != "read") {
		return errors.New("invite_issuer_revoked")
	}
	if auth.Right == "read" && (v.PrincipalID == "" || v.PrincipalID != auth.Grant.PrincipalID) {
		return errors.New("invite_issuer_revoked")
	}
	a := actorForGrant(auth.Grant)
	if _, _, err := s.access(ctx, tx, a, v.RoomID, auth.Right); err != nil {
		return errors.New("invite_issuer_revoked")
	}
	var revision uint64
	if err := tx.QueryRowContext(ctx, "SELECT revision FROM members WHERE room_id=? AND principal_id=? AND active=1", v.RoomID, a.ID).Scan(&revision); err != nil || revision != auth.MemberRevision {
		return errors.New("invite_issuer_revoked")
	}
	return nil
}
func inviteByID(ctx context.Context, q sqlReader, id string) (Invite, error) {
	var v Invite
	err := q.QueryRowContext(ctx, "SELECT id,room_id,role,expires_at,used_by,revoked,principal_id FROM invites WHERE id=?", id).Scan(&v.ID, &v.RoomID, &v.Role, &v.ExpiresAt, &v.UsedBy, &v.Revoked, &v.PrincipalID)
	return v, err
}
func (s *Store) bumpACL(ctx context.Context, tx *sql.Tx, a Actor, r Room) (Result, error) {
	r.ACLRevision++
	r.Revision++
	_, err := tx.ExecContext(ctx, "UPDATE rooms SET acl_revision=?,revision=? WHERE id=?", r.ACLRevision, r.Revision, r.ID)
	if err != nil {
		return Result{}, err
	}
	err = s.appendEvent(ctx, tx, r, "membership", r.ID, r.Revision, a.ID)
	return Result{Room: &r}, err
}

// AcceptInvite is the sole unauthenticated operation. The caller must generate
// and retain a random device credential BEFORE sending it; a lost response is
// recovered by authenticating that same credential, never consuming a new invite.
func (s *Store) AcceptInvite(ctx context.Context, code, credential, deviceID, name string) (Actor, Room, error) {
	parts := strings.Split(code, ".")
	if len(parts) != 2 || !hmac.Equal([]byte(parts[1]), []byte(s.keyed("invite", parts[0]))) {
		return Actor{}, Room{}, errors.New("invite_invalid")
	}
	if len(credential) < 40 || len(credential) > 256 || len(deviceID) < 8 || len(deviceID) > 128 {
		return Actor{}, Room{}, errors.New("guest_credential_invalid")
	}
	var err error
	if name, err = text(name, 1, 60); err != nil {
		return Actor{}, Room{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Actor{}, Room{}, err
	}
	defer tx.Rollback()
	v, err := inviteByID(ctx, tx, parts[0])
	if err != nil || v.Revoked || v.ExpiresAt <= s.now().UnixMilli() {
		return Actor{}, Room{}, errors.New("invite_invalid")
	}
	r, err := roomByID(ctx, tx, v.RoomID)
	if err != nil || r.Archived || (r.ExpiresAt > 0 && r.ExpiresAt <= s.now().UnixMilli()) {
		return Actor{}, Room{}, errors.New("invite_invalid")
	}
	hash := digest([]byte(credential))
	a := Actor{ID: "human_" + s.keyed("principal", hash)[:24], Name: name, DeviceID: deviceID, CredentialHash: hash}
	if v.UsedBy != "" {
		if v.PrincipalID != "" {
			a.ID = v.PrincipalID
		}
		var priorHash string
		if err = tx.QueryRowContext(ctx, "SELECT credential_hash FROM invites WHERE id=?", v.ID).Scan(&priorHash); err != nil || priorHash != hash || v.UsedBy != a.ID {
			return Actor{}, Room{}, errors.New("invite_used")
		}
		if err = s.actorOK(ctx, tx, a); err != nil {
			return Actor{}, Room{}, err
		}
		if _, _, err = s.access(ctx, tx, a, r.ID, "read"); err != nil {
			return Actor{}, Room{}, err
		}
		return a, r, tx.Commit()
	}
	if err = s.validateInviteIssuer(ctx, tx, v); err != nil {
		return Actor{}, Room{}, err
	}
	var existing Actor
	existing.CredentialHash = hash
	credErr := tx.QueryRowContext(ctx, "SELECT principal_id,device_id,name FROM credentials WHERE token_hash=? AND active=1", hash).Scan(&existing.ID, &existing.DeviceID, &existing.Name)
	if credErr == nil {
		if existing.DeviceID != deviceID {
			return Actor{}, Room{}, errors.New("guest_credential_device_mismatch")
		}
		a = existing
	} else if !errors.Is(credErr, sql.ErrNoRows) {
		return Actor{}, Room{}, credErr
	}
	if v.PrincipalID != "" {
		if credErr == nil && a.ID != v.PrincipalID {
			return Actor{}, Room{}, errors.New("device_invite_principal_mismatch")
		}
		var memberName string
		var active bool
		if e2 := tx.QueryRowContext(ctx, "SELECT name,active FROM members WHERE room_id=? AND principal_id=?", r.ID, v.PrincipalID).Scan(&memberName, &active); e2 != nil || !active {
			return Actor{}, Room{}, errors.New("device_invite_principal_revoked")
		}
		a.ID = v.PrincipalID
		a.Name = memberName
	}
	if errors.Is(credErr, sql.ErrNoRows) {
		var collision int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM credentials WHERE principal_id=? AND device_id=? AND active=1", a.ID, deviceID).Scan(&collision); err != nil {
			return Actor{}, Room{}, err
		}
		if collision > 0 {
			return Actor{}, Room{}, errors.New("device_already_enrolled")
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO credentials VALUES(?,?,?,?,1,?)", hash, a.ID, deviceID, a.Name, s.now().UnixMilli()); err != nil {
			return Actor{}, Room{}, err
		}
	}
	if v.PrincipalID == "" {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM members WHERE room_id=? AND active=1", r.ID).Scan(&count); err != nil {
			return Actor{}, Room{}, err
		}
		if count >= 100 {
			return Actor{}, Room{}, errors.New("member_limit")
		}
		var oldRole string
		memberErr := tx.QueryRowContext(ctx, "SELECT role FROM members WHERE room_id=? AND principal_id=?", r.ID, a.ID).Scan(&oldRole)
		if memberErr == nil && oldRole == "owner" {
			return Actor{}, Room{}, errors.New("member_already_owner")
		}
		if memberErr != nil && !errors.Is(memberErr, sql.ErrNoRows) {
			return Actor{}, Room{}, memberErr
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO members VALUES(?,?,?,?,1,1) ON CONFLICT(room_id,principal_id) DO UPDATE SET role=excluded.role,active=1,revision=members.revision+1", r.ID, a.ID, a.Name, v.Role); err != nil {
			return Actor{}, Room{}, err
		}
		bumped, e2 := s.bumpACL(ctx, tx, a, r)
		if e2 != nil {
			return Actor{}, Room{}, e2
		}
		r = *bumped.Room
	}
	if _, err = tx.ExecContext(ctx, "UPDATE invites SET used_by=?,credential_hash=? WHERE id=?", a.ID, hash, v.ID); err != nil {
		return Actor{}, Room{}, err
	}

	if err = tx.Commit(); err != nil {
		return Actor{}, Room{}, err
	}
	return a, r, nil
}

func (s *Store) roomPreference(ctx context.Context, q sqlReader, a Actor, r *Room) error {
	err := q.QueryRowContext(ctx, "SELECT pinned,revision FROM room_preferences WHERE room_id=? AND principal_id=?", r.ID, a.ID).Scan(&r.Pinned, &r.PreferenceRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	return err
}
