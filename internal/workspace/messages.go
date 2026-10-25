package workspace

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// verifyReference resolves immutable input in the same authorized room. A
// reference never grants access to a file/session/room named in free text.
func verifyReference(ctx context.Context, q sqlReader, r Room, ref Reference) (Entity, error) {
	e, err := getEntity(ctx, q, r.ID, ref.ID)
	if err != nil {
		return Entity{}, err
	}
	if e.Kind == "task" && ref.Kind == "task" {
		t, err := bodyOf[Task](e)
		if err != nil || t.Axes.Final != "sealed" || t.FinalHash == "" || t.FinalHash != ref.Hash || uint64(t.Draft) != ref.Revision || (!t.FinalTruncated && digest([]byte(t.FinalText)) != t.FinalHash) || (t.FinalTruncated && t.ResultAttachmentID == "") {
			return Entity{}, errors.New("result_not_sealed")
		}
		return e, nil
	}
	if e.Kind != ref.Kind || e.Revision != ref.Revision || e.Hash != ref.Hash {
		return Entity{}, errors.New("reference_changed")
	}
	if e.Kind == "message" {
		m, err := bodyOf[Message](e)
		if err != nil || m.Deleted {
			return Entity{}, errors.New("reference_deleted")
		}
	}
	if e.Kind == "knowledge" {
		k, err := bodyOf[Knowledge](e)
		if err != nil || k.State != "accepted" {
			return Entity{}, errors.New("knowledge_not_accepted")
		}
	}
	if e.Kind == "task" {
		t, err := bodyOf[Task](e)
		if err != nil || t.Axes.Final != "sealed" || t.FinalHash == "" || digest([]byte(t.FinalText)) != t.FinalHash {
			return Entity{}, errors.New("result_not_sealed")
		}
	}
	return e, nil
}
func references(ctx context.Context, q sqlReader, r Room, refs []Reference) error {
	if len(refs) > MaxContextReferences {
		return errors.New("context_reference_limit")
	}
	seen := map[string]bool{}
	for _, ref := range refs {
		if seen[ref.ID] {
			return errors.New("duplicate_reference")
		}
		seen[ref.ID] = true
		if _, err := verifyReference(ctx, q, r, ref); err != nil {
			return err
		}
	}
	return nil
}
func referenceTo(e Entity) Reference {
	if e.Kind == "task" {
		if t, err := bodyOf[Task](e); err == nil && t.Axes.Final == "sealed" {
			return Reference{Kind: e.Kind, ID: e.ID, Revision: uint64(t.Draft), Hash: t.FinalHash}
		}
	}
	return Reference{Kind: e.Kind, ID: e.ID, Revision: e.Revision, Hash: e.Hash}
}
func (s *Store) messageMutation(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	right := "comment"
	if c.Action == "mark_read" || c.Action == "read_notification" {
		right = "read"
	}
	_, role, err := s.access(ctx, tx, a, r.ID, right)
	if err != nil {
		return Result{}, err
	}
	out := Result{}
	switch c.Action {
	case "post_message":
		var in Message
		if err = decode(c.Data, &in); err != nil {
			return out, err
		}
		if in.TaskID != "" || in.Deleted {
			return out, errors.New("message_origin_reserved")
		}
		if in.Body, err = text(in.Body, 0, 32000); err != nil {
			return out, err
		}
		if in.Body == "" && len(in.Attachments) == 0 {
			return out, errors.New("message_empty")
		}
		if err = references(ctx, tx, r, in.Attachments); err != nil {
			return out, err
		}
		for _, ref := range in.Attachments {
			if ref.Kind != "attachment" {
				return out, errors.New("attachment_reference_required")
			}
		}
		e := Entity{ID: s.entityID("message", a, c.MutationID), Kind: "message"}
		if in.ThreadID == "" {
			in.ThreadID = e.ID
		} else {
			root, err := getEntity(ctx, tx, r.ID, in.ThreadID)
			if err != nil || root.Kind != "message" {
				return out, errors.New("thread_unavailable")
			}
			m, err := bodyOf[Message](root)
			if err != nil || m.ThreadID != root.ID || m.Deleted {
				return out, errors.New("thread_unavailable")
			}
		}
		if in.ReplyID != "" {
			reply, err := getEntity(ctx, tx, r.ID, in.ReplyID)
			if err != nil || reply.Kind != "message" {
				return out, errors.New("reply_unavailable")
			}
			m, err := bodyOf[Message](reply)
			if err != nil || m.ThreadID != in.ThreadID || m.Deleted {
				return out, errors.New("reply_thread_mismatch")
			}
		}
		if in.Quote != nil {
			if _, err = verifyReference(ctx, tx, r, *in.Quote); err != nil {
				return out, err
			}
			if in.Quote.Kind != "message" {
				return out, errors.New("quote_message_required")
			}
		}
		if len(in.Mentions) > 32 {
			return out, errors.New("mention_limit")
		}
		seen := map[string]bool{}
		for _, id := range in.Mentions {
			if seen[id] {
				return out, errors.New("duplicate_mention")
			}
			seen[id] = true
			var active bool
			if err = tx.QueryRowContext(ctx, "SELECT active FROM members WHERE room_id=? AND principal_id=?", r.ID, id).Scan(&active); err != nil || !active {
				return out, errors.New("mention_member_unavailable")
			}
		}
		e, err = s.put(ctx, tx, a, r, e, in)
		if err != nil {
			return out, err
		}
		for _, id := range in.Mentions {
			if err = s.notify(ctx, tx, a, r, id, "mention", e.ID); err != nil {
				return out, err
			}
		}
		if in.DispatchAgentID != "" {
			if _, _, err = s.access(ctx, tx, a, r.ID, "dispatch"); err != nil {
				return out, err
			}
			instruction := in.Body
			if instruction == "" {
				instruction = "Review this message's pinned attachments and state any limitations."
			}
			refs := append([]Reference{{Kind: e.Kind, ID: e.ID, Revision: e.Revision, Hash: e.Hash}}, in.Attachments...)
			task, e2 := s.createTask(ctx, tx, a, r, c.MutationID+"/explicit-dispatch", TaskInput{Title: "訊息交辦", Instruction: instruction, AgentID: in.DispatchAgentID, ThreadID: in.ThreadID, References: refs}, "", "message", 1, "", nil)
			if e2 != nil {
				return out, e2
			}
			in.TaskID = task.ID
			e, err = s.put(ctx, tx, a, r, e, in)
			if err != nil {
				return out, err
			}
		}
		return withEntity(out, e, nil)
	case "edit_message", "delete_message":
		e, err := s.getForUpdate(ctx, tx, c, "message")
		if err != nil {
			return out, err
		}
		if e.CreatedBy != a.ID && role != "owner" && role != "admin" {
			return out, errors.New("message_author_required")
		}
		m, err := bodyOf[Message](e)
		if err != nil {
			return out, err
		}
		if m.TaskID != "" && strings.HasPrefix(e.CreatedBy, "agent:") {
			return out, errors.New("sealed_result_message_immutable")
		}
		// Version rows retain the exact text/hash used by an admitted task even
		// when the discussion message is edited or tombstoned later.
		versionID := e.ID + "_v" + uintString(e.Revision)
		if _, exists := getEntity(ctx, tx, r.ID, versionID); exists != nil {
			if _, err = s.put(ctx, tx, a, r, Entity{ID: versionID, Kind: "message_version"}, MessageVersion{MessageID: e.ID, Revision: e.Revision, Author: e.CreatedBy, OriginalHash: e.Hash, Message: m}); err != nil {
				return out, err
			}
		}
		if c.Action == "delete_message" {
			m.Body = ""
			m.Attachments = []Reference{}
			m.Quote = nil
			m.Mentions = []string{}
			m.Deleted = true
		} else {
			var in struct {
				Body string `json:"body"`
			}
			if err = decode(c.Data, &in); err != nil {
				return out, err
			}
			if m.Deleted {
				return out, errors.New("message_deleted")
			}
			if m.Body, err = text(in.Body, 1, 32000); err != nil {
				return out, err
			}
		}
		e, err = s.put(ctx, tx, a, r, e, m)
		return withEntity(out, e, err)
	case "set_reaction":
		var in struct {
			MessageID string `json:"message_id"`
			Emoji     string `json:"emoji"`
			Enabled   bool   `json:"enabled"`
		}
		if err = decode(c.Data, &in); err != nil {
			return out, err
		}
		if _, err = text(in.Emoji, 1, 8); err != nil {
			return out, err
		}
		e, err := getEntity(ctx, tx, r.ID, in.MessageID)
		if err != nil || e.Kind != "message" {
			return out, errors.New("message_unavailable")
		}
		id := "cw_reaction_" + s.keyed("reaction", r.ID+"/"+a.ID+"/"+in.MessageID+"/"+in.Emoji)[:24]
		reaction := Entity{ID: id, Kind: "reaction"}
		if old, e := getEntity(ctx, tx, r.ID, id); e == nil {
			reaction = old
		}
		body := struct {
			MessageID   string `json:"message_id"`
			PrincipalID string `json:"principal_id"`
			Emoji       string `json:"emoji"`
			Enabled     bool   `json:"enabled"`
		}{in.MessageID, a.ID, in.Emoji, in.Enabled}
		reaction, err = s.put(ctx, tx, a, r, reaction, body)
		return withEntity(out, reaction, err)
	case "mark_read":
		var in struct {
			MessageIDs []string `json:"message_ids"`
		}
		if err = decode(c.Data, &in); err != nil {
			return out, err
		}
		if len(in.MessageIDs) == 0 || len(in.MessageIDs) > 100 {
			return out, errors.New("read_range_invalid")
		}
		var through int64
		for _, id := range in.MessageIDs {
			message, e2 := getEntity(ctx, tx, r.ID, id)
			if e2 != nil || message.Kind != "message" {
				return out, errors.New("read_message_unavailable")
			}
			if _, err = tx.ExecContext(ctx, "INSERT OR IGNORE INTO read_messages VALUES(?,?,?,?)", r.ID, a.ID, id, message.Sequence); err != nil {
				return out, err
			}
			if message.Sequence > through {
				through = message.Sequence
			}
		}
		id := "cw_read_" + s.keyed("read", r.ID+"/"+a.ID)[:24]
		e := Entity{ID: id, Kind: "read_cursor"}
		if old, e2 := getEntity(ctx, tx, r.ID, id); e2 == nil {
			e = old
		}
		body := struct {
			PrincipalID string `json:"principal_id"`
			Through     int64  `json:"last_seen_sequence"`
			Coverage    string `json:"coverage"`
		}{a.ID, through, "explicit_message_ids_only"}
		e, err = s.put(ctx, tx, a, r, e, body)
		return withEntity(out, e, err)
	case "read_notification":
		e, err := s.getForUpdate(ctx, tx, c, "notification")
		if err != nil {
			return out, err
		}
		n, err := bodyOf[Notification](e)
		if err != nil || n.PrincipalID != a.ID {
			return out, errors.New("room_permission_denied")
		}
		n.Read = true
		e, err = s.put(ctx, tx, a, r, e, n)
		return withEntity(out, e, err)
	}
	return out, errors.New("workspace_action_unsupported")
}
