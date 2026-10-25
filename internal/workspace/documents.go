package workspace

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
)

func uintString(v uint64) string { return strconv.FormatUint(v, 10) }
func (s *Store) documentMutation(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	right := "edit"
	switch c.Action {
	case "accept_document", "resolve_proposal", "resolve_knowledge":
		right = "review"
	case "media_comment":
		right = "comment"
	}
	if _, _, err := s.access(ctx, tx, a, r.ID, right); err != nil {
		return Result{}, err
	}
	out := Result{}
	switch c.Action {
	case "save_document":
		var in struct {
			Title      string      `json:"title"`
			Body       string      `json:"body"`
			References []Reference `json:"references"`
		}
		if err := decode(c.Data, &in); err != nil {
			return out, err
		}
		var err error
		if in.Title, err = text(in.Title, 1, 120); err != nil {
			return out, err
		}
		if in.Body, err = text(in.Body, 0, 64000); err != nil {
			return out, err
		}
		if err = references(ctx, tx, r, in.References); err != nil {
			return out, err
		}
		e := Entity{ID: s.entityID("document", a, c.MutationID), Kind: "document"}
		if c.EntityID != "" {
			e, err = s.getForUpdate(ctx, tx, c, "document")
			if err != nil {
				return out, err
			}
		} else if err = fresh(c); err != nil {
			return out, err
		}
		e, err = s.saveDocument(ctx, tx, a, r, e, in.Title, in.Body, in.References, "")
		return withEntity(out, e, err)
	case "accept_document":
		e, err := s.getForUpdate(ctx, tx, c, "document")
		if err != nil {
			return out, err
		}
		d, err := bodyOf[Document](e)
		if err != nil {
			return out, err
		}
		d.AcceptedVersion = d.VersionID
		e, err = s.put(ctx, tx, a, r, e, d)
		return withEntity(out, e, err)
	case "restore_document":
		e, err := s.getForUpdate(ctx, tx, c, "document")
		if err != nil {
			return out, err
		}
		var in struct {
			VersionID string `json:"version_id"`
		}
		if err = decode(c.Data, &in); err != nil {
			return out, err
		}
		v, err := getEntity(ctx, tx, r.ID, in.VersionID)
		if err != nil || v.Kind != "document_version" {
			return out, errors.New("document_version_unavailable")
		}
		old, err := bodyOf[DocumentVersion](v)
		if err != nil || old.DocumentID != e.ID {
			return out, errors.New("document_version_mismatch")
		}
		if err = references(ctx, tx, r, old.References); err != nil {
			return out, err
		}
		e, err = s.saveDocument(ctx, tx, a, r, e, old.Title, old.Body, old.References, old.Submission)
		return withEntity(out, e, err)
	case "propose_document":
		var in DocumentProposal
		if err := decode(c.Data, &in); err != nil {
			return out, err
		}
		if in.TaskID != "" {
			return out, errors.New("proposal_origin_reserved")
		}
		doc, err := getEntity(ctx, tx, r.ID, in.DocumentID)
		if err != nil || doc.Kind != "document" {
			return out, errors.New("document_unavailable")
		}
		if doc.Revision != in.Base {
			return out, errors.New("revision_conflict")
		}
		if in.Title, err = text(in.Title, 1, 120); err != nil {
			return out, err
		}
		if in.Body, err = text(in.Body, 0, 64000); err != nil {
			return out, err
		}
		in.State = "proposed"
		e, err := s.put(ctx, tx, a, r, Entity{ID: s.entityID("proposal", a, c.MutationID), Kind: "document_proposal"}, in)
		return withEntity(out, e, err)
	case "resolve_proposal":
		e, err := s.getForUpdate(ctx, tx, c, "document_proposal")
		if err != nil {
			return out, err
		}
		p, err := bodyOf[DocumentProposal](e)
		if err != nil || p.State != "proposed" {
			return out, errors.New("proposal_not_pending")
		}
		var in struct {
			Accept bool `json:"accept"`
		}
		if err = decode(c.Data, &in); err != nil {
			return out, err
		}
		p.State = "rejected"
		if in.Accept {
			doc, err := getEntity(ctx, tx, r.ID, p.DocumentID)
			if err != nil || doc.Kind != "document" {
				return out, errors.New("document_unavailable")
			}
			if doc.Revision != p.Base {
				return out, errors.New("revision_conflict")
			}
			if _, err = s.saveDocument(ctx, tx, a, r, doc, p.Title, p.Body, nil, p.TaskID); err != nil {
				return out, err
			}
			p.State = "accepted"
		}
		e, err = s.put(ctx, tx, a, r, e, p)
		return withEntity(out, e, err)
	case "propose_knowledge":
		var in struct {
			Text       string      `json:"text"`
			References []Reference `json:"references"`
			Supersedes string      `json:"supersedes"`
		}
		if err := decode(c.Data, &in); err != nil {
			return out, err
		}
		v, err := text(in.Text, 1, 12000)
		if err != nil {
			return out, err
		}
		if err = references(ctx, tx, r, in.References); err != nil {
			return out, err
		}
		if in.Supersedes != "" {
			old, err := getEntity(ctx, tx, r.ID, in.Supersedes)
			if err != nil || old.Kind != "knowledge" {
				return out, errors.New("knowledge_unavailable")
			}
		}
		e, err := s.put(ctx, tx, a, r, Entity{ID: s.entityID("knowledge", a, c.MutationID), Kind: "knowledge"}, Knowledge{Text: v, State: "proposed", References: in.References, Supersedes: in.Supersedes})
		return withEntity(out, e, err)
	case "resolve_knowledge":
		e, err := s.getForUpdate(ctx, tx, c, "knowledge")
		if err != nil {
			return out, err
		}
		k, err := bodyOf[Knowledge](e)
		if err != nil || k.State != "proposed" {
			return out, errors.New("knowledge_not_pending")
		}
		var in struct {
			Accept bool `json:"accept"`
		}
		if err = decode(c.Data, &in); err != nil {
			return out, err
		}
		k.State = "rejected"
		if in.Accept {
			if err = references(ctx, tx, r, k.References); err != nil {
				return out, err
			}
			k.State = "accepted"
			k.AcceptedBy = a.ID
			if k.Supersedes != "" {
				old, err := getEntity(ctx, tx, r.ID, k.Supersedes)
				if err != nil {
					return out, err
				}
				prior, err := bodyOf[Knowledge](old)
				if err != nil {
					return out, err
				}
				prior.State = "superseded"
				if _, err = s.put(ctx, tx, a, r, old, prior); err != nil {
					return out, err
				}
			}
		}
		e, err = s.put(ctx, tx, a, r, e, k)
		return withEntity(out, e, err)
	case "record_decision":
		var in struct {
			Text       string      `json:"text"`
			Supersedes string      `json:"supersedes"`
			References []Reference `json:"references"`
		}
		if err := decode(c.Data, &in); err != nil {
			return out, err
		}
		v, err := text(in.Text, 1, 12000)
		if err != nil {
			return out, err
		}
		if err = references(ctx, tx, r, in.References); err != nil {
			return out, err
		}
		if in.Supersedes != "" {
			old, err := getEntity(ctx, tx, r.ID, in.Supersedes)
			if err != nil || old.Kind != "decision" {
				return out, errors.New("decision_unavailable")
			}
		}
		in.Text = v
		e, err := s.put(ctx, tx, a, r, Entity{ID: s.entityID("decision", a, c.MutationID), Kind: "decision"}, in)
		return withEntity(out, e, err)
	case "media_comment":
		var in MediaComment
		if err := decode(c.Data, &in); err != nil {
			return out, err
		}
		v, err := text(in.Text, 1, 8000)
		if err != nil {
			return out, err
		}
		if in.Milliseconds < 0 || in.Milliseconds > 7*24*60*60*1000 {
			return out, errors.New("media_timecode_invalid")
		}
		e, err := getEntity(ctx, tx, r.ID, in.AttachmentID)
		if err != nil || e.Kind != "attachment" {
			return out, errors.New("attachment_unavailable")
		}
		file, err := bodyOf[Attachment](e)
		if err != nil || file.Hash != in.FileHash {
			return out, errors.New("attachment_hash_mismatch")
		}
		in.Text = v
		e, err = s.put(ctx, tx, a, r, Entity{ID: s.entityID("media_comment", a, c.MutationID), Kind: "media_comment"}, in)
		return withEntity(out, e, err)
	}
	return out, errors.New("workspace_action_unsupported")
}
func (s *Store) saveDocument(ctx context.Context, tx *sql.Tx, a Actor, r Room, e Entity, title, body string, refs []Reference, submission string) (Entity, error) {
	versionID := e.ID + "_version_" + uintString(e.Revision+1)
	v := DocumentVersion{DocumentID: e.ID, Title: title, Body: body, Base: e.Revision, Submission: submission, References: refs}
	if _, err := s.put(ctx, tx, a, r, Entity{ID: versionID, Kind: "document_version"}, v); err != nil {
		return Entity{}, err
	}
	return s.put(ctx, tx, a, r, e, Document{Title: title, Body: body, VersionID: versionID})
}
