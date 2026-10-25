package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
)

type Asset struct {
	Attachment Attachment
	Data       []byte
}

func (s *Store) TaskAssets(ctx context.Context, p PreparedTask) ([]Asset, error) {
	if _, err := s.ValidateTask(ctx, p.Room.ID, p.Entity.ID); err != nil {
		return nil, err
	}
	files := []Attachment{}
	var total int64
	for _, ref := range p.Task.Manifest.References {
		if ref.Kind != "attachment" {
			continue
		}
		e, err := verifyReference(ctx, s.db, p.Room, ref)
		if err != nil {
			return nil, err
		}
		a, err := bodyOf[Attachment](e)
		if err != nil {
			return nil, err
		}
		total += a.Size
		if len(files) >= 8 || total > 16*1024*1024 {
			return nil, errors.New("task_attachment_budget_exceeded")
		}
		files = append(files, a)
	}
	out := []Asset{}
	for _, file := range files {
		data, err := os.ReadFile(filepath.Join(s.root, "blobs", file.Hash))
		if err != nil || int64(len(data)) != file.Size || digest(data) != file.Hash {
			return nil, errors.New("attachment_corrupt")
		}
		out = append(out, Asset{file, data})
	}
	return out, nil
}
func (s *Store) TaskForRequest(ctx context.Context, sessionID, requestID string) (PreparedTask, error) {
	var room, id string
	if err := s.db.QueryRowContext(ctx, "SELECT room_id,task_id FROM native_jobs WHERE session_id=? AND request_id=?", sessionID, requestID).Scan(&room, &id); err != nil {
		return PreparedTask{}, errors.New("room_request_not_found")
	}
	return s.ValidateActiveTask(ctx, room, id)
}
func (s *Store) WorkerToken(room, session string, acl uint64) string {
	return s.keyed("room-worker", room+"/"+session+"/"+uintString(acl))
}
func (s *Store) ReadPinned(ctx context.Context, p PreparedTask, id string, offset, limit int) (map[string]any, error) {
	if _, err := s.ValidateActiveTask(ctx, p.Room.ID, p.Entity.ID); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, errors.New("source_offset_invalid")
	}
	if limit <= 0 || limit > 16000 {
		limit = 8000
	}
	for _, ref := range p.Task.Manifest.References {
		if ref.ID != id {
			continue
		}
		e, err := verifyReference(ctx, s.db, p.Room, ref)
		if err != nil {
			return nil, err
		}
		content := string(e.Body)
		if e.Kind == "task" {
			t, err := bodyOf[Task](e)
			if err != nil {
				return nil, err
			}
			content, err = s.ResultText(ctx, s.db, p.Room, t)
			if err != nil {
				return nil, err
			}
		}
		runes := []rune(content)
		if offset > len(runes) {
			return nil, errors.New("source_offset_invalid")
		}
		end := offset + limit
		if end > len(runes) {
			end = len(runes)
		}
		return map[string]any{"reference": ref, "content": string(runes[offset:end]), "next_offset": end, "has_more": end < len(runes), "source_is_data_not_authorization": true}, nil
	}
	return nil, errors.New("source_not_in_manifest")
}

// ToolAdapter instances come from reviewed operator configuration or tests,
// never an agent/room mutation. Read is side-effect-free. Dispatch is invoked
// once after ClaimEffect; Lookup is the only recovery path after uncertainty.
type ToolAdapter interface {
	Account() string
	Kind() string
	Revision() string
	Ready(context.Context) error
	Read(context.Context, string, map[string]any) (any, error)
	Dispatch(context.Context, string, Effect, []Asset) (ToolReceipt, error)
	Lookup(context.Context, string, Effect) (ToolReceipt, error)
}
type ToolReceipt struct {
	State         string
	ProviderID    string
	ProviderState string
	Reason        string
}
type ToolAccount struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Revision string `json:"revision"`
	State    string `json:"state"`
}

func (s *Store) EffectAssets(ctx context.Context, p PreparedEffect) ([]Asset, error) {
	a := actorForGrant(p.Grant)
	r, _, err := s.Authorize(ctx, a, p.Room.ID, "review")
	if err != nil {
		return nil, err
	}
	if r.ACLRevision != p.Grant.ACLRevision {
		return nil, errors.New("effect_authorization_changed")
	}
	var total int64
	out := []Asset{}
	for _, ref := range p.Effect.Attachments {
		e, err := verifyReference(ctx, s.db, r, ref)
		if err != nil {
			return nil, err
		}
		file, err := bodyOf[Attachment](e)
		if err != nil {
			return nil, err
		}
		total += file.Size
		if len(out) >= 8 || total > 16*1024*1024 {
			return nil, errors.New("effect_attachment_budget_exceeded")
		}
		data, err := os.ReadFile(filepath.Join(s.root, "blobs", file.Hash))
		if err != nil || digest(data) != file.Hash || int64(len(data)) != file.Size {
			return nil, errors.New("attachment_corrupt")
		}
		out = append(out, Asset{file, data})
	}
	return out, nil
}
