package workspace

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

func (s *Store) uploadAttachment(ctx context.Context, tx *sql.Tx, a Actor, r Room, c Command) (Result, error) {
	if _, _, err := s.access(ctx, tx, a, r.ID, "edit"); err != nil {
		return Result{}, err
	}
	var in struct {
		Name string `json:"name"`
		MIME string `json:"mime"`
		Data string `json:"data"`
		Hash string `json:"hash"`
	}
	if err := decode(c.Data, &in); err != nil {
		return Result{}, err
	}
	name, err := text(in.Name, 1, 180)
	if err != nil {
		return Result{}, err
	}
	if strings.ContainsAny(name, "/\\\x00\r\n") || name == "." || name == ".." {
		return Result{}, errors.New("attachment_name_invalid")
	}
	mimeType, _, err := mime.ParseMediaType(in.MIME)
	if err != nil {
		return Result{}, errors.New("attachment_mime_invalid")
	}
	data, err := base64.StdEncoding.DecodeString(in.Data)
	if err != nil || len(data) == 0 || len(data) > MaxAttachmentBytes {
		return Result{}, errors.New("attachment_size_invalid")
	}
	hash := digest(data)
	if hash != in.Hash {
		return Result{}, errors.New("attachment_hash_mismatch")
	}
	path := filepath.Join(s.root, "blobs", hash)
	if existing, e := os.ReadFile(path); e == nil {
		if digest(existing) != hash {
			return Result{}, errors.New("attachment_corrupt")
		}
	} else if os.IsNotExist(e) {
		f, e := os.CreateTemp(filepath.Join(s.root, "blobs"), ".upload-*")
		if e != nil {
			return Result{}, e
		}
		tmp := f.Name()
		defer os.Remove(tmp)
		if e = f.Chmod(0600); e == nil {
			_, e = f.Write(data)
		}
		if e == nil {
			e = f.Sync()
		}
		closeErr := f.Close()
		if e == nil {
			e = closeErr
		}
		if e != nil {
			return Result{}, e
		}
		if e = os.Rename(tmp, path); e != nil {
			return Result{}, e
		}
		if d, e := os.Open(filepath.Dir(path)); e == nil {
			e = d.Sync()
			_ = d.Close()
			if e != nil {
				return Result{}, e
			}
		}
	} else {
		return Result{}, e
	}
	e, err := s.put(ctx, tx, a, r, Entity{ID: s.entityID("attachment", a, c.MutationID), Kind: "attachment"}, Attachment{Name: name, MIME: mimeType, Hash: hash, Size: int64(len(data)), CreatedBy: a.ID})
	return withEntity(Result{}, e, err)
}
func (s *Store) putBlob(data []byte) (string, error) {
	hash := digest(data)
	path := filepath.Join(s.root, "blobs", hash)
	if existing, err := os.ReadFile(path); err == nil {
		if digest(existing) != hash {
			return "", errors.New("attachment_corrupt")
		}
		return hash, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	f, err := os.CreateTemp(filepath.Join(s.root, "blobs"), ".result-*")
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	if err = os.Rename(tmp, path); err != nil {
		return "", err
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		err = dir.Sync()
		_ = dir.Close()
		if err != nil {
			return "", err
		}
	}
	return hash, nil
}
func resultPreview(v string) string {
	if len(v) <= 32000 {
		return v
	}
	end := 32000
	for end > 0 && !utf8.RuneStart(v[end]) {
		end--
	}
	return v[:end]
}
func (s *Store) ResultText(ctx context.Context, q sqlReader, r Room, t Task) (string, error) {
	if !t.FinalTruncated {
		if digest([]byte(t.FinalText)) != t.FinalHash {
			return "", errors.New("result_hash_mismatch")
		}
		return t.FinalText, nil
	}
	e, err := getEntity(ctx, q, r.ID, t.ResultAttachmentID)
	if err != nil || e.Kind != "attachment" {
		return "", errors.New("result_artifact_unavailable")
	}
	file, err := bodyOf[Attachment](e)
	if err != nil || file.Hash != t.FinalHash || file.Size > MaxResultBytes {
		return "", errors.New("result_artifact_mismatch")
	}
	data, err := os.ReadFile(filepath.Join(s.root, "blobs", file.Hash))
	if err != nil || int64(len(data)) != file.Size || digest(data) != t.FinalHash {
		return "", errors.New("result_artifact_corrupt")
	}
	return string(data), nil
}
func (s *Store) readAttachment(ctx context.Context, a Actor, r Room, c Command) (Result, error) {
	e, err := getEntity(ctx, s.db, r.ID, c.EntityID)
	if err != nil || e.Kind != "attachment" {
		return Result{}, errors.New("attachment_unavailable")
	}
	file, err := bodyOf[Attachment](e)
	if err != nil {
		return Result{}, err
	}
	if len(file.Hash) != 64 || strings.ContainsAny(file.Hash, "/\\.") {
		return Result{}, errors.New("attachment_corrupt")
	}
	data, err := os.ReadFile(filepath.Join(s.root, "blobs", file.Hash))
	if err != nil || int64(len(data)) != file.Size || digest(data) != file.Hash {
		return Result{}, errors.New("attachment_corrupt")
	}
	if c.Offset < 0 || c.Offset > file.Size {
		return Result{}, errors.New("attachment_range_invalid")
	}
	limit := c.Limit
	if limit <= 0 || limit > 32768 {
		limit = 32768
	}
	end := c.Offset + int64(limit)
	if end > file.Size {
		end = file.Size
	}
	return Result{Entity: &e, Bytes: data[c.Offset:end], NextOffset: end, HasMore: end < file.Size}, nil
}
