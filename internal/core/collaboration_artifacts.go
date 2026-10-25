package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"everything-go/internal/coordination"
)

const maxCollaborationArtifactBytes = 10 * 1024 * 1024

type collaborationArtifactInput struct {
	MutationID string `json:"mutation_id"`
	Name       string `json:"name"`
	Kind       string `json:"kind" enum:"text,file"`
	Text       string `json:"text,omitempty"`
	Path       string `json:"path,omitempty"`
	Evidence   string `json:"evidence,omitempty"`
}
type collaborationArtifactResult struct {
	ArtifactID string `json:"artifact_id"`
	EvidenceID string `json:"evidence_id"`
	Digest     string `json:"digest"`
	Trust      string `json:"trust"`
}

func bytesDigest(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }

func readBoundedFile(path string, limit int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errors.New("artifact_not_regular_or_too_large")
	}
	body, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, errors.New("artifact_too_large")
	}
	return body, nil
}

func readContainedFile(rootPath, path string, limit int64) ([]byte, error) {
	canonicalRoot, err := filepath.EvalSymlinks(rootPath)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(canonicalRoot, path)
	if err != nil || !filepath.IsLocal(rel) {
		return nil, errors.New("artifact_path_forbidden")
	}
	parts := strings.Split(filepath.Clean(rel), string(filepath.Separator))
	if len(parts) > 128 {
		return nil, errors.New("artifact_path_too_deep")
	}
	root, err := os.OpenRoot(canonicalRoot)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	for _, part := range parts[:len(parts)-1] {
		before, err := root.Lstat(part)
		if err != nil || !before.IsDir() || before.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("artifact_source_changed")
		}
		next, err := root.OpenRoot(part)
		if err != nil {
			return nil, err
		}
		defer next.Close()
		after, err := next.Stat(".")
		if err != nil || !os.SameFile(before, after) {
			return nil, errors.New("artifact_source_changed")
		}
		root = next
	}
	name := parts[len(parts)-1]
	before, err := root.Lstat(name)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() > limit {
		return nil, errors.New("artifact_source_changed")
	}
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, errors.New("artifact_source_changed")
	}
	body, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(body)) > limit {
		return nil, errors.New("artifact_too_large")
	}
	after, err := f.Stat()
	if err != nil || opened.Size() != after.Size() || !opened.ModTime().Equal(after.ModTime()) {
		return nil, errors.New("artifact_source_changed")
	}
	return body, nil
}

func collaborationSourcePath(root, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || strings.ContainsRune(relative, 0) {
		return "", errors.New("artifact_path_forbidden")
	}
	clean := filepath.Clean(relative)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("artifact_path_forbidden")
	}
	for _, part := range strings.Split(filepath.ToSlash(clean), "/") {
		lower := strings.ToLower(part)
		if lower == ".git" || lower == ".ssh" || lower == ".codex" || lower == ".claude" || strings.HasPrefix(lower, ".env") || strings.HasPrefix(lower, "id_rsa") || strings.HasSuffix(lower, ".pem") || strings.HasSuffix(lower, ".key") {
			return "", errors.New("artifact_sensitive_path_forbidden")
		}
	}
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(filepath.Join(canonicalRoot, clean))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(canonicalRoot, canonical)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("artifact_path_forbidden")
	}
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		lower := strings.ToLower(part)
		if lower == ".ssh" || lower == ".git" || lower == ".codex" || lower == ".claude" || strings.HasPrefix(lower, ".env") || strings.HasPrefix(lower, "id_rsa") || strings.HasPrefix(lower, "id_ed25519") || strings.HasSuffix(lower, ".pem") || strings.HasSuffix(lower, ".key") {
			return "", errors.New("artifact_sensitive_path_forbidden")
		}
	}
	return canonical, nil
}

func (h *Hub) saveCollaborationArtifact(p coordination.Principal, projectID, taskID string, in collaborationArtifactInput) (collaborationArtifactResult, error) {
	out := collaborationArtifactResult{}
	if in.MutationID == "" || len(in.MutationID) > 100 || strings.TrimSpace(in.Name) == "" || len(in.Name) > 240 || len(in.Evidence) > 8000 {
		return out, errors.New("artifact_invalid")
	}
	encoded, _ := json.Marshal(in)
	intent := bytesDigest(encoded)
	key := "artifact:" + p.SessionID + ":" + p.RunID + ":" + in.MutationID
	h.pmMu.Lock()
	defer h.pmMu.Unlock()
	_, err := h.work.UpdateCollaboration(context.Background(), func(s *coordination.State) error {
		s.NormalizeCollaboration()
		task, ok := s.Tasks[taskID]
		meta := s.Collaboration.Tasks[taskID]
		if meta.Phase != "execution" {
			return errors.New("artifact_execution_phase_required")
		}
		if !ok || task.ProjectID != projectID || task.SessionID != p.SessionID || p.RunID == "" || meta.ActiveRunID != p.RunID || meta.ActiveEpoch != task.Epoch || task.Epoch != p.Epoch {
			return errors.New("scope_forbidden")
		}
		type receipt struct {
			collaborationArtifactResult
			Hash string `json:"hash"`
		}
		if prior, ok := s.Collaboration.Receipts[key]; ok {
			var r receipt
			if err := json.Unmarshal(prior, &r); err != nil {
				return err
			}
			if r.Hash != intent {
				return errors.New("mutation_intent_conflict")
			}
			out = r.collaborationArtifactResult
			return nil
		}
		var body []byte
		var sourcePath string
		var sourceRoot string
		switch in.Kind {
		case "text":
			if in.Path != "" || strings.TrimSpace(in.Text) == "" || len(in.Text) > 64000 {
				return errors.New("artifact_invalid")
			}
			body = []byte(in.Text)
		case "file":
			if in.Text != "" {
				return errors.New("artifact_invalid")
			}
			path, err := collaborationSourcePath(s.Projects[projectID].Cwd, in.Path)
			if err != nil {
				return err
			}
			before, err := os.Stat(path)
			if err != nil {
				return err
			}
			sourceRoot, err = filepath.EvalSymlinks(s.Projects[projectID].Cwd)
			if err != nil {
				return err
			}
			body, err = readContainedFile(sourceRoot, path, maxCollaborationArtifactBytes)
			if err != nil {
				return err
			}
			after, err := os.Lstat(path)
			if err != nil || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(before, after) {
				return errors.New("artifact_source_changed")
			}
			sourcePath = path
		default:
			return errors.New("artifact_kind_invalid")
		}
		count := 0
		var total int64
		for _, a := range s.Collaboration.Artifacts {
			if a.TaskID == taskID {
				count++
				total += a.Size
			}
		}
		if count >= 100 || total+int64(len(body)) > 64*1024*1024 {
			return errors.New("artifact_storage_budget_exhausted")
		}
		digest := bytesDigest(body)
		id := "artifact_" + bytesDigest([]byte(key))[:24]
		dir := filepath.Join(h.cfg.DataDir, "collaboration-artifacts")
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		path := filepath.Join(dir, id+"-"+digest)
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err == nil {
			if _, err = f.Write(body); err == nil {
				err = f.Sync()
			}
			closeErr := f.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		} else {
			existing, err := readBoundedFile(path, maxCollaborationArtifactBytes)
			if err != nil || bytesDigest(existing) != digest {
				return errors.New("artifact_snapshot_conflict")
			}
		}
		at := time.Now().UnixMilli()
		artifact := coordination.Artifact{ID: id, ProjectID: projectID, TaskID: taskID, Name: in.Name, Kind: in.Kind, Digest: digest, Size: int64(len(body)), Locator: path, SourcePath: sourcePath, SourceRoot: sourceRoot, CreatedAt: at}
		s.Collaboration.Artifacts[id] = artifact
		eid := "evidence_" + bytesDigest([]byte(key))[:24]
		text := in.Evidence
		if text == "" {
			text = "Stored delivery artifact: " + in.Name
		}
		s.Collaboration.Evidence[eid] = coordination.Evidence{ID: eid, ProjectID: projectID, TaskID: taskID, ArtifactID: id, Text: text, Trust: "source_linked", Observer: "agent:" + p.SessionID, CreatedAt: at}
		out = collaborationArtifactResult{ArtifactID: id, EvidenceID: eid, Digest: digest, Trust: "source_linked"}
		raw, _ := json.Marshal(receipt{out, intent})
		s.Collaboration.Receipts[key] = raw
		s.AddEvent(p, projectID, taskID, "artifact_saved", in.Name, meta.ActiveRequestID, at, false)
		return nil
	})
	if err == nil {
		h.broadcastCollaboration()
	}
	return out, err
}

func (h *Hub) artifactSnapshotPath(a coordination.Artifact) (string, error) {
	if a.ID == "" || a.Locator == "" || a.Digest == "" {
		return "", errors.New("artifact_unavailable")
	}
	root := filepath.Join(h.cfg.DataDir, "collaboration-artifacts")
	rel, err := filepath.Rel(root, a.Locator)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("artifact_path_forbidden")
	}
	return collaborationSourcePath(root, rel)
}

func (h *Hub) verifyCollaborationArtifact(a coordination.Artifact) error {
	path, err := h.artifactSnapshotPath(a)
	if err != nil {
		return err
	}
	body, err := readContainedFile(filepath.Join(h.cfg.DataDir, "collaboration-artifacts"), path, maxCollaborationArtifactBytes)
	if err != nil || bytesDigest(body) != a.Digest {
		return errors.New("artifact_stale")
	}
	if a.SourcePath != "" {
		body, err = readContainedFile(a.SourceRoot, a.SourcePath, maxCollaborationArtifactBytes)
		if err != nil || bytesDigest(body) != a.Digest {
			return errors.New("artifact_source_stale")
		}
	}
	return nil
}

func (h *Hub) readCollaborationArtifact(a coordination.Artifact) (string, error) {
	path, err := h.artifactSnapshotPath(a)
	if err != nil {
		return "", err
	}
	body, err := readContainedFile(filepath.Join(h.cfg.DataDir, "collaboration-artifacts"), path, maxCollaborationArtifactBytes)
	if err != nil || bytesDigest(body) != a.Digest {
		return "", errors.New("artifact_stale")
	}
	if !utf8.Valid(body) {
		return fmt.Sprintf("Binary artifact: %s (%d bytes), SHA-256 %s", a.Name, len(body), a.Digest), nil
	}
	if len(body) > 64000 {
		return string(body[:64000]) + "\n[Preview truncated; immutable artifact remains stored.]", nil
	}
	return string(body), nil
}
