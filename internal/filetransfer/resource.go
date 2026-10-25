package filetransfer

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

// ReadCompletedForDevice resolves a known upload; callers cannot supply paths.
// Reads are bounded, tied to the uploading device and checked against the
// canonical manifest hash. Original user files are never removed by a consumer.
func (s *Service) ReadCompletedForDevice(device, id string, limit int64) ([]byte, string, string, error) {
	if device == "" || !regexp.MustCompile(`^d_[a-f0-9]{32}$`).MatchString(id) || limit <= 0 {
		return nil, "", "", errors.New("resource invalid")
	}
	s.mu.Lock()
	state, err := s.loadState(id)
	s.mu.Unlock()
	if err != nil || state.DeviceID != device || !state.Complete || state.ExpectedSize <= 0 || state.ExpectedSize > limit {
		return nil, "", "", errors.New("resource unavailable")
	}
	root, err := os.OpenRoot(s.downloadDir)
	if err != nil {
		return nil, "", "", err
	}
	defer root.Close()
	rel, err := filepath.Rel(s.downloadDir, state.FinalPath)
	if err != nil {
		return nil, "", "", err
	}
	f, err := root.Open(rel)
	if err != nil {
		return nil, "", "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != state.ExpectedSize {
		return nil, "", "", errors.New("resource changed")
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || len(data) > int(limit) {
		return nil, "", "", errors.New("resource limit")
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != state.SHA256 {
		return nil, "", "", errors.New("resource integrity")
	}
	return data, state.Name, state.MediaType, nil
}
