package filetransfer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxLegacyUploadBytes = int64(512 * 1024 * 1024)
	maxDropUploadBytes   = int64(2 * 1024 * 1024 * 1024)
	maxDropChunkBytes    = int64(4 * 1024 * 1024)
	dropPartialTTL       = 24 * time.Hour
)

type Authorizer func(*http.Request) bool

type Service struct {
	dataDir      string
	downloadDir  string
	stagingDir   string
	legacyDir    string
	allowedRoots []string
	authorize    Authorizer
	mu           sync.Mutex
	now          func() time.Time
}

type dropState struct {
	Version       int    `json:"version"`
	UploadID      string `json:"upload_id"`
	RequestID     string `json:"request_id"`
	DeviceID      string `json:"device_id"`
	Name          string `json:"name"`
	MediaType     string `json:"media_type"`
	ExpectedSize  int64  `json:"expected_size"`
	ExpectedSHA   string `json:"expected_sha256,omitempty"`
	ReceivedBytes int64  `json:"received_bytes"`
	SHA256        string `json:"sha256,omitempty"`
	PartPath      string `json:"part_path,omitempty"`
	FinalPath     string `json:"final_path,omitempty"`
	Complete      bool   `json:"complete"`
	CreatedAt     int64  `json:"created_at"`
	UpdatedAt     int64  `json:"updated_at"`
}

type initRequest struct {
	RequestID   string `json:"request_id"`
	DeviceID    string `json:"device_id"`
	Name        string `json:"name"`
	MediaType   string `json:"media_type,omitempty"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      string `json:"sha256,omitempty"`
	Destination string `json:"destination,omitempty"`
}

func NewService(dataDir, rootDir string, authorize Authorizer) (*Service, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	downloadDir := filepath.Join(home, "Downloads", "Bridge Drop")
	service := &Service{
		dataDir: dataDir, downloadDir: downloadDir,
		stagingDir: filepath.Join(downloadDir, ".bridge-drop-staging"),
		legacyDir:  filepath.Join(home, "Downloads", "bridge-inbox"),
		authorize:  authorize, now: time.Now,
	}
	for _, root := range []string{rootDir, dataDir, filepath.Join(home, "Downloads")} {
		if abs, absErr := filepath.Abs(root); absErr == nil && abs != "" {
			service.allowedRoots = append(service.allowedRoots, abs)
		}
	}
	for _, dir := range []string{service.dataDirPath(), service.downloadDir, service.stagingDir, service.legacyDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
	}
	service.cleanupStale()
	return service, nil
}

func (s *Service) dataDirPath() string { return filepath.Join(s.dataDir, "drop-uploads") }

func (s *Service) UploadHandler() http.Handler   { return http.HandlerFunc(s.handleLegacyUpload) }
func (s *Service) DownloadHandler() http.Handler { return http.HandlerFunc(s.handleDownload) }
func (s *Service) DropHandler() http.Handler     { return http.HandlerFunc(s.handleDrop) }

func (s *Service) authorized(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodOptions {
		if !setCORS(w, r) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return false
		}
		w.WriteHeader(http.StatusNoContent)
		return false
	}
	setCORS(w, r)
	if s.authorize == nil || !s.authorize(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func (s *Service) handleLegacyUpload(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxLegacyUploadBytes+1024*1024)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "parse error: "+err.Error(), http.StatusBadRequest)
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "missing 'file' field", http.StatusBadRequest)
		return
	}
	defer file.Close()
	name, err := safeFilename(header.Filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	finalPath := uniquePath(s.legacyDir, name)
	partPath := filepath.Join(s.legacyDir, "."+filepath.Base(finalPath)+"."+strconv.FormatInt(s.now().UnixNano(), 36)+".part")
	out, err := os.OpenFile(partPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		http.Error(w, "create failed", http.StatusInternalServerError)
		return
	}
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(out, hash), io.LimitReader(file, maxLegacyUploadBytes+1))
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil || n > maxLegacyUploadBytes {
		_ = os.Remove(partPath)
		http.Error(w, "upload failed or exceeds 512 MB", http.StatusBadRequest)
		return
	}
	if err := os.Rename(partPath, finalPath); err != nil {
		_ = os.Remove(partPath)
		http.Error(w, "finalize failed", http.StatusInternalServerError)
		return
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	log.Printf("[upload] %s -> %s (%d bytes sha256=%s)", header.Filename, finalPath, n, digest)
	writeJSON(w, http.StatusCreated, map[string]any{"path": finalPath, "filename": filepath.Base(finalPath), "size": n, "sha256": digest})
}

func (s *Service) handleDownload(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	raw := r.URL.Query().Get("path")
	decoded, err := url.QueryUnescape(raw)
	if raw == "" || err != nil {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	cleaned, err := filepath.Abs(filepath.Clean(decoded))
	if err != nil || !s.pathAllowed(cleaned) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	info, err := os.Stat(cleaned)
	if err != nil || !info.Mode().IsRegular() {
		http.Error(w, "file not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Disposition", `attachment; filename="`+strings.ReplaceAll(filepath.Base(cleaned), `"`, "")+`"`)
	http.ServeFile(w, r, cleaned)
}

func (s *Service) handleDrop(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(w, r) {
		return
	}
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/drop/v1/uploads"), "/")
	parts := []string{}
	if path != "" {
		parts = strings.Split(path, "/")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && len(parts) == 0:
		s.initDrop(w, r)
	case r.Method == http.MethodGet && len(parts) == 1:
		s.statusDrop(w, parts[0])
	case r.Method == http.MethodPut && len(parts) == 1:
		s.writeDrop(w, r, parts[0])
	case r.Method == http.MethodPost && len(parts) == 2 && parts[1] == "complete":
		s.completeDrop(w, parts[0])
	case r.Method == http.MethodDelete && len(parts) == 1:
		s.cancelDrop(w, parts[0])
	default:
		http.Error(w, "method or path not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Service) initDrop(w http.ResponseWriter, r *http.Request) {
	var input initRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "invalid upload metadata", http.StatusBadRequest)
		return
	}
	name, err := safeFilename(input.Name)
	input.RequestID, input.DeviceID = strings.TrimSpace(input.RequestID), strings.TrimSpace(input.DeviceID)
	input.SHA256 = strings.ToLower(strings.TrimSpace(input.SHA256))
	if err != nil || input.RequestID == "" || len(input.RequestID) > 128 || input.DeviceID == "" || len(input.DeviceID) > 128 || input.SizeBytes <= 0 || input.SizeBytes > maxDropUploadBytes || (input.SHA256 != "" && !validSHA256(input.SHA256)) || (input.Destination != "" && input.Destination != "downloads") {
		http.Error(w, "invalid upload metadata", http.StatusBadRequest)
		return
	}
	id := dropID(input.DeviceID, input.RequestID)
	if state, loadErr := s.loadState(id); loadErr == nil {
		if state.DeviceID != input.DeviceID || state.RequestID != input.RequestID || state.Name != name || state.ExpectedSize != input.SizeBytes || state.ExpectedSHA != input.SHA256 {
			http.Error(w, "upload request metadata conflicts with existing upload", http.StatusConflict)
			return
		}
		writeJSON(w, http.StatusOK, state)
		return
	} else if !errors.Is(loadErr, os.ErrNotExist) {
		http.Error(w, "cannot load upload state", http.StatusInternalServerError)
		return
	}
	now := s.now().UnixMilli()
	state := dropState{Version: 1, UploadID: id, RequestID: input.RequestID, DeviceID: input.DeviceID,
		Name: name, MediaType: strings.TrimSpace(input.MediaType), ExpectedSize: input.SizeBytes,
		ExpectedSHA: input.SHA256, PartPath: filepath.Join(s.stagingDir, id+".part"), CreatedAt: now, UpdatedAt: now}
	file, err := os.OpenFile(state.PartPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		http.Error(w, "cannot create upload", http.StatusInternalServerError)
		return
	}
	_ = file.Close()
	if err := s.saveState(state); err != nil {
		_ = os.Remove(state.PartPath)
		http.Error(w, "cannot save upload state", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, state)
}

func (s *Service) statusDrop(w http.ResponseWriter, id string) {
	state, err := s.loadState(id)
	if err != nil {
		http.Error(w, "upload not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Service) writeDrop(w http.ResponseWriter, r *http.Request, id string) {
	state, err := s.loadState(id)
	if err != nil || state.Complete {
		http.Error(w, "upload not writable", http.StatusConflict)
		return
	}
	offset, err := strconv.ParseInt(strings.TrimSpace(r.Header.Get("Upload-Offset")), 10, 64)
	if err != nil || offset < 0 {
		http.Error(w, "Upload-Offset is required", http.StatusBadRequest)
		return
	}
	if offset != state.ReceivedBytes {
		w.Header().Set("Upload-Offset", strconv.FormatInt(state.ReceivedBytes, 10))
		http.Error(w, "upload offset conflict", http.StatusConflict)
		return
	}
	if r.ContentLength < 0 || r.ContentLength > maxDropChunkBytes || state.ReceivedBytes+r.ContentLength > state.ExpectedSize {
		http.Error(w, "invalid chunk size", http.StatusBadRequest)
		return
	}
	file, err := os.OpenFile(state.PartPath, os.O_WRONLY, 0o600)
	if err != nil {
		http.Error(w, "partial upload missing", http.StatusConflict)
		return
	}
	if _, err = file.Seek(offset, io.SeekStart); err == nil {
		var n int64
		n, err = io.Copy(file, io.LimitReader(r.Body, maxDropChunkBytes+1))
		if err == nil && n != r.ContentLength {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			state.ReceivedBytes += n
		}
	}
	if syncErr := file.Sync(); err == nil {
		err = syncErr
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		http.Error(w, "chunk write failed", http.StatusInternalServerError)
		return
	}
	state.UpdatedAt = s.now().UnixMilli()
	if err := s.saveState(state); err != nil {
		http.Error(w, "cannot persist upload cursor", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Upload-Offset", strconv.FormatInt(state.ReceivedBytes, 10))
	writeJSON(w, http.StatusAccepted, state)
}

func (s *Service) completeDrop(w http.ResponseWriter, id string) {
	state, err := s.loadState(id)
	if err != nil {
		http.Error(w, "upload not found", http.StatusNotFound)
		return
	}
	if state.Complete {
		writeJSON(w, http.StatusOK, state)
		return
	}
	info, err := os.Stat(state.PartPath)
	if err != nil || info.Size() != state.ExpectedSize || state.ReceivedBytes != state.ExpectedSize {
		http.Error(w, "upload is incomplete", http.StatusConflict)
		return
	}
	digest, err := hashFile(state.PartPath)
	if err != nil {
		http.Error(w, "cannot verify upload", http.StatusInternalServerError)
		return
	}
	if state.ExpectedSHA != "" && digest != state.ExpectedSHA {
		http.Error(w, "sha256 mismatch", http.StatusUnprocessableEntity)
		return
	}
	finalPath := uniquePath(s.downloadDir, state.Name)
	if err := os.Rename(state.PartPath, finalPath); err != nil {
		http.Error(w, "cannot finalize upload", http.StatusInternalServerError)
		return
	}
	state.PartPath, state.FinalPath, state.SHA256, state.Complete = "", finalPath, digest, true
	state.UpdatedAt = s.now().UnixMilli()
	if err := s.saveState(state); err != nil {
		http.Error(w, "file finalized but manifest persistence failed", http.StatusInternalServerError)
		return
	}
	log.Printf("[drop] complete id=%s device=%s path=%s bytes=%d sha256=%s", state.UploadID, state.DeviceID, state.FinalPath, state.ExpectedSize, state.SHA256)
	writeJSON(w, http.StatusCreated, state)
}

func (s *Service) cancelDrop(w http.ResponseWriter, id string) {
	state, err := s.loadState(id)
	if err != nil {
		http.Error(w, "upload not found", http.StatusNotFound)
		return
	}
	if state.Complete {
		http.Error(w, "completed upload cannot be cancelled", http.StatusConflict)
		return
	}
	_ = os.Remove(state.PartPath)
	_ = os.Remove(s.statePath(id))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) statePath(id string) string { return filepath.Join(s.dataDirPath(), id+".json") }

func (s *Service) loadState(id string) (dropState, error) {
	if !strings.HasPrefix(id, "d_") || len(id) != 34 {
		return dropState{}, os.ErrNotExist
	}
	raw, err := os.ReadFile(s.statePath(id))
	if err != nil {
		return dropState{}, err
	}
	var state dropState
	if json.Unmarshal(raw, &state) != nil || state.Version != 1 || state.UploadID != id {
		return dropState{}, errors.New("invalid upload state")
	}
	return state, nil
}

func (s *Service) saveState(state dropState) error {
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.statePath(state.UploadID) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.statePath(state.UploadID))
}

func (s *Service) cleanupStale() {
	cutoff := s.now().Add(-dropPartialTTL).UnixMilli()
	entries, _ := os.ReadDir(s.dataDirPath())
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		state, err := s.loadState(id)
		if err == nil && !state.Complete && state.UpdatedAt < cutoff {
			_ = os.Remove(state.PartPath)
			_ = os.Remove(s.statePath(id))
		}
	}
}

func (s *Service) pathAllowed(path string) bool {
	for _, root := range s.allowedRoots {
		rel, err := filepath.Rel(root, path)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return true
		}
	}
	return false
}

func safeFilename(raw string) (string, error) {
	name := strings.TrimSpace(filepath.Base(raw))
	if name == "" || name == "." || name == ".." || (name != raw && strings.ContainsAny(raw, "/\\")) || len(name) > 240 || strings.ContainsRune(name, 0) {
		return "", errors.New("invalid filename")
	}
	return name, nil
}

func dropID(deviceID, requestID string) string {
	sum := sha256.Sum256([]byte(deviceID + "\x00" + requestID))
	return "d_" + hex.EncodeToString(sum[:16])
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func hashFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func uniquePath(dir, name string) string {
	path := filepath.Join(dir, name)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return path
	}
	ext, stem := filepath.Ext(name), strings.TrimSuffix(name, filepath.Ext(name))
	for i := 1; i < 10000; i++ {
		candidate := filepath.Join(dir, fmt.Sprintf("%s_%d%s", stem, i, ext))
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate
		}
	}
	return filepath.Join(dir, stem+"_"+strconv.FormatInt(time.Now().UnixNano(), 36)+ext)
}

func setCORS(w http.ResponseWriter, r *http.Request) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	allowed := err == nil && (((parsed.Scheme == "capacitor" || parsed.Scheme == "tauri") && parsed.Host == "localhost") ||
		((parsed.Scheme == "http" || parsed.Scheme == "https") && (parsed.Hostname() == "localhost" || parsed.Hostname() == "127.0.0.1")))
	if !allowed {
		return false
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Vary", "Origin")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Upload-Offset, Idempotency-Key, X-Bridge-Device-ID")
	w.Header().Set("Access-Control-Expose-Headers", "Upload-Offset")
	return true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
