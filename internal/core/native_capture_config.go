package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	contract "everything-go/contracts/task-api/v1"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
)

type NativeArtifactVerifier struct {
	Node       string `json:"node"`
	NodeHash   string `json:"node_sha256"`
	Script     string `json:"script"`
	ScriptHash string `json:"script_sha256"`
	SDK4Hash   string `json:"sdk4_sha256,omitempty"`
	SDKHash    string `json:"sdk_sha256"`
}
type NativeCaptureConfig struct {
	Version              int                     `json:"version"`
	Authority            string                  `json:"authority_instance_id"`
	Source               string                  `json:"source_session_id"`
	Thread               string                  `json:"source_native_thread_id"`
	Revision             uint64                  `json:"source_config_revision"`
	Sink                 string                  `json:"sink_name"`
	Operations           []string                `json:"read_operations"`
	Tasks                map[string]string       `json:"selected_task_hashes"`
	AdditionalTaskHashes map[string][]string     `json:"additional_task_hashes,omitempty"`
	WorkerReadProofs     bool                    `json:"worker_read_proofs,omitempty"`
	Verifier             *NativeArtifactVerifier `json:"artifact_verifier,omitempty"`
}

func captureOwned(p string, mode os.FileMode) bool {
	st, e := os.Lstat(p)
	if e != nil || st.Mode().Perm()&0077 != 0 || st.Mode()&os.ModeSymlink != 0 {
		return false
	}
	if mode == os.ModeDir && !st.IsDir() || mode == 0 && !st.Mode().IsRegular() {
		return false
	}
	v := reflect.ValueOf(st.Sys())
	if v.Kind() != reflect.Pointer {
		return false
	}
	u := v.Elem().FieldByName("Uid")
	return u.IsValid() && u.CanUint() && u.Uint() == uint64(os.Geteuid())
}
func captureHash(p string) string {
	b, e := os.ReadFile(p)
	if e != nil {
		return ""
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (h *Hub) DisableNativeResponseCapture() { h.nativeCapture.Store(nil) }

// Only startup/operator code calls this. JSON conveys no caller/lease/grants.
func (h *Hub) ConfigureNativeResponseCapture(path string) error {
	h.DisableNativeResponseCapture()
	if path == "" {
		return nil
	}
	reject := errors.New("native_capture_config_refused")
	canonical, e := filepath.EvalSymlinks(path)
	if e != nil || canonical != filepath.Clean(path) || !filepath.IsAbs(path) || !captureOwned(path, 0) {
		return reject
	}
	st, _ := os.Stat(path)
	if st.Size() > 32768 {
		return reject
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		return reject
	}
	if _, e = contract.Parse(raw); e != nil {
		return reject
	}
	var cfg NativeCaptureConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&cfg) != nil {
		return reject
	}
	s, found := h.registry.Get(cfg.Source)
	if (cfg.Version != 1 && cfg.Version != 2 && cfg.Version != 3) || cfg.Authority == "" || cfg.Authority != h.cfg.InstanceID || !found || (s.Backend() != "claude" && s.Backend() != "codex") || cfg.Thread == "" || s.ResumeID() != cfg.Thread || s.SettingsSnapshot().ConfigRevision != cfg.Revision {
		return reject
	}
	var fields map[string]json.RawMessage
	json.Unmarshal(raw, &fields)
	if _, present := fields["additional_task_hashes"]; present && cfg.Version < 2 {
		return reject
	}
	if _, present := fields["worker_read_proofs"]; present && cfg.Version != 3 {
		return reject
	}
	// Version 2 admits a finite set of explicitly selected historical hashes
	// for the SAME already-owned tasks. It never adds tasks or caller rights.
	for id, hashes := range cfg.AdditionalTaskHashes {
		primary, selected := cfg.Tasks[id]
		if !selected || len(hashes) == 0 || len(hashes) > 4 {
			return reject
		}
		seen := map[string]bool{primary: true}
		for _, hash := range hashes {
			if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(hash) || seen[hash] {
				return reject
			}
			seen[hash] = true
		}
	}
	if h.delegations == nil {
		return reject
	}
	if child, e := h.delegations.HasChild(context.Background(), s.ID); e != nil || child {
		return reject
	}
	maxOperations := 5
	if cfg.Version == 3 {
		maxOperations = 6
	}
	if len(cfg.Tasks) == 0 || len(cfg.Tasks) > 4 || len(cfg.Operations) == 0 || len(cfg.Operations) > maxOperations || !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(cfg.Sink) {
		return reject
	}
	ops := map[string]bool{}
	for _, op := range cfg.Operations {
		switch op {
		case "get", "read_result", "capabilities", "artifact_capabilities", "read_artifact_bytes":
		case "events":
			if cfg.Version != 3 {
				return reject
			}
		default:
			return reject
		}
		if ops[op] {
			return reject
		}
		ops[op] = true
	}
	for id, digest := range cfg.Tasks {
		if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(digest) {
			return reject
		}
		ref, e := apiRef(id)
		if e != nil || ref[0] != h.cfg.InstanceID || ref[1] != "delegation" {
			return reject
		}
		record, e := h.delegations.TaskJournal().FindTask(context.Background(), id)
		if e != nil || record.ScopeID != "session:"+s.ID || record.Path != "delegation" || record.NativeID != ref[2] {
			return reject
		}
		owner, found, e := h.delegations.ByChildRequest(context.Background(), record.SessionID, record.RequestID)
		if e != nil || !found || owner.ParentSessionID != s.ID {
			return reject
		}
	}
	root, e := filepath.EvalSymlinks(h.cfg.DataDir)
	if e != nil {
		return reject
	}
	dir := root
	for _, part := range []string{"native-task-evidence", cfg.Sink} {
		dir = filepath.Join(dir, part)
		if e := os.Mkdir(dir, 0700); e != nil && !os.IsExist(e) {
			return reject
		}
		if !captureOwned(dir, os.ModeDir) {
			return reject
		}
	}
	if cfg.Verifier != nil {
		if !ops["read_artifact_bytes"] || !ops["artifact_capabilities"] || !verifierPinned(cfg.Verifier) {
			return reject
		}
	}
	h.nativeCapture.Store(&NativeResponseCapture{Version: cfg.Version, Directory: dir, SourceSession: cfg.Source, SourceThread: cfg.Thread, SourceRevision: cfg.Revision, Authority: cfg.Authority, ConfigPath: path, ConfigHash: captureHash(path), Operations: ops, Tasks: cfg.Tasks, AdditionalTaskHashes: cfg.AdditionalTaskHashes, Verifier: cfg.Verifier, WorkerReadProofs: cfg.WorkerReadProofs})
	return nil
}
func (h *Hub) captureConfigCurrent(p *NativeResponseCapture) bool {
	if p != h.nativeCapture.Load() {
		return false
	}
	if p.ConfigPath == "" {
		return true
	} // trusted test/embedding setter, never model-facing
	if p.Authority != h.cfg.InstanceID || !captureOwned(p.ConfigPath, 0) || captureHash(p.ConfigPath) != p.ConfigHash || !captureOwned(p.Directory, os.ModeDir) {
		h.nativeCapture.CompareAndSwap(p, nil)
		return false
	}
	return true
}
