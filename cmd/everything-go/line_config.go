package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"everything-go/internal/linebridge"
	"flag"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

// Operator intent belongs to the data directory, never a disposable release.
// Paths to the signed runtime are resolved from the actual executable.
type lineDiskConfig struct {
	Version                 int                               `json:"version"`
	Enabled                 bool                              `json:"enabled"`
	RequireExistingStore    bool                              `json:"require_existing_store"`
	Node                    string                            `json:"node"`
	KeychainHelper          string                            `json:"keychain_helper"`
	LegacyDir               string                            `json:"legacy_dir"`
	AllowWrite              bool                              `json:"allow_write"`
	AllowEvents             bool                              `json:"allow_events"`
	AllowSchedules          bool                              `json:"allow_schedules"`
	AllowResources          bool                              `json:"allow_resources"`
	AllowCalls              bool                              `json:"allow_calls"`
	RestoreExistingAccounts bool                              `json:"restore_existing_accounts"`
	VoiceDispatchAccounts   []linebridge.VoiceDispatchAccount `json:"voice_dispatch_accounts,omitempty"`
}
type lineRuntimeConfig struct {
	Node, Entry, Shared, Manifest, Helper, Legacy string
	Blocked                                       map[string]bool
	RestoreExisting                               bool
	VoiceDispatchAccounts                         []linebridge.VoiceDispatchAccount
}

func readLineConfig(dataDir, executable string) (*lineRuntimeConfig, error) {
	path := filepath.Join(dataDir, "line-service.json")
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		// Existing operators with an explicit environment retain that contract.
		if os.Getenv("LINE_SERVICE_ENTRY") != "" {
			x := &lineRuntimeConfig{Node: os.Getenv("LINEJS_NODE"), Entry: os.Getenv("LINE_SERVICE_ENTRY"), Shared: os.Getenv("LINEJS_SHARED_DIR"), Manifest: os.Getenv("LINE_SERVICE_MANIFEST"), Helper: os.Getenv("LINEJS_KEYCHAIN_HELPER"), Legacy: os.Getenv("LINE_LEGACY_DATA_DIR"), Blocked: map[string]bool{}}
			for scope, key := range map[string]string{"messages.send": "LINE_SERVICE_ALLOW_WRITE", "events": "LINE_SERVICE_ALLOW_EVENTS", "schedule": "LINE_SERVICE_ALLOW_SCHEDULES", "resources": "LINE_SERVICE_ALLOW_RESOURCES", "call": "LINEJS_ALLOW_CALLS"} {
				x.Blocked[scope] = os.Getenv(key) != "1"
			}
			if x.Node == "" {
				x.Node, _ = exec.LookPath("node")
			}
			return x, validateLineRuntime(x, false)
		}
		if st, e := os.Stat(filepath.Join(dataDir, "line", "line.sqlite")); e == nil && st.Size() > 0 {
			return nil, errors.New("line_configuration_missing_existing_store")
		}
		return nil, nil
	}
	if err != nil {
		return nil, errors.New("line_configuration_unavailable")
	}
	owner, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || owner.Uid != uint32(os.Getuid()) || info.Size() > 16384 {
		return nil, errors.New("line_private_configuration_required")
	}
	f, e := os.Open(path)
	if e != nil {
		return nil, errors.New("line_configuration_unavailable")
	}
	defer f.Close()
	actual, e := f.Stat()
	if e != nil || !os.SameFile(info, actual) {
		return nil, errors.New("line_configuration_changed")
	}
	raw, e := io.ReadAll(io.LimitReader(f, 16385))
	if e != nil || len(raw) > 16384 {
		return nil, errors.New("line_configuration_invalid")
	}
	// Reject missing/null enablement and duplicate flat operator fields. Neither
	// should silently turn an existing service off after an edited config.
	header := map[string]json.RawMessage{}
	scan := json.NewDecoder(bytes.NewReader(raw))
	token, e := scan.Token()
	if e != nil || token != json.Delim('{') {
		return nil, errors.New("line_configuration_invalid")
	}
	for scan.More() {
		token, e = scan.Token()
		if e != nil {
			return nil, errors.New("line_configuration_invalid")
		}
		name, ok := token.(string)
		if !ok {
			return nil, errors.New("line_configuration_invalid")
		}
		if _, seen := header[name]; seen {
			return nil, errors.New("line_configuration_ambiguous")
		}
		var value json.RawMessage
		if scan.Decode(&value) != nil {
			return nil, errors.New("line_configuration_invalid")
		}
		header[name] = value
	}
	enable := bytes.TrimSpace(header["enabled"])
	if !bytes.Equal(enable, []byte("true")) && !bytes.Equal(enable, []byte("false")) {
		return nil, errors.New("line_configuration_invalid")
	}
	var config lineDiskConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || config.Version != 1 || decoder.Decode(new(any)) != io.EOF {
		return nil, errors.New("line_configuration_invalid")
	}
	if linebridge.ValidateVoiceDispatchAccounts(config.VoiceDispatchAccounts) != nil {
		return nil, errors.New("line_voice_dispatch_configuration_invalid")
	}
	if !config.Enabled {
		return nil, nil
	}
	if config.RequireExistingStore {
		info, e := os.Lstat(filepath.Join(dataDir, "line", "line.sqlite"))
		if e != nil || !info.Mode().IsRegular() || info.Size() == 0 {
			return nil, errors.New("line_existing_store_required")
		}
	}
	if config.LegacyDir != "" && !filepath.IsAbs(config.LegacyDir) {
		return nil, errors.New("line_configuration_invalid")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(executable), "..", "Resources", "line"))
	x := &lineRuntimeConfig{VoiceDispatchAccounts: config.VoiceDispatchAccounts, RestoreExisting: config.RestoreExistingAccounts, Node: config.Node, Helper: config.KeychainHelper, Legacy: config.LegacyDir, Entry: filepath.Join(root, "account", "dist", "server.js"), Shared: filepath.Join(root, "voice"), Manifest: filepath.Join(root, "manifest.json"), Blocked: map[string]bool{"messages.send": !config.AllowWrite, "events": !config.AllowEvents, "schedule": !config.AllowSchedules, "resources": !config.AllowResources, "call": !config.AllowCalls}}
	return x, validateLineRuntime(x, true)
}
func validateLineRuntime(x *lineRuntimeConfig, requireBundle bool) error {
	for _, path := range []string{x.Node, x.Helper, x.Entry, x.Shared} {
		if !filepath.IsAbs(path) {
			return errors.New("line_configuration_invalid")
		}
	}
	for _, path := range []string{x.Helper, x.Entry} {
		info, e := os.Stat(path)
		if e != nil || !info.Mode().IsRegular() {
			return errors.New("line_runtime_path_unavailable")
		}
	}
	if x.Manifest == "" && !requireBundle {
		return nil
	}
	if !filepath.IsAbs(x.Manifest) {
		return errors.New("line_configuration_invalid")
	}
	return linebridge.VerifyBundle(x.Manifest, x.Node, x.Entry, x.Shared)
}

// Pure release preflight: verifies private config and bundled resources without
// opening account credentials, connecting LINE, running schedules or sending.
func runLineCheck(args []string, out io.Writer) int {
	f := flag.NewFlagSet("line-check", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	dir := f.String("data-dir", "", "original data directory")
	if f.Parse(args) != nil || f.NArg() != 0 || !filepath.IsAbs(*dir) {
		json.NewEncoder(out).Encode(map[string]any{"ok": false, "code": "line_configuration_invalid"})
		return 1
	}
	exe, e := os.Executable()
	if e != nil {
		return 1
	}
	config, e := readLineConfig(*dir, exe)
	if e != nil {
		json.NewEncoder(out).Encode(map[string]any{"ok": false, "code": e.Error()})
		return 1
	}
	json.NewEncoder(out).Encode(map[string]any{"ok": true, "line_enabled": config != nil, "bundled_resources_verified": config != nil, "account_network_effect": false})
	return 0
}
