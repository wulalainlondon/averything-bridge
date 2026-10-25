package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"everything-go/internal/linebridge"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func lineConfigFixture(t *testing.T) (string, string, lineDiskConfig) {
	t.Helper()
	root := t.TempDir()
	data := filepath.Join(root, "data")
	os.Mkdir(data, 0700)
	app := filepath.Join(root, "one.app", "Contents")
	exe := filepath.Join(app, "MacOS", "everything-go")
	os.MkdirAll(filepath.Dir(exe), 0700)
	cfg := lineDiskConfig{Version: 1, Enabled: true, Node: filepath.Join(root, "node"), KeychainHelper: filepath.Join(root, "helper"), AllowWrite: true}
	os.WriteFile(cfg.Node, []byte("pinned-node"), 0700)
	os.WriteFile(cfg.KeychainHelper, []byte("existing-helper"), 0700)
	root2 := filepath.Join(app, "Resources", "line")
	files := map[string]string{}
	for _, name := range []string{"account/dist/server.js", "voice/credentials.mjs", "voice/route-worker.mjs"} {
		p := filepath.Join(root2, name)
		os.MkdirAll(filepath.Dir(p), 0700)
		b := []byte(name)
		os.WriteFile(p, b, 0600)
		h := sha256.Sum256(b)
		files[name] = hex.EncodeToString(h[:])
	}
	nodeHash := sha256.Sum256([]byte("pinned-node"))
	manifest, _ := json.Marshal(map[string]any{"schema_version": 1, "node": map[string]string{"sha256": hex.EncodeToString(nodeHash[:])}, "files": files})
	os.WriteFile(filepath.Join(root2, "manifest.json"), manifest, 0600)
	writeLineConfig(t, data, cfg)
	return data, exe, cfg
}
func writeLineConfig(t *testing.T, data string, cfg lineDiskConfig) {
	t.Helper()
	b, e := json.Marshal(cfg)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(data, "line-service.json"), b, 0600); e != nil {
		t.Fatal(e)
	}
}
func TestLineStableConfigResolvesCurrentReleaseWithoutRetiredPaths(t *testing.T) {
	data, exe, cfg := lineConfigFixture(t)
	before, _ := os.ReadFile(filepath.Join(data, "line-service.json"))
	a, e := readLineConfig(data, exe)
	if e != nil || a.Blocked["messages.send"] || !a.Blocked["call"] {
		t.Fatal(a, e)
	}
	second := strings.Replace(exe, "one.app", "two.app", 1)
	files := []string{"account/dist/server.js", "voice/credentials.mjs", "voice/route-worker.mjs", "manifest.json"}
	for _, name := range files {
		p := filepath.Join(filepath.Dir(exe), "..", "Resources", "line", name)
		b, _ := os.ReadFile(p)
		q := filepath.Join(filepath.Dir(second), "..", "Resources", "line", name)
		os.MkdirAll(filepath.Dir(q), 0700)
		os.WriteFile(q, b, 0600)
	}
	os.RemoveAll(filepath.Join(filepath.Dir(exe), "..", "Resources"))
	b, e := readLineConfig(data, second)
	after, _ := os.ReadFile(filepath.Join(data, "line-service.json"))
	if e != nil || !strings.Contains(b.Entry, "two.app") || string(before) != string(after) || b.Helper != cfg.KeychainHelper {
		t.Fatal(b, e)
	}
}
func TestLineExistingStoreCannotSilentlyLoseConfig(t *testing.T) {
	t.Setenv("LINE_SERVICE_ENTRY", "")
	data := t.TempDir()
	os.Mkdir(filepath.Join(data, "line"), 0700)
	os.WriteFile(filepath.Join(data, "line", "line.sqlite"), []byte("existing"), 0600)
	if _, e := readLineConfig(data, "/app/Contents/MacOS/Go"); e == nil || e.Error() != "line_configuration_missing_existing_store" {
		t.Fatal(e)
	}
	writeLineConfig(t, data, lineDiskConfig{Version: 1, Enabled: false})
	if x, e := readLineConfig(data, "/app/Contents/MacOS/Go"); e != nil || x != nil {
		t.Fatal(x, e)
	}
}
func TestLineConfigRefusesWorldReadableSymlinkAndUnknownFields(t *testing.T) {
	for _, variant := range []string{"permissions", "symlink", "unknown", "trailing"} {
		t.Run(variant, func(t *testing.T) {
			data, exe, _ := lineConfigFixture(t)
			p := filepath.Join(data, "line-service.json")
			switch variant {
			case "permissions":
				os.Chmod(p, 0644)
			case "symlink":
				os.Rename(p, p+".real")
				os.Symlink(p+".real", p)
			case "unknown":
				os.WriteFile(p, []byte(`{"version":1,"enabled":false,"token":"forbidden"}`), 0600)
			case "trailing":
				os.WriteFile(p, []byte(`{"version":1,"enabled":false} {}`), 0600)
			}
			if _, e := readLineConfig(data, exe); e == nil {
				t.Fatal("unsafe config accepted")
			}
		})
	}
}
func TestLineConfigPinsNodeAndBundleBeforeCredentials(t *testing.T) {
	for _, kind := range []string{"node", "bundle"} {
		t.Run(kind, func(t *testing.T) {
			data, exe, cfg := lineConfigFixture(t)
			target := cfg.Node
			if kind == "bundle" {
				target = filepath.Join(filepath.Dir(exe), "..", "Resources", "line", "account", "dist", "server.js")
			}
			os.WriteFile(target, []byte("changed"), 0600)
			if _, e := readLineConfig(data, exe); e == nil {
				t.Fatal("changed resource accepted")
			}
		})
	}
}

func TestLinePrivateConfigTakesPrecedenceOverRetiredEnvironment(t *testing.T) {
	data, exe, _ := lineConfigFixture(t)
	t.Setenv("LINE_SERVICE_ENTRY", "/retired/release/server.js")
	config, e := readLineConfig(data, exe)
	if e != nil || strings.Contains(config.Entry, "retired") {
		t.Fatal(config, e)
	}
}
func TestLineNoStoreAndNoConfigRemainsIntentionallyDisabled(t *testing.T) {
	t.Setenv("LINE_SERVICE_ENTRY", "")
	if config, e := readLineConfig(t.TempDir(), "/app/Contents/MacOS/Go"); e != nil || config != nil {
		t.Fatal(config, e)
	}
}

func TestLineEnabledRecoveryCannotCreateAReplacementEmptyStore(t *testing.T) {
	data, exe, cfg := lineConfigFixture(t)
	cfg.RequireExistingStore = true
	writeLineConfig(t, data, cfg)
	if _, e := readLineConfig(data, exe); e == nil || e.Error() != "line_existing_store_required" {
		t.Fatal(e)
	}
}

func TestLineCorruptEnablementCannotSilentlyDisableExistingService(t *testing.T) {
	for _, raw := range []string{`{"version":1}`, `{"version":1,"enabled":null}`, `{"version":1,"enabled":true,"enabled":false}`} {
		data, exe, _ := lineConfigFixture(t)
		os.WriteFile(filepath.Join(data, "line-service.json"), []byte(raw), 0600)
		if _, e := readLineConfig(data, exe); e == nil {
			t.Fatal("ambiguous enablement accepted")
		}
	}
}

func TestLineVoiceDispatchDefaultsAreExplicitPinnedOperatorIntent(t *testing.T) {
	data, exe, cfg := lineConfigFixture(t)
	const account = "ln_11111111111111111111111111111111"
	cfg.VoiceDispatchAccounts = []linebridge.VoiceDispatchAccount{{Account: account, Binding: 28}}
	writeLineConfig(t, data, cfg)
	actual, err := readLineConfig(data, exe)
	if err != nil || len(actual.VoiceDispatchAccounts) != 1 || actual.VoiceDispatchAccounts[0].Binding != 28 {
		t.Fatal(actual, err)
	}
	cfg.VoiceDispatchAccounts = append(cfg.VoiceDispatchAccounts, cfg.VoiceDispatchAccounts[0])
	writeLineConfig(t, data, cfg)
	if _, err = readLineConfig(data, exe); err == nil {
		t.Fatal("ambiguous duplicate account accepted")
	}
}
