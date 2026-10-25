package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestFCMCredentialsArePrivateRuntimeInputOnly(t *testing.T) {
	dataDir := t.TempDir()
	defaultPath := filepath.Join(dataDir, "fcm_service_account.json")
	if got := runtimeServiceAccountPath("", dataDir); got != defaultPath {
		t.Fatal(got)
	}
	if got := runtimeServiceAccountPath(" /private/operator/key.json ", dataDir); got != "/private/operator/key.json" {
		t.Fatal(got)
	}
	if notifier, err := loadRuntimeFCM("", dataDir); notifier != nil || err == nil {
		t.Fatal("missing runtime file did not disable FCM")
	}
	// Parsing is local; no token is requested and no real key is used.
	fixture := []byte(`{"type":"service_account","project_id":"runtime-test","private_key":"not-a-real-key","client_email":"runtime@example.invalid","token_uri":"https://oauth2.example.invalid/token"}`)
	if err := os.WriteFile(defaultPath, fixture, 0600); err != nil {
		t.Fatal(err)
	}
	if notifier, err := loadRuntimeFCM("", dataDir); notifier == nil || err != nil {
		t.Fatal("private runtime credentials did not load", err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(defaultPath, 0644); err != nil {
			t.Fatal(err)
		}
		if notifier, err := loadRuntimeFCM("", dataDir); notifier != nil || err == nil {
			t.Fatal("world-readable credentials were accepted")
		}
	}
}

func TestFCMInvalidRuntimeInputNeverLogsCredentialContents(t *testing.T) {
	dataDir := t.TempDir()
	path := filepath.Join(dataDir, "fcm_service_account.json")
	marker := "PRIVATE_DATA_MUST_NOT_APPEAR"
	if err := os.WriteFile(path, []byte(marker), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := loadRuntimeFCM("", dataDir)
	if err == nil || strings.Contains(err.Error(), marker) {
		t.Fatal("invalid input exposed contents", err)
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", (64<<10)+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if notifier, err := loadRuntimeFCM("", dataDir); notifier != nil || err == nil {
		t.Fatal("oversized credentials accepted")
	}
	if notifier, err := loadRuntimeFCM(dataDir, dataDir); notifier != nil || err == nil {
		t.Fatal("directory accepted as credentials")
	}
}

func TestRelayModeNeedsNoGoogleKeyAndNeverFallsBack(t *testing.T) {
	dir := t.TempDir()
	n, err := loadConfiguredPush("https://push.example.invalid", "", dir, "bridge-test")
	if err != nil || n == nil || !n.RelayEnabled() {
		t.Fatal("relay mode required a Google key", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "fcm_service_account.json")); !os.IsNotExist(err) {
		t.Fatal("relay provisioned a Google key")
	}
	// A valid local runtime Google fixture must not mask an unsafe relay URL.
	fixture := []byte(`{"type":"service_account","project_id":"runtime-test","private_key":"not-a-real-key","client_email":"runtime@example.invalid"}`)
	if err := os.WriteFile(filepath.Join(dir, "fcm_service_account.json"), fixture, 0600); err != nil {
		t.Fatal(err)
	}
	if n, err := loadConfiguredPush("http://untrusted.example", "", dir, "bridge-test"); n != nil || err == nil {
		t.Fatal("invalid relay silently fell back to Google credentials")
	}
}
