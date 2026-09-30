package deviceinventory

import (
	"context"
	"encoding/json"
	"everything-go/internal/protocol"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func testInfo(build string) protocol.ClientInfo {
	return protocol.ClientInfo{Platform: "ios", AppID: "com.morrie.text", Version: "1.2.60", Build: build, Channel: "apple-sandbox"}
}

func TestInventoryIdentityPersistenceAndPrivacy(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	token, device := "secret-pairing-credential", "private-device-id"
	if err := s.SyncBindings([]Binding{{token, device}}); err != nil {
		t.Fatal(err)
	}
	if s.Bind(token, "spoofed") != "" || s.Bind("wrong", device) != "" {
		t.Fatal("untrusted binding accepted")
	}
	if got := s.Snapshot().Devices[0]; got.Online || got.VersionStatus != "not_reported" {
		t.Fatal(got)
	}
	key := s.Bind(token, device)
	info := testInfo("65")
	if err := s.Observe(key, "connection", device, "Phone", "ios", &info); err != nil {
		t.Fatal(err)
	}
	got := s.Snapshot().Devices[0]
	if !got.Online || got.Info.Build != "65" {
		t.Fatal(got)
	}
	raw, _ := json.Marshal(s.Snapshot())
	for _, secret := range []string{token, device, key, s.state.Salt} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("snapshot leaks identity")
		}
	}
	fi, err := os.Stat(filepath.Join(dir, "device_inventory.json"))
	if err != nil || fi.Mode().Perm() != 0600 {
		t.Fatal("unsafe permissions", err)
	}
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got = reopened.Snapshot().Devices[0]
	if got.Online || got.Info.Build != "65" {
		t.Fatal(got)
	}
	s.Disconnect(key, "connection")
	if s.Snapshot().Devices[0].Online {
		t.Fatal("still online")
	}
	if err := s.SyncBindings(nil); err != nil {
		t.Fatal(err)
	}
	if s.Bind(token, device) != "" || s.Snapshot().Devices[0].VersionStatus != "unpaired" {
		t.Fatal("revocation failed")
	}
	if err := s.SyncBindings([]Binding{{token, "replacement"}}); err != nil {
		t.Fatal(err)
	}
	if s.Snapshot().Devices[0].Info != nil {
		t.Fatal("replacement inherited version")
	}
}

func TestCatalogNeverGuessesVersions(t *testing.T) {
	old, target, newer := testInfo("64"), testInfo("65"), testInfo("66")
	c := Catalog{Schema: 1, Releases: []Release{{old, 1}, {target, 2}, {newer, 3}}, Targets: []protocol.ClientInfo{target}}
	unknown, android, channel := testInfo("999"), target, target
	android.Platform = "android"
	channel.Channel = "unknown"
	for _, tc := range []struct {
		info   *protocol.ClientInfo
		status string
	}{{nil, "not_reported"}, {&old, "older_version"}, {&target, "matches_target"}, {&newer, "newer_than_target"}, {&unknown, "unrecognized_version"}, {&android, "target_not_set"}, {&channel, "channel_unknown"}} {
		if got := c.Status(tc.info); got != tc.status {
			t.Errorf("got %s want %s", got, tc.status)
		}
	}
}

func TestInvalidStorageAndReports(t *testing.T) {
	for _, content := range []string{`{"schema_version":1,"records":{}}`, `{"schema_version":1,"credential_hmac_salt":"bad","records":{}}`, `{"schema_version":1,"credential_hmac_salt":"abcd","records":{"key":null}}`} {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "device_inventory.json"), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(dir); err == nil {
			t.Fatal("accepted corrupt state")
		}
	}
	dir := t.TempDir()
	if err := os.Symlink("missing", filepath.Join(dir, "device_inventory.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("accepted symlink")
	}
	info := testInfo("65")
	info.Version = " \n"
	if ValidInfo(&info) {
		t.Fatal("invalid text accepted")
	}
}

func TestConcurrentObservationsAndLocalAdmin(t *testing.T) {
	// Keep the Unix path below macOS's sockaddr_un limit.
	dir, err := os.MkdirTemp("/tmp", "inventory-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SyncBindings([]Binding{{"token", "device"}}); err != nil {
		t.Fatal(err)
	}
	key := s.Bind("token", "device")
	info := testInfo("65")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				if err := s.Observe(key, "conn", "device", "Phone", "ios", &info); err != nil {
					t.Error(err)
				}
				_ = s.Snapshot()
			}
		}()
	}
	wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, err := ServeAdmin(ctx, dir, s.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	result, err := Query(ctx, dir)
	if err != nil || len(result.Devices) != 1 {
		t.Fatal(result, err)
	}
	fi, err := os.Stat(socketPath(dir))
	if err != nil || fi.Mode().Perm() != 0600 {
		t.Fatal("unsafe socket", err)
	}
	if _, err := ServeAdmin(ctx, dir, s.Snapshot); err == nil {
		t.Fatal("replaced active admin")
	}
}
