package widgetaccess

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestReadGrantIdentityExpiryRenewalAndRestart(t *testing.T) {
	dir := t.TempDir()
	s, err := New(dir, "bridge")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1700000000, 0)
	token, original := s.Issue("phone", "full-private-credential", "", now)
	if strings.Contains(token, "full-private-credential") {
		t.Fatal("leaked full credential")
	}
	restored, err := New(dir, "bridge")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := restored.Parse(token, now)
	if err != nil || claims.DeviceID != "phone" || claims.Scope != "widget:snapshot" {
		t.Fatalf("invalid grant: %+v %v", claims, err)
	}
	if _, err := s.Parse(token, now.Add(8*24*time.Hour)); err == nil {
		t.Fatal("expired grant accepted")
	}
	if _, err := s.Parse(token+"x", now); err == nil {
		t.Fatal("tampered grant accepted")
	}
	other, _ := New(dir, "other")
	if _, err := other.Parse(token, now); err == nil {
		t.Fatal("wrong authority accepted")
	}
	_, renewed := s.Issue("phone", "full-private-credential", token, now.Add(8*24*time.Hour))
	if renewed.Since != original.Since {
		t.Fatal("renewal lost unread horizon")
	}
	_, rotated := s.Issue("phone", "rotated-credential", token, now.Add(time.Hour))
	if rotated.Since == original.Since {
		t.Fatal("reused another credential's history horizon")
	}
}

func TestGrantDoesNotRevealLegacyDeviceIDCredential(t *testing.T) {
	s, err := New(t.TempDir(), "bridge")
	if err != nil {
		t.Fatal(err)
	}
	credential := "dev_legacy-device-id-credential"
	token, _ := s.Issue(credential, credential, "", time.Now())
	parts := strings.Split(token, ".")
	encrypted, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || strings.Contains(string(encrypted), credential) || strings.Contains(string(encrypted), "device_id") {
		t.Fatal("grant disclosed the full legacy credential")
	}
	claims, err := s.Parse(token, time.Now())
	if err != nil || claims.DeviceID != credential {
		t.Fatal("opaque grant did not round trip")
	}
}
