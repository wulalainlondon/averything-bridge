package workspacetools

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"everything-go/internal/workspace"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func respond(status int, v any) *http.Response {
	b, _ := json.Marshal(v)
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(b)))}
}
func adapter(t *testing.T, kind string) *Google {
	t.Helper()
	p := Profile{ID: "original-fixture-account", Kind: kind, Provider: "google", Revision: "rev-1", Subject: "fixture@example.com", Keychain: KeychainProfile{"EXACT-FIXTURE-SERVICE", "EXACT-FIXTURE-ACCOUNT"}, Calendars: []string{"primary"}}
	if kind == "email" {
		p.AllowedActions = []string{"list_messages", "read_message", "send"}
	} else {
		p.AllowedActions = []string{"list_events", "read_event", "create_event"}
	}
	a, err := newGoogle(p)
	if err != nil {
		t.Fatal(err)
	}
	a.secret = func(_ context.Context, profile KeychainProfile) ([]byte, error) {
		if profile != p.Keychain {
			t.Fatal("substitute credential profile")
		}
		return json.Marshal(map[string]any{"access_token": "SYNTHETIC-NOT-A-CREDENTIAL", "expiry": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
	}
	a.http = &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
		t.Fatal("unhandled synthetic HTTP request")
		return nil, nil
	})}
	return a
}
func TestGoogleOriginalIdentityAndKeychainRevocation(t *testing.T) {
	a := adapter(t, "email")
	var calls atomic.Int32
	a.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		if r.Method != "GET" || r.URL.String() != "https://www.googleapis.com/oauth2/v3/userinfo" || r.Header.Get("Authorization") != "Bearer SYNTHETIC-NOT-A-CREDENTIAL" {
			t.Fatal("identity request invalid", r.Method, r.URL.Host)
		}
		return respond(200, map[string]any{"email": "someone-else@example.com", "email_verified": true}), nil
	})
	if a.Ready(context.Background()) == nil {
		t.Fatal("wrong original subject accepted")
	}
	a.http.Transport = roundTrip(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return respond(200, map[string]any{"email": "fixture@example.com", "email_verified": true}), nil
	})
	if err := a.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	before := calls.Load()
	a.secret = func(context.Context, KeychainProfile) ([]byte, error) {
		return nil, errors.New("original_keychain_profile_unavailable")
	}
	if a.Ready(context.Background()) == nil || calls.Load() != before {
		t.Fatal("cached bearer bypassed original credential revocation")
	}
}
func TestGoogleEmailUnknownLookupNeverResendsAndBindsMIME(t *testing.T) {
	a := adapter(t, "email")
	e := workspace.Effect{Adapter: "email", Action: "send", Account: a.Account(), Destination: "Person <person@example.com>", Title: "確認事項", Content: "Pinned draft", ContentHash: strings.Repeat("a", 64), Parameters: json.RawMessage(`{}`)}
	id := "cw-original-effect"
	var posts, gets int
	phase := 0
	a.http.Transport = accountTransport(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.RawQuery, "SYNTHETIC") {
			t.Fatal("credential leaked in URL")
		}
		if r.Method == "POST" {
			posts++
			var body map[string]string
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				t.Fatal("bad send")
			}
			raw, err := base64.RawURLEncoding.DecodeString(body["raw"])
			if err != nil || !strings.Contains(string(raw), "Message-ID: "+a.messageID(id)) || !strings.Contains(string(raw), "X-Bridge-Content-Hash: "+e.ContentHash) || !strings.Contains(string(raw), "filename=notes.txt") {
				t.Fatal("unbound MIME")
			}
			return nil, errors.New("synthetic response lost after provider accepted")
		}
		gets++
		if strings.HasSuffix(r.URL.Path, "/messages") {
			return respond(200, map[string]any{"messages": []any{map[string]string{"id": "provider-original"}}}), nil
		}
		hash := e.ContentHash
		if phase == 0 {
			hash = "wrong"
		}
		return respond(200, map[string]any{"payload": map[string]any{"headers": []any{map[string]string{"name": "Message-ID", "value": a.messageID(id)}, map[string]string{"name": "X-Bridge-Content-Hash", "value": hash}}}}), nil
	})
	if err := a.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	receipt, err := a.Dispatch(context.Background(), id, e, []workspace.Asset{{Attachment: workspace.Attachment{Name: "notes.txt", MIME: "text/plain"}, Data: []byte("attachment body")}})
	if err != nil || receipt.State != "unknown" {
		t.Fatal(receipt, err)
	}
	r, err := a.Lookup(context.Background(), id, e)
	if err != nil || r.State != "unknown" {
		t.Fatal("mismatched content recovered as sent", r, err)
	}
	phase = 1
	r, err = a.Lookup(context.Background(), id, e)
	if err != nil || r.State != "acknowledged" || r.ProviderID != "provider-original" || posts != 1 || gets != 4 {
		t.Fatal("unknown recovery replayed or lost identity", r, err, posts, gets)
	}
	if _, err = a.email(id, workspace.Effect{Destination: "x@example.com\r\nBcc:evil@example.com", Title: "x"}, nil); err == nil {
		t.Fatal("header injection accepted")
	}
}
func TestGoogleCalendarConflictAndOriginalEventRecovery(t *testing.T) {
	a := adapter(t, "calendar")
	params := EventParameters{Start: "2026-11-01T01:30:00-04:00", End: "2026-11-01T01:45:00-04:00", Timezone: "America/New_York", Attendees: []string{"guest@example.com"}}
	b, _ := json.Marshal(params)
	e := workspace.Effect{Adapter: "calendar", Action: "create_event", Account: a.Account(), Destination: "primary", Title: "Original meeting", Content: "draft", ContentHash: strings.Repeat("c", 64), Parameters: b}
	var posts int
	lookupHash := "wrong"
	a.http.Transport = accountTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == "POST" {
			posts++
			var event map[string]any
			_ = json.NewDecoder(r.Body).Decode(&event)
			if event["id"] != a.operation("original") || r.URL.Query().Get("sendUpdates") != "all" {
				t.Fatal("original event identity missing")
			}
			return nil, errors.New("synthetic lost response")
		}
		if strings.HasSuffix(r.URL.Path, "/events") {
			return respond(200, map[string]any{"items": []any{map[string]string{"id": "existing-overlap"}}}), nil
		}
		return respond(200, map[string]any{"id": a.operation("original"), "extendedProperties": map[string]any{"private": map[string]string{"bridgeIntent": a.operation("original"), "bridgeContentHash": lookupHash}}}), nil
	})
	if err := a.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.Validate(context.Background(), e) == nil {
		t.Fatal("conflict silently approved")
	}
	params.AllowConflicts = true
	e.Parameters, _ = json.Marshal(params)
	if err := a.Validate(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	r, err := a.Dispatch(context.Background(), "original", e, nil)
	if err != nil || r.State != "unknown" {
		t.Fatal(r, err)
	}
	r, err = a.Lookup(context.Background(), "original", e)
	if err != nil || r.State != "unknown" {
		t.Fatal(r, err)
	}
	lookupHash = e.ContentHash
	r, err = a.Lookup(context.Background(), "original", e)
	if err != nil || r.State != "acknowledged" || posts != 1 {
		t.Fatal(r, err, posts)
	}
	e.Destination = "ungranted-calendar"
	if a.Validate(context.Background(), e) == nil {
		t.Fatal("substitute calendar accepted")
	}
}
func TestGoogleOperatorConfigPrivateAndImmutable(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "tools.json")
	p := Profile{ID: "fixture", Kind: "email", Provider: "google", Revision: "r1", Subject: "fixture@example.com", Keychain: KeychainProfile{"exact", "account"}, AllowedActions: []string{"send"}}
	b, _ := json.Marshal(Configuration{Version: 1, Accounts: []Profile{p}})
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	all, err := Load(path)
	if err != nil || len(all) != 1 {
		t.Fatal(err)
	}
	a := all[0].(*Google)
	if err = a.configurationCurrent(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, []byte(`{"version":1,"accounts":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if a.configurationCurrent() == nil {
		t.Fatal("changed operator profile reused old permission")
	}
	if err = os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err = Load(path); err == nil {
		t.Fatal("public config accepted")
	}
	if err = os.Symlink(path, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err = Load(filepath.Join(root, "link")); err == nil {
		t.Fatal("symlinked credential profile accepted")
	}
}

func accountTransport(f roundTrip) http.RoundTripper {
	return roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/oauth2/v3/userinfo" {
			return respond(200, map[string]any{"email": "fixture@example.com", "email_verified": true}), nil
		}
		return f(r)
	})
}
func TestTokenReplacementCannotBorrowAnotherMailboxBetweenIdentityAndRead(t *testing.T) {
	a := adapter(t, "email")
	var changed bool
	var accountReads int
	a.secret = func(context.Context, KeychainProfile) ([]byte, error) {
		token := "original-synthetic-token"
		if changed {
			token = "different-subject-synthetic-token"
		}
		return json.Marshal(map[string]any{"access_token": token, "expiry": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
	}
	a.http.Transport = roundTrip(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/oauth2/v3/userinfo" {
			return respond(200, map[string]any{"email": "fixture@example.com", "email_verified": true}), nil
		}
		accountReads++
		return respond(200, map[string]any{"messages": []any{}}), nil
	})
	if err := a.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	changed = true
	if _, err := a.Read(context.Background(), "list_messages", nil); err == nil || accountReads != 0 {
		t.Fatal("replacement token read an unverified mailbox", err, accountReads)
	}
}
