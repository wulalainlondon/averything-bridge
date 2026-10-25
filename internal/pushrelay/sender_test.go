package pushrelay

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

func TestFCMSenderUsesOnlyServerCredentialAndVerifiedDestination(t *testing.T) {
	var captured struct {
		Message struct {
			Token string            `json:"token"`
			Data  map[string]string `json:"data"`
		} `json:"message"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer server-only-fixture-oauth" {
			t.Error("server OAuth missing")
		}
		if json.NewDecoder(r.Body).Decode(&captured) != nil {
			t.Error("provider envelope invalid")
		}
		_, _ = io.WriteString(w, `{"name":"projects/test/messages/1"}`)
	}))
	defer server.Close()
	sender := &FCMSender{tokens: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "server-only-fixture-oauth"}), endpoint: server.URL, http: server.Client()}
	result := sender.Send(context.Background(), testPhoneToken, Message{Data: map[string]string{"type": "task_done"}})
	if result.Status != Sent || captured.Message.Token != testPhoneToken || captured.Message.Data["type"] != "task_done" {
		t.Fatal("verified destination/payload changed")
	}
}

func TestFCMSenderSanitizesFailuresAndClassifiesOnlyConfirmedInvalidToken(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
		want   ProviderResult
	}{
		{"unregistered", 404, `{"error":{"details":[{"errorCode":"UNREGISTERED"}]}}`, ProviderResult{Status: Failed, Error: "UNREGISTERED"}},
		{"wrong-project", 404, `{"error":{"message":"MUST_NOT_APPEAR token="}}`, ProviderResult{Status: Failed, Error: "provider_rejected"}},
		{"quota", 429, `{"error":{"message":"MUST_NOT_APPEAR"}}`, ProviderResult{Status: Retryable, Error: "provider_retryable"}},
		{"temporary", 503, `{"error":{"message":"MUST_NOT_APPEAR"}}`, ProviderResult{Status: Retryable, Error: "provider_retryable"}},
		{"invalid-payload", 400, `{"error":{"message":"MUST_NOT_APPEAR"}}`, ProviderResult{Status: Failed, Error: "provider_rejected"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			sender := &FCMSender{tokens: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "fixture-oauth"}), endpoint: server.URL, http: server.Client()}
			result := sender.Send(context.Background(), testPhoneToken, Message{Data: map[string]string{"type": "task_done"}})
			if result != test.want || strings.Contains(result.Error, "MUST_NOT_APPEAR") {
				t.Fatal("provider classification/sanitization incorrect")
			}
		})
	}
}

func TestClientDoesNotForwardCredentialAcrossRedirect(t *testing.T) {
	contacted := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { contacted = true; w.WriteHeader(200) }))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer origin.Close()
	client, err := NewClient(origin.URL, "bridge-a", t.TempDir()+"/client.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Enroll(context.Background()); err == nil || contacted {
		t.Fatal("broker credential followed an untrusted redirect")
	}
}

func TestDatabaseRefusesPublicDirectoryAndSymlink(t *testing.T) {
	public := t.TempDir()
	if err := os.Chmod(public, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(public + "/state.sqlite"); err == nil {
		t.Fatal("public directory unexpectedly accepted")
	}
	private := t.TempDir() + "/private"
	if err := os.Mkdir(private, 0700); err != nil {
		t.Fatal(err)
	}
	file := private + "/original"
	if err := os.WriteFile(file, []byte("not a database"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(file, private+"/state.sqlite"); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(private + "/state.sqlite"); err == nil {
		t.Fatal("symlink database accepted")
	}
}
