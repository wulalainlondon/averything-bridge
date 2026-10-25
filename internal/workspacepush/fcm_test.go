package workspacepush

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"golang.org/x/oauth2"
	"io"
	"net/http"
	"strings"
	"testing"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestFCMSendsOnlyGenericMetadataAndExactConfiguredIdentity(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	credential := map[string]string{"type": "service_account", "project_id": "fixture-project", "client_email": "fixture@example.invalid", "token_uri": "https://oauth2.googleapis.com/token", "private_key": string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))}
	raw, _ := json.Marshal(credential)
	calls := 0
	client := &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		calls++
		body := ""
		status := 200
		if r.URL.Host == "oauth2.googleapis.com" {
			body = `{"access_token":"fixture-access","expires_in":3600,"token_type":"Bearer"}`
		} else if r.URL.Host == "fcm.googleapis.com" {
			b, _ := io.ReadAll(r.Body)
			var data map[string]any
			if json.Unmarshal(b, &data) != nil {
				t.Fatal("payload")
			}
			msg := data["message"].(map[string]any)
			notification := msg["notification"].(map[string]any)
			if notification["title"] != "協作頻道" || notification["body"] != "有新的頻道待辦" {
				t.Fatal("private push content", notification)
			}
			fields := msg["data"].(map[string]any)
			if len(fields) != 5 || fields["room_id"] != "room-fixture" || fields["authority_instance_id"] != "authority-fixture" {
				t.Fatal(fields)
			}
			if r.Header.Get("Authorization") != "Bearer fixture-access" {
				t.Fatal("auth not applied")
			}
			body = `{"name":"projects/fixture-project/messages/fixture-original"}`
		} else {
			t.Fatal("unexpected network target", r.URL.Host)
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	s := &Sender{config: Configuration{ProjectID: "fixture-project", Subject: "fixture@example.invalid"}, current: func() bool { return true }, secret: func(context.Context) ([]byte, error) { return raw, nil }, http: client}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	receipt := s.Send(ctx, "fixture-target-token", "android", "authority-fixture", "room-fixture", "cw_notice_fixture")
	if receipt.State != "accepted" || calls != 2 {
		t.Fatal(receipt, calls)
	}
	s.config.Subject = "different@example.invalid"
	receipt = s.Send(ctx, "fixture-target-token", "android", "authority-fixture", "room-fixture", "cw_notice_fixture")
	if receipt.State != "failed" || calls != 2 {
		t.Fatal("identity mismatch contacted provider", receipt, calls)
	}
}
