package automation

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSentryPollBaselinesThenNormalizesChangesWithoutSecretsOrPII(t *testing.T) {
	responses := []string{
		`[{"id":"1","shortId":"APP-1","title":"old","culprit":"worker","count":"2","level":"error","status":"unresolved","firstSeen":"2026-08-30T00:00:00Z","lastSeen":"2026-08-30T00:01:00Z","permalink":"https://sentry.io/issues/1/","project":{"slug":"mobile"}}]`,
		`[{"id":"1","shortId":"APP-1","title":"failure for user@example.com from 192.168.1.20","culprit":"POST /users/user@example.com","count":"3","level":"fatal","status":"unresolved","firstSeen":"2026-08-30T00:00:00Z","lastSeen":"2026-08-30T00:02:00Z","permalink":"https://sentry.io/issues/1/","platform":"javascript","issueCategory":"error","project":{"slug":"mobile"}}]`,
	}
	requestCount := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/api/0/organizations/acme/issues/" {
			t.Fatalf("request=%s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer secret-token" || request.URL.Query().Get("project") != "mobile" {
			t.Fatalf("missing auth or project: %s", request.URL.Redacted())
		}
		if strings.Contains(request.URL.String(), "secret-token") || request.URL.Query().Get("query") != "" || request.URL.Query().Get("sort") != "date" {
			t.Fatalf("unsafe or invalid query: %s", request.URL.Redacted())
		}
		body := responses[requestCount]
		requestCount++
		return sentryResponse(http.StatusOK, body, nil), nil
	})}
	connector := SentryConnector{Client: client, BaseURL: "https://sentry.io", Environment: "prod", StatsPeriod: "7d"}
	account := Account{ID: "primary", ExternalAccountID: "acme/mobile", DisplayName: "Mobile", CredentialRef: "env:SENTRY"}
	secrets := staticSecrets{"env:SENTRY": "secret-token"}

	baseline, err := connector.Poll(context.Background(), account, PollState{Cursor: json.RawMessage(`{}`)}, secrets)
	if err != nil || len(baseline.Events) != 0 || len(baseline.Cursor) == 0 {
		t.Fatalf("baseline=%+v err=%v", baseline, err)
	}
	batch, err := connector.Poll(context.Background(), account, PollState{Cursor: baseline.Cursor}, secrets)
	if err != nil || len(batch.Events) != 1 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	event := batch.Events[0]
	encoded, _ := json.Marshal(event)
	if event.Kind != "issue.updated" || event.Severity != "error" || event.Source != "sentry.primary" {
		t.Fatalf("event=%+v", event)
	}
	if strings.Contains(string(encoded), "secret-token") || strings.Contains(string(encoded), "user@example.com") || strings.Contains(string(encoded), "192.168.1.20") {
		t.Fatalf("event leaked secret or PII: %s", encoded)
	}
	if !strings.Contains(event.Title, "[redacted-email]") || !strings.Contains(event.Title, "[redacted-ip]") {
		t.Fatalf("title was not redacted: %q", event.Title)
	}
	if !strings.Contains(string(event.Data), `"project":"mobile"`) || !strings.Contains(string(event.Data), `"issue_id":"1"`) {
		t.Fatalf("data=%s", event.Data)
	}
}

func TestSentryPollPaginatesAndClassifiesCreatedIssues(t *testing.T) {
	requestCount := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requestCount++
		switch requestCount {
		case 1:
			header := http.Header{"Link": {`<https://sentry.io/api/0/organizations/acme/issues/?cursor=next>; rel="next"; results="true"`}}
			return sentryResponse(http.StatusOK, `[{"id":"1","shortId":"APP-1","title":"one","count":"1","level":"warning","status":"unresolved","firstSeen":"2026-08-30T00:02:00Z","lastSeen":"2026-08-30T00:02:00Z"}]`, header), nil
		case 2:
			if request.URL.Query().Get("cursor") != "next" {
				t.Fatalf("cursor=%q", request.URL.Query().Get("cursor"))
			}
			return sentryResponse(http.StatusOK, `[{"id":"2","shortId":"APP-2","title":"two","count":"1","level":"info","status":"unresolved","firstSeen":"2026-08-30T00:03:00Z","lastSeen":"2026-08-30T00:03:00Z"}]`, nil), nil
		default:
			t.Fatalf("unexpected request %d", requestCount)
			return nil, nil
		}
	})}
	state := PollState{Cursor: json.RawMessage(`{"version":1,"initialized":true,"polled_at_ms":1788048060000,"issues":{}}`)}
	batch, err := (SentryConnector{Client: client, BaseURL: "https://sentry.io"}).Poll(context.Background(),
		Account{ID: "a", ExternalAccountID: "acme/mobile", CredentialRef: "env:S"}, state, staticSecrets{"env:S": "token"})
	if err != nil || len(batch.Events) != 2 || requestCount != 2 {
		t.Fatalf("batch=%+v requests=%d err=%v", batch, requestCount, err)
	}
	for _, event := range batch.Events {
		if event.Kind != "issue.created" {
			t.Fatalf("kind=%q event=%+v", event.Kind, event)
		}
	}
}

func TestSentryPollDetectsRegressionAndUsesStableEventKey(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return sentryResponse(http.StatusOK, `[{"id":"9","shortId":"APP-9","title":"again","count":"11","level":"error","status":"unresolved","firstSeen":"2026-08-29T00:00:00Z","lastSeen":"2026-08-30T01:00:00Z"}]`, nil), nil
	})}
	state := PollState{Cursor: json.RawMessage(`{"version":1,"initialized":true,"polled_at_ms":1788050000000,"issues":{"9":{"count":"10","status":"resolved","last_seen_ms":1788048000000}}}`)}
	connector := SentryConnector{Client: client, BaseURL: "https://sentry.io"}
	account := Account{ID: "a", ExternalAccountID: "acme/mobile", CredentialRef: "env:S"}
	first, err := connector.Poll(context.Background(), account, state, staticSecrets{"env:S": "token"})
	if err != nil || len(first.Events) != 1 || first.Events[0].Kind != "issue.regressed" {
		t.Fatalf("batch=%+v err=%v", first, err)
	}
	second, err := connector.Poll(context.Background(), account, state, staticSecrets{"env:S": "token"})
	if err != nil || len(second.Events) != 1 || second.Events[0].EventKey != first.Events[0].EventKey {
		t.Fatalf("event key is not deterministic: first=%+v second=%+v err=%v", first, second, err)
	}
}

func TestSentryPollHonorsETagAndRejectsUnsafePagination(t *testing.T) {
	state := PollState{Cursor: json.RawMessage(`{"version":1,"initialized":true,"issues":{}}`), ETag: `"revision-1"`}
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("If-None-Match") != `"revision-1"` {
			t.Fatalf("etag header=%q", request.Header.Get("If-None-Match"))
		}
		return sentryResponse(http.StatusNotModified, "", http.Header{"ETag": {`"revision-1"`}}), nil
	})}
	batch, err := (SentryConnector{Client: client, BaseURL: "https://sentry.io"}).Poll(context.Background(),
		Account{ID: "a", ExternalAccountID: "acme/mobile", CredentialRef: "env:S"}, state, staticSecrets{"env:S": "token"})
	if err != nil || string(batch.Cursor) != string(state.Cursor) || len(batch.Events) != 0 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}

	unsafeClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		header := http.Header{"Link": {`<https://attacker.example/steal?cursor=x>; rel="next"; results="true"`}}
		return sentryResponse(http.StatusOK, `[]`, header), nil
	})}
	_, err = (SentryConnector{Client: unsafeClient, BaseURL: "https://sentry.io"}).Poll(context.Background(),
		Account{ID: "a", ExternalAccountID: "acme/mobile", CredentialRef: "env:S"}, PollState{}, staticSecrets{"env:S": "token"})
	if err == nil || err.Error() != "sentry_invalid_paging_url" {
		t.Fatalf("unsafe paging err=%v", err)
	}
}

func TestSentryPollReturnsSafeHTTPErrorCodesAndIsReadOnly(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return sentryResponse(status, `{"detail":"do not persist this"}`, nil), nil
			})}
			_, err := (SentryConnector{Client: client, BaseURL: "https://sentry.io"}).Poll(context.Background(),
				Account{ID: "a", ExternalAccountID: "acme/mobile", CredentialRef: "env:S"}, PollState{}, staticSecrets{"env:S": "token"})
			if err == nil || err.Error() != "sentry_http_"+httpStatusNumber(status) {
				t.Fatalf("status=%d err=%v", status, err)
			}
		})
	}
	_, err := (SentryConnector{}).Execute(context.Background(), Account{}, Proposal{ActionType: "issue.resolve"}, staticSecrets{})
	if err == nil || err.Error() != "unsupported_sentry_action" {
		t.Fatalf("execute err=%v", err)
	}
}

func TestSentryBootstrapRequiresCompleteLocalConfiguration(t *testing.T) {
	store := openTestStore(t)
	// Bootstrap inspects every supported provider. Keep this test isolated from
	// credentials exported by the signed local launcher.
	for _, name := range []string{"FB_JUDGE_PAGE_ID", "FB_ULALA_PAGE_ID", "THREADS_USER_ID"} {
		t.Setenv(name, "")
	}
	t.Setenv("SENTRY_ORG", "acme")
	t.Setenv("SENTRY_PROJECT", "mobile")
	t.Setenv("SENTRY_AUTH_TOKEN", "")
	created, err := BootstrapAccountsFromEnv(context.Background(), store)
	if err != nil || created != 0 {
		t.Fatalf("incomplete created=%d err=%v", created, err)
	}
	if _, err := store.GetAccount(context.Background(), "sentry-primary"); err != ErrNotFound {
		t.Fatalf("incomplete Sentry account exists: %v", err)
	}
	t.Setenv("SENTRY_AUTH_TOKEN", "local-only-token")
	t.Setenv("SENTRY_DISPLAY_NAME", "Production Mobile")
	created, err = BootstrapAccountsFromEnv(context.Background(), store)
	if err != nil || created != 1 {
		t.Fatalf("complete created=%d err=%v", created, err)
	}
	account, err := store.GetAccount(context.Background(), "sentry-primary")
	if err != nil || account.ExternalAccountID != "acme/mobile" || account.DisplayName != "Production Mobile" || !account.PollEnabled || account.WebhookEnabled || account.CredentialRef != "env:SENTRY_AUTH_TOKEN" {
		t.Fatalf("account=%+v err=%v", account, err)
	}
	encoded, _ := json.Marshal(account)
	if strings.Contains(string(encoded), "local-only-token") {
		t.Fatalf("account persisted token: %s", encoded)
	}
	t.Setenv("SENTRY_WEBHOOK_SECRET", "local-webhook-secret")
	created, err = BootstrapAccountsFromEnv(context.Background(), store)
	if err != nil || created != 0 {
		t.Fatalf("webhook promotion created=%d err=%v", created, err)
	}
	account, err = store.GetAccount(context.Background(), "sentry-primary")
	if err != nil || !account.WebhookEnabled || account.AppSecretRef != "env:SENTRY_WEBHOOK_SECRET" {
		t.Fatalf("webhook account=%+v err=%v", account, err)
	}
}

func TestSanitizeSentryTextKeepsUTF8Valid(t *testing.T) {
	value := sanitizeSentryText("錯誤訊息 user@example.com", 13)
	if !utf8.ValidString(value) || len(value) > 13 {
		t.Fatalf("invalid clipped text %q bytes=%d", value, len(value))
	}
}

func TestSentryWebhookSignatureNormalizationAndPollDedupeKey(t *testing.T) {
	body := []byte(`{"action":"unresolved","data":{"issue":{"id":"9","shortId":"APP-9","title":"regressed","culprit":"worker","count":"11","level":"error","status":"unresolved","substatus":"regressed","firstSeen":"2026-08-29T00:00:00Z","lastSeen":"2026-08-30T01:00:00Z","web_url":"https://acme.sentry.io/issues/9/","project":{"slug":"mobile"}}}}`)
	mac := hmac.New(sha256.New, []byte("client-secret"))
	_, _ = mac.Write(body)
	signature := hex.EncodeToString(mac.Sum(nil))
	if !ValidSentryWebhookSignature(body, "client-secret", signature) || ValidSentryWebhookSignature(body, "wrong", signature) {
		t.Fatal("Sentry webhook signature verification mismatch")
	}
	account := Account{ID: "primary", ExternalAccountID: "acme/mobile"}
	webhook, ignored, err := NormalizeSentryWebhook(account, "issue", body, 1788052000000)
	if err != nil || ignored || webhook.Kind != "issue.regressed" || webhook.URL != "https://acme.sentry.io/issues/9/" {
		t.Fatalf("webhook=%+v ignored=%v err=%v", webhook, ignored, err)
	}
	var payload sentryWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	poll := sentryEvent(account, "acme", "mobile", payload.Data.Issue, payload.Data.Issue.sentryFingerprint(), "issue.regressed", 1788052000000)
	if poll.EventKey != webhook.EventKey {
		t.Fatalf("poll key=%q webhook key=%q", poll.EventKey, webhook.EventKey)
	}
	if _, ignored, err := NormalizeSentryWebhook(account, "metric_alert", body, 0); err != nil || !ignored {
		t.Fatalf("unsupported resource ignored=%v err=%v", ignored, err)
	}
}

func sentryResponse(status int, body string, header http.Header) *http.Response {
	if header == nil {
		header = make(http.Header)
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: header}
}

func httpStatusNumber(status int) string {
	return strconv.Itoa(status)
}
