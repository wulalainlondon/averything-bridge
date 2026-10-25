package pushrelay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

type FCMSender struct {
	tokens   oauth2.TokenSource
	endpoint string
	http     *http.Client
}

// NewFCMSender uses Application Default Credentials. On a managed Google
// runtime attach a dedicated service identity with messages.create; do not
// create/download a private key for each customer Bridge.
func NewFCMSender(ctx context.Context, project string) (*FCMSender, error) {
	if !regexp.MustCompile(`^[a-z][a-z0-9-]{4,61}[a-z0-9]$`).MatchString(project) {
		return nil, errors.New("invalid Firebase project")
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})
	credentials, err := google.FindDefaultCredentials(ctx, "https://www.googleapis.com/auth/firebase.messaging")
	if err != nil {
		return nil, errors.New("server Google credentials unavailable")
	}
	return &FCMSender{tokens: oauth2.ReuseTokenSource(nil, credentials.TokenSource), endpoint: "https://fcm.googleapis.com/v1/projects/" + project + "/messages:send", http: &http.Client{Timeout: 12 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func (s *FCMSender) Send(ctx context.Context, token string, message Message) ProviderResult {
	body, err := jsonBytes(message)
	if err != nil {
		return ProviderResult{Status: Failed, Error: "invalid_message"}
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(body, &fields); err != nil {
		return ProviderResult{Status: Failed, Error: "invalid_message"}
	}
	fields["token"], _ = json.Marshal(token)
	if message.LiveActivity != nil {
		if !message.LiveActivity.Valid() || message.activityToken == "" {
			return ProviderResult{Status: Failed, Error: "invalid_message"}
		}
		delete(fields, "live_activity")
		fields["apns"], _ = json.Marshal(liveAPNS(*message.LiveActivity, message.activityToken))
	}
	body, err = json.Marshal(struct {
		Message map[string]json.RawMessage `json:"message"`
	}{fields})
	if err != nil {
		return ProviderResult{Status: Failed, Error: "invalid_message"}
	}
	auth, err := s.tokens.Token()
	if err != nil {
		return ProviderResult{Status: Retryable, Error: "provider_auth_unavailable"}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return ProviderResult{Status: Failed, Error: "invalid_message"}
	}
	request.Header.Set("Authorization", "Bearer "+auth.AccessToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := s.http.Do(request)
	if err != nil {
		return ProviderResult{Status: Uncertain, Error: "provider_delivery_uncertain"}
	}
	defer response.Body.Close()
	if response.StatusCode == 200 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
		return ProviderResult{Status: Sent}
	}
	data, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
	var failure struct {
		Error struct {
			Details []struct {
				ErrorCode string `json:"errorCode"`
			} `json:"details"`
		} `json:"error"`
	}
	_ = json.Unmarshal(data, &failure)
	for _, detail := range failure.Error.Details {
		if detail.ErrorCode == "UNREGISTERED" && response.StatusCode == 404 {
			return ProviderResult{Status: Failed, Error: "UNREGISTERED"}
		}
	}
	if response.StatusCode == 429 || response.StatusCode >= 500 {
		return ProviderResult{Status: Retryable, Error: "provider_retryable"}
	}
	// Do not reflect Google's response: it can contain the registration token,
	// project details, or a caller's private notification text.
	return ProviderResult{Status: Failed, Error: "provider_rejected"}
}
