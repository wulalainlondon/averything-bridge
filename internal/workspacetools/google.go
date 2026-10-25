package workspacetools

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/mail"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"everything-go/internal/workspace"
)

type Google struct {
	profile              Profile
	revision             string
	allowed              map[string]bool
	http                 *http.Client
	secret               func(context.Context, KeychainProfile) ([]byte, error)
	now                  func() time.Time
	tokenMu              sync.Mutex
	token                string
	expiry               time.Time
	credentialHash       [32]byte
	configurationCurrent func() error
	identityMu           sync.RWMutex
	verifiedToken        [32]byte
	verifiedUntil        time.Time
}
type oauthCredential struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	Expiry       string `json:"expiry"`
}

func (a *Google) Account() string  { return a.profile.ID }
func (a *Google) Kind() string     { return a.profile.Kind }
func (a *Google) Revision() string { return a.revision }
func (a *Google) tokenFor(ctx context.Context) (string, error) {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	if a.configurationCurrent != nil {
		if err := a.configurationCurrent(); err != nil {
			return "", err
		}
	}
	b, err := a.secret(ctx, a.profile.Keychain)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(b)
	if hash == a.credentialHash && a.token != "" && a.expiry.After(a.now().Add(time.Minute)) {
		return a.token, nil
	}
	a.token, a.expiry, a.credentialHash = "", time.Time{}, hash
	var c oauthCredential
	if json.Unmarshal(b, &c) != nil {
		return "", errors.New("original_keychain_oauth_format_invalid")
	}
	expiry, e := time.Parse(time.RFC3339, c.Expiry)
	if c.AccessToken != "" && e == nil && expiry.After(a.now().Add(time.Minute)) {
		a.token, a.expiry = c.AccessToken, expiry
		return a.token, nil
	}
	if c.RefreshToken == "" || c.ClientID == "" || c.ClientSecret == "" {
		return "", errors.New("original_oauth_refresh_or_expiry_missing")
	}
	form := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {c.RefreshToken}, "client_id": {c.ClientID}, "client_secret": {c.ClientSecret}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://oauth2.googleapis.com/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.New("oauth_refresh_prepare_failed")
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := a.http.Do(request)
	if err != nil {
		return "", errors.New("oauth_refresh_unavailable")
	}
	defer res.Body.Close()
	var next struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if res.StatusCode != 200 || json.NewDecoder(io.LimitReader(res.Body, 65536)).Decode(&next) != nil || next.AccessToken == "" || next.ExpiresIn <= 0 {
		return "", errors.New("original_oauth_refresh_rejected")
	}
	a.token = next.AccessToken
	a.expiry = a.now().Add(time.Duration(next.ExpiresIn) * time.Second)
	return a.token, nil
}

type response struct {
	Status    int
	Body      map[string]any
	TokenHash [32]byte
}

func (a *Google) call(ctx context.Context, method, endpoint string, body any) (response, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.User != nil || (u.Host != "gmail.googleapis.com" && u.Host != "www.googleapis.com") {
		return response{}, errors.New("tool_endpoint_forbidden")
	}
	token, err := a.tokenFor(ctx)
	if err != nil {
		return response{}, err
	}
	tokenHash := sha256.Sum256([]byte(token))
	if !(u.Path == "/oauth2/v3/userinfo" && u.RawQuery == "" && method == http.MethodGet) {
		a.identityMu.RLock()
		verified := a.verifiedToken == tokenHash && a.verifiedUntil.After(a.now())
		a.identityMu.RUnlock()
		if !verified {
			return response{}, errors.New("original_account_token_identity_requires_recheck")
		}
	}
	var input io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return response{}, errors.New("tool_payload_invalid")
		}
		input = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, input)
	if err != nil {
		return response{}, errors.New("tool_request_invalid")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := a.http.Do(req)
	if err != nil {
		return response{}, errors.New("provider_reply_unknown")
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 8*1024*1024+1))
	if err != nil || len(raw) > 8*1024*1024 {
		return response{Status: res.StatusCode}, errors.New("provider_reply_unreadable")
	}
	v := map[string]any{}
	if len(raw) > 0 && json.Unmarshal(raw, &v) != nil {
		return response{Status: res.StatusCode}, errors.New("provider_reply_unreadable")
	}
	return response{Status: res.StatusCode, Body: v, TokenHash: tokenHash}, nil
}
func (a *Google) Ready(ctx context.Context) error {
	a.identityMu.Lock()
	a.verifiedToken = [32]byte{}
	a.verifiedUntil = time.Time{}
	a.identityMu.Unlock()
	res, err := a.call(ctx, http.MethodGet, "https://www.googleapis.com/oauth2/v3/userinfo", nil)
	if err != nil {
		return err
	}
	email, _ := res.Body["email"].(string)
	verified, _ := res.Body["email_verified"].(bool)
	if res.Status != 200 || !verified || !strings.EqualFold(email, a.profile.Subject) {
		return errors.New("original_account_identity_unverified")
	}
	a.identityMu.Lock()
	a.verifiedToken = res.TokenHash
	a.verifiedUntil = a.now().Add(time.Minute)
	a.identityMu.Unlock()
	return nil
}
func getString(p map[string]any, k string) string { s, _ := p[k].(string); return s }
func strictParams(p map[string]any, keys ...string) bool {
	for k := range p {
		found := false
		for _, allowed := range keys {
			if k == allowed {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func (a *Google) calendar(id string) bool {
	for _, v := range a.profile.Calendars {
		if v == id {
			return true
		}
	}
	return false
}
func (a *Google) Read(ctx context.Context, action string, p map[string]any) (any, error) {
	if !a.allowed[action] {
		return nil, errors.New("original_account_action_not_granted")
	}
	if action == "health" {
		if err := a.Ready(ctx); err != nil {
			return nil, err
		}
		return map[string]any{"account": a.Account(), "kind": a.Kind(), "state": "identity_verified"}, nil
	}
	var endpoint string
	switch action {
	case "list_messages":
		if a.Kind() != "email" || !strictParams(p, "query", "page_token") {
			return nil, errors.New("tool_read_parameters_invalid")
		}
		q := url.Values{"maxResults": {"25"}}
		if s := getString(p, "query"); s != "" {
			if len(s) > 2000 {
				return nil, errors.New("tool_query_limit")
			}
			q.Set("q", s)
		}
		if s := getString(p, "page_token"); s != "" {
			q.Set("pageToken", s)
		}
		endpoint = "https://gmail.googleapis.com/gmail/v1/users/me/messages?" + q.Encode()
	case "read_message":
		id := getString(p, "message_id")
		if a.Kind() != "email" || !strictParams(p, "message_id") || !safeID(id) {
			return nil, errors.New("tool_read_parameters_invalid")
		}
		endpoint = "https://gmail.googleapis.com/gmail/v1/users/me/messages/" + url.PathEscape(id) + "?format=full"
	case "list_events", "read_event":
		calendar := getString(p, "calendar_id")
		if calendar == "" && len(a.profile.Calendars) > 0 {
			calendar = a.profile.Calendars[0]
		}
		if a.Kind() != "calendar" || !a.calendar(calendar) || !strictParams(p, "calendar_id", "event_id", "time_min", "time_max", "page_token") {
			return nil, errors.New("original_calendar_scope_required")
		}
		endpoint = "https://www.googleapis.com/calendar/v3/calendars/" + url.PathEscape(calendar) + "/events"
		if action == "read_event" {
			id := getString(p, "event_id")
			if !safeID(id) {
				return nil, errors.New("event_id_invalid")
			}
			endpoint += "/" + url.PathEscape(id)
		} else {
			q := url.Values{"maxResults": {"50"}, "singleEvents": {"true"}, "orderBy": {"startTime"}}
			minimum := getString(p, "time_min")
			if minimum == "" {
				minimum = a.now().UTC().Format(time.RFC3339)
			}
			if _, e := time.Parse(time.RFC3339, minimum); e != nil {
				return nil, errors.New("calendar_range_invalid")
			}
			q.Set("timeMin", minimum)
			if maximum := getString(p, "time_max"); maximum != "" {
				if _, e := time.Parse(time.RFC3339, maximum); e != nil {
					return nil, errors.New("calendar_range_invalid")
				}
				q.Set("timeMax", maximum)
			}
			if next := getString(p, "page_token"); next != "" {
				q.Set("pageToken", next)
			}
			endpoint += "?" + q.Encode()
		}
	default:
		return nil, errors.New("tool_read_action_forbidden")
	}
	res, err := a.call(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if res.Status != 200 {
		return nil, fmt.Errorf("provider_read_status_%d", res.Status)
	}
	return res.Body, nil
}
func safeID(id string) bool {
	return id != "" && len(id) <= 1024 && !strings.ContainsAny(id, "/\\?#\x00\r\n") && id != "." && id != ".."
}
func (a *Google) operation(id string) string {
	h := sha256.Sum256([]byte(a.Account() + "\x00" + a.Revision() + "\x00" + id))
	return hex.EncodeToString(h[:])
}
func (a *Google) messageID(id string) string {
	return "<bridge." + a.operation(id) + "@workspace.invalid>"
}
func (a *Google) email(id string, e workspace.Effect, assets []workspace.Asset) (string, error) {
	from, err := mail.ParseAddress(a.profile.Subject)
	if err != nil {
		return "", errors.New("email_sender_invalid")
	}
	to, err := mail.ParseAddressList(e.Destination)
	if err != nil || len(to) == 0 || len(to) > 100 || strings.ContainsAny(e.Destination+e.Title, "\r\n\x00") {
		return "", errors.New("email_destination_or_header_invalid")
	}
	recipients := []string{}
	for _, v := range to {
		recipients = append(recipients, v.String())
	}
	var b bytes.Buffer
	boundary := "bridge_" + a.operation(id)[:32]
	fmt.Fprintf(&b, "From: %s\r\nTo: %s\r\nSubject: %s\r\nMessage-ID: %s\r\nX-Bridge-Content-Hash: %s\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=%q\r\n\r\n", from.String(), strings.Join(recipients, ", "), mime.QEncoding.Encode("utf-8", e.Title), a.messageID(id), e.ContentHash, boundary)
	w := multipart.NewWriter(&b)
	if w.SetBoundary(boundary) != nil {
		return "", errors.New("email_boundary_invalid")
	}
	text, err := w.CreatePart(textproto.MIMEHeader{"Content-Type": {"text/plain; charset=utf-8"}, "Content-Transfer-Encoding": {"base64"}})
	if err != nil {
		return "", err
	}
	writeBase64(text, []byte(e.Content))
	for _, asset := range assets {
		if strings.ContainsAny(asset.Attachment.Name, "\r\n\x00") {
			return "", errors.New("email_attachment_invalid")
		}
		part, err := w.CreatePart(textproto.MIMEHeader{"Content-Type": {mime.FormatMediaType(asset.Attachment.MIME, map[string]string{"name": asset.Attachment.Name})}, "Content-Disposition": {mime.FormatMediaType("attachment", map[string]string{"filename": asset.Attachment.Name})}, "Content-Transfer-Encoding": {"base64"}})
		if err != nil {
			return "", err
		}
		writeBase64(part, asset.Data)
	}
	if err = w.Close(); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b.Bytes()), nil
}
func writeBase64(w io.Writer, b []byte) {
	s := base64.StdEncoding.EncodeToString(b)
	for len(s) > 76 {
		_, _ = io.WriteString(w, s[:76]+"\r\n")
		s = s[76:]
	}
	_, _ = io.WriteString(w, s+"\r\n")
}

type EventParameters struct {
	Start          string   `json:"start"`
	End            string   `json:"end"`
	Timezone       string   `json:"timezone"`
	Attendees      []string `json:"attendees,omitempty"`
	Location       string   `json:"location,omitempty"`
	AllowConflicts bool     `json:"allow_conflicts,omitempty"`
}

func (a *Google) event(id string, e workspace.Effect, assets []workspace.Asset) (map[string]any, EventParameters, error) {
	var p EventParameters
	d := json.NewDecoder(bytes.NewReader(e.Parameters))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil {
		return nil, p, errors.New("calendar_parameters_invalid")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, p, errors.New("calendar_parameters_invalid")
	}
	start, se := time.Parse(time.RFC3339, p.Start)
	end, ee := time.Parse(time.RFC3339, p.End)
	_, ze := time.LoadLocation(p.Timezone)
	if !a.calendar(e.Destination) || se != nil || ee != nil || ze != nil || !end.After(start) || end.Sub(start) > 31*24*time.Hour || len(p.Attendees) > 100 || len(assets) > 0 {
		return nil, p, errors.New("calendar_time_scope_or_attachment_invalid")
	}
	attendees := []map[string]string{}
	for _, address := range p.Attendees {
		email, err := mail.ParseAddress(address)
		if err != nil {
			return nil, p, errors.New("calendar_attendee_invalid")
		}
		attendees = append(attendees, map[string]string{"email": email.Address})
	}
	body := map[string]any{"id": a.operation(id), "summary": e.Title, "description": e.Content, "start": map[string]string{"dateTime": start.Format(time.RFC3339), "timeZone": p.Timezone}, "end": map[string]string{"dateTime": end.Format(time.RFC3339), "timeZone": p.Timezone}, "attendees": attendees, "location": p.Location, "extendedProperties": map[string]any{"private": map[string]string{"bridgeIntent": a.operation(id), "bridgeContentHash": e.ContentHash}}}
	return body, p, nil
}

// Validate is invoked before the human approval receipt is committed. Conflict
// override is itself part of the hashed draft; approval cannot silently add it.
func (a *Google) Validate(ctx context.Context, e workspace.Effect) error {
	if e.Adapter != a.Kind() || e.Account != a.Account() || !a.allowed[e.Action] {
		return errors.New("original_account_action_not_granted")
	}
	if e.Adapter == "email" {
		_, err := a.email("validation-only", e, nil)
		if err != nil {
			return err
		}
		var p map[string]any
		if len(e.Parameters) > 0 && string(e.Parameters) != "null" {
			if json.Unmarshal(e.Parameters, &p) != nil || len(p) != 0 {
				return errors.New("email_parameters_invalid")
			}
		}
		return nil
	}
	_, p, err := a.event("validation-only", e, nil)
	if err != nil {
		return err
	}
	r, err := a.Read(ctx, "list_events", map[string]any{"calendar_id": e.Destination, "time_min": p.Start, "time_max": p.End})
	if err != nil {
		return err
	}
	data, _ := r.(map[string]any)
	items, _ := data["items"].([]any)
	if len(items) > 0 && !p.AllowConflicts {
		return errors.New("calendar_conflict_requires_revised_explicit_override")
	}
	return nil
}
func (a *Google) Dispatch(ctx context.Context, id string, e workspace.Effect, assets []workspace.Asset) (workspace.ToolReceipt, error) {
	if e.Adapter != a.Kind() || e.Account != a.Account() || !a.allowed[e.Action] {
		return workspace.ToolReceipt{State: "held", Reason: "original_account_action_not_granted"}, nil
	}
	var endpoint string
	var body any
	if e.Adapter == "email" {
		raw, err := a.email(id, e, assets)
		if err != nil {
			return workspace.ToolReceipt{State: "failed", Reason: err.Error()}, nil
		}
		endpoint = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
		body = map[string]string{"raw": raw}
	} else {
		event, _, err := a.event(id, e, assets)
		if err != nil {
			return workspace.ToolReceipt{State: "failed", Reason: err.Error()}, nil
		}
		endpoint = "https://www.googleapis.com/calendar/v3/calendars/" + url.PathEscape(e.Destination) + "/events?sendUpdates=all"
		body = event
	}
	// Exactly one request here. The HTTP client has no POST retry/idempotency
	// header; a network error or ambiguous status is recovered only with Lookup.
	res, err := a.call(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return workspace.ToolReceipt{State: "unknown", Reason: "provider_effect_reply_unknown"}, nil
	}
	if res.Status >= 200 && res.Status < 300 {
		providerID, _ := res.Body["id"].(string)
		if providerID == "" {
			return workspace.ToolReceipt{State: "unknown", Reason: "provider_effect_id_missing"}, nil
		}
		return workspace.ToolReceipt{State: "acknowledged", ProviderID: providerID, ProviderState: "accepted_by_google_not_read_or_human_delivery"}, nil
	}
	if res.Status == 400 || res.Status == 401 || res.Status == 403 || res.Status == 404 || res.Status == 413 {
		return workspace.ToolReceipt{State: "failed", Reason: "provider_rejected_status_" + strconv.Itoa(res.Status)}, nil
	}
	return workspace.ToolReceipt{State: "unknown", Reason: "provider_effect_status_" + strconv.Itoa(res.Status)}, nil
}
func (a *Google) Lookup(ctx context.Context, id string, e workspace.Effect) (workspace.ToolReceipt, error) {
	if e.Adapter != a.Kind() || e.Account != a.Account() {
		return workspace.ToolReceipt{State: "held", Reason: "original_account_changed"}, nil
	}
	if e.Adapter == "calendar" {
		if !a.calendar(e.Destination) {
			return workspace.ToolReceipt{State: "held", Reason: "original_calendar_scope_changed"}, nil
		}
		res, err := a.call(ctx, http.MethodGet, "https://www.googleapis.com/calendar/v3/calendars/"+url.PathEscape(e.Destination)+"/events/"+a.operation(id), nil)
		if err != nil {
			return workspace.ToolReceipt{}, err
		}
		ext, _ := res.Body["extendedProperties"].(map[string]any)
		private, _ := ext["private"].(map[string]any)
		providerID, _ := res.Body["id"].(string)
		if res.Status == 200 && providerID == a.operation(id) && private["bridgeContentHash"] == e.ContentHash && private["bridgeIntent"] == a.operation(id) {
			return workspace.ToolReceipt{State: "acknowledged", ProviderID: providerID, ProviderState: "original_event_observed"}, nil
		}
		return workspace.ToolReceipt{State: "unknown", Reason: "original_event_not_proven_no_replay"}, nil
	}
	q := url.Values{"q": {"in:sent rfc822msgid:" + a.messageID(id)}, "maxResults": {"10"}}
	res, err := a.call(ctx, http.MethodGet, "https://gmail.googleapis.com/gmail/v1/users/me/messages?"+q.Encode(), nil)
	if err != nil {
		return workspace.ToolReceipt{}, err
	}
	rows, _ := res.Body["messages"].([]any)
	if res.Status != 200 || len(rows) != 1 {
		return workspace.ToolReceipt{State: "unknown", Reason: "original_sent_message_not_proven_no_replay"}, nil
	}
	row, _ := rows[0].(map[string]any)
	providerID, _ := row["id"].(string)
	if !safeID(providerID) {
		return workspace.ToolReceipt{State: "unknown", Reason: "provider_message_id_invalid"}, nil
	}
	meta, err := a.call(ctx, http.MethodGet, "https://gmail.googleapis.com/gmail/v1/users/me/messages/"+url.PathEscape(providerID)+"?format=metadata&metadataHeaders=Message-ID&metadataHeaders=X-Bridge-Content-Hash", nil)
	if err != nil {
		return workspace.ToolReceipt{}, err
	}
	payload, _ := meta.Body["payload"].(map[string]any)
	headers, _ := payload["headers"].([]any)
	foundID, foundHash := false, false
	for _, entry := range headers {
		h, _ := entry.(map[string]any)
		name, _ := h["name"].(string)
		if strings.EqualFold(name, "Message-ID") && h["value"] == a.messageID(id) {
			foundID = true
		}
		if strings.EqualFold(name, "X-Bridge-Content-Hash") && h["value"] == e.ContentHash {
			foundHash = true
		}
	}
	if meta.Status == 200 && foundID && foundHash {
		return workspace.ToolReceipt{State: "acknowledged", ProviderID: providerID, ProviderState: "original_sent_message_observed_not_read"}, nil
	}
	return workspace.ToolReceipt{State: "unknown", Reason: "original_message_content_binding_unproven"}, nil
}
