package automation

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"everything-go/internal/eventinbox"
)

const (
	defaultSentryBaseURL     = "https://sentry.io"
	defaultSentryStatsPeriod = "24h"
	maxSentryPages           = 3
	maxSentryIssues          = 300
)

var (
	sentrySlugPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	sentryPeriodPattern = regexp.MustCompile(`^[1-9][0-9]{0,2}[smhdw]$`)
	sentryEmailPattern  = regexp.MustCompile(`(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b`)
	sentryIPv4Pattern   = regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`)
)

// SentryConnector reconciles the recent issue index into Bridge's canonical
// Event Inbox. It intentionally exposes no write actions: Sentry changes must
// remain human-owned and cannot be approved through the generic proposal path.
type SentryConnector struct {
	Client      *http.Client
	BaseURL     string
	Environment string
	StatsPeriod string
}

func (SentryConnector) Provider() string { return "sentry" }

type sentryIssueFingerprint struct {
	Count     string `json:"count"`
	Status    string `json:"status"`
	Substatus string `json:"substatus,omitempty"`
	LastSeen  int64  `json:"last_seen_ms"`
}

type sentryCursor struct {
	Version     int                               `json:"version"`
	Initialized bool                              `json:"initialized"`
	PolledAtMS  int64                             `json:"polled_at_ms"`
	Issues      map[string]sentryIssueFingerprint `json:"issues"`
}

type sentryIssue struct {
	ID            string `json:"id"`
	ShortID       string `json:"shortId"`
	Title         string `json:"title"`
	Culprit       string `json:"culprit"`
	Count         string `json:"count"`
	Level         string `json:"level"`
	Status        string `json:"status"`
	Substatus     string `json:"substatus"`
	Platform      string `json:"platform"`
	IssueCategory string `json:"issueCategory"`
	FirstSeen     string `json:"firstSeen"`
	LastSeen      string `json:"lastSeen"`
	Permalink     string `json:"permalink"`
	WebURL        string `json:"web_url"`
	Project       struct {
		ID   string `json:"id"`
		Slug string `json:"slug"`
		Name string `json:"name"`
	} `json:"project"`
}

type sentryWebhookPayload struct {
	Action string `json:"action"`
	Data   struct {
		Issue sentryIssue `json:"issue"`
	} `json:"data"`
}

func (connector SentryConnector) Poll(ctx context.Context, account Account, state PollState, secrets SecretResolver) (PollBatch, error) {
	organization, project, err := parseSentryAccount(account.ExternalAccountID)
	if err != nil {
		return PollBatch{}, err
	}
	token, err := secrets.Resolve(account.CredentialRef)
	if err != nil {
		return PollBatch{}, err
	}
	baseURL, err := connector.baseURL()
	if err != nil {
		return PollBatch{}, err
	}
	cursor := sentryCursor{}
	if len(state.Cursor) > 0 {
		if err := json.Unmarshal(state.Cursor, &cursor); err != nil {
			return PollBatch{}, errors.New("sentry_invalid_cursor")
		}
	}
	if cursor.Issues == nil {
		cursor.Issues = make(map[string]sentryIssueFingerprint)
	}

	client := connector.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	endpoint := strings.TrimRight(baseURL.String(), "/") + "/api/0/organizations/" + url.PathEscape(organization) + "/issues/"
	query := url.Values{
		"project":     {project},
		"query":       {""},
		"sort":        {"date"},
		"statsPeriod": {connector.statsPeriod()},
		"limit":       {"100"},
	}
	if environment := connector.environment(); environment != "" {
		query.Set("environment", environment)
	}
	next := endpoint + "?" + query.Encode()
	batch := PollBatch{ETag: state.ETag}
	issues := make([]sentryIssue, 0, 100)

	for page := 0; next != "" && page < maxSentryPages && len(issues) < maxSentryIssues; page++ {
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, next, nil)
		if requestErr != nil {
			return PollBatch{}, errors.New("sentry_invalid_url")
		}
		if !sameSentryOrigin(request.URL, baseURL) {
			return PollBatch{}, errors.New("sentry_invalid_paging_url")
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Accept", "application/json")
		request.Header.Set("User-Agent", "Everything-Go/Sentry-Connector")
		if page == 0 && state.ETag != "" {
			request.Header.Set("If-None-Match", state.ETag)
		}
		response, requestErr := client.Do(request)
		if requestErr != nil {
			return PollBatch{}, errors.New("sentry_network_error")
		}
		if response.StatusCode == http.StatusNotModified && page == 0 {
			response.Body.Close()
			batch.Cursor = state.Cursor
			return batch, nil
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			response.Body.Close()
			return PollBatch{}, fmt.Errorf("sentry_http_%d", response.StatusCode)
		}
		var pageIssues []sentryIssue
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, 8*1024*1024)).Decode(&pageIssues)
		response.Body.Close()
		if decodeErr != nil {
			return PollBatch{}, errors.New("sentry_invalid_response")
		}
		if page == 0 {
			batch.ETag = strings.TrimSpace(response.Header.Get("ETag"))
		}
		remaining := maxSentryIssues - len(issues)
		if len(pageIssues) > remaining {
			pageIssues = pageIssues[:remaining]
		}
		issues = append(issues, pageIssues...)
		next, err = sentryNextPage(response.Header.Get("Link"), baseURL)
		if err != nil {
			return PollBatch{}, err
		}
	}

	now := time.Now().UnixMilli()
	nextFingerprints := make(map[string]sentryIssueFingerprint, len(issues))
	for _, issue := range issues {
		if strings.TrimSpace(issue.ID) == "" {
			continue
		}
		fingerprint := issue.sentryFingerprint()
		nextFingerprints[issue.ID] = fingerprint
		if !cursor.Initialized {
			continue
		}
		previous, known := cursor.Issues[issue.ID]
		kind := ""
		switch {
		case !known && parseSentryTime(issue.FirstSeen) > cursor.PolledAtMS:
			kind = "issue.created"
		case !known:
			kind = "issue.observed"
		case previous.Status == "resolved" && fingerprint.Status == "unresolved":
			kind = "issue.regressed"
		case previous.Status != fingerprint.Status || previous.Substatus != fingerprint.Substatus:
			kind = "issue.status_changed"
		case previous.Count != fingerprint.Count || previous.LastSeen != fingerprint.LastSeen:
			kind = "issue.updated"
		}
		if kind == "" {
			continue
		}
		batch.Events = append(batch.Events, sentryEvent(account, organization, project, issue, fingerprint, kind, now))
	}
	sort.SliceStable(batch.Events, func(i, j int) bool {
		if batch.Events[i].OccurredAt == batch.Events[j].OccurredAt {
			return batch.Events[i].EventKey < batch.Events[j].EventKey
		}
		return batch.Events[i].OccurredAt < batch.Events[j].OccurredAt
	})
	cursor.Version, cursor.Initialized, cursor.PolledAtMS, cursor.Issues = 1, true, now, nextFingerprints
	batch.Cursor, _ = json.Marshal(cursor)
	return batch, nil
}

func (SentryConnector) Execute(context.Context, Account, Proposal, SecretResolver) (ActionResult, error) {
	return ActionResult{}, errors.New("unsupported_sentry_action")
}

// ValidSentryWebhookSignature verifies the raw request body exactly as Sentry's
// Integration Platform specifies. The signature is a lowercase hex HMAC-SHA256
// generated with the custom integration Client Secret.
func ValidSentryWebhookSignature(body []byte, secret, signature string) bool {
	provided, err := hex.DecodeString(strings.TrimSpace(signature))
	if err != nil || len(provided) != sha256.Size || strings.TrimSpace(secret) == "" {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return hmac.Equal(provided, mac.Sum(nil))
}

// NormalizeSentryWebhook maps supported issue lifecycle callbacks onto the
// same event keys used by polling, so webhook delivery and reconciliation are
// idempotent across retries and Bridge restarts.
func NormalizeSentryWebhook(account Account, resource string, body []byte, now int64) (eventinbox.Input, bool, error) {
	if strings.ToLower(strings.TrimSpace(resource)) != "issue" {
		return eventinbox.Input{}, true, nil
	}
	organization, project, err := parseSentryAccount(account.ExternalAccountID)
	if err != nil {
		return eventinbox.Input{}, false, err
	}
	var payload sentryWebhookPayload
	if json.Unmarshal(body, &payload) != nil || strings.TrimSpace(payload.Data.Issue.ID) == "" {
		return eventinbox.Input{}, false, errors.New("invalid Sentry webhook payload")
	}
	issueProject := strings.TrimSpace(payload.Data.Issue.Project.Slug)
	if issueProject != "" && issueProject != project {
		return eventinbox.Input{}, true, nil
	}
	kind := ""
	switch strings.ToLower(strings.TrimSpace(payload.Action)) {
	case "created":
		kind = "issue.created"
	case "unresolved":
		if strings.EqualFold(payload.Data.Issue.Substatus, "regressed") {
			kind = "issue.regressed"
		} else {
			kind = "issue.status_changed"
		}
	case "resolved", "archived", "ignored":
		kind = "issue.status_changed"
	default:
		return eventinbox.Input{}, true, nil
	}
	fingerprint := payload.Data.Issue.sentryFingerprint()
	return sentryEvent(account, organization, project, payload.Data.Issue, fingerprint, kind, now), false, nil
}

func (connector SentryConnector) baseURL() (*url.URL, error) {
	value := strings.TrimSpace(connector.BaseURL)
	if value == "" {
		value = strings.TrimSpace(os.Getenv("SENTRY_BASE_URL"))
	}
	if value == "" {
		value = defaultSentryBaseURL
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("sentry_invalid_base_url")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return parsed, nil
}

func (connector SentryConnector) environment() string {
	if value := strings.TrimSpace(connector.Environment); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv("SENTRY_ENVIRONMENT"))
}

func (connector SentryConnector) statsPeriod() string {
	value := strings.TrimSpace(connector.StatsPeriod)
	if value == "" {
		value = strings.TrimSpace(os.Getenv("SENTRY_STATS_PERIOD"))
	}
	if !sentryPeriodPattern.MatchString(value) {
		return defaultSentryStatsPeriod
	}
	return value
}

func parseSentryAccount(value string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 2 || !sentrySlugPattern.MatchString(parts[0]) || !sentrySlugPattern.MatchString(parts[1]) {
		return "", "", errors.New("sentry_account_must_be_organization_project")
	}
	return parts[0], parts[1], nil
}

func sameSentryOrigin(candidate, base *url.URL) bool {
	return candidate != nil && candidate.Scheme == base.Scheme && strings.EqualFold(candidate.Host, base.Host) && candidate.User == nil
}

func sentryNextPage(link string, base *url.URL) (string, error) {
	for _, part := range strings.Split(link, ",") {
		if !strings.Contains(part, `rel="next"`) || !strings.Contains(part, `results="true"`) {
			continue
		}
		start, end := strings.Index(part, "<"), strings.Index(part, ">")
		if start < 0 || end <= start+1 {
			return "", errors.New("sentry_invalid_paging_url")
		}
		next, err := url.Parse(strings.TrimSpace(part[start+1 : end]))
		if err != nil || !sameSentryOrigin(next, base) {
			return "", errors.New("sentry_invalid_paging_url")
		}
		return next.String(), nil
	}
	return "", nil
}

func (issue sentryIssue) sentryFingerprint() sentryIssueFingerprint {
	return sentryIssueFingerprint{
		Count: strings.TrimSpace(issue.Count), Status: strings.ToLower(strings.TrimSpace(issue.Status)),
		Substatus: strings.ToLower(strings.TrimSpace(issue.Substatus)), LastSeen: parseSentryTime(issue.LastSeen),
	}
}

func sentryEvent(account Account, organization, project string, issue sentryIssue, fingerprint sentryIssueFingerprint, kind string, now int64) eventinbox.Input {
	projectSlug := strings.TrimSpace(issue.Project.Slug)
	if projectSlug == "" {
		projectSlug = project
	}
	occurredAt := fingerprint.LastSeen
	if kind == "issue.status_changed" || kind == "issue.regressed" || occurredAt == 0 {
		occurredAt = now
	}
	data, _ := json.Marshal(map[string]any{
		"organization": organization, "project": projectSlug, "issue_id": issue.ID,
		"short_id": strings.TrimSpace(issue.ShortID), "level": strings.ToLower(strings.TrimSpace(issue.Level)),
		"status": fingerprint.Status, "substatus": fingerprint.Substatus, "count": fingerprint.Count,
		"first_seen": parseSentryTime(issue.FirstSeen), "last_seen": fingerprint.LastSeen,
		"platform": strings.TrimSpace(issue.Platform), "issue_category": strings.TrimSpace(issue.IssueCategory),
	})
	digest := sha256.Sum256([]byte(strings.Join([]string{issue.ID, fingerprint.Count, fingerprint.Status, fingerprint.Substatus, strconv.FormatInt(fingerprint.LastSeen, 10)}, "\x00")))
	title := sanitizeSentryText(issue.Title, 500)
	if title == "" {
		identifier := strings.TrimSpace(issue.ShortID)
		if identifier == "" {
			identifier = issue.ID
		}
		title = "Sentry issue " + identifier
	}
	body := sanitizeSentryText(issue.Culprit, 1000)
	permalink := issue.Permalink
	if strings.TrimSpace(permalink) == "" {
		permalink = issue.WebURL
	}
	return eventinbox.Input{
		Source: "sentry." + account.ID, EventKey: "issue:" + issue.ID + ":" + hex.EncodeToString(digest[:8]),
		Kind: kind, Severity: sentrySeverity(issue.Level), Title: title, Body: body,
		URL: safeSentryURL(permalink), Data: data, OccurredAt: occurredAt,
	}
}

func parseSentryTime(value string) int64 {
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(value))
	if err != nil {
		return 0
	}
	return parsed.UnixMilli()
}

func sentrySeverity(level string) string {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "fatal", "error":
		return "error"
	case "warning", "warn":
		return "warning"
	default:
		return "info"
	}
}

func sanitizeSentryText(value string, limit int) string {
	value = sentryEmailPattern.ReplaceAllString(strings.TrimSpace(value), "[redacted-email]")
	value = sentryIPv4Pattern.ReplaceAllString(value, "[redacted-ip]")
	for len(value) > limit {
		_, size := utf8.DecodeLastRuneInString(value)
		if size <= 0 {
			return ""
		}
		value = value[:len(value)-size]
	}
	return value
}

func safeSentryURL(value string) string {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" || parsed.User != nil {
		return ""
	}
	return parsed.String()
}
