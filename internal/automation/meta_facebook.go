package automation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"everything-go/internal/eventinbox"
)

type MetaFacebookConnector struct {
	Client *http.Client
}

func (MetaFacebookConnector) Provider() string { return "meta.facebook" }

type metaCursor struct {
	SinceMS int64 `json:"since_ms"`
}

type metaFeedResponse struct {
	Data []struct {
		ID           string `json:"id"`
		Message      string `json:"message"`
		CreatedTime  string `json:"created_time"`
		UpdatedTime  string `json:"updated_time"`
		PermalinkURL string `json:"permalink_url"`
		Comments     struct {
			Data []struct {
				ID          string `json:"id"`
				Message     string `json:"message"`
				CreatedTime string `json:"created_time"`
				From        struct {
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"from"`
			} `json:"data"`
		} `json:"comments"`
	} `json:"data"`
	Paging struct {
		Next string `json:"next"`
	} `json:"paging"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    int    `json:"code"`
	} `json:"error,omitempty"`
}

func (connector MetaFacebookConnector) Poll(ctx context.Context, account Account, state PollState, secrets SecretResolver) (PollBatch, error) {
	version := strings.TrimSpace(os.Getenv("META_GRAPH_API_VERSION"))
	if version == "" || !strings.HasPrefix(version, "v") {
		return PollBatch{}, errors.New("meta_graph_api_version_required")
	}
	token, err := secrets.Resolve(account.CredentialRef)
	if err != nil {
		return PollBatch{}, err
	}
	cursor := metaCursor{}
	if len(state.Cursor) > 0 {
		_ = json.Unmarshal(state.Cursor, &cursor)
	}
	query := url.Values{
		"fields":       {"id,message,created_time,updated_time,permalink_url,comments.limit(100){id,message,created_time,from}"},
		"limit":        {"50"},
		"access_token": {token},
	}
	if cursor.SinceMS > 0 {
		query.Set("since", strconv.FormatInt(cursor.SinceMS/1000, 10))
	}
	next := "https://graph.facebook.com/" + url.PathEscape(version) + "/" + url.PathEscape(account.ExternalAccountID) + "/feed?" + query.Encode()
	client := connector.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	batch := PollBatch{}
	maxSeen := cursor.SinceMS
	initialBaseline := cursor.SinceMS == 0
	maxPages := 5
	if initialBaseline {
		maxPages = 1
	}
	for page := 0; next != "" && page < maxPages; page++ {
		parsed, parseErr := url.Parse(next)
		if parseErr != nil || parsed.Scheme != "https" || parsed.Hostname() != "graph.facebook.com" {
			return PollBatch{}, errors.New("meta_invalid_paging_url")
		}
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, next, nil)
		response, requestErr := client.Do(request)
		if requestErr != nil {
			return PollBatch{}, errors.New("meta_network_error")
		}
		var payload metaFeedResponse
		decodeErr := json.NewDecoder(io.LimitReader(response.Body, 4*1024*1024)).Decode(&payload)
		response.Body.Close()
		if decodeErr != nil {
			return PollBatch{}, errors.New("meta_invalid_response")
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 || payload.Error != nil {
			code := response.StatusCode
			if payload.Error != nil && payload.Error.Code != 0 {
				code = payload.Error.Code
			}
			return PollBatch{}, fmt.Errorf("meta_http_%d", code)
		}
		for _, post := range payload.Data {
			created := parseMetaTime(post.CreatedTime)
			updated := parseMetaTime(post.UpdatedTime)
			if updated == 0 {
				updated = created
			}
			maxSeen = maxInt64(maxSeen, updated)
			data, _ := json.Marshal(map[string]any{"account_id": account.ExternalAccountID, "post_id": post.ID})
			if !initialBaseline && updated > cursor.SinceMS {
				batch.Events = append(batch.Events, eventinbox.Input{Source: "meta.facebook." + account.ID,
					EventKey: "post:" + post.ID + ":" + strconv.FormatInt(updated, 10), Kind: "post.updated", Severity: "info",
					Title: account.DisplayName + " post updated", Body: post.Message, URL: post.PermalinkURL,
					Data: data, OccurredAt: updated})
			}
			for _, comment := range post.Comments.Data {
				occurred := parseMetaTime(comment.CreatedTime)
				maxSeen = maxInt64(maxSeen, occurred)
				commentData, _ := json.Marshal(map[string]any{"account_id": account.ExternalAccountID, "post_id": post.ID,
					"comment_id": comment.ID, "author_id": comment.From.ID, "author_name": comment.From.Name})
				if !initialBaseline && occurred > cursor.SinceMS {
					batch.Events = append(batch.Events, eventinbox.Input{Source: "meta.facebook." + account.ID,
						EventKey: "comment:" + comment.ID, Kind: "comment.created", Severity: "info",
						Title: account.DisplayName + " received a comment", Body: comment.Message,
						URL: post.PermalinkURL, Data: commentData, OccurredAt: occurred})
				}
			}
		}
		next = payload.Paging.Next
	}
	cursor.SinceMS = maxSeen
	if cursor.SinceMS == 0 {
		cursor.SinceMS = time.Now().UnixMilli()
	}
	batch.Cursor, _ = json.Marshal(cursor)
	return batch, nil
}

func (connector MetaFacebookConnector) Execute(ctx context.Context, account Account, proposal Proposal, secrets SecretResolver) (ActionResult, error) {
	version := strings.TrimSpace(os.Getenv("META_GRAPH_API_VERSION"))
	if version == "" || !strings.HasPrefix(version, "v") {
		return ActionResult{}, errors.New("meta_graph_api_version_required")
	}
	token, err := secrets.Resolve(account.CredentialRef)
	if err != nil {
		return ActionResult{}, err
	}
	var payload struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(proposal.Payload, &payload) != nil || strings.TrimSpace(payload.Message) == "" || len(payload.Message) > 10000 {
		return ActionResult{}, errors.New("invalid_message_payload")
	}
	target, edge := proposal.TargetID, "comments"
	if proposal.ActionType == "facebook.page_post.publish" {
		target, edge = account.ExternalAccountID, "feed"
	} else if proposal.ActionType != "facebook.comment.reply" {
		return ActionResult{}, errors.New("unsupported_meta_action")
	}
	endpoint := "https://graph.facebook.com/" + url.PathEscape(version) + "/" + url.PathEscape(target) + "/" + edge
	form := url.Values{"message": {payload.Message}, "access_token": {token}}
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := connector.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return ActionResult{}, errors.New("meta_network_error")
	}
	defer response.Body.Close()
	var result struct {
		ID    string `json:"id"`
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 1024*1024)).Decode(&result) != nil {
		return ActionResult{}, errors.New("meta_invalid_response")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || result.Error != nil || result.ID == "" {
		return ActionResult{}, fmt.Errorf("meta_http_%d", response.StatusCode)
	}
	return ActionResult{ProviderResultID: result.ID}, nil
}

func parseMetaTime(value string) int64 {
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05-0700"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UnixMilli()
		}
	}
	return 0
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
