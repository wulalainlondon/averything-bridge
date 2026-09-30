package automation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"everything-go/internal/eventinbox"
)

type MetaThreadsConnector struct{ Client *http.Client }

func (MetaThreadsConnector) Provider() string { return "meta.threads" }

type threadsListResponse struct {
	Data []struct {
		ID         string `json:"id"`
		Text       string `json:"text"`
		Timestamp  string `json:"timestamp"`
		Permalink  string `json:"permalink"`
		HasReplies bool   `json:"has_replies"`
	} `json:"data"`
	Error *struct {
		Code int `json:"code"`
	} `json:"error,omitempty"`
}

type threadsConversationResponse struct {
	Data []struct {
		ID             string `json:"id"`
		Text           string `json:"text"`
		Timestamp      string `json:"timestamp"`
		Username       string `json:"username"`
		IsReply        bool   `json:"is_reply"`
		IsReplyOwnedBy bool   `json:"is_reply_owned_by_me"`
		RootPost       struct {
			ID string `json:"id"`
		} `json:"root_post"`
		RepliedTo struct {
			ID string `json:"id"`
		} `json:"replied_to"`
	} `json:"data"`
	Error *struct {
		Code int `json:"code"`
	} `json:"error,omitempty"`
}

func (connector MetaThreadsConnector) Poll(ctx context.Context, account Account, state PollState, secrets SecretResolver) (PollBatch, error) {
	token, err := secrets.Resolve(account.CredentialRef)
	if err != nil {
		return PollBatch{}, err
	}
	client := connector.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	query := url.Values{"fields": {"id,text,timestamp,permalink,has_replies"}, "limit": {"20"}, "access_token": {token}}
	var posts threadsListResponse
	if err := threadsGET(ctx, client, "https://graph.threads.net/me/threads?"+query.Encode(), &posts); err != nil {
		return PollBatch{}, err
	}
	if posts.Error != nil {
		return PollBatch{}, fmt.Errorf("threads_http_%d", posts.Error.Code)
	}
	batch := PollBatch{}
	cursor := metaCursor{}
	_ = json.Unmarshal(state.Cursor, &cursor)
	initialBaseline := cursor.SinceMS == 0
	maxSeen := cursor.SinceMS
	for _, post := range posts.Data {
		occurred := parseMetaTime(post.Timestamp)
		maxSeen = maxInt64(maxSeen, occurred)
		data, _ := json.Marshal(map[string]any{"account_id": account.ExternalAccountID, "thread_id": post.ID, "has_replies": post.HasReplies})
		if !initialBaseline && occurred > cursor.SinceMS {
			batch.Events = append(batch.Events, eventinbox.Input{Source: "meta.threads." + account.ID,
				EventKey: "post:" + post.ID + ":" + strconv.FormatInt(occurred, 10), Kind: "post.observed", Severity: "info",
				Title: account.DisplayName + " Threads post", Body: post.Text, URL: post.Permalink, Data: data, OccurredAt: occurred})
		}
		if initialBaseline || !post.HasReplies {
			continue
		}
		replyQuery := url.Values{"fields": {"id,text,timestamp,username,is_reply,is_reply_owned_by_me,root_post,replied_to"},
			"reverse": {"false"}, "access_token": {token}}
		var conversation threadsConversationResponse
		if err := threadsGET(ctx, client, "https://graph.threads.net/"+url.PathEscape(post.ID)+"/conversation?"+replyQuery.Encode(), &conversation); err != nil {
			return PollBatch{}, err
		}
		if conversation.Error != nil {
			return PollBatch{}, fmt.Errorf("threads_http_%d", conversation.Error.Code)
		}
		for _, reply := range conversation.Data {
			if !reply.IsReply || reply.IsReplyOwnedBy || reply.ID == "" {
				continue
			}
			replyAt := parseMetaTime(reply.Timestamp)
			maxSeen = maxInt64(maxSeen, replyAt)
			replyData, _ := json.Marshal(map[string]string{"account_id": account.ExternalAccountID, "thread_id": post.ID,
				"reply_id": reply.ID, "username": reply.Username, "replied_to_id": reply.RepliedTo.ID})
			if replyAt > cursor.SinceMS {
				batch.Events = append(batch.Events, eventinbox.Input{Source: "meta.threads." + account.ID,
					EventKey: "reply:" + reply.ID, Kind: "reply.created", Severity: "info",
					Title: account.DisplayName + " received a Threads reply", Body: reply.Text,
					URL: post.Permalink, Data: replyData, OccurredAt: replyAt})
			}
		}
	}
	if maxSeen == 0 {
		maxSeen = time.Now().UnixMilli()
	}
	batch.Cursor, _ = json.Marshal(metaCursor{SinceMS: maxSeen})
	return batch, nil
}

func (connector MetaThreadsConnector) Execute(ctx context.Context, account Account, proposal Proposal, secrets SecretResolver) (ActionResult, error) {
	if proposal.ActionType != "threads.post.publish" {
		return ActionResult{}, errors.New("unsupported_threads_action")
	}
	var payload struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(proposal.Payload, &payload) != nil || strings.TrimSpace(payload.Message) == "" || len(payload.Message) > 5000 {
		return ActionResult{}, errors.New("invalid_message_payload")
	}
	accountToken, err := secrets.Resolve(account.CredentialRef)
	if err != nil {
		return ActionResult{}, err
	}
	query := url.Values{"text": {payload.Message}, "media_type": {"TEXT"}, "auto_publish_text": {"true"}, "access_token": {accountToken}}
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://graph.threads.net/me/threads?"+query.Encode(), nil)
	client := connector.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return ActionResult{}, errors.New("threads_network_error")
	}
	defer response.Body.Close()
	var result struct {
		ID    string `json:"id"`
		Error *struct {
			Code int `json:"code"`
		} `json:"error,omitempty"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 1024*1024)).Decode(&result) != nil {
		return ActionResult{}, errors.New("threads_invalid_response")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || result.Error != nil || result.ID == "" {
		return ActionResult{}, fmt.Errorf("threads_http_%d", response.StatusCode)
	}
	return ActionResult{ProviderResultID: result.ID}, nil
}

func threadsGET(ctx context.Context, client *http.Client, endpoint string, target any) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() != "graph.threads.net" {
		return errors.New("threads_invalid_url")
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	response, err := client.Do(request)
	if err != nil {
		return errors.New("threads_network_error")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("threads_http_%d", response.StatusCode)
	}
	if json.NewDecoder(io.LimitReader(response.Body, 4*1024*1024)).Decode(target) != nil {
		return errors.New("threads_invalid_response")
	}
	return nil
}
