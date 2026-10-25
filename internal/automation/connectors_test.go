package automation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"everything-go/internal/eventinbox"
)

type staticSecrets map[string]string

func (s staticSecrets) Resolve(reference string) (string, error) {
	if value := s[reference]; value != "" {
		return value, nil
	}
	return "", errors.New("missing")
}

type fakeConnector struct{ batch PollBatch }

func (fakeConnector) Provider() string { return "fake" }
func (f fakeConnector) Poll(context.Context, Account, PollState, SecretResolver) (PollBatch, error) {
	return f.batch, nil
}
func (fakeConnector) Execute(context.Context, Account, Proposal, SecretResolver) (ActionResult, error) {
	return ActionResult{}, errors.New("unsupported")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return fn(request) }

func TestManagerAdvancesCursorOnlyAfterEventsCommit(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	if _, _, err := store.UpsertAccount(ctx, Account{ID: "a1", Provider: "fake", ExternalAccountID: "x",
		DisplayName: "Fake", CredentialRef: "env:FAKE", Enabled: true, PollEnabled: true, PollIntervalSeconds: 60}, 0); err != nil {
		t.Fatal(err)
	}
	manager := NewManager(store, staticSecrets{}, fakeConnector{batch: PollBatch{
		Events: []eventinbox.Input{{Source: "fake", EventKey: "1", Kind: "created", Title: "One"}},
		Cursor: json.RawMessage(`{"after":"1"}`),
	}})
	manager.now = store.now
	processed, err := manager.PollOnce(ctx, "worker", func(context.Context, eventinbox.Input) error { return errors.New("db down") })
	if !processed || err == nil {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	snapshot, err := store.Snapshot(ctx)
	if err != nil || len(snapshot.PollState) != 1 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	if string(snapshot.PollState[0].Cursor) != "{}" || snapshot.PollState[0].LastErrorCode != "event_commit_failed" {
		t.Fatalf("cursor advanced after failed commit: %+v", snapshot.PollState[0])
	}
}

func TestMetaFacebookPollNormalizesPostsAndCommentsWithoutCredentialData(t *testing.T) {
	t.Setenv("META_GRAPH_API_VERSION", "v21.0")
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Hostname() != "graph.facebook.com" || request.URL.Query().Get("access_token") != "secret-token" {
			t.Fatalf("request URL=%s", request.URL.Redacted())
		}
		body := `{"data":[{"id":"post1","message":"Post","created_time":"2026-08-29T08:00:00+0000","updated_time":"2026-08-29T08:01:00+0000","permalink_url":"https://facebook.example/post1","comments":{"data":[{"id":"comment1","message":"Hello","created_time":"2026-08-29T08:02:00+0000","from":{"id":"user1","name":"Reader"}}]}}]}`
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	connector := MetaFacebookConnector{Client: client}
	baseline, err := connector.Poll(context.Background(), Account{ID: "judge", ExternalAccountID: "page1", DisplayName: "Judge", CredentialRef: "env:TOKEN"},
		PollState{Cursor: json.RawMessage(`{}`)}, staticSecrets{"env:TOKEN": "secret-token"})
	if err != nil || len(baseline.Events) != 0 || len(baseline.Cursor) == 0 {
		t.Fatalf("baseline=%+v err=%v", baseline, err)
	}
	since := time.Date(2026, 8, 29, 7, 59, 0, 0, time.UTC).UnixMilli()
	batch, err := connector.Poll(context.Background(), Account{ID: "judge", ExternalAccountID: "page1", DisplayName: "Judge", CredentialRef: "env:TOKEN"},
		PollState{Cursor: json.RawMessage(`{"since_ms":` + strconv.FormatInt(since, 10) + `}`)}, staticSecrets{"env:TOKEN": "secret-token"})
	if err != nil || len(batch.Events) != 2 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	for _, event := range batch.Events {
		encoded, _ := json.Marshal(event)
		if strings.Contains(string(encoded), "secret-token") || event.Source != "meta.facebook.judge" {
			t.Fatalf("unsafe event=%s", encoded)
		}
	}
	if batch.Events[1].Kind != "comment.created" || batch.Events[1].EventKey != "comment:comment1" || !strings.Contains(string(batch.Events[1].Data), "Reader") {
		t.Fatalf("comment=%+v", batch.Events[1])
	}
	var cursor metaCursor
	if json.Unmarshal(batch.Cursor, &cursor) != nil || cursor.SinceMS != time.Date(2026, 8, 29, 8, 2, 0, 0, time.UTC).UnixMilli() {
		t.Fatalf("cursor=%s", batch.Cursor)
	}
}

func TestMetaFacebookExecutesOnlyTypedApprovedShape(t *testing.T) {
	t.Setenv("META_GRAPH_API_VERSION", "v21.0")
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || !strings.HasSuffix(request.URL.Path, "/comment1/comments") {
			t.Fatalf("request=%s %s", request.Method, request.URL.Path)
		}
		body, _ := io.ReadAll(request.Body)
		if !strings.Contains(string(body), "message=Approved+reply") || !strings.Contains(string(body), "access_token=secret-token") {
			t.Fatalf("form=%s", body)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"id":"reply1"}`)), Header: make(http.Header)}, nil
	})}
	result, err := (MetaFacebookConnector{Client: client}).Execute(context.Background(),
		Account{ExternalAccountID: "page", CredentialRef: "env:TOKEN"},
		Proposal{ActionType: "facebook.comment.reply", TargetID: "comment1", Payload: json.RawMessage(`{"message":"Approved reply"}`)},
		staticSecrets{"env:TOKEN": "secret-token"})
	if err != nil || result.ProviderResultID != "reply1" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestMetaThreadsPollsRepliesAndPublishesOnlyApprovedTextShape(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Query().Get("access_token") != "threads-secret" {
			t.Fatalf("missing Threads credential")
		}
		var body string
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/me/threads":
			body = `{"data":[{"id":"thread1","text":"Root","timestamp":"2026-08-29T08:00:00+0000","permalink":"https://threads.net/t/1","has_replies":true}]}`
		case request.Method == http.MethodGet && request.URL.Path == "/thread1/conversation":
			body = `{"data":[{"id":"reply1","text":"Reader reply","timestamp":"2026-08-29T08:01:00+0000","username":"reader","is_reply":true,"is_reply_owned_by_me":false,"root_post":{"id":"thread1"},"replied_to":{"id":"thread1"}}]}`
		case request.Method == http.MethodPost && request.URL.Path == "/me/threads":
			if request.URL.Query().Get("auto_publish_text") != "true" || request.URL.Query().Get("text") != "Approved Threads post" {
				t.Fatalf("publish query=%v", request.URL.Query())
			}
			body = `{"id":"published1"}`
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}
	connector := MetaThreadsConnector{Client: client}
	account := Account{ID: "threads", ExternalAccountID: "user", DisplayName: "Threads", CredentialRef: "env:THREADS"}
	baseline, err := connector.Poll(context.Background(), account, PollState{}, staticSecrets{"env:THREADS": "threads-secret"})
	if err != nil || len(baseline.Events) != 0 {
		t.Fatalf("baseline=%+v err=%v", baseline, err)
	}
	since := time.Date(2026, 8, 29, 7, 59, 0, 0, time.UTC).UnixMilli()
	batch, err := connector.Poll(context.Background(), account, PollState{Cursor: json.RawMessage(`{"since_ms":` + strconv.FormatInt(since, 10) + `}`)}, staticSecrets{"env:THREADS": "threads-secret"})
	if err != nil || len(batch.Events) != 2 || batch.Events[1].Kind != "reply.created" || batch.Events[1].EventKey != "reply:reply1" {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	result, err := connector.Execute(context.Background(), account, Proposal{ActionType: "threads.post.publish",
		Payload: json.RawMessage(`{"message":"Approved Threads post"}`)}, staticSecrets{"env:THREADS": "threads-secret"})
	if err != nil || result.ProviderResultID != "published1" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
