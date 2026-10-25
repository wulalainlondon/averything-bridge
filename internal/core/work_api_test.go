package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"everything-go/internal/workitems"
)

func TestWorkAPILoopbackContextAndHumanOnlyDone(t *testing.T) {
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	ctx := context.Background()
	if _, err := service.CreateProject(ctx, workitems.CreateProjectInput{ID: "p1", Name: "Bridge", Context: "Signed release only"}); err != nil {
		t.Fatal(err)
	}
	item, err := service.CreateItem(ctx, workitems.CreateItemInput{ID: "i1", ProjectID: "p1", Title: "Release"})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/work/v1/items/i1/context", nil)
	req.RemoteAddr = "127.0.0.1:42000"
	response := httptest.NewRecorder()
	h.ServeWorkAPI(response, req)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "Signed release only") || !strings.Contains(response.Body.String(), "Never mark this item done") {
		t.Fatalf("context response status=%d body=%s", response.Code, response.Body.String())
	}

	body := `{"action":"done","expected_version":` + formatUint(item.Version) + `}`
	req = httptest.NewRequest(http.MethodPost, "/api/work/v1/items/i1/events", strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:42000"
	response = httptest.NewRecorder()
	h.ServeWorkAPI(response, req)
	if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), "unsupported agent action") {
		t.Fatalf("done response status=%d body=%s", response.Code, response.Body.String())
	}
	current, err := service.GetItem(ctx, item.ID)
	if err != nil || current.Lifecycle == workitems.LifecycleDone {
		t.Fatalf("agent API completed item: %+v err=%v", current, err)
	}
}

func TestWorkAPIRequestReviewPushesAttentionBeforeRunCompletion(t *testing.T) {
	h, _ := newTestHub(t)
	service := attachWorkService(t, h, t.TempDir())
	ctx := context.Background()
	if _, err := service.CreateProject(ctx, workitems.CreateProjectInput{ID: "p1", Name: "Player review"}); err != nil {
		t.Fatal(err)
	}
	item, err := service.CreateItem(ctx, workitems.CreateItemInput{ID: "i1", ProjectID: "p1", Title: "Review player message"})
	if err != nil {
		t.Fatal(err)
	}
	item, err = service.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version, Lifecycle: workitems.LifecycleReady, Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	item, err = service.MoveItem(ctx, workitems.MoveItemInput{ID: item.ID, ExpectedVersion: item.Version, Lifecycle: workitems.LifecycleActive, Actor: workitems.Actor{Type: workitems.ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	type pushed struct {
		instanceID string
		itemID     string
		title      string
		revision   uint64
		kind       string
	}
	pushes := make(chan pushed, 1)
	h.workAttentionPush = func(instanceID, itemID, title string, revision uint64, kind string) {
		pushes <- pushed{instanceID: instanceID, itemID: itemID, title: title, revision: revision, kind: kind}
	}

	body := `{"action":"request_review","expected_version":` + formatUint(item.Version) + `}`
	req := httptest.NewRequest(http.MethodPost, "/api/work/v1/items/i1/events", strings.NewReader(body))
	req.RemoteAddr = "127.0.0.1:42000"
	response := httptest.NewRecorder()
	h.ServeWorkAPI(response, req)
	if response.Code != http.StatusAccepted {
		t.Fatalf("request review response status=%d body=%s", response.Code, response.Body.String())
	}

	select {
	case got := <-pushes:
		if got.instanceID != "i1" || got.itemID != item.ID || got.title != item.Title || got.kind != "review_ready" || got.revision <= item.ActivityRevision {
			t.Fatalf("work attention push=%+v item=%+v", got, item)
		}
	case <-time.After(time.Second):
		t.Fatal("request_review did not push work attention")
	}
}

func TestWorkAPIRemoteRequiresBearerToken(t *testing.T) {
	h, _ := newTestHub(t)
	attachWorkService(t, h, t.TempDir())
	req := httptest.NewRequest(http.MethodGet, "/api/work/v1/items/missing/context", nil)
	req.RemoteAddr = "100.64.0.2:42000"
	response := httptest.NewRecorder()
	h.ServeWorkAPI(response, req)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("remote response status=%d", response.Code)
	}
}
