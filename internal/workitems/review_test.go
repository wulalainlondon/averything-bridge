package workitems

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func seedReviewItem(t *testing.T, s *Store) WorkItem {
	t.Helper()
	item := seedItem(t, s, "review-project", "review-item")
	item = move(t, s, item, LifecycleReady, ActorUser)
	var err error
	_, item, err = s.LinkSession(context.Background(), LinkSessionInput{ID: "review-link", WorkItemID: item.ID,
		SessionID: "review-session", ExpectedVersion: item.Version, Actor: Actor{Type: ActorUser}})
	if err != nil {
		t.Fatal(err)
	}
	item = move(t, s, item, LifecycleActive, ActorAgent)
	return move(t, s, item, LifecycleReview, ActorAgent)
}

func TestReviewDecisionAcceptPersistsHumanFeedback(t *testing.T) {
	s := openTestStore(t)
	item := seedReviewItem(t, s)
	result, err := s.DecideReview(context.Background(), ReviewDecisionInput{WorkItemID: item.ID,
		ExpectedVersion: item.Version, Decision: "accept", Feedback: "Verified on Note20.",
		CommentID: "review-comment", Actor: Actor{Type: ActorMobile, DeviceID: "note20"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Item.Lifecycle != LifecycleDone || result.Item.AcceptedAt == nil || result.Run != nil ||
		result.Comment == nil || result.Comment.Body != "Verified on Note20." {
		t.Fatalf("accept result=%+v", result)
	}
	snapshot, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Comments) != 1 || snapshot.Comments[0].AuthorType != ActorMobile {
		t.Fatalf("comments=%+v", snapshot.Comments)
	}
	activity := snapshot.Activities[len(snapshot.Activities)-1]
	if activity.Kind != "review_accepted" || activity.Actor != ActorMobile {
		t.Fatalf("activity=%+v", activity)
	}
	var payload struct {
		ReviewDecision ReviewDecisionRecord `json:"review_decision"`
	}
	if err := json.Unmarshal([]byte(activity.Payload), &payload); err != nil || payload.ReviewDecision.Feedback != "Verified on Note20." {
		t.Fatalf("activity payload=%q parsed=%+v err=%v", activity.Payload, payload, err)
	}
}

func TestReviewDecisionChangesAtomicallyQueuesLinkedSession(t *testing.T) {
	s := openTestStore(t)
	item := seedReviewItem(t, s)
	result, err := s.DecideReview(context.Background(), ReviewDecisionInput{WorkItemID: item.ID,
		ExpectedVersion: item.Version, Decision: "request_changes", Feedback: "Add the missing ledger evidence.",
		CommentID: "feedback-comment", RunID: "feedback-run", RequestID: "feedback-request",
		Actor: Actor{Type: ActorMobile, DeviceID: "note20"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Item.Lifecycle != LifecycleActive || result.Item.AcceptedAt != nil || result.Run == nil ||
		result.Run.Status != "queued" || result.Run.SessionID != "review-session" ||
		!strings.Contains(result.Run.Instruction, "Add the missing ledger evidence.") {
		t.Fatalf("request changes result=%+v", result)
	}
	snapshot, err := s.Snapshot(context.Background())
	if err != nil || len(snapshot.Runs) != 1 || len(snapshot.Comments) != 1 {
		t.Fatalf("snapshot runs=%+v comments=%+v err=%v", snapshot.Runs, snapshot.Comments, err)
	}
	claimed, _, ok, err := s.ClaimNextRun(context.Background(), result.Run.AvailableAt)
	if err != nil || !ok || claimed.ID != "feedback-run" {
		t.Fatalf("durable feedback run=%+v ok=%v err=%v", claimed, ok, err)
	}
}

func TestReviewDecisionRejectsUnsafeOrIncompleteDecisions(t *testing.T) {
	s := openTestStore(t)
	item := seedReviewItem(t, s)
	_, err := s.DecideReview(context.Background(), ReviewDecisionInput{WorkItemID: item.ID,
		ExpectedVersion: item.Version, Decision: "accept", Actor: Actor{Type: ActorAgent}})
	if !errors.Is(err, ErrHumanRequired) {
		t.Fatalf("agent accept error=%v", err)
	}
	_, err = s.DecideReview(context.Background(), ReviewDecisionInput{WorkItemID: item.ID,
		ExpectedVersion: item.Version, Decision: "needs_more_info", Actor: Actor{Type: ActorMobile}})
	if err == nil || !strings.Contains(err.Error(), "feedback is required") {
		t.Fatalf("missing feedback error=%v", err)
	}
	current, _ := s.GetItem(context.Background(), item.ID)
	if current.Version != item.Version || current.Lifecycle != LifecycleReview {
		t.Fatalf("rejected decision mutated item=%+v", current)
	}
}

func TestReviewDecisionCanReopenAcceptedOutcome(t *testing.T) {
	s := openTestStore(t)
	item := seedReviewItem(t, s)
	accepted, err := s.DecideReview(context.Background(), ReviewDecisionInput{WorkItemID: item.ID,
		ExpectedVersion: item.Version, Decision: "accept", Actor: Actor{Type: ActorMobile}})
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := s.DecideReview(context.Background(), ReviewDecisionInput{WorkItemID: item.ID,
		ExpectedVersion: accepted.Item.Version, Decision: "reopen", Feedback: "The player supplied a new screenshot.",
		Actor: Actor{Type: ActorMobile}})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Item.Lifecycle != LifecycleActive || reopened.Item.AcceptedAt != nil || reopened.Run == nil {
		t.Fatalf("reopen result=%+v", reopened)
	}
}
