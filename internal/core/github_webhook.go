package core

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"everything-go/internal/eventinbox"
)

const maxGitHubWebhookBytes = 2 * 1024 * 1024

type githubIdentity struct {
	Login string `json:"login"`
}

type githubRepository struct {
	FullName string `json:"full_name"`
}

type githubPingPayload struct {
	Zen        string           `json:"zen"`
	HookID     int64            `json:"hook_id"`
	Repository githubRepository `json:"repository"`
	Sender     githubIdentity   `json:"sender"`
}

type githubWorkflowRun struct {
	ID         int64          `json:"id"`
	Name       string         `json:"name"`
	RunNumber  int64          `json:"run_number"`
	RunAttempt int64          `json:"run_attempt"`
	Event      string         `json:"event"`
	Status     string         `json:"status"`
	Conclusion string         `json:"conclusion"`
	HeadBranch string         `json:"head_branch"`
	HeadSHA    string         `json:"head_sha"`
	HTMLURL    string         `json:"html_url"`
	Actor      githubIdentity `json:"actor"`
	UpdatedAt  string         `json:"updated_at"`
}

type githubWorkflowRunPayload struct {
	Action      string            `json:"action"`
	WorkflowRun githubWorkflowRun `json:"workflow_run"`
	Repository  githubRepository  `json:"repository"`
	Sender      githubIdentity    `json:"sender"`
}

// ServeGitHubWebhook is a provider adapter. It verifies the exact raw request
// body using GitHub's HMAC-SHA256 signature, then maps supported deliveries to
// the source-neutral Event Inbox envelope. GitHub payloads never enter the
// canonical store or app wire contract directly.
func (h *Hub) ServeGitHubWebhook(w http.ResponseWriter, r *http.Request) {
	if h.events == nil {
		http.Error(w, "event inbox unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	secret := strings.TrimSpace(os.Getenv("GITHUB_WEBHOOK_SECRET"))
	if secret == "" {
		http.Error(w, "github webhook unavailable", http.StatusServiceUnavailable)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxGitHubWebhookBytes))
	if err != nil {
		http.Error(w, "invalid webhook body", http.StatusBadRequest)
		return
	}
	if !validGitHubSignature([]byte(secret), body, r.Header.Get("X-Hub-Signature-256")) {
		http.Error(w, "invalid github signature", http.StatusUnauthorized)
		return
	}
	deliveryID := strings.TrimSpace(r.Header.Get("X-GitHub-Delivery"))
	if deliveryID == "" || len(deliveryID) > 128 {
		http.Error(w, "missing or invalid X-GitHub-Delivery", http.StatusBadRequest)
		return
	}

	input, ignored, err := normalizeGitHubDelivery(r.Header.Get("X-GitHub-Event"), deliveryID, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if ignored {
		writeWebhookResponse(w, http.StatusAccepted, map[string]any{"status": "ignored"})
		return
	}
	event, deduped, err := h.events.Insert(r.Context(), input)
	if err != nil {
		http.Error(w, "store github event: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := h.publishExternalEvent(event, deduped); err != nil {
		http.Error(w, "encode github event: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeWebhookResponse(w, http.StatusAccepted, map[string]any{
		"status": "accepted", "event_id": event.ID, "deduplicated": deduped,
	})
}

func validGitHubSignature(secret, body []byte, raw string) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(raw, prefix) {
		return false
	}
	provided, err := hex.DecodeString(strings.TrimPrefix(raw, prefix))
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(body)
	return hmac.Equal(provided, mac.Sum(nil))
}

func normalizeGitHubDelivery(eventType, deliveryID string, body []byte) (eventinbox.Input, bool, error) {
	switch strings.TrimSpace(eventType) {
	case "ping":
		var payload githubPingPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			return eventinbox.Input{}, false, errors.New("invalid github ping payload")
		}
		repo := strings.TrimSpace(payload.Repository.FullName)
		if repo == "" {
			repo = "GitHub repository"
		}
		data, _ := json.Marshal(map[string]any{
			"delivery_id": deliveryID, "hook_id": payload.HookID,
			"repository": repo, "sender": payload.Sender.Login,
		})
		return eventinbox.Input{
			Source: "github", EventKey: "delivery:" + deliveryID, Kind: "webhook.ping",
			Severity: "success", Title: "GitHub webhook connected",
			Body: repo + " can now deliver events to Bridge.", Data: data,
		}, false, nil

	case "workflow_run":
		var payload githubWorkflowRunPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			return eventinbox.Input{}, false, errors.New("invalid github workflow_run payload")
		}
		if payload.Action != "completed" {
			return eventinbox.Input{}, true, nil
		}
		run := payload.WorkflowRun
		repo := strings.TrimSpace(payload.Repository.FullName)
		if repo == "" || run.ID <= 0 || strings.TrimSpace(run.Name) == "" {
			return eventinbox.Input{}, false, errors.New("github workflow_run is missing repository, id, or name")
		}
		severity, result := githubConclusionPresentation(run.Conclusion)
		occurredAt := time.Now().UnixMilli()
		if parsed, err := time.Parse(time.RFC3339, run.UpdatedAt); err == nil {
			occurredAt = parsed.UnixMilli()
		}
		sha := strings.TrimSpace(run.HeadSHA)
		if len(sha) > 12 {
			sha = sha[:12]
		}
		bodyText := fmt.Sprintf("%s on %s", result, strings.TrimSpace(run.HeadBranch))
		if run.Actor.Login != "" {
			bodyText += " · " + run.Actor.Login
		}
		if sha != "" {
			bodyText += " · " + sha
		}
		data, _ := json.Marshal(map[string]any{
			"delivery_id": deliveryID, "repository": repo, "workflow_run_id": run.ID,
			"workflow": run.Name, "run_number": run.RunNumber, "run_attempt": run.RunAttempt,
			"trigger": run.Event, "status": run.Status, "conclusion": run.Conclusion,
			"branch": run.HeadBranch, "sha": run.HeadSHA, "actor": run.Actor.Login,
		})
		return eventinbox.Input{
			Source: "github.actions", EventKey: githubWorkflowEventKey(repo, run),
			Kind: "workflow_run.completed", Severity: severity,
			Title: clipUTF8Bytes(fmt.Sprintf("%s · %s #%d %s", repo, run.Name, run.RunNumber, result), 240),
			Body:  bodyText, URL: run.HTMLURL, Data: data, OccurredAt: occurredAt,
		}, false, nil

	default:
		return eventinbox.Input{}, true, nil
	}
}

func githubConclusionPresentation(conclusion string) (severity, result string) {
	switch strings.ToLower(strings.TrimSpace(conclusion)) {
	case "success":
		return "success", "succeeded"
	case "neutral":
		return "info", "completed neutrally"
	case "skipped":
		return "info", "skipped"
	case "cancelled":
		return "warning", "cancelled"
	case "stale":
		return "warning", "became stale"
	case "failure":
		return "error", "failed"
	case "timed_out":
		return "error", "timed out"
	case "action_required":
		return "error", "needs action"
	case "startup_failure":
		return "error", "failed to start"
	default:
		return "warning", "completed with an unknown result"
	}
}

func githubWorkflowEventKey(repo string, run githubWorkflowRun) string {
	semantic := fmt.Sprintf("%s\x00%d\x00%d\x00completed", repo, run.ID, run.RunAttempt)
	digest := sha256.Sum256([]byte(semantic))
	return "workflow_run:" + hex.EncodeToString(digest[:16])
}

func clipUTF8Bytes(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	if limit <= len("…") {
		return ""
	}
	end := limit - len("…")
	for end > 0 && (value[end]&0xc0) == 0x80 {
		end--
	}
	return strings.TrimSpace(value[:end]) + "…"
}

func writeWebhookResponse(w http.ResponseWriter, status int, payload map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
