package fcm

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func testNotifier(path, endpoint string, client *http.Client) *Notifier {
	return &Notifier{
		projectID:         "test",
		tokenSource:       oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "oauth"}),
		registryPath:      path,
		endpoint:          endpoint,
		http:              client,
		devices:           make(map[string]deviceRegistration),
		statusLastSent:    make(map[string]time.Time),
		statusLastPhase:   make(map[string]string),
		statusPending:     make(map[string]v1message),
		statusTimers:      make(map[string]*time.Timer),
		statusMinInterval: 20 * time.Millisecond,
	}
}

func TestPerDeviceRegistryPersistsWithoutOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fcm_tokens.json")
	n := testNotifier(path, "", http.DefaultClient)
	n.SetToken("note20", "token-note20", "android")
	n.SetToken("ipad", "token-ipad", "ios")

	reloaded := testNotifier(path, "", http.DefaultClient)
	reloaded.loadRegistry()
	targets := reloaded.targets()
	sort.Slice(targets, func(i, j int) bool { return targets[i].deviceID < targets[j].deviceID })
	if len(targets) != 2 || targets[0].deviceID != "ipad" || targets[0].token != "token-ipad" ||
		targets[1].deviceID != "note20" || targets[1].token != "token-note20" {
		t.Fatalf("registry did not preserve both devices: %+v", targets)
	}
	if reloaded.devices["ipad"].Platform != "ios" || reloaded.devices["note20"].Platform != "android" {
		t.Fatalf("registry did not preserve device platforms: %+v", reloaded.devices)
	}
}

func TestTokenMovesToNewDeviceWithoutDuplicatePush(t *testing.T) {
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), "", http.DefaultClient)
	n.SetToken("old-install", "same-token")
	n.SetToken("new-install", "same-token")
	targets := n.targets()
	if len(targets) != 1 || targets[0].deviceID != "new-install" {
		t.Fatalf("token was not moved to the new stable device: %+v", targets)
	}
}

func TestLegacySingleTokenIsImported(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "fcm_token.txt"), []byte("legacy-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	n := testNotifier(filepath.Join(dir, "fcm_tokens.json"), "", http.DefaultClient)
	n.loadRegistry()
	targets := n.targets()
	if len(targets) != 1 || targets[0].deviceID != legacyDeviceID || targets[0].token != "legacy-token" {
		t.Fatalf("legacy token was not imported: %+v", targets)
	}
	if _, err := os.Stat(filepath.Join(dir, "fcm_tokens.json")); err != nil {
		t.Fatalf("imported registry was not persisted: %v", err)
	}
}

func TestNotifyFansOutAndInvalidatesOnlyFailedDevice(t *testing.T) {
	var mu sync.Mutex
	var received []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var msg v1message
		_ = json.Unmarshal(body, &msg)
		mu.Lock()
		received = append(received, msg.Message.Token)
		mu.Unlock()
		if msg.Message.Token == "bad-token" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"status":"UNREGISTERED"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.SetToken("note20", "good-token")
	n.SetToken("old-phone", "bad-token")
	n.NotifyTaskDone("session", "done", "s1")

	mu.Lock()
	sort.Strings(received)
	gotReceived := append([]string(nil), received...)
	mu.Unlock()
	if strings.Join(gotReceived, ",") != "bad-token,good-token" {
		t.Fatalf("notification did not fan out once per token: %v", gotReceived)
	}
	targets := n.targets()
	if len(targets) != 1 || targets[0].deviceID != "note20" || targets[0].token != "good-token" {
		t.Fatalf("fatal response removed the wrong registrations: %+v", targets)
	}
}

func TestWorkAttentionCarriesStableDedupFacts(t *testing.T) {
	var got v1message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.SetToken("note20", "token", "android")
	n.NotifyWorkAttention("morrie", "wi1", "Review release", 42, "review_ready")
	data := got.Message.Data
	if data["type"] != "work_attention" || data["authority_instance_id"] != "morrie" ||
		data["work_item_id"] != "wi1" || data["activity_revision"] != "42" || data["kind"] != "review_ready" {
		t.Fatalf("work attention payload=%+v", data)
	}
}

func TestExternalEventCarriesCanonicalIdentity(t *testing.T) {
	var got v1message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.SetToken("note20", "token", "android")
	n.NotifyExternalEvent("wulala", "evt_1", "github", "ci_failed", "error", "CI failed", "race test")
	data := got.Message.Data
	if data["type"] != "external_event" || data["authority_instance_id"] != "wulala" ||
		data["event_id"] != "evt_1" || data["source"] != "github" || data["kind"] != "ci_failed" {
		t.Fatalf("external event payload=%+v", data)
	}
}

func TestSessionStatusIsDataOnlyRevisionedAndAndroidCollapsible(t *testing.T) {
	var got v1message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.SetToken("note20", "token", "android")
	n.NotifySessionStatusWithAuthority("wulala", "Wulala", "s1", "Release QA", "running", "running_tool", "go test ./...", 7, 100, 90, "request-7", 0,
		ReplyAction{URL: "http://100.64.0.1/reply", FallbackURL: "http://192.168.1.2/reply", Capability: "signed", ExpiresAt: 999})
	data := got.Message.Data
	if data["type"] != "session_status" || data["authority_instance_id"] != "wulala" ||
		data["session_id"] != "s1" || data["revision"] != "7" || data["phase"] != "running" ||
		data["stage"] != "running_tool" || data["active_request_id"] != "request-7" || got.Message.Notification != nil {
		t.Fatalf("session status payload=%+v notification=%+v", data, got.Message.Notification)
	}
	if data["session_key"] != "sk1:wulala:s1" || data["schema_version"] != "3" || data["authority_name"] != "Wulala" {
		t.Fatalf("session identity payload=%+v", data)
	}
	if got.Message.Android == nil || got.Message.Android.Priority != "high" ||
		got.Message.Android.CollapseKey != "session-status-wulala-s1" {
		t.Fatalf("android delivery config=%+v", got.Message.Android)
	}
	if data["reply_url"] != "http://100.64.0.1/reply" || data["reply_fallback_url"] != "http://192.168.1.2/reply" ||
		data["reply_capability"] != "signed" || data["reply_expires_at"] != "999" {
		t.Fatalf("reply action payload=%+v", data)
	}
}

func TestTaskDoneUsesNativeAndroidPayloadAndLegacyNotificationElsewhere(t *testing.T) {
	var mu sync.Mutex
	got := make(map[string]v1message)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var msg v1message
		_ = json.NewDecoder(r.Body).Decode(&msg)
		mu.Lock()
		got[msg.Message.Token] = msg
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.SetToken("note20", "android-token", "android")
	n.SetToken("ipad", "ios-token", "ios")
	n.SetToken("legacy", "legacy-token")
	n.NotifyTaskDoneWithAuthority("wulala", "Wulala", "Release QA", "Done.", "s1", "request-7",
		ReplyAction{URL: "http://100.64.0.1/reply", Capability: "signed", ExpiresAt: 999},
		TerminalProjection{Revision: 8, UpdatedAt: 1000, ActiveStartedAt: 900, CompletedAt: 1000})
	mu.Lock()
	android, androidOK := got["android-token"]
	ios, iosOK := got["ios-token"]
	legacy, legacyOK := got["legacy-token"]
	mu.Unlock()
	if !androidOK || !iosOK || !legacyOK {
		t.Fatalf("platform fanout=%v", got)
	}
	for _, msg := range []v1message{android, ios, legacy} {
		data := msg.Message.Data
		if data["revision"] != "8" || data["completed_at"] != "1000" || data["active_started_at"] != "900" || data["updated_at"] != "1000" {
			t.Fatalf("completion cannot be ordered independently: %+v", data)
		}
	}
	if android.Message.Notification != nil || android.Message.Data["reply_capability"] != "signed" || android.Message.Data["authority_name"] != "Wulala" ||
		android.Message.Data["request_id"] != "request-7" {
		t.Fatalf("Android task_done must be native data-only: %+v", android.Message)
	}
	if ios.Message.Notification == nil || legacy.Message.Notification == nil {
		t.Fatalf("non-Android clients lost visible notification ios=%+v legacy=%+v", ios.Message, legacy.Message)
	}
	if ios.Message.APNS == nil || ios.Message.APNS.Payload.APS.Category != "BRIDGE_SESSION_REPLY" ||
		ios.Message.APNS.Payload.APS.ThreadID == "" || ios.Message.APNS.Headers["apns-collapse-id"] == "" {
		t.Fatalf("iOS completion is not actionable/collapsible: %+v", ios.Message.APNS)
	}
	if legacy.Message.APNS != nil {
		t.Fatalf("unknown legacy platform must not receive iOS-only APNS config: %+v", legacy.Message.APNS)
	}
}

func TestSessionStatusCoalescesProgressButTerminalFlushes(t *testing.T) {
	var mu sync.Mutex
	var revisions []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got v1message
		_ = json.NewDecoder(r.Body).Decode(&got)
		mu.Lock()
		revisions = append(revisions, got.Message.Data["revision"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.statusMinInterval = time.Hour
	n.SetToken("note20", "token", "android")
	n.NotifySessionStatus("wulala", "s1", "QA", "running", "thinking", "", 1, 1, 1, "request-1", 0, ReplyAction{})
	n.NotifySessionStatus("wulala", "s1", "QA", "running", "running_tool", "test", 2, 2, 1, "request-1", 0, ReplyAction{})
	n.NotifySessionStatus("wulala", "s1", "QA", "completed", "completed", "", 3, 3, 1, "request-1", 0, ReplyAction{})
	mu.Lock()
	got := append([]string(nil), revisions...)
	mu.Unlock()
	if strings.Join(got, ",") != "1,3" {
		t.Fatalf("expected first progress and immediate terminal only, got %v", got)
	}
}

func TestSessionStatusPhaseChangesBypassProgressThrottle(t *testing.T) {
	var mu sync.Mutex
	var phases []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got v1message
		_ = json.NewDecoder(r.Body).Decode(&got)
		mu.Lock()
		phases = append(phases, got.Message.Data["phase"])
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.statusMinInterval = time.Hour
	n.SetToken("note20", "token", "android")
	n.NotifySessionStatus("wulala", "s1", "QA", "queued", "queued", "", 1, 1, 1, "request-1", 1, ReplyAction{})
	n.NotifySessionStatus("wulala", "s1", "QA", "running", "thinking", "", 2, 2, 1, "request-1", 0, ReplyAction{})
	mu.Lock()
	got := append([]string(nil), phases...)
	mu.Unlock()
	if strings.Join(got, ",") != "queued,running" {
		t.Fatalf("phase transition was delayed: %v", got)
	}
}

func TestSessionProgressTargetsAndroidOnlyButLifecycleStartAlsoTargetsIOS(t *testing.T) {
	var mu sync.Mutex
	var messages []v1message
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var got v1message
		_ = json.NewDecoder(r.Body).Decode(&got)
		mu.Lock()
		messages = append(messages, got)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	n := testNotifier(filepath.Join(t.TempDir(), "fcm_tokens.json"), server.URL, server.Client())
	n.SetToken("note20", "android-token", "android")
	n.SetToken("ipad", "ios-token", "ios")
	n.SetToken("legacy", "unknown-token")
	n.NotifySessionStatus("wulala", "s1", "QA", "running", "thinking", "", 1, 1, 1, "request-1", 0, ReplyAction{})
	mu.Lock()
	progress := append([]v1message(nil), messages...)
	mu.Unlock()
	if len(progress) != 1 || progress[0].Message.Token != "android-token" {
		t.Fatalf("fine-grained progress reached wrong platforms: %+v", progress)
	}

	n.statusMinInterval = 0
	n.NotifySessionStatus("wulala", "s2", "QA Start", "running", "preparing", "", 1, 2, 2, "request-2", 0,
		ReplyAction{URL: "http://100.64.0.1/reply", Capability: "signed", ExpiresAt: 999})
	mu.Lock()
	started := append([]v1message(nil), messages[len(progress):]...)
	mu.Unlock()
	if len(started) != 2 {
		t.Fatalf("lifecycle start fanout=%+v", started)
	}
	byToken := make(map[string]v1message, len(started))
	for _, message := range started {
		byToken[message.Message.Token] = message
	}
	android, androidOK := byToken["android-token"]
	ios, iosOK := byToken["ios-token"]
	if !androidOK || !iosOK || byToken["unknown-token"].Message.Token != "" {
		t.Fatalf("lifecycle start targets=%+v", byToken)
	}
	if android.Message.Notification != nil || android.Message.APNS != nil {
		t.Fatalf("Android status stopped being native data-only: %+v", android.Message)
	}
	if ios.Message.Notification == nil || ios.Message.APNS == nil ||
		ios.Message.APNS.Payload.APS.Category != "BRIDGE_SESSION_REPLY" ||
		ios.Message.APNS.Payload.APS.ThreadID == "" ||
		ios.Message.APNS.Headers["apns-collapse-id"] == "" {
		t.Fatalf("iOS status is not visible/actionable/collapsible: %+v", ios.Message)
	}
}

// TestLiveSessionStatus is an explicit, opt-in device smoke test. It is skipped
// in normal CI because it requires real Firebase credentials and a registered
// test phone. The terminal revision always runs so an interrupted test does not
// intentionally leave a permanent QA card behind.
func TestLiveSessionStatus(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_FCM_LIVE_TEST") != "1" {
		t.Skip("set EVERYTHING_GO_FCM_LIVE_TEST=1 for the real-device FCM smoke test")
	}
	serviceAccount := os.Getenv("EVERYTHING_GO_FCM_SERVICE_ACCOUNT")
	registry := os.Getenv("EVERYTHING_GO_FCM_TOKEN_REGISTRY")
	if serviceAccount == "" || registry == "" {
		t.Fatal("live test requires EVERYTHING_GO_FCM_SERVICE_ACCOUNT and EVERYTHING_GO_FCM_TOKEN_REGISTRY")
	}
	n, err := New(serviceAccount, registry)
	if err != nil {
		t.Fatal(err)
	}
	n.statusMinInterval = 10 * time.Millisecond
	authority := envOr("EVERYTHING_GO_FCM_TEST_AUTHORITY", "qa-live-status")
	authorityName := envOr("EVERYTHING_GO_FCM_TEST_AUTHORITY_NAME", "Wulala")
	sessionID := envOr("EVERYTHING_GO_FCM_TEST_SESSION", "qa-lock-screen")
	sessionName := envOr("EVERYTHING_GO_FCM_TEST_SESSION_NAME", "鎖定畫面狀態卡驗證")
	startRevision := uint64(1)
	if raw := os.Getenv("EVERYTHING_GO_FCM_TEST_REVISION"); raw != "" {
		if parsed, parseErr := strconv.ParseUint(raw, 10, 64); parseErr == nil && parsed > 0 {
			startRevision = parsed
		}
	}
	n.NotifySessionStatusWithAuthority(authority, authorityName, sessionID, sessionName,
		"running", "running_tool", "執行 Release 實機測試", startRevision, time.Now().UnixMilli(), time.Now().UnixMilli(), "release-test", 0, ReplyAction{})
	t.Cleanup(func() {
		n.NotifySessionStatusWithAuthority(authority, authorityName, sessionID, sessionName,
			"completed", "completed", "", startRevision+1, time.Now().UnixMilli(), 0, "release-test", 0, ReplyAction{})
	})
	duration := 15 * time.Second
	if raw := os.Getenv("EVERYTHING_GO_FCM_TEST_DURATION"); raw != "" {
		if parsed, parseErr := time.ParseDuration(raw); parseErr == nil && parsed > 0 {
			duration = parsed
		}
	}
	time.Sleep(duration)
}

func TestLiveWaitingNotification(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_FCM_LIVE_TEST") != "1" {
		t.Skip("set EVERYTHING_GO_FCM_LIVE_TEST=1 for the real-device FCM smoke test")
	}
	n := liveTestNotifier(t)
	authority := envOr("EVERYTHING_GO_FCM_TEST_AUTHORITY", "qa-live-waiting")
	sessionID := envOr("EVERYTHING_GO_FCM_TEST_SESSION", "qa-waiting")
	revision := uint64(time.Now().Unix() % 1_000_000)
	n.NotifySessionStatusWithAuthority(authority, "Wulala", sessionID, "等待回覆實測",
		"waiting", "waiting_user", "需要確認部署方式", revision, time.Now().UnixMilli(), time.Now().Add(-2*time.Minute).UnixMilli(), "waiting-request", 0,
		ReplyAction{URL: "http://127.0.0.1:9/reply", Capability: "qa-capability", ExpiresAt: time.Now().Add(time.Hour).UnixMilli()})
	t.Cleanup(func() {
		n.NotifySessionStatusWithAuthority(authority, "Wulala", sessionID, "等待回覆實測",
			"completed", "completed", "", revision+1, time.Now().UnixMilli(), 0, "waiting-request", 0, ReplyAction{})
	})
	time.Sleep(30 * time.Second)
}

func TestLiveMultiSessionNotification(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_FCM_LIVE_TEST") != "1" {
		t.Skip("set EVERYTHING_GO_FCM_LIVE_TEST=1 for the real-device FCM smoke test")
	}
	n := liveTestNotifier(t)
	revision := uint64(time.Now().Unix() % 1_000_000)
	type item struct{ authority, authorityName, sessionID, sessionName string }
	items := []item{
		{"eg_wulala_qa", "Wulala", "qa-multi-wulala", "通知工作 A"},
		{"eg_morrie_qa", "Morrie", "qa-multi-morrie", "通知工作 B"},
	}
	for index, candidate := range items {
		n.NotifySessionStatusWithAuthority(candidate.authority, candidate.authorityName, candidate.sessionID, candidate.sessionName,
			"running", "thinking", "", revision+uint64(index), time.Now().UnixMilli(), time.Now().Add(-time.Minute).UnixMilli(), "multi-request", 0, ReplyAction{})
	}
	t.Cleanup(func() {
		for index, candidate := range items {
			n.NotifySessionStatusWithAuthority(candidate.authority, candidate.authorityName, candidate.sessionID, candidate.sessionName,
				"completed", "completed", "", revision+uint64(index)+10, time.Now().UnixMilli(), 0, "multi-request", 0, ReplyAction{})
		}
	})
	time.Sleep(30 * time.Second)
}

func TestLiveTaskDoneNotification(t *testing.T) {
	if os.Getenv("EVERYTHING_GO_FCM_LIVE_TEST") != "1" {
		t.Skip("set EVERYTHING_GO_FCM_LIVE_TEST=1 for the real-device FCM smoke test")
	}
	n := liveTestNotifier(t)
	revision := uint64(time.Now().Unix() % 1_000_000)
	n.NotifySessionStatusWithAuthority(
		"eg_wulala_qa", "Wulala", "qa-task-done", "完成通知實測",
		"running", "thinking", "", revision, time.Now().UnixMilli(), time.Now().UnixMilli(),
		"qa-task-done-request", 0, ReplyAction{},
	)
	time.Sleep(time.Second)
	n.NotifyTaskDoneWithAuthority(
		"eg_wulala_qa", "Wulala", "完成通知實測",
		"已完成 P0–P2 通知狀態打磨，可查看結果。", "qa-task-done", "qa-task-done-request",
		ReplyAction{},
	)
	time.Sleep(10 * time.Second)
}

func liveTestNotifier(t *testing.T) *Notifier {
	t.Helper()
	serviceAccount := os.Getenv("EVERYTHING_GO_FCM_SERVICE_ACCOUNT")
	registry := os.Getenv("EVERYTHING_GO_FCM_TOKEN_REGISTRY")
	if serviceAccount == "" || registry == "" {
		t.Fatal("live test requires EVERYTHING_GO_FCM_SERVICE_ACCOUNT and EVERYTHING_GO_FCM_TOKEN_REGISTRY")
	}
	n, err := New(serviceAccount, registry)
	if err != nil {
		t.Fatal(err)
	}
	n.statusMinInterval = 10 * time.Millisecond
	return n
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func TestSummarizeStripsMarkdown(t *testing.T) {
	got := summarize("Here is **bold** and `code` and a [link](http://x).")
	if strings.ContainsAny(got, "*`") {
		t.Fatalf("markdown marks not stripped: %q", got)
	}
	if !strings.Contains(got, "link") || strings.Contains(got, "http://x") {
		t.Fatalf("link text should remain, URL should go: %q", got)
	}
}

func TestSummarizeUsesLastParagraph(t *testing.T) {
	got := summarize("first paragraph\n\nsecond paragraph is the summary.")
	if !strings.HasPrefix(got, "second paragraph") {
		t.Fatalf("should summarize the last paragraph, got %q", got)
	}
}

func TestSummarizeFirstSentenceOnly(t *testing.T) {
	got := summarize("Done. More detail follows here that should be dropped.")
	if got != "Done." {
		t.Fatalf("should keep only the first sentence, got %q", got)
	}
}

func TestSummarizeCJKSentenceWithSpace(t *testing.T) {
	// The Python bridge's split requires whitespace after the punctuation
	// ((?<=[。！？!?.])\s+), so a space-separated CJK sentence splits.
	got := summarize("完成了。 後面這句應該被丟掉。")
	if got != "完成了。" {
		t.Fatalf("CJK first-sentence split (with space) failed, got %q", got)
	}
}

func TestSummarizeCJKNoSpaceMatchesPython(t *testing.T) {
	// Without trailing whitespace there is nothing for \s+ to match, so — exactly
	// like the Python bridge — the whole text is kept (no CJK sentence split).
	got := summarize("完成了。後面這句也留著。")
	if got != "完成了。後面這句也留著。" {
		t.Fatalf("CJK without space should keep full text (Python parity), got %q", got)
	}
}

func TestSummarizeCapsAt120Runes(t *testing.T) {
	long := strings.Repeat("漢", 200) // 200 CJK runes, no sentence breaks
	got := summarize(long)
	r := []rune(got)
	// 120 runes + the ellipsis.
	if len(r) != 121 || r[120] != '…' {
		t.Fatalf("expected 120 runes + ellipsis, got %d runes (last=%q)", len(r), string(r[len(r)-1]))
	}
}

func TestSummarizeEmpty(t *testing.T) {
	if got := summarize(""); got != "" {
		t.Fatalf("empty input should summarize to empty, got %q", got)
	}
}

func TestTokenFatal(t *testing.T) {
	if !tokenFatal(404, nil) {
		t.Fatal("404 should be fatal")
	}
	if !tokenFatal(400, []byte(`{"error":{"status":"INVALID_ARGUMENT"}}`)) {
		t.Fatal("400 INVALID_ARGUMENT should be fatal")
	}
	if !tokenFatal(404, []byte("UNREGISTERED")) {
		t.Fatal("UNREGISTERED should be fatal")
	}
	if tokenFatal(503, []byte("temporary")) {
		t.Fatal("503 transient should NOT be fatal")
	}
}
