package core

import (
	"context"
	"encoding/json"
	"everything-go/internal/history"
	"everything-go/internal/recap"
	"everything-go/internal/session"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

type recapUIExec struct {
	*floatingHistoryExec
	store *recap.Store
}

func (e *recapUIExec) SetGoal(context.Context, *session.Session, string, string, *int) error {
	return nil
}
func (e *recapUIExec) GetGoal(context.Context, *session.Session) error   { return nil }
func (e *recapUIExec) ClearGoal(context.Context, *session.Session) error { return nil }

func (e *recapUIExec) SessionRecap(ctx context.Context, s *session.Session, generate, force bool) (recap.Result, error) {
	messages, _ := e.provider.LoadHistory(s.ResumeID(), history.Opts{Limit: 100})
	_, hash := recap.Excerpt(messages.Messages)
	return e.store.Get(ctx, s.ID, s.ResumeID(), hash, generate, force, func(context.Context) (recap.Generated, error) {
		select {
		case <-ctx.Done():
			return recap.Generated{}, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
		next := "[QA] 讓 Note20 上線後重送 APK。"
		return recap.Generated{Summary: "[QA fixture] Morrie 手機模型清單刷新已修正，APK 1.3.51 已建置。S10+ 已升級，但 6.1 模型選單尚未實機驗收；Note20 離線，Taildrop 傳送未完成。", NextAction: &next}, nil
	})
}

// Isolated real WebSocket gateway/cache fixture, with a deterministic model
// adapter. No production data, sessions, pairing credentials or daemon writes.
func TestSessionRecapUIHarness(t *testing.T) {
	manifest := os.Getenv("BRIDGE_RECAP_QA_MANIFEST")
	if manifest == "" {
		t.Skip("opt-in recap browser fixture")
	}
	h, base := newTestHub(t)
	h.cfg.InstanceID, h.cfg.InstanceName = "recap_qa", "Recap QA"
	if err := h.pairing.Claim("recap-qa-token", "qa-browser"); err != nil {
		t.Fatal(err)
	}
	provider := &floatingHistoryProvider{byResume: map[string][]map[string]any{"recap-native": {
		history.CompleteMsg("codex", "recap-native", "u1", "user", "修正手機模型清單並傳送 APK", 1000, nil),
		history.CompleteMsg("codex", "recap-native", "a1", "assistant", "已建置 APK；Note20 離線，仍待傳送與驗證。", 2000, nil),
	}}}
	exec := &recapUIExec{floatingHistoryExec: &floatingHistoryExec{fakeExec: base, provider: provider}, store: &recap.Store{Dir: t.TempDir()}}
	h.SetExecutor(exec)
	h.registry.Create("recap-ui", "Recap 模型刷新 QA", t.TempDir(), "codex", "fixture", "read-only", "recap-native")
	server := httptest.NewServer(http.HandlerFunc(h.ServeWS))
	defer server.Close()
	raw, _ := json.Marshal(map[string]string{"url": "ws" + strings.TrimPrefix(server.URL, "http"), "token": "recap-qa-token", "authority": "recap_qa", "session_id": "recap-ui"})
	if err := os.WriteFile(manifest, raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("RECAP_UI_QA_READY")
	deadline := time.Now().Add(20 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(manifest + ".stop"); err == nil {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("recap UI fixture deadline reached")
}
