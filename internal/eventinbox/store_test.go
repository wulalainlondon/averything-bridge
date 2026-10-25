package eventinbox

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(t.TempDir(), "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestInsertDeduplicatesBySourceEventKey(t *testing.T) {
	store := openTestStore(t)
	input := Input{Source: "github", EventKey: "run-42", Kind: "ci_failed", Severity: "error", Title: "CI failed", Data: json.RawMessage(`{"run_id":42}`)}
	first, deduped, err := store.Insert(context.Background(), input)
	if err != nil || deduped || first.ID == "" || first.Sequence == 0 {
		t.Fatalf("first=%+v deduped=%v err=%v", first, deduped, err)
	}
	second, deduped, err := store.Insert(context.Background(), input)
	if err != nil || !deduped || second.ID != first.ID || second.Sequence != first.Sequence {
		t.Fatalf("second=%+v deduped=%v err=%v", second, deduped, err)
	}
}

func TestReadAndDismissArePerDeviceAndPersist(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	event, _, err := store.Insert(context.Background(), Input{Source: "monitor", EventKey: "alert-1", Kind: "alert", Severity: "warning", Title: "Needs attention"})
	if err != nil {
		t.Fatal(err)
	}
	yes := true
	if _, err := store.Mark(context.Background(), event.ID, "desktop", &yes, &yes); err != nil {
		t.Fatal(err)
	}
	desktop, _ := store.Snapshot(context.Background(), "desktop", 0)
	phone, _ := store.Snapshot(context.Background(), "phone", 0)
	if len(desktop.Items) != 0 || len(phone.Items) != 1 || phone.Items[0].Read {
		t.Fatalf("desktop=%+v phone=%+v", desktop, phone)
	}
	_ = store.Close()
	reloaded, err := Open(dir, "bridge-test")
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Close()
	phone, _ = reloaded.Snapshot(context.Background(), "phone", 0)
	if len(phone.Items) != 1 || phone.Items[0].ID != event.ID {
		t.Fatalf("restart lost phone event: %+v", phone)
	}
}

func TestInsertRejectsExpiredOrInvalidData(t *testing.T) {
	store := openTestStore(t)
	store.now = func() time.Time { return time.UnixMilli(1000) }
	if _, _, err := store.Insert(context.Background(), Input{Source: "x", EventKey: "expired", Kind: "x", Severity: "info", Title: "x", ExpiresAt: 999}); err == nil {
		t.Fatal("expired event accepted")
	}
	if _, _, err := store.Insert(context.Background(), Input{Source: "x", EventKey: "bad-json", Kind: "x", Severity: "info", Title: "x", Data: json.RawMessage(`{`)}); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	if _, _, err := store.Insert(context.Background(), Input{Source: "x", EventKey: "array-json", Kind: "x", Severity: "info", Title: "x", Data: json.RawMessage(`[]`)}); err == nil {
		t.Fatal("non-object JSON accepted")
	}
	if _, _, err := store.Insert(context.Background(), Input{Source: "", EventKey: "missing-source", Kind: "x", Severity: "info", Title: "x"}); err == nil {
		t.Fatal("missing canonical source accepted")
	}
	if _, _, err := store.Insert(context.Background(), Input{Source: "x", EventKey: "unsafe-url", Kind: "x", Severity: "info", Title: "x", URL: "javascript:alert(1)"}); err == nil {
		t.Fatal("unsafe canonical URL accepted")
	}
}

func TestAttachmentFetchStateIsDurableURLFreeAndIdempotent(t *testing.T) {
	store := openTestStore(t)
	input := Input{Source: "meta.facebook.judge", EventKey: "message:mid-1", Kind: "message.received",
		Severity: "info", Title: "Player report"}
	attachments := []AttachmentInput{{ExternalID: "mid-1:0", SourceURL: "https://cdn.example/proof.jpg",
		MIMEType: "image/jpeg", DisplayName: "proof.jpg", Ordinal: 0}}
	event, deduped, err := store.InsertWithAttachments(context.Background(), input, attachments)
	if err != nil || deduped || len(event.Attachments) != 1 || event.Attachments[0].Status != "pending" {
		t.Fatalf("event=%+v deduped=%v err=%v", event, deduped, err)
	}
	job, ok, err := store.ClaimAttachmentFetch(context.Background(), "worker", time.Now().UnixMilli(), time.Minute)
	if err != nil || !ok || job.SourceURL == "" || job.Attempt != 1 {
		t.Fatalf("job=%+v ok=%v err=%v", job, ok, err)
	}
	if err := store.CompleteAttachmentFetch(context.Background(), job.ID, "worker", "/private/proof.jpg", "image/jpeg", strings.Repeat("a", 64), 42); err != nil {
		t.Fatal(err)
	}
	stored, err := store.Get(context.Background(), event.ID)
	if err != nil || stored.Attachments[0].Status != "available" || stored.Attachments[0].LocalPath == "" || stored.Attachments[0].URL != "" {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
	_, deduped, err = store.InsertWithAttachments(context.Background(), input, attachments)
	if err != nil || !deduped {
		t.Fatalf("duplicate deduped=%v err=%v", deduped, err)
	}
	stored, _ = store.Get(context.Background(), event.ID)
	if len(stored.Attachments) != 1 {
		t.Fatalf("duplicate created attachments: %+v", stored.Attachments)
	}
}
