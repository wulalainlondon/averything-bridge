package inbox

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
)

func writeTemp(t *testing.T, dir, name string, body []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, body, 0o644); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	return p
}

func TestPushInlineAndPending(t *testing.T) {
	dir := t.TempDir()
	src := writeTemp(t, dir, "hello.txt", []byte("hi there"))
	s := New(dir)

	item, err := s.Push(src, "sender", []string{"phoneA", "phoneB"})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if item.Filename != "hello.txt" || item.Size != 8 {
		t.Fatalf("unexpected item: %+v", item)
	}
	if got, _ := base64.StdEncoding.DecodeString(item.Data); string(got) != "hi there" {
		t.Fatalf("data roundtrip mismatch: %q", got)
	}

	// Targeted device sees it; sender (not in targets) does not.
	if p := s.Pending("phoneA"); len(p) != 1 || p[0].FileID != item.FileID {
		t.Fatalf("phoneA should have 1 pending, got %d", len(p))
	}
	if p := s.Pending("sender"); len(p) != 0 {
		t.Fatalf("sender is not a target, should have 0 pending, got %d", len(p))
	}
}

func TestAckDeletesWhenAllTargetsAck(t *testing.T) {
	dir := t.TempDir()
	src := writeTemp(t, dir, "f.bin", []byte("x"))
	s := New(dir)
	item, _ := s.Push(src, "sender", []string{"phoneA", "phoneB"})

	if del := s.Ack(item.FileID, "phoneA"); del {
		t.Fatal("must not delete until ALL targets ack")
	}
	if p := s.Pending("phoneA"); len(p) != 0 {
		t.Fatal("phoneA already acked → not pending for it")
	}
	if p := s.Pending("phoneB"); len(p) != 1 {
		t.Fatal("phoneB still pending")
	}
	if del := s.Ack(item.FileID, "phoneB"); !del {
		t.Fatal("entry should be deleted once all targets acked")
	}
	if p := s.Pending("phoneB"); len(p) != 0 {
		t.Fatal("deleted entry should not be pending")
	}
}

func TestPushOversizeRejected(t *testing.T) {
	dir := t.TempDir()
	big := make([]byte, inlineMaxBytes+1)
	src := writeTemp(t, dir, "big.bin", big)
	s := New(dir)
	if _, err := s.Push(src, "sender", nil); err == nil {
		t.Fatal("oversize file must be rejected (no Storage fallback in Go)")
	}
}

func TestPushMissingFile(t *testing.T) {
	s := New(t.TempDir())
	if _, err := s.Push("/no/such/file", "sender", nil); err == nil {
		t.Fatal("missing file must error")
	}
}

func TestMimeOverrideAPK(t *testing.T) {
	dir := t.TempDir()
	src := writeTemp(t, dir, "app.apk", []byte("PK\x03\x04"))
	s := New(dir)
	item, err := s.Push(src, "sender", []string{"phoneA"})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if item.MimeType != "application/vnd.android.package-archive" {
		t.Fatalf("apk mime override missing: %q", item.MimeType)
	}
}

func TestPersistenceReload(t *testing.T) {
	dir := t.TempDir()
	src := writeTemp(t, dir, "keep.txt", []byte("persist me"))
	s1 := New(dir)
	item, _ := s1.Push(src, "sender", []string{"phoneA"})

	// A fresh store over the same dir recovers the un-acked entry.
	s2 := New(dir)
	p := s2.Pending("phoneA")
	if len(p) != 1 || p[0].FileID != item.FileID {
		t.Fatalf("reloaded store lost the pending entry: %+v", p)
	}
	if string(mustDecode(t, p[0].Data)) != "persist me" {
		t.Fatal("reloaded data mismatch")
	}
}

func TestUntargetedAckDeletes(t *testing.T) {
	dir := t.TempDir()
	src := writeTemp(t, dir, "u.txt", []byte("u"))
	s := New(dir)
	item, _ := s.Push(src, "sender", nil) // no targets (no other devices connected)
	if del := s.Ack(item.FileID, "phoneA"); !del {
		t.Fatal("untargeted entry should delete on first ack")
	}
}

func mustDecode(t *testing.T, b64 string) []byte {
	t.Helper()
	d, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return d
}
