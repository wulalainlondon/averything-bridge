package mediaservice

import (
	"bytes"
	"context"
	"everything-go/internal/backend"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFixedIPCPermissionLifecycleAndInstanceGuard(t *testing.T) {
	root := t.TempDir()
	manifest := filepath.Join(t.TempDir(), "private", "manifest.json")
	s, e := OpenServer(root, "instance", manifest, func(ctx context.Context, q Request) (Result, error) {
		return decorate(backend.VoiceMediaResult{State: "idle"}, q.ID, false, false), nil
	})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	q := testRequest()
	q.Method = "status"
	q.Input = backend.VoiceMediaRequest{}
	r, e := Send(context.Background(), manifest, q)
	if e != nil || r.State != "idle" {
		t.Fatal(r, e)
	}
	if _, e = OpenServer(root, "instance", manifest, func(context.Context, Request) (Result, error) {
		t.Fatal("duplicate server effect")
		return Result{}, nil
	}); e == nil {
		t.Fatal("duplicate listener")
	}
	if e = os.Chmod(manifest, 0644); e != nil {
		t.Fatal(e)
	}
	if _, e = Send(context.Background(), manifest, q); e == nil {
		t.Fatal("public manifest accepted")
	}
	os.Chmod(manifest, 0600)
	s.Close()
	if _, e = Send(context.Background(), manifest, q); e == nil {
		t.Fatal("retired socket accepted")
	}
}
func TestCLIHasNoManualTargetAndFailsWithoutAmbientContext(t *testing.T) {
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CODEX_SESSION_ID", "")
	var out, errout bytes.Buffer
	if code := RunCLI([]string{"--id", "vop_actual_query", "--method", "status"}, &out, &errout); code != 1 || !strings.Contains(out.String(), "origin_context_unavailable") {
		t.Fatal(code, out.String())
	}
	out.Reset()
	if code := RunCLI([]string{"--thread", "foreign"}, &out, &errout); code != 2 {
		t.Fatal("manual identity accepted", code)
	}
	t.Setenv("CODEX_THREAD_ID", "thread")
	t.Setenv("CODEX_SESSION_ID", "different")
	if code := RunCLI([]string{"--id", "vop_actual_query"}, &out, &errout); code != 1 {
		t.Fatal(code)
	}
}
