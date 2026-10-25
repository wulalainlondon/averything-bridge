package voicecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestVoiceControlPrivateControlRejectsInvalidMethodsAndKeepsSeparateMediaEntry(t *testing.T) {
	root := t.TempDir()
	manifest := filepath.Join(root, "private", "current.json")
	var calls atomic.Int32
	server, e := OpenServer(root, "voice-instance", manifest, func(_ context.Context, q Request) (Result, error) {
		calls.Add(1)
		return Result{OK: true, Result: q.Method}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	q := Request{ID: "vtr_local_readonly", Thread: "exact-native-thread", Method: "list_sessions", Params: json.RawMessage(`{}`)}
	got, e := Send(context.Background(), manifest, q)
	if e != nil || !got.OK || calls.Load() != 1 {
		t.Fatal(got, e)
	}
	for _, method := range []string{"grant", "create_account", "play_music", "handoff_call", "resume"} {
		q.Method = method
		if _, e = Send(context.Background(), manifest, q); e == nil {
			t.Fatal("unsupported method accepted", method)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("invalid request reached caller resolver")
	}
	if _, e = OpenServer(root, "voice-instance", manifest, func(context.Context, Request) (Result, error) { return Result{}, nil }); e == nil {
		t.Fatal("two socket owners")
	}
	os.Chmod(manifest, 0644)
	q.Method = "list_sessions"
	if _, e = Send(context.Background(), manifest, q); e == nil {
		t.Fatal("public manifest accepted")
	}
}
func TestVoiceControlCLIRejectsAbsentOriginManagementAndMalformedPayload(t *testing.T) {
	for _, args := range [][]string{{"--id", "vop_media_intent", "--method", "list_sessions"}, {"--id", "vtr_bad_method", "--method", "grant"}, {"--id", "vtr_bad_payload", "--method", "send_message", "--params", "[]"}, {"--id", "vtr_bad_payload", "--method", "send_message", "--params", "{}{}"}} {
		if _, e := ParseInvocation(args); e == nil {
			t.Fatal("unsafe CLI accepted", args)
		}
	}
	t.Setenv("CODEX_THREAD_ID", "")
	var out bytes.Buffer
	if RunCLI([]string{"--id", "vtr_no_caller_intent", "--method", "list_sessions"}, &out, &out) == 0 || !strings.Contains(out.String(), "voice_control_origin_unavailable") {
		t.Fatal(out.String())
	}
}

func TestVoiceControlCLIProofSurvivesJSONTransportEscapingWithoutWeakeningIdentity(t *testing.T) {
	q := Request{ID: "vtr_escaped_payload", Thread: "native", Method: "voice_transfer", Params: json.RawMessage(`{"content":{"text":"<tag> & 文字"},"expected_revision":9007199254740993}`)}
	before := q.Fingerprint()
	encoded, e := json.Marshal(q)
	if e != nil {
		t.Fatal(e)
	}
	var decoded Request
	if e = json.Unmarshal(encoded, &decoded); e != nil {
		t.Fatal(e)
	}
	if !q.Valid() || !decoded.Valid() || before != decoded.Fingerprint() {
		t.Fatal("JSON transport altered source proof")
	}
	decoded.Params = json.RawMessage(`{"content":{"text":"<tag> & 文字"},"expected_revision":9007199254740992}`)
	if before == decoded.Fingerprint() {
		t.Fatal("large numeric identity rounded")
	}
	decoded.Params = json.RawMessage(`{"content":{"text":"first","text":"second"}}`)
	if decoded.Valid() {
		t.Fatal("ambiguous JSON accepted")
	}
}
