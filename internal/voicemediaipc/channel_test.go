package voicemediaipc

import (
	"bytes"
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func TestPrivateChannelBindingAndExpiry(t *testing.T) {
	var calls atomic.Int32
	c, e := Open("thread-one", "voice-one", func(_ context.Context, r Request) (any, error) {
		calls.Add(1)
		return map[string]string{"state": "fixture_observed", "transport": "local_user_socket"}, nil
	})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	for path, want := range map[string]os.FileMode{c.root: 0700, c.Path: 0600} {
		s, e := os.Stat(path)
		if e != nil || s.Mode().Perm() != want {
			t.Fatal(s, e)
		}
	}
	request := Request{ID: "vop_one_intent", Thread: "thread-one", Voice: "voice-one", Method: "status", Input: backend.VoiceMediaRequest{}}
	bad := request
	bad.Thread = "other-thread"
	if _, e = Send(context.Background(), c.Path, bad); e == nil {
		t.Fatal("foreignthread accepted")
	}
	bad = request
	bad.Voice = "other-voice"
	if _, e = Send(context.Background(), c.Path, bad); e == nil {
		t.Fatal("foreignvoice accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("foreignhandler reached")
	}
	result, e := Send(context.Background(), c.Path, request)
	var value map[string]string
	json.Unmarshal(result, &value)
	if e != nil || value["state"] != "fixture_observed" || calls.Load() != 1 {
		t.Fatal(string(result), e)
	}
	c.Close()
	if _, e = Send(context.Background(), c.Path, request); e == nil {
		t.Fatal("retiredchannel accepted")
	}
	if _, e = os.Stat(c.root); !os.IsNotExist(e) {
		t.Fatal("privatefiles retained")
	}
}

func TestCLIRejectsInvalidVolumeBeforeAnyConnection(t *testing.T) {
	var output bytes.Buffer
	code := RunCLI([]string{"--socket", "/does/not/exist", "--thread", "t", "--voice", "v", "--id", "vop_valid_intent", "--method", "control", "--action", "volume", "--volume", "-2"}, &output, io.Discard)
	if code != 2 || !strings.Contains(output.String(), "voice_media_volume_invalid") {
		t.Fatal(code, output.String())
	}
}
