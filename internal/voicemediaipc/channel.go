// Package voicemediaipc provides a private, per-call local-user control channel.
// It is not a native-tool caller or a general Bridge/operator API.
package voicemediaipc

import (
	"context"
	"encoding/json"
	"errors"
	"everything-go/internal/backend"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type Request struct {
	ID     string                    `json:"id"`
	Thread string                    `json:"thread"`
	Voice  string                    `json:"voice"`
	Method string                    `json:"method"`
	Input  backend.VoiceMediaRequest `json:"input"`
}
type Response struct {
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}
type Channel struct {
	Path     string
	root     string
	listener net.Listener
	once     sync.Once
}

var operationID = regexp.MustCompile(`^vop_[A-Za-z0-9_-]{8,100}$`)

func Open(thread, voice string, handler func(context.Context, Request) (any, error)) (*Channel, error) {
	root, err := os.MkdirTemp("", "bgvm-")
	if err != nil {
		return nil, err
	}
	path := filepath.Join(root, "m.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		os.RemoveAll(root)
		return nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		listener.Close()
		os.RemoveAll(root)
		return nil, err
	}
	c := &Channel{Path: path, root: root, listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
				var req Request
				decoder := json.NewDecoder(io.LimitReader(conn, 8193))
				decoder.DisallowUnknownFields()
				response := Response{}
				if decoder.Decode(&req) != nil || !operationID.MatchString(req.ID) || req.Thread != thread || req.Voice != voice {
					response.Error = "voice_media_local_binding_invalid"
				} else {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					result, err := handler(ctx, req)
					cancel()
					if err != nil {
						response.Error = err.Error()
					} else {
						response.Result = result
					}
				}
				_ = json.NewEncoder(conn).Encode(response)
			}()
		}
	}()
	return c, nil
}
func (c *Channel) Close() {
	if c != nil {
		c.once.Do(func() { c.listener.Close(); os.RemoveAll(c.root) })
	}
}
func Send(ctx context.Context, path string, req Request) (json.RawMessage, error) {
	if !filepath.IsAbs(path) || !operationID.MatchString(req.ID) || req.Thread == "" || req.Voice == "" {
		return nil, errors.New("voice_media_local_binding_invalid")
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, errors.New("voice_media_local_channel_unavailable")
	}
	defer conn.Close()
	deadline := time.Now().Add(8 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	if err = json.NewEncoder(conn).Encode(req); err != nil {
		return nil, errors.New("voice_media_local_write_failed")
	}
	var result struct {
		Result json.RawMessage `json:"result"`
		Error  string          `json:"error"`
	}
	if json.NewDecoder(io.LimitReader(conn, 32768)).Decode(&result) != nil {
		return nil, errors.New("voice_media_local_result_unknown_do_not_replay")
	}
	if result.Error != "" {
		return nil, errors.New(result.Error)
	}
	return result.Result, nil
}
func RunCLI(args []string, out, stderr io.Writer) int {
	flags := flag.NewFlagSet("voice-media", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("socket", "", "Exact private socket from this voice's instructions")
	thread := flags.String("thread", "", "Exact native thread")
	voice := flags.String("voice", "", "Exact current voice")
	id := flags.String("id", "", "Unique vop_ intent ID; reuse only for the same intent")
	method := flags.String("method", "status", "play_youtube, control or status")
	url := flags.String("url", "", "Actual HTTPS YouTube URL")
	title := flags.String("title", "", "Track label")
	action := flags.String("action", "", "pause, resume, stop or volume")
	volume := flags.Int("volume", -1, "Music volume0-100")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return 2
	}
	req := Request{ID: *id, Thread: *thread, Voice: *voice, Method: *method, Input: backend.VoiceMediaRequest{URL: *url, Title: *title, Action: *action}}
	providedVolume := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "volume" {
			providedVolume = true
		}
	})
	if providedVolume && (*volume < 0 || *volume > 100) {
		_ = json.NewEncoder(out).Encode(Response{Error: "voice_media_volume_invalid"})
		return 2
	}
	if providedVolume {
		req.Input.Volume = volume
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	result, err := Send(ctx, *path, req)
	if err != nil {
		_ = json.NewEncoder(out).Encode(Response{Error: err.Error()})
		return 1
	}
	fmt.Fprintln(out, string(result))
	return 0
}
