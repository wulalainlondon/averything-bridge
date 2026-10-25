package goexec

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/session"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSettingsResumeOnlyAfterExplicitMissingThread(t *testing.T) {
	for _, scenario := range []struct {
		name, firstError, resumeError string
		want                          int
	}{
		{"unloaded", `{"code":-32600,"message":"thread not found: original"}`, "", 3},
		{"deleted", `{"code":-32600,"message":"thread not found: original"}`, `{"code":-32600,"message":"no rollout found"}`, 2},
		{"rejected", `{"code":-32600,"message":"invalid model"}`, "", 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			c := NewCodex(&capSink{}, "codex")
			c.appServerMode = "daemon"
			s := session.NewRegistry().Create("logical", "settings", t.TempDir(), backend.Codex, "gpt-5.6-sol", "danger-full-access", "original")
			s.SetEffort("high")
			writer := &rpcCaptureWriter{writes: make(chan []byte, 4)}
			c.rpc.setWriter(writer)
			done := make(chan error, 1)
			go func() { done <- c.UpdateSessionSettings(context.Background(), s) }()
			for i := 0; i < scenario.want; i++ {
				var frame struct {
					ID     int            `json:"id"`
					Method string         `json:"method"`
					Params map[string]any `json:"params"`
				}
				select {
				case raw := <-writer.writes:
					if err := json.Unmarshal(raw, &frame); err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("missing RPC")
				}
				wantMethod := "thread/settings/update"
				if i == 1 {
					wantMethod = "thread/resume"
				}
				if frame.Method != wantMethod || frame.Params["threadId"] != "original" {
					t.Fatalf("unexpected RPC: %+v", frame)
				}
				result := `"result":{}`
				if i == 0 {
					result = `"error":` + scenario.firstError
				}
				if i == 1 {
					if frame.Params["excludeTurns"] != true {
						t.Fatal("history not excluded")
					}
					result = `"result":{"thread":{"id":"original","model":"old-model"}}`
					if scenario.resumeError != "" {
						result = `"error":` + scenario.resumeError
					}
				}
				if i == 2 && (frame.Params["model"] != "gpt-5.6-sol" || frame.Params["effort"] != "high") {
					t.Fatal("requested settings lost")
				}
				c.rpc.dispatchResponse(json.RawMessage(fmt.Sprintf(`{"id":%d,%s}`, frame.ID, result)))
			}
			select {
			case err := <-done:
				if (err == nil) != (scenario.name == "unloaded") {
					t.Fatalf("result: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("settings did not finish")
			}
			if s.ResumeID() != "original" {
				t.Fatal("conversation identity changed")
			}
			select {
			case extra := <-writer.writes:
				t.Fatalf("unexpected retry/create: %s", extra)
			default:
			}
		})
	}
}

func TestSettingsConnectsDaemonBeforeFirstRPC(t *testing.T) {
	c := NewCodex(&capSink{}, "codex")
	c.appServerMode = "daemon"
	c.remoteReconnect = false
	home := t.TempDir()
	c.sessionsRoot = filepath.Join(home, "sessions")
	c.codexBin = filepath.Join(home, "fake-codex")
	if err := os.WriteFile(c.codexBin, []byte("#!/bin/sh\necho '{}'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	methods := make(chan string, 8)
	socket, _ := healthServer(t, func(method, id string) (any, bool) {
		methods <- method
		return map[string]any{}, true
	})
	c.appServerSocket = socket
	t.Cleanup(func() { c.startMu.Lock(); defer c.startMu.Unlock(); _ = c.stopServerLocked() })
	s := session.NewRegistry().Create("logical", "settings", home, backend.Codex, "gpt-5.6-sol", "danger-full-access", "original")
	if err := c.UpdateSessionSettings(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if first, second := <-methods, <-methods; first != "initialize" || second != "thread/settings/update" {
		t.Fatalf("RPC order: %s %s", first, second)
	}
}
