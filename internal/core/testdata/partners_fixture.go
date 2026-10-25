// Standalone loopback-only integration fixture. No real provider, private
// account, device grant or production data is loaded by this executable.
package main

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/core"
	"everything-go/internal/governance"
	"everything-go/internal/history"
	"everything-go/internal/partners"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type fixture struct {
	mu       sync.Mutex
	hub      *core.Hub
	rows     map[string][]map[string]any
	received []map[string]string
}

func (f *fixture) Send(ctx context.Context, s *session.Session, id, content string, _ []backend.ImageAttachment, _ []backend.FileAttachment) error {
	effective := backend.PresentationContent(ctx, content)
	thread := s.ResumeID()
	initialized := thread == ""
	if initialized {
		thread = "native-fixture-" + s.ID
		s.SetResumeID(thread)
	}
	f.mu.Lock()
	f.received = append(f.received, map[string]string{"request_id": id, "session_id": s.ID, "thread_id": thread, "content": content, "effective": effective})
	f.rows[thread] = append(f.rows[thread], map[string]any{"source_message_id": id + "-user", "request_id": id, "role": "user", "content": effective, "timestamp": time.Now().UnixMilli()})
	f.mu.Unlock()
	if initialized {
		f.hub.Emit(protocol.NewSessionUUID(s.ID, thread))
	}
	answer := "隔離端到端回覆：已收到這筆原交辦。搭檔角色與原始輸入已分別保存；沒有呼叫真實 AI。"
	f.hub.Emit(protocol.NewTextChunk(s.ID, id, answer))
	f.mu.Lock()
	f.rows[thread] = append(f.rows[thread], map[string]any{"source_message_id": id + "-answer", "request_id": id, "role": "assistant", "content": answer, "timestamp": time.Now().UnixMilli()})
	f.mu.Unlock()
	f.hub.Emit(protocol.NewDone(s.ID, id))
	return nil
}
func (f *fixture) Stop(_ context.Context, s *session.Session) error {
	f.hub.Emit(protocol.NewStopped(s.ID, ""))
	return nil
}
func (f *fixture) Clear(context.Context, *session.Session) error                { return nil }
func (f *fixture) Close(context.Context, *session.Session) error                { return nil }
func (f *fixture) AllProviders() []backend.HistoryProvider                      { return []backend.HistoryProvider{f} }
func (f *fixture) ProviderFor(*session.Session) (backend.HistoryProvider, bool) { return f, true }
func (f *fixture) LoadHistory(thread string, opts history.Opts) (*history.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := make([]map[string]any, 0, len(f.rows[thread]))
	for i, row := range f.rows[thread] {
		copy := make(map[string]any, len(row)+2)
		for k, v := range row {
			copy[k] = v
		}
		copy["source_session_id"] = thread
		copy["source"] = "codex"
		copy["source_message_id"] = "codex:" + thread + ":line:" + strconv.Itoa(i+1)
		rows = append(rows, copy)
	}
	r := history.Slice(rows, opts)
	r.SourceGeneration = "fixture-generation:" + thread
	return r, nil
}
func (f *fixture) HistorySourceGeneration(thread string) (string, error) {
	return "fixture-generation:" + thread, nil
}
func (f *fixture) ResumableSessions(int) ([]history.ResumableSession, error) { return nil, nil }
func main() {
	root := os.Getenv("PARTNERS_FIXTURE_DATA")
	if root == "" {
		log.Fatal("isolated data directory required")
	}
	if e := os.MkdirAll(root, 0700); e != nil {
		log.Fatal(e)
	}
	os.Setenv("BRIDGE_AUTH_TOKEN", "partners-integration-fixture-token")
	registry := session.NewRegistry()
	s := registry.Create("original-fixture-session", "小橋", root, backend.Codex, "fixture-model", "read-only", "")
	s.SetResumeID("original-fixture-thread")
	pair := governance.NewPairing(filepath.Join(root, "pairing.json"))
	if e := pair.Claim("partners-integration-fixture-token", "native-ux-fixture-device"); e != nil {
		log.Fatal(e)
	}
	store, e := partners.Open(root)
	if e != nil {
		log.Fatal(e)
	}
	if _, ok := store.ForSession(s.ID); !ok {
		_, e = store.Save("fixture-seed", "fixture-seed-001", 0, partners.Profile{SessionID: s.ID, NativeThreadID: s.ResumeID(), Name: "小橋", Label: "原生與後端整合驗收", Color: "mint", Main: true})
		if e != nil {
			log.Fatal(e)
		}
	}
	// Restore only this fixture's own durable teammate identities, never user data.
	_, persisted := store.Snapshot()
	for _, p := range persisted {
		if p.SessionID != s.ID {
			registry.Create(p.SessionID, p.Name, root, backend.Codex, "fixture-model", "read-only", p.NativeThreadID)
		}
	}
	// The Hub opens its own authoritative Store after seeding.
	h := core.NewHub(registry, core.Config{InstanceID: "partners-native-integration", InstanceName: "搭檔隔離整合", RootDir: root, DataDir: root, Backends: []backend.Definition{{ID: backend.Codex, Label: "Codex fixture", DefaultModel: "fixture-model", Models: []backend.Model{{ID: "fixture-model", Label: "Synthetic fixture"}}, Capabilities: backend.Capabilities{History: true, Sandbox: true}}}}, pair, 0)
	f := &fixture{hub: h, rows: map[string][]map[string]any{"original-fixture-thread": {{"source_message_id": "fixture-intro", "role": "assistant", "content": "這是原生桌面與真實 Bridge gateway 的隔離整合驗收。所有角色、對話與輸出都是合成資料。"}}}}
	h.SetExecutor(f)
	mux := http.NewServeMux()
	mux.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		json.NewEncoder(w).Encode(f.received)
	})
	mux.HandleFunc("/", h.ServeWS)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		log.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(root, "endpoint.json"), []byte(`{"url":"ws://`+listener.Addr().String()+`"}`), 0600); e != nil {
		log.Fatal(e)
	}
	log.Fatal(http.Serve(listener, mux))
}
