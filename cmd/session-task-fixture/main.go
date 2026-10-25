// Isolated authenticated session-task UI/protocol fixture. Never starts a model.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"everything-go/internal/backend"
	"everything-go/internal/core"
	"everything-go/internal/governance"
	"everything-go/internal/history"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
)

type fixture struct {
	sink     backend.Sink
	mu       sync.Mutex
	dir      string
	messages map[string][]map[string]any
	holds    map[string]chan struct{}
}

func (f *fixture) append(thread, request, turn, role, text string, final bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := history.CompleteMsg("codex", thread, fmt.Sprintf("fixture:%s:%d", thread, len(f.messages[thread])), role, text, time.Now().UnixMilli(), []map[string]any{{"type": "text", "text": text}})
	m["request_id"] = request
	m["source_turn_id"] = turn
	m["history_read_result_verified"] = final
	f.messages[thread] = append(f.messages[thread], m)
	data, err := json.Marshal(f.messages)
	if err != nil {
		return err
	}
	path := filepath.Join(f.dir, "fixture-history.json")
	if err = os.WriteFile(path+".tmp", data, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
func (f *fixture) Send(_ context.Context, s *session.Session, id, text string, _ []backend.ImageAttachment, _ []backend.FileAttachment) error {
	if !strings.HasPrefix(text, "fixture:") {
		f.sink.Emit(backend.NewError(s.ID, id, "fixture_only", "Isolated fixture accepts fixture: directives only; no model was called"))
		return nil
	}
	thread := s.ResumeID()
	turn := "fixture-native-" + id
	if err := f.append(thread, id, turn, "user", text, false); err != nil {
		return err
	}
	if strings.HasPrefix(text, "fixture:handoff") {
		f.mu.Lock()
		ch := make(chan struct{})
		f.holds[id] = ch
		f.mu.Unlock()
		<-ch
	}
	f.sink.Emit(backend.NativeTaskAccepted{SessionID: s.ID, RequestID: id, ThreadID: thread, TurnID: turn})
	f.sink.Emit(backend.NewTurnProgress(s.ID, id, "thinking", "Isolated fixture; no model"))
	if strings.HasPrefix(text, "fixture:hold") {
		f.mu.Lock()
		ch := make(chan struct{})
		f.holds[id] = ch
		f.mu.Unlock()
		<-ch
	}
	if strings.HasPrefix(text, "fixture:fail") {
		f.sink.Emit(backend.NewError(s.ID, id, "fixture_failure", "Deterministic fixture failure"))
		return nil
	}
	if strings.HasPrefix(text, "fixture:history") {
		for i := 0; i < 140; i++ {
			if err := f.append(thread, "fixture_legacy", fmt.Sprintf("filler-%d", i), "assistant", fmt.Sprintf("隔離歷史頁 %d；不是獨立成果。", i), false); err != nil {
				return err
			}
		}
	}
	final := "隔離 fixture 完成回報：" + id + "。此段是 matching 原請求的 final，供定位驗收；沒有呼叫正式模型。"
	if err := f.append(thread, id, turn, "assistant", final, true); err != nil {
		return err
	}
	f.sink.Emit(protocol.NewTextChunk(s.ID, id, final))
	f.sink.Emit(backend.CompletedAnswer{SessionID: s.ID, RequestID: id, Text: final})
	f.sink.Emit(protocol.NewDone(s.ID, id))
	return nil
}
func (f *fixture) Stop(context.Context, *session.Session) error  { return nil }
func (f *fixture) Clear(context.Context, *session.Session) error { return nil }
func (f *fixture) Close(context.Context, *session.Session) error { return nil }
func (f *fixture) LoadHistory(id string, opts history.Opts) (*history.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return history.Slice(f.messages[id], opts), nil
}
func (f *fixture) ResumableSessions(int) ([]history.ResumableSession, error)    { return nil, nil }
func (f *fixture) ProviderFor(*session.Session) (backend.HistoryProvider, bool) { return f, true }
func (f *fixture) AllProviders() []backend.HistoryProvider                      { return []backend.HistoryProvider{f} }
func main() {
	addr := flag.String("listen", "127.0.0.1:18766", "loopback only")
	dir := flag.String("data-dir", "", "explicit isolated directory")
	flag.Parse()
	host, _, err := net.SplitHostPort(*addr)
	if err != nil || !net.ParseIP(host).IsLoopback() || *dir == "" || !strings.Contains(filepath.Base(*dir), "task-fixture") {
		log.Fatal("loopback and dedicated task-fixture directory required")
	}
	if value := os.Getenv("BRIDGE_AUTH_TOKEN"); value != "" {
		log.Fatal("fixture must not inherit a production auth override")
	}
	if err = os.MkdirAll(*dir, 0700); err != nil {
		log.Fatal(err)
	}
	marker := filepath.Join(*dir, "fixture-only.marker")
	if _, err = os.Stat(marker); os.IsNotExist(err) {
		entries, _ := os.ReadDir(*dir)
		if len(entries) > 0 {
			log.Fatal("initial fixture directory must be empty")
		}
		os.WriteFile(marker, []byte("no-model-session-task-v1\n"), 0600)
	}
	pairing := governance.NewPairing(filepath.Join(*dir, "pairing.json"))
	tokenPath := filepath.Join(*dir, "driver-token")
	token, err := os.ReadFile(tokenPath)
	if os.IsNotExist(err) {
		data := make([]byte, 24)
		rand.Read(data)
		token = []byte(hex.EncodeToString(data))
		os.WriteFile(tokenPath, token, 0600)
		pairing.Claim(string(token), "session-task-fixture-driver")
	}
	if len(token) == 0 {
		log.Fatal("driver credential unavailable")
	}
	reg := session.NewRegistry()
	reg.AttachStore(session.NewStore(filepath.Join(*dir, "saved_sessions.json")))
	for _, id := range []string{"task-parent", "task-target", "task-empty"} {
		if _, ok := reg.Get(id); !ok {
			s := reg.Create(id, map[string]string{"task-parent": "任務驗收 fixture", "task-target": "下派目標 fixture", "task-empty": "空狀態 fixture"}[id], *dir, backend.Codex, "fixture", "read-only", "")
			s.SetResumeID("fixture-thread-" + id)
		}
	}
	if err = reg.PersistDurably(); err != nil {
		log.Fatal(err)
	}
	ds, err := sessiondispatch.Open(*dir)
	if err != nil {
		log.Fatal(err)
	}
	grant, _ := ds.Grant(context.Background(), "task-parent")
	if !grant.Enabled {
		_, err = ds.SetGrant(context.Background(), "task-parent", sessiondispatch.Grant{Enabled: true, Local: true, Sessions: []string{"task-fixture:task-target"}}, grant.Revision)
		if err != nil {
			log.Fatal(err)
		}
	}
	ds.Close()
	hub := core.NewHub(reg, core.Config{DataDir: *dir, RootDir: *dir, InstanceID: "task-fixture", InstanceName: "隔離任務驗收（無模型）", Backends: []backend.Definition{{ID: backend.Codex, Label: "Fixture only", Capabilities: backend.Capabilities{History: true}}}}, pairing, 18766)
	f := &fixture{sink: hub, dir: *dir, messages: map[string][]map[string]any{}, holds: map[string]chan struct{}{}}
	if data, err := os.ReadFile(filepath.Join(*dir, "fixture-history.json")); err == nil {
		if json.Unmarshal(data, &f.messages) != nil {
			log.Fatal("fixture history invalid")
		}
	}
	hub.SetExecutor(f)
	mux := http.NewServeMux()
	mux.HandleFunc("/", hub.ServeWS)
	mux.HandleFunc("/qa/enroll", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || !hub.HTTPAuthorized(r) {
			http.Error(w, "forbidden", 403)
			return
		}
		pairing.OpenEnrollment(2 * time.Minute)
		fmt.Fprint(w, "pairing window open")
	})
	mux.HandleFunc("/qa/release", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || !hub.HTTPAuthorized(r) {
			http.Error(w, "forbidden", 403)
			return
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		id := r.URL.Query().Get("request_id")
		ch, ok := f.holds[id]
		if !ok {
			http.Error(w, "hold not found", 404)
			return
		}
		delete(f.holds, id)
		close(ch)
		fmt.Fprint(w, "released exact fixture request")
	})
	log.Print("ready: loopback authenticated fixture; no model, no app-server, no production sessions")
	log.Fatal(http.ListenAndServe(*addr, mux))
}
