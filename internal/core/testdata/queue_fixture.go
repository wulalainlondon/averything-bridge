package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"everything-go/internal/backend"
	"everything-go/internal/core"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/session"
)

type fixture struct {
	mu      sync.Mutex
	hub     *core.Hub
	active  map[string]string
	sent    []string
	steered []string
	mode    string
}

func (f *fixture) Send(_ context.Context, s *session.Session, id, content string, _ []backend.ImageAttachment, _ []backend.FileAttachment) error {
	f.mu.Lock()
	f.active[s.ID] = id
	f.sent = append(f.sent, id)
	f.mu.Unlock()
	f.hub.Emit(protocol.NewTextChunk(s.ID, id, "QA 回合進行中，等待測試完成指令。"))
	return nil
}
func (f *fixture) Stop(_ context.Context, s *session.Session) error    { f.finish(s.ID, true); return nil }
func (f *fixture) Clear(ctx context.Context, s *session.Session) error { return f.Stop(ctx, s) }
func (f *fixture) Close(ctx context.Context, s *session.Session) error { return f.Stop(ctx, s) }
func (f *fixture) Steer(_ context.Context, s *session.Session, id, content string, _ []backend.ImageAttachment, _ []backend.FileAttachment) (backend.SteerResult, error) {
	f.mu.Lock()
	active := f.active[s.ID]
	mode := f.mode
	if active != "" && mode == "" {
		f.steered = append(f.steered, id)
	}
	f.mu.Unlock()
	if mode == "reject" {
		return backend.SteerResult{}, backend.ErrSteerRejected
	}
	if mode == "uncertain" {
		return backend.SteerResult{}, errors.New("fixture lost acknowledgment")
	}
	if active == "" {
		return backend.SteerResult{}, backend.ErrNoActiveTurn
	}
	f.hub.Emit(protocol.NewTextChunk(s.ID, active, "\n收到立即插隊："+content))
	return backend.SteerResult{TurnID: "fixture-turn", RequestID: active}, nil
}
func (f *fixture) finish(sid string, stop bool) {
	f.mu.Lock()
	id := f.active[sid]
	delete(f.active, sid)
	f.mu.Unlock()
	if id != "" {
		if stop {
			f.hub.Emit(protocol.NewStopped(sid, id))
		} else {
			f.hub.Emit(protocol.NewDone(sid, id))
		}
	}
}
func (f *fixture) ProviderFor(s *session.Session) (backend.HistoryProvider, bool) { return nil, false }
func main() {
	os.Setenv("BRIDGE_AUTH_TOKEN", "queue-fixture-token")
	dir := os.Getenv("QUEUE_FIXTURE_DATA")
	if dir == "" {
		dir, _ = os.MkdirTemp("", "bridge-queue-fixture-")
	}
	reg := session.NewRegistry()
	reg.Create("queue-qa", "Queue QA", dir, backend.Codex, "fixture", "", "")
	pairing := governance.NewPairing(filepath.Join(dir,"pairing.json"))
	if err:=pairing.Claim("queue-fixture-token","queue-fixture");err!=nil{log.Fatal(err)}
	h := core.NewHub(reg, core.Config{InstanceID: "queue-fixture", InstanceName: "Queue QA Bridge", RootDir: dir, DataDir: dir, Backends: []backend.Definition{{ID: backend.Codex, Label: "Codex", Models: []backend.Model{{ID: "fixture", Label: "Fixture"}}, Capabilities: backend.Capabilities{History: true, Steering: true}}}}, pairing, 18766)
	f := &fixture{hub: h, active: map[string]string{}, sent: []string{}, steered: []string{}}
	h.SetExecutor(f)
	mux := http.NewServeMux()
	mux.HandleFunc("/fixture/state", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"sent": f.sent, "steered": f.steered, "active": f.active})
	})
	mux.HandleFunc("/fixture/finish", func(w http.ResponseWriter, r *http.Request) { f.finish("queue-qa", false); w.WriteHeader(204) })
	mux.HandleFunc("/fixture/mode", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.mode = r.URL.Query().Get("value")
		f.mu.Unlock()
		w.WriteHeader(204)
	})
	mux.HandleFunc("/", h.ServeWS)
	log.Fatal(http.ListenAndServe("127.0.0.1:18766", mux))
}
