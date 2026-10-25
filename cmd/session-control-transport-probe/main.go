// An isolated authenticated transport fixture. It never connects to a model or app-server.
package main

import (
	"context"
	"encoding/json"
	"everything-go/internal/backend"
	"everything-go/internal/core"
	"everything-go/internal/governance"
	"everything-go/internal/protocol"
	"everything-go/internal/relay"
	"everything-go/internal/session"
	"everything-go/internal/sessiondispatch"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
)

type fixture struct {
	sink  backend.Sink
	count atomic.Int32
}

func (f *fixture) Send(_ context.Context, s *session.Session, id, text string, _ []backend.ImageAttachment, _ []backend.FileAttachment) error {
	n := f.count.Add(1)
	raw, _ := json.Marshal(map[string]any{"fixture": "transport_only_no_model", "executions": n})
	f.sink.Emit(backend.CompletedAnswer{SessionID: s.ID, RequestID: id, Text: string(raw)})
	f.sink.Emit(protocol.NewDone(s.ID, id))
	return nil
}
func (f *fixture) Stop(context.Context, *session.Session) error  { return nil }
func (f *fixture) Clear(context.Context, *session.Session) error { return nil }
func (f *fixture) Close(context.Context, *session.Session) error { return nil }
func main() {
	addr := flag.String("listen", "", "Tailscale IP and unused port")
	dir := flag.String("data-dir", "", "dedicated empty fixture directory")
	flag.Parse()
	host, _, e := net.SplitHostPort(*addr)
	_, ts, _ := net.ParseCIDR("100.64.0.0/10")
	ip := net.ParseIP(host)
	if e != nil || ip == nil || !ts.Contains(ip) || *dir == "" {
		log.Fatal("explicit Tailscale listen address and dedicated directory required")
	}
	if e := os.MkdirAll(*dir, 0700); e != nil {
		log.Fatal(e)
	}
	entries, e := os.ReadDir(*dir)
	if e != nil || len(entries) > 0 {
		log.Fatal("fixture data directory must be empty")
	}
	store, e := sessiondispatch.Open(*dir)
	if e != nil {
		log.Fatal(e)
	}
	if _, e = store.SetGrant(context.Background(), "peer:lab-source", sessiondispatch.Grant{Enabled: true, Local: true}, 0); e != nil {
		log.Fatal(e)
	}
	store.Close()
	reg := session.NewRegistry()
	target := reg.Create("lab-target", "Isolated transport fixture", *dir, backend.Codex, "", "read-only", "")
	target.SetResumeID("lab-thread")
	hub := core.NewHub(reg, core.Config{DataDir: *dir, RootDir: *dir, InstanceID: "lab-receiver", InstanceName: "Transport fixture"}, governance.NewPairing(filepath.Join(*dir, "pairing.json")), 0)
	defer hub.CloseSessionDispatches()
	defer hub.CloseDelegations()
	hub.SetExecutor(&fixture{sink: hub})
	peers, e := relay.LoadPeers()
	if e != nil {
		log.Fatal(e)
	}
	hub.SetRelay(nil, peers)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/session-control/v1/", hub.ServeSessionControlAPI)
	listener, e := net.Listen("tcp", *addr)
	if e != nil {
		log.Fatal(e)
	}
	log.Print("ready: transport fixture only; no model, no app-server, no production sessions")
	if e := http.Serve(listener, mux); e != nil {
		log.Fatal(e)
	}
}
