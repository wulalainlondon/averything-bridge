// Command remote-desktop-qa runs the production desktop handler as a temporary
// acceptance sidecar, without restarting the resident Bridge. It is not part
// of release packaging.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"everything-go/internal/governance"
	"everything-go/internal/remotedesktop"
)

func main() {
	host := flag.String("host", "", "Mac Tailscale IPv4")
	peer := flag.String("peer", "", "one phone Tailscale IPv4")
	port := flag.Int("port", 8777, "Tailscale-only QA listen port")
	lifetime := flag.Duration("duration", 10*time.Minute, "QA sidecar lifetime (maximum 30 minutes)")
	pairingPath := flag.String("pairing", "", "resident Bridge pairing.json")
	helperPath := flag.String("helper", "", "signed bridge-remote-helper")
	streamHelper := flag.String("stream-helper", "", "persistent H.264 desktop helper")
	flag.Parse()
	ip := net.ParseIP(strings.TrimSpace(*host)).To4()
	if ip == nil || ip[0] != 100 || ip[1] < 64 || ip[1] > 127 || *pairingPath == "" || *helperPath == "" {
		log.Fatal("explicit Tailscale host, pairing path, and signed helper required")
	}
	if *port < 1024 || *port > 65535 {
		log.Fatal("invalid QA port")
	}
	if *lifetime <= 0 || *lifetime > 30*time.Minute {
		log.Fatal("QA sidecar lifetime must be at most 30 minutes")
	}
	if _, err := os.Stat(*helperPath); err != nil {
		log.Fatal(err)
	}
	pairing := governance.NewPairing(*pairingPath)
	if !pairing.IsLocked() {
		log.Fatal("pairing store is unclaimed")
	}
	handler, err := remotedesktop.NewHandler(*peer, func(r *http.Request) bool {
		return pairing.LockedTo(strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")))
	}, remotedesktop.Helper{Path: *helperPath})
	if err != nil {
		log.Fatal(err)
	}
	if *streamHelper != "" {
		video, err := remotedesktop.NewVideoService(ip.String(), *peer, *streamHelper, filepath.Dir(*pairingPath))
		if err != nil {
			log.Fatal(err)
		}
		handler.SetVideoService(video)
		defer video.Close()
	}
	listener, err := net.Listen("tcp4", net.JoinHostPort(ip.String(), strconv.Itoa(*port)))
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *lifetime)
	defer cancel()
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Printf("remote desktop QA sidecar listening on %s for one paired peer; %s limit", listener.Addr(), *lifetime)
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
