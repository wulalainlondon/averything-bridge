// Command push-relay is the hosted push broker, not customer Bridge software.
// No Google credential is included in its image or public release artifacts.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"everything-go/internal/pushrelay"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:8090", "HTTP listener behind a trusted HTTPS terminator")
	dataDir := flag.String("data-dir", "push-relay-data", "private persistent relay state directory (0700)")
	project := flag.String("firebase-project", os.Getenv("GOOGLE_CLOUD_PROJECT"), "Firebase project used by the released mobile App")
	revoke := flag.String("revoke-bridge", "", "operator-only: disable a broker Bridge ID and exit")
	flag.Parse()
	store, err := pushrelay.OpenStore(filepath.Join(*dataDir, "push-relay.sqlite"))
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *revoke != "" {
		if err = store.RevokeBridge(ctx, *revoke); err != nil {
			log.Fatal("Bridge revocation failed")
		}
		log.Print("Bridge revoked")
		return
	}
	sender, err := pushrelay.NewFCMSender(ctx, *project)
	if err != nil {
		log.Fatal(err)
	}
	handler := pushrelay.NewServer(store, sender)
	server := &http.Server{Addr: *listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 25 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 8 << 10}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := handler.Prune(ctx); err != nil {
					log.Print("relay maintenance failed")
				}
			}
		}
	}()
	log.Print("push relay ready (Google ADC, device-scoped delivery); HTTPS and durable state required")
	if err = server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("push relay listener failed")
	}
}
