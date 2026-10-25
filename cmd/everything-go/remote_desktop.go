package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"everything-go/internal/remotedesktop"
)

const remoteDesktopPort = "8777"

func isTailscaleIPv4(value string) bool {
	ip := net.ParseIP(strings.TrimSpace(value)).To4()
	return ip != nil && ip[0] == 100 && ip[1] >= 64 && ip[1] <= 127
}

// startRemoteDesktop is opt-in and uses a separate listener bound to the Mac's
// Tailscale interface. The public Bridge mux and Funnel never see these routes.
func startRemoteDesktop(hostIP, dataDir string, auth func(*http.Request) bool) net.Listener {
	if os.Getenv("BRIDGE_REMOTE_DESKTOP_ENABLED") != "1" {
		return nil
	}
	peerIP := strings.TrimSpace(os.Getenv("BRIDGE_REMOTE_DESKTOP_PEER_IP"))
	if runtime.GOOS != "darwin" || !isTailscaleIPv4(hostIP) || !isTailscaleIPv4(peerIP) || hostIP == peerIP {
		log.Printf("[remote-desktop] disabled: macOS and explicit distinct Tailscale host/peer IPs required")
		return nil
	}
	executable, err := os.Executable()
	if err != nil {
		log.Printf("[remote-desktop] disabled: cannot locate signed helper: %v", err)
		return nil
	}
	helperPath := filepath.Join(filepath.Dir(executable), "bridge-remote-helper")
	if info, err := os.Stat(helperPath); err != nil || info.IsDir() || info.Mode()&0111 == 0 {
		log.Printf("[remote-desktop] disabled: executable helper missing")
		return nil
	}
	handler, err := remotedesktop.NewHandler(peerIP, auth, remotedesktop.Helper{Path: helperPath})
	if err != nil {
		log.Printf("[remote-desktop] disabled: %v", err)
		return nil
	}
	streamPath := filepath.Join(filepath.Dir(executable), "bridge-remote-stream")
	if video, err := remotedesktop.NewVideoService(hostIP, peerIP, streamPath, dataDir); err == nil {
		handler.SetVideoService(video)
	} else {
		log.Printf("[remote-desktop] H.264 video disabled: %v", err)
	}
	addr := net.JoinHostPort(hostIP, remoteDesktopPort)
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		log.Printf("[remote-desktop] disabled: cannot bind Tailscale interface: %v", err)
		return nil
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 7 * time.Second, WriteTimeout: 7 * time.Second, IdleTimeout: 10 * time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("[remote-desktop] listener stopped: %v", err)
		}
	}()
	log.Printf("[remote-desktop] enabled on %s for one paired Tailscale peer", addr)
	return listener
}
