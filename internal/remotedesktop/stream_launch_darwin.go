//go:build darwin

package remotedesktop

import (
	"context"
	"fmt"
	"html"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// openStream starts the signed capture helper as its own GUI LaunchAgent.
// ScreenCaptureKit's responsible process is then the capture agent, not the
// background-only Bridge. A private Unix socket carries H.264 in memory only.
func openStream(ctx context.Context, helperPath, dataDir string) (io.ReadCloser, func(), error) {
	runDir, err := os.MkdirTemp(dataDir, "remote-stream-")
	if err != nil {
		return nil, nil, err
	}
	socketPath := filepath.Join(runDir, "video.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		_ = os.RemoveAll(runDir)
		return nil, nil, err
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
		_ = listener.Close()
		_ = os.RemoveAll(runDir)
		return nil, nil, err
	}
	label := "com.wulala.bridge-remote-stream." + filepath.Base(runDir)
	jobPath := filepath.Join(runDir, "job.plist")
	stderrPath := filepath.Join(runDir, "stderr.log")
	job := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>%s</string>
<key>ProgramArguments</key><array><string>%s</string><string>--socket</string><string>%s</string></array>
<key>RunAtLoad</key><true/><key>KeepAlive</key><false/>
<key>StandardOutPath</key><string>/dev/null</string>
<key>StandardErrorPath</key><string>%s</string>
</dict></plist>`, html.EscapeString(label), html.EscapeString(helperPath), html.EscapeString(socketPath), html.EscapeString(stderrPath))
	if err := os.WriteFile(jobPath, []byte(job), 0600); err != nil {
		_ = listener.Close()
		_ = os.RemoveAll(runDir)
		return nil, nil, err
	}
	domain := fmt.Sprintf("gui/%d", os.Getuid())
	booted := false
	var conn net.Conn
	var once sync.Once
	done := make(chan struct{})
	cleanup := func() {
		once.Do(func() {
			close(done)
			if conn != nil {
				_ = conn.Close()
			}
			_ = listener.Close()
			if booted {
				stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = exec.CommandContext(stopCtx, "launchctl", "bootout", domain+"/"+label).Run()
				stopCancel()
			}
			if file, err := os.Open(stderrPath); err == nil {
				message, _ := io.ReadAll(io.LimitReader(file, 8192))
				_ = file.Close()
				if line := strings.TrimSpace(string(message)); line != "" {
					log.Printf("[remote-desktop] stream helper: %s", line)
				}
			}
			_ = os.RemoveAll(runDir)
		})
	}
	startCtx, startCancel := context.WithTimeout(ctx, 5*time.Second)
	output, err := exec.CommandContext(startCtx, "launchctl", "bootstrap", domain, jobPath).CombinedOutput()
	startCancel()
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("bootstrap capture agent: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	booted = true
	unixListener := listener.(*net.UnixListener)
	_ = unixListener.SetDeadline(time.Now().Add(5 * time.Second))
	ready := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-ready:
		}
	}()
	conn, err = listener.Accept()
	close(ready)
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("capture agent did not connect: %w", err)
	}
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return conn, cleanup, nil
}
