package deviceinventory

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func socketPath(dir string) string { return filepath.Join(dir, ".device-admin", "query.sock") }

// ServeAdmin is deliberately Unix-only: no HTTP route is added to the public
// Bridge/tunnel, and filesystem permissions are the administrator boundary.
func ServeAdmin(ctx context.Context, dir string, snapshot func() Snapshot) (func(), error) {
	parent := filepath.Dir(socketPath(dir))
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, err
	}
	stat, err := os.Lstat(parent)
	if err != nil || !stat.IsDir() || stat.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid device admin directory")
	}
	if err = os.Chmod(parent, 0700); err != nil {
		return nil, err
	}
	p := socketPath(dir)
	if st, err := os.Lstat(p); err == nil {
		if st.Mode()&os.ModeSocket == 0 {
			return nil, errors.New("device admin path is not a socket")
		}
		if conn, dialErr := net.DialTimeout("unix", p, 250*time.Millisecond); dialErr == nil {
			conn.Close()
			return nil, errors.New("device admin already running")
		}
		if err = os.Remove(p); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	listener, err := net.Listen("unix", p)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(p, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/devices", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_ = json.NewEncoder(w).Encode(snapshot())
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 4096}
	go func() { _ = server.Serve(listener) }()
	go func() { <-ctx.Done(); _ = server.Close() }()
	return func() { _ = server.Close() }, nil
}

func Query(ctx context.Context, dir string) (Snapshot, error) {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath(dir))
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://device-admin/devices", nil)
	if err != nil {
		return Snapshot{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return Snapshot{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Snapshot{}, errors.New("device admin query rejected")
	}
	var result Snapshot
	err = json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&result)
	return result, err
}
