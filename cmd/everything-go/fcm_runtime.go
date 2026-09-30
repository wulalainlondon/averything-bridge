package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"everything-go/internal/fcm"
)

func runtimeServiceAccountPath(configured, dataDir string) string {
	if configured = strings.TrimSpace(configured); configured != "" {
		return configured
	}
	return filepath.Join(dataDir, "fcm_service_account.json")
}

func loadRuntimeFCM(configured, dataDir string) (*fcm.Notifier, error) {
	f, err := os.Open(runtimeServiceAccountPath(configured, dataDir))
	if err != nil {
		return nil, fmt.Errorf("private runtime service account unavailable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("runtime service account must be a regular file")
	}
	if runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("runtime service account must be private (0600)")
	}
	const maxBytes = 64 << 10
	data, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil || len(data) > maxBytes {
		return nil, fmt.Errorf("runtime service account could not be read safely")
	}
	notifier, err := fcm.NewFromBytes(data, filepath.Join(dataDir, "fcm_tokens.json"))
	if err != nil {
		return nil, fmt.Errorf("runtime service account is invalid")
	}
	return notifier, nil
}
