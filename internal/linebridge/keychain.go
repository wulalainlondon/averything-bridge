package linebridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

func StoreKey(ctx context.Context, helper, dir string) ([]byte, error) {
	if !filepath.IsAbs(helper) || !filepath.IsAbs(dir) {
		return nil, Fail("line_keychain_configuration_invalid")
	}
	digest := sha256.Sum256([]byte(filepath.Join(dir, "operations")))
	service := "tw.wulala.linejs-lab." + hex.EncodeToString(digest[:])[:20]
	operation := "get"
	if _, err := os.Stat(filepath.Join(dir, "line.sqlite")); os.IsNotExist(err) {
		operation = "create"
	} else if err != nil {
		return nil, Fail("line_store_unavailable")
	}
	limited, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(limited, helper, operation, service)
	command.Stderr = io.Discard
	var output bytes.Buffer
	command.Stdout = &output
	if command.Run() != nil {
		return nil, Fail("line_keychain_unavailable")
	}
	key, err := base64.StdEncoding.DecodeString(output.String())
	if err != nil || len(key) != 32 {
		return nil, Fail("line_keychain_invalid")
	}
	return key, nil
}
