package history

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type sourceGenerationRecord struct {
	Version    int    `json:"version"`
	Identity   string `json:"identity"`
	Generation string `json:"generation"`
	Size       int64  `json:"size"`
	Mtime      int64  `json:"mtime_ns"`
}

// Tiny durable metadata is separate from the bulk, rebuildable parsed-history
// SQLite cache. A missing record creates a new random generation, never reuses
// a guessed generation after cache deletion. Native transcripts are read-only.
func retainedFileGeneration(f *os.File, path, identity string, snapshot os.FileInfo) (string, error) {
	current, e := f.Stat()
	if e != nil {
		return "", e
	}
	if current.Size() < snapshot.Size() || current.Size() == snapshot.Size() && current.ModTime() != snapshot.ModTime() {
		return "", fmt.Errorf("history file changed while indexing")
	}
	root := os.Getenv("EVERYTHING_GO_HISTORY_GENERATION_DIR")
	if root == "" {
		base, e := os.UserConfigDir()
		if e != nil {
			return "", e
		}
		root = filepath.Join(base, "everything-go", "history-source-generations-v1")
	}
	if e = os.MkdirAll(root, 0700); e != nil {
		return "", e
	}
	dirInfo, e := os.Lstat(root)
	if e != nil {
		return "", e
	}
	if !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("invalid history generation directory")
	}
	sum := sha256.Sum256([]byte(path))
	recordPath := filepath.Join(root, hex.EncodeToString(sum[:])+".json")
	lockPath := recordPath + ".lock"
	if info, e := os.Lstat(lockPath); e == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return "", fmt.Errorf("invalid generation lock")
	}
	lock, e := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return "", e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); e != nil {
		return "", e
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	// Refresh after taking the lock: another reader may have already indexed an
	// append. An older in-flight read must not label that append as a truncation.
	current, e = f.Stat()
	if e != nil {
		return "", e
	}
	if current.Size() < snapshot.Size() || current.Size() == snapshot.Size() && current.ModTime() != snapshot.ModTime() {
		return "", fmt.Errorf("history source changed while reading")
	}
	var previous sourceGenerationRecord
	exists := false
	if info, err := os.Lstat(recordPath); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 16384 {
			return "", fmt.Errorf("invalid history generation metadata")
		}
		data, err := os.ReadFile(recordPath)
		if err != nil {
			return "", err
		}
		if json.Unmarshal(data, &previous) != nil || previous.Version != 1 || previous.Generation == "" {
			return "", fmt.Errorf("history generation metadata requires recovery")
		}
		exists = true
	} else if !os.IsNotExist(err) {
		return "", err
	}
	generation := previous.Generation
	if !exists || previous.Identity != identity || current.Size() < previous.Size || current.Size() == previous.Size && current.ModTime().UnixNano() != previous.Mtime {
		var nonce [16]byte
		if _, e = io.ReadFull(rand.Reader, nonce[:]); e != nil {
			return "", e
		}
		generation = hex.EncodeToString(nonce[:])
	}
	next := sourceGenerationRecord{1, identity, generation, current.Size(), current.ModTime().UnixNano()}
	if exists && next == previous {
		return generation, nil
	}
	data, e := json.Marshal(next)
	if e != nil {
		return "", e
	}
	out, e := os.CreateTemp(root, ".generation-*")
	if e != nil {
		return "", e
	}
	defer os.Remove(out.Name())
	if e = out.Chmod(0600); e == nil {
		_, e = out.Write(data)
	}
	if e == nil {
		e = out.Sync()
	}
	closeErr := out.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return "", e
	}
	if e = os.Rename(out.Name(), recordPath); e != nil {
		return "", e
	}
	directory, e := os.Open(root)
	if e != nil {
		return "", e
	}
	defer directory.Close()
	if e = directory.Sync(); e != nil {
		return "", e
	}
	return generation, nil
}
