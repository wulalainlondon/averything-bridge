package history

import (
	"errors"
	"fmt"
	"io"
	"os"
)

var ErrPermissionDenied = errors.New("history_permission_denied")

// AccessError preserves the OS error for callers while giving the UI a stable
// distinction between an empty transcript and a source it cannot read.
type AccessError struct {
	Backend string
	Path    string
	Err     error
}

func (e *AccessError) Error() string {
	if os.IsPermission(e.Err) {
		return fmt.Sprintf("history_permission_denied: %s history at %s; grant access to this history source again", e.Backend, e.Path)
	}
	return fmt.Sprintf("history_read_failed: %s history at %s: %v", e.Backend, e.Path, e.Err)
}

func (e *AccessError) Unwrap() error { return e.Err }
func (e *AccessError) Is(target error) bool {
	return target == ErrPermissionDenied && os.IsPermission(e.Err)
}

func SourceError(backend, path string, err error) error {
	if err == nil {
		return nil
	}
	return &AccessError{Backend: backend, Path: path, Err: err}
}

// CheckDirectory also checks enumeration permission, including when a previous
// successful scan is cached. A source that has not been created yet is empty.
func CheckDirectory(backend, path string) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return SourceError(backend, path, err)
	}
	defer f.Close()
	_, err = f.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return SourceError(backend, path, err)
}

// CheckFile must run before serving a cached transcript after access changes.
func CheckFile(backend, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return SourceError(backend, path, err)
	}
	return f.Close()
}

func ErrorCode(err error) string {
	if errors.Is(err, ErrPermissionDenied) || os.IsPermission(err) {
		return "history_permission_denied"
	}
	return "history_read_failed"
}
