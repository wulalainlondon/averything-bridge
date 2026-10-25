//go:build unix

package handoff

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestHandoffFIFOAndUnreadableFailWithoutBlocking(t *testing.T) {
	p, _ := fixture(t)
	os.Remove(filepath.Join(p.Workspace, p.Path))
	if e := syscall.Mkfifo(filepath.Join(p.Workspace, p.Path), 0600); e != nil {
		t.Fatal(e)
	}
	if s := Read(p, p.Workspace, testNow); s.Reason != "file_not_regular" {
		t.Fatal(s)
	}
	os.Remove(filepath.Join(p.Workspace, p.Path))
	os.WriteFile(filepath.Join(p.Workspace, p.Path), []byte("private"), 0000)
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file mode permissions")
	}
	if s := Read(p, p.Workspace, testNow); s.Reason != "file_unreadable" {
		t.Fatal(s)
	}
}
