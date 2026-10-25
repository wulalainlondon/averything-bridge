//go:build unix

package goexec

import (
	"os"
	"syscall"
)

// A replaced FIFO must not block before the regular-file/inode recheck.
func openExactRegular(root *os.Root, path string) (*os.File, error) {
	return root.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
}
