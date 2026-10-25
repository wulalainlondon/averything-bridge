//go:build unix

package handoff

import "syscall"

const safeReadFlags = syscall.O_NONBLOCK | syscall.O_NOFOLLOW
