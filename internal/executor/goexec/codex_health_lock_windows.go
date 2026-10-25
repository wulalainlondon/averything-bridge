//go:build windows

package goexec

import "fmt"

func lockCodexRecovery(path string) (func(), error) {
	return nil, fmt.Errorf("automatic daemon recovery is unsupported on Windows; use managed restart")
}
