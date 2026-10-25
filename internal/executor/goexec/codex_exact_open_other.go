//go:build !unix

package goexec

import "os"

func openExactRegular(root *os.Root, path string) (*os.File, error) {
	return nil, exactFinalError("final_revision_unsupported")
}
