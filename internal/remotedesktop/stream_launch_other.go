//go:build !darwin

package remotedesktop

import (
	"context"
	"io"
	"os/exec"
)

func openStream(ctx context.Context, helperPath, _ string) (io.ReadCloser, func(), error) {
	cmd := exec.CommandContext(ctx, helperPath)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	return pipe, func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }, nil
}
