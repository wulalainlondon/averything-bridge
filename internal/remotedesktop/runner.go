package remotedesktop

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
)

// Helper runs the signed, one-shot macOS helper. It owns no network socket and
// never writes a captured frame to disk.
type Helper struct {
	Path string
}

func (h Helper) run(ctx context.Context, args ...string) ([]byte, error) {
	if h.Path == "" {
		return nil, errors.New("remote desktop helper not configured")
	}
	output, err := exec.CommandContext(ctx, h.Path, args...).Output()
	if err != nil {
		return nil, fmt.Errorf("remote desktop helper failed: %w", err)
	}
	return output, nil
}

func (h Helper) Status(ctx context.Context) (Availability, error) {
	output, err := h.run(ctx, "status")
	if err != nil {
		return Availability{}, err
	}
	var available Availability
	if err := json.Unmarshal(output, &available); err != nil {
		return Availability{}, err
	}
	return available, nil
}

func (h Helper) Capture(ctx context.Context) ([]byte, error) {
	return h.run(ctx, "capture")
}

func (h Helper) Click(ctx context.Context, x, y float64) error {
	return h.ClickButton(ctx, x, y, "left")
}

func (h Helper) ClickButton(ctx context.Context, x, y float64, button string) error {
	_, err := h.run(ctx, "click", strconv.FormatFloat(x, 'f', 8, 64), strconv.FormatFloat(y, 'f', 8, 64), button)
	return err
}

func (h Helper) Pointer(ctx context.Context, action string, x, y float64) error {
	_, err := h.run(ctx, "pointer", action, strconv.FormatFloat(x, 'f', 8, 64), strconv.FormatFloat(y, 'f', 8, 64))
	return err
}

func (h Helper) Text(ctx context.Context, value string) error {
	if h.Path == "" {
		return errors.New("remote desktop helper not configured")
	}
	cmd := exec.CommandContext(ctx, h.Path, "text")
	cmd.Stdin = bytes.NewBufferString(value)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("remote desktop text input failed: %w", err)
	}
	return nil
}

func (h Helper) Key(ctx context.Context, value string) error {
	_, err := h.run(ctx, "key", value)
	return err
}
