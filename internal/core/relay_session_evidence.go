package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"everything-go/internal/attachmentjournal"
	"everything-go/internal/relay"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func (h *Hub) materializeRelaySessionEvidence(ctx context.Context, record relay.Record) (string, error) {
	if len(record.Job.Attachments) == 0 {
		return "", nil
	}
	peer, ok := h.relayPeers[record.Job.OriginInstanceID]
	if !ok {
		return "", errors.New("origin unavailable")
	}
	evidence := "\n[Verified local evidence]\n"

	for _, descriptor := range record.Job.Attachments {

		path := "/api/relay/v1/blobs/" + descriptor.ID
		resp, err := h.relayRequest(ctx, peer, http.MethodGet, path, nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			return "", errors.New("blob unavailable")
		}
		targetDir := filepath.Join(h.cfg.DataDir, "relay-blobs")
		_ = os.MkdirAll(targetDir, 0o700)
		targetPath := filepath.Join(targetDir, descriptor.SHA256+extensionForMIME(descriptor.MIMEType))
		tmp, err := os.CreateTemp(targetDir, ".relay-*.part")
		if err != nil {
			resp.Body.Close()
			return "", err
		}
		hash := sha256.New()
		n, copyErr := io.Copy(io.MultiWriter(tmp, hash), io.LimitReader(resp.Body, maxExternalAttachmentBytes+1))
		resp.Body.Close()
		closeErr := tmp.Close()
		digest := hex.EncodeToString(hash.Sum(nil))
		if copyErr != nil || closeErr != nil || n != descriptor.SizeBytes || digest != descriptor.SHA256 {
			_ = os.Remove(tmp.Name())
			return "", errors.New("blob integrity mismatch")
		}
		if _, err := os.Stat(targetPath); errors.Is(err, os.ErrNotExist) {
			if err := os.Rename(tmp.Name(), targetPath); err != nil {
				_ = os.Remove(tmp.Name())
				return "", err
			}
		} else {
			_ = os.Remove(tmp.Name())
		}
		kind := "media"
		mediaType := "image"
		if strings.HasPrefix(descriptor.MIMEType, "video/") {
			mediaType = "video"
		} else if !strings.HasPrefix(descriptor.MIMEType, "image/") {
			kind, mediaType = "document", ""
		}
		canonical, _ := h.attachments.AddCanonical(attachmentjournal.Record{SessionID: record.Job.TargetSessionID,
			RequestID: record.Job.ID, Kind: kind, Path: targetPath, MIMEType: descriptor.MIMEType,
			DisplayName: descriptor.DisplayName, MediaType: mediaType})
		if canonical.AttachmentID == "" {
			return "", errors.New("canonical relay attachment rejected")
		}
		evidence += descriptor.DisplayName + ": " + targetPath + " (sha256 " + descriptor.SHA256 + ")\n"
	}
	return evidence, nil
}
