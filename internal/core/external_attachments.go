package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"everything-go/internal/eventinbox"
)

const (
	maxExternalAttachmentBytes = int64(20 * 1024 * 1024)
	attachmentFetchLease       = 2 * time.Minute
)

func (h *Hub) drainExternalAttachmentQueue(ctx context.Context) {
	if h.events == nil || h.cfg.DataDir == "" {
		return
	}
	owner := h.cfg.InstanceID + ":" + h.gen
	for count := 0; count < 2; count++ {
		job, ok, err := h.events.ClaimAttachmentFetch(ctx, owner, time.Now().UnixMilli(), attachmentFetchLease)
		if err != nil || !ok {
			return
		}
		path, mimeType, digest, size, fetchErr := h.fetchExternalAttachment(ctx, job)
		if fetchErr != nil {
			terminal := job.Attempt >= 5 || errors.Is(fetchErr, errUnsafeAttachmentURL) || errors.Is(fetchErr, errUnsupportedAttachment)
			delay := time.Duration(job.Attempt*job.Attempt) * 15 * time.Second
			_ = h.events.FailAttachmentFetch(ctx, job.ID, owner, attachmentErrorCode(fetchErr), time.Now().Add(delay).UnixMilli(), terminal)
			continue
		}
		if err := h.events.CompleteAttachmentFetch(ctx, job.ID, owner, path, mimeType, digest, size); err != nil {
			_ = os.Remove(path)
			return
		}
		if event, getErr := h.events.Get(ctx, job.EventID); getErr == nil {
			_ = h.publishExternalEventUpdate(event)
		}
	}
}

var (
	errUnsafeAttachmentURL   = errors.New("unsafe_attachment_url")
	errUnsupportedAttachment = errors.New("unsupported_attachment")
)

func (h *Hub) fetchExternalAttachment(ctx context.Context, job eventinbox.AttachmentFetch) (string, string, string, int64, error) {
	parsed, err := url.Parse(job.SourceURL)
	if err != nil || errSafePublicHTTPS(ctx, parsed) != nil {
		return "", "", "", 0, errUnsafeAttachmentURL
	}
	client := &http.Client{Timeout: 45 * time.Second}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 || errSafePublicHTTPS(req.Context(), req.URL) != nil {
			return errUnsafeAttachmentURL
		}
		return nil
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	resp, err := client.Do(req)
	if err != nil {
		return "", "", "", 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", "", "", 0, fmt.Errorf("http_%d", resp.StatusCode)
	}
	if resp.ContentLength > maxExternalAttachmentBytes {
		return "", "", "", 0, errUnsupportedAttachment
	}
	declared := strings.ToLower(strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0]))
	if declared == "" {
		declared = strings.ToLower(strings.TrimSpace(strings.Split(job.MIMEType, ";")[0]))
	}
	if !allowedExternalMIME(declared) {
		return "", "", "", 0, errUnsupportedAttachment
	}
	dir := filepath.Join(h.cfg.DataDir, "external-event-blobs")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", "", "", 0, err
	}
	tmp, err := os.CreateTemp(dir, ".fetch-*.part")
	if err != nil {
		return "", "", "", 0, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	hash := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(tmp, hash), io.LimitReader(resp.Body, maxExternalAttachmentBytes+1))
	syncErr, closeErr := tmp.Sync(), tmp.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil || n > maxExternalAttachmentBytes {
		return "", "", "", 0, errUnsupportedAttachment
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	ext := extensionForMIME(declared)
	finalPath := filepath.Join(dir, digest+ext)
	if _, statErr := os.Stat(finalPath); statErr == nil {
		return finalPath, declared, digest, n, nil
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return "", "", "", 0, err
	}
	return finalPath, declared, digest, n, nil
}

func errSafePublicHTTPS(ctx context.Context, parsed *url.URL) error {
	if parsed == nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil {
		return errUnsafeAttachmentURL
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, parsed.Hostname())
	if err != nil || len(addresses) == 0 {
		return errUnsafeAttachmentURL
	}
	for _, address := range addresses {
		ip := address.IP
		if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
			return errUnsafeAttachmentURL
		}
	}
	return nil
}

func allowedExternalMIME(value string) bool {
	switch value {
	case "image/jpeg", "image/png", "image/gif", "image/webp", "video/mp4", "video/quicktime", "audio/mpeg", "audio/mp4", "application/pdf":
		return true
	default:
		return false
	}
}

func extensionForMIME(value string) string {
	switch value {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "video/mp4":
		return ".mp4"
	case "video/quicktime":
		return ".mov"
	case "audio/mpeg":
		return ".mp3"
	case "audio/mp4":
		return ".m4a"
	case "application/pdf":
		return ".pdf"
	default:
		return ".bin"
	}
}

func attachmentErrorCode(err error) string {
	value := strings.Trim(strings.ToLower(strings.ReplaceAll(err.Error(), " ", "_")), "_")
	if len(value) > 80 {
		value = value[:80]
	}
	return value
}
