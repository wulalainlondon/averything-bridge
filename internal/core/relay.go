package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"everything-go/internal/attachmentjournal"
	"everything-go/internal/backend"
	"everything-go/internal/relay"
	"everything-go/internal/workitems"
)

const relayLease = 2 * time.Minute

func (h *Hub) StartRelayScheduler(ctx context.Context) {
	if h.relay == nil || !h.relayScheduler.CompareAndSwap(false, true) {
		return
	}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			h.drainRelayOutbound(ctx)
			h.drainRelayInbound(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			case <-h.relayWake:
			}
		}
	}()
}

func (h *Hub) WakeRelayScheduler() {
	select {
	case h.relayWake <- struct{}{}:
	default:
	}
}

func (h *Hub) ServeRelayAPI(w http.ResponseWriter, r *http.Request) {
	if h.relay == nil {
		http.Error(w, "relay unavailable", http.StatusServiceUnavailable)
		return
	}
	body := []byte(nil)
	if r.Method == http.MethodPost {
		var err error
		body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, 128*1024))
		if err != nil {
			http.Error(w, "invalid relay body", http.StatusBadRequest)
			return
		}
	}
	origin, ok := h.authorizeRelayRequest(r, body)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/relay/v1/"), "/")
	parts := strings.Split(path, "/")
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodPost && path == "jobs":
		var job relay.Job
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&job) != nil || decoder.Decode(&struct{}{}) != io.EOF || job.OriginInstanceID != origin || job.TargetInstanceID != h.cfg.InstanceID {
			http.Error(w, "invalid relay job", http.StatusBadRequest)
			return
		}
		created, err := h.relay.AcceptInbound(r.Context(), job)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		h.WakeRelayScheduler()
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted", "deduplicated": !created, "relay_job_id": job.ID})
	case r.Method == http.MethodGet && len(parts) == 2 && parts[0] == "jobs":
		record, err := h.relay.Get(r.Context(), parts[1], "inbound")
		if err != nil || record.Job.OriginInstanceID != origin {
			http.Error(w, "relay job not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(record)
	case r.Method == http.MethodGet && len(parts) == 2 && parts[0] == "blobs":
		if !h.relay.CanPeerReadAttachment(r.Context(), origin, parts[1]) {
			http.Error(w, "relay blob not found", http.StatusNotFound)
			return
		}
		attachment, err := h.events.GetAttachment(r.Context(), parts[1])
		if err != nil || attachment.Status != "available" || attachment.LocalPath == "" {
			http.Error(w, "relay blob not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", attachment.MIMEType)
		w.Header().Set("X-Content-SHA256", attachment.SHA256)
		http.ServeFile(w, r, attachment.LocalPath)
	default:
		http.Error(w, "relay path not found", http.StatusNotFound)
	}
}

func (h *Hub) authorizeRelayRequest(r *http.Request, body []byte) (string, bool) {
	if !relayRemoteAllowed(r.RemoteAddr) {
		return "", false
	}
	origin := strings.TrimSpace(r.Header.Get("X-Bridge-Relay-Instance"))
	peer, ok := h.relayPeers[origin]
	if !ok {
		return "", false
	}
	secret, err := peer.Secret()
	if err != nil {
		return "", false
	}
	headers := map[string]string{"timestamp": r.Header.Get("X-Bridge-Relay-Timestamp"),
		"nonce": r.Header.Get("X-Bridge-Relay-Nonce"), "signature": r.Header.Get("X-Bridge-Relay-Signature")}
	if relay.Verify(secret, r.Method, r.URL.Path, body, headers, time.Now()) != nil {
		return "", false
	}
	nonceKey := origin + ":" + headers["nonce"]
	h.relayNonceMu.Lock()
	defer h.relayNonceMu.Unlock()
	now := time.Now().Unix()
	for key, expires := range h.relayNonces {
		if expires < now {
			delete(h.relayNonces, key)
		}
	}
	if h.relayNonces[nonceKey] >= now {
		return "", false
	}
	h.relayNonces[nonceKey] = now + 600
	return origin, true
}

func relayRemoteAllowed(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil {
		return false
	}
	if ip.IsLoopback() {
		return strings.TrimSpace(os.Getenv("BRIDGE_RELAY_ALLOW_LOOPBACK")) == "1"
	}
	if v4 := ip.To4(); v4 != nil {
		return v4[0] == 100 && v4[1]&0xc0 == 0x40 // 100.64.0.0/10
	}
	return strings.HasPrefix(strings.ToLower(ip.String()), "fd7a:115c:a1e0:")
}

func (h *Hub) relayRequest(ctx context.Context, peer relay.Peer, method, path string, body []byte) (*http.Response, error) {
	secret, err := peer.Secret()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, peer.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for key, value := range relay.Sign(secret, h.cfg.InstanceID, method, path, body, time.Now()) {
		req.Header.Set(key, value)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 45 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return errors.New("relay redirects are forbidden")
	}}
	return client.Do(req)
}

func (h *Hub) drainRelayOutbound(ctx context.Context) {
	record, ok, err := h.relay.Claim(ctx, "outbound", h.cfg.InstanceID+":"+h.gen, time.Now().UnixMilli(), relayLease)
	if err != nil || !ok {
		return
	}
	peer, exists := h.relayPeers[record.Job.TargetInstanceID]
	if !exists {
		_ = h.relay.Update(ctx, record.Job.ID, "outbound", "failed", "target_peer_unavailable", "", "", "", 0)
		return
	}
	if record.Reason == "remote_accepted" {
		h.pollRelayOutbound(ctx, record, peer)
		return
	}
	body, _ := json.Marshal(record.Job)
	resp, err := h.relayRequest(ctx, peer, http.MethodPost, "/api/relay/v1/jobs", body)
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp != nil {
			resp.Body.Close()
		}
		h.retryRelay(record, "target_unreachable")
		return
	}
	resp.Body.Close()
	_ = h.relay.Update(ctx, record.Job.ID, "outbound", "polling", "remote_accepted", "", "", "", time.Now().Add(5*time.Second).UnixMilli())
}

func (h *Hub) pollRelayOutbound(ctx context.Context, record relay.Record, peer relay.Peer) {
	path := "/api/relay/v1/jobs/" + record.Job.ID
	resp, err := h.relayRequest(ctx, peer, http.MethodGet, path, nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		h.retryRelay(record, "status_unreachable")
		return
	}
	defer resp.Body.Close()
	var remote relay.Record
	if json.NewDecoder(io.LimitReader(resp.Body, 256*1024)).Decode(&remote) != nil {
		h.retryRelay(record, "invalid_status")
		return
	}
	terminal := remote.Status == "review_ready" || remote.Status == "failed" || remote.Status == "expired"
	if terminal {
		_ = h.relay.Update(ctx, record.Job.ID, "outbound", remote.Status, remote.Reason, remote.RunID, remote.RequestID, remote.Result, 0)
		_, _ = h.automation.CompleteRelay(ctx, record.Job.ID, remote.Status, remote.Reason, remote.Result)
		h.broadcastAutomationSnapshot()
		return
	}
	if changed, _ := h.automation.SetRelayProgress(ctx, record.Job.ID, remote.Status, remote.Reason); changed {
		h.broadcastAutomationSnapshot()
	}
	_ = h.relay.Update(ctx, record.Job.ID, "outbound", "polling", "remote_accepted", remote.RunID, remote.RequestID, remote.Result, time.Now().Add(5*time.Second).UnixMilli())
}

func (h *Hub) retryRelay(record relay.Record, reason string) {
	if record.Attempt >= 8 {
		_ = h.relay.Update(context.Background(), record.Job.ID, record.Direction, "failed", reason, record.RunID, record.RequestID, record.Result, 0)
		return
	}
	delay := time.Duration(record.Attempt*record.Attempt) * 5 * time.Second
	status := "pending"
	if record.Reason == "remote_accepted" {
		status = "polling"
	}
	_ = h.relay.Update(context.Background(), record.Job.ID, record.Direction, status, record.Reason, record.RunID, record.RequestID, record.Result, time.Now().Add(delay).UnixMilli())
}

func (h *Hub) drainRelayInbound(ctx context.Context) {
	record, ok, err := h.relay.Claim(ctx, "inbound", h.cfg.InstanceID+":"+h.gen, time.Now().UnixMilli(), relayLease)
	if err != nil || !ok {
		return
	}
	if record.Job.ExpiresAt > 0 && record.Job.ExpiresAt <= time.Now().UnixMilli() {
		_ = h.relay.Update(ctx, record.Job.ID, "inbound", "expired", "job_expired", "", "", "", 0)
		return
	}
	sess, exists := h.registry.Get(record.Job.TargetSessionID)
	if !exists {
		h.deferRelayInbound(record, "target_session_unavailable", time.Minute)
		return
	}
	if snap := sess.Snapshot(); record.Job.ReviewOnly && snap.Sandbox != "read-only" && snap.Backend != backend.Codex {
		h.deferRelayInbound(record, "target_session_not_read_only", time.Minute)
		return
	}
	item, err := h.work.GetItem(ctx, record.Job.TargetWorkItemID)
	if err != nil || (item.Lifecycle != workitems.LifecycleReady && item.Lifecycle != workitems.LifecycleActive) {
		h.deferRelayInbound(record, "target_work_item_not_ready", 30*time.Second)
		return
	}
	if err := h.materializeRelayAttachments(ctx, record, &item); err != nil {
		h.deferRelayInbound(record, "attachment_transfer_failed", 30*time.Second)
		return
	}
	runID, requestID := "rrun_"+shortRelayID(record.Job.ID), record.Job.ID
	if existing, found := h.findWorkRun(ctx, runID); found {
		_ = h.relay.Update(ctx, record.Job.ID, "inbound", existing.Status, "", runID, existing.RequestID, "", 0)
		return
	}
	run, updated, err := h.work.StartRun(ctx, workitems.StartRunInput{ID: runID, WorkItemID: item.ID,
		SessionID: record.Job.TargetSessionID, RequestID: requestID, Kind: "research",
		Instruction: relayReviewInstruction(record.Job), ExpectedVersion: item.Version,
		Actor: workitems.Actor{Type: workitems.ActorSystem}})
	if err != nil {
		h.deferRelayInbound(record, "run_create_failed", 30*time.Second)
		return
	}
	_ = h.relay.Update(ctx, record.Job.ID, "inbound", "queued", "", run.ID, run.RequestID, "", 0)
	h.broadcastWorkRevision(updated.ActivityRevision)
	h.WakeWorkScheduler()
}

func (h *Hub) deferRelayInbound(record relay.Record, reason string, delay time.Duration) {
	_ = h.relay.Update(context.Background(), record.Job.ID, "inbound", "accepted", reason, "", "", "", time.Now().Add(delay).UnixMilli())
}

func (h *Hub) materializeRelayAttachments(ctx context.Context, record relay.Record, item *workitems.WorkItem) error {
	if len(record.Job.Attachments) == 0 {
		return nil
	}
	peer, ok := h.relayPeers[record.Job.OriginInstanceID]
	if !ok {
		return errors.New("origin peer unavailable")
	}
	pack, _ := h.work.BuildContextPack(ctx, item.ID, 1000)
	existing := map[string]bool{}
	for _, ref := range pack.Attachments {
		existing[ref.ID] = true
	}
	for _, descriptor := range record.Job.Attachments {
		refID := "wra_" + shortRelayID(descriptor.ID)
		if existing[refID] {
			continue
		}
		path := "/api/relay/v1/blobs/" + descriptor.ID
		resp, err := h.relayRequest(ctx, peer, http.MethodGet, path, nil)
		if err != nil || resp.StatusCode != http.StatusOK {
			if resp != nil {
				resp.Body.Close()
			}
			return errors.New("blob unavailable")
		}
		targetDir := filepath.Join(h.cfg.DataDir, "relay-blobs")
		_ = os.MkdirAll(targetDir, 0o700)
		targetPath := filepath.Join(targetDir, descriptor.SHA256+extensionForMIME(descriptor.MIMEType))
		tmp, err := os.CreateTemp(targetDir, ".relay-*.part")
		if err != nil {
			resp.Body.Close()
			return err
		}
		hash := sha256.New()
		n, copyErr := io.Copy(io.MultiWriter(tmp, hash), io.LimitReader(resp.Body, maxExternalAttachmentBytes+1))
		resp.Body.Close()
		closeErr := tmp.Close()
		digest := hex.EncodeToString(hash.Sum(nil))
		if copyErr != nil || closeErr != nil || n != descriptor.SizeBytes || digest != descriptor.SHA256 {
			_ = os.Remove(tmp.Name())
			return errors.New("blob integrity mismatch")
		}
		if _, err := os.Stat(targetPath); errors.Is(err, os.ErrNotExist) {
			if err := os.Rename(tmp.Name(), targetPath); err != nil {
				_ = os.Remove(tmp.Name())
				return err
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
			return errors.New("canonical relay attachment rejected")
		}
		found, newlyPinned := h.attachments.Pin(canonical.AttachmentID, item.ID)
		if !found {
			return errors.New("canonical relay attachment unavailable")
		}
		ref, updated, addErr := h.work.AddAttachment(ctx, workitems.AddAttachmentInput{ID: refID,
			WorkItemID: item.ID, AttachmentID: canonical.AttachmentID, DisplayName: descriptor.DisplayName,
			SortKey: int64(descriptor.Ordinal+1) * 1024, ExpectedVersion: item.Version,
			Actor: workitems.Actor{Type: workitems.ActorSystem}})
		_ = ref
		if addErr != nil {
			if newlyPinned {
				h.attachments.Unpin(canonical.AttachmentID, item.ID)
			}
			return addErr
		}
		*item = updated
	}
	return nil
}

func (h *Hub) findWorkRun(ctx context.Context, runID string) (workitems.Run, bool) {
	snapshot, err := h.work.Snapshot(ctx)
	if err != nil {
		return workitems.Run{}, false
	}
	for _, run := range snapshot.Runs {
		if run.ID == runID {
			return run, true
		}
	}
	return workitems.Run{}, false
}

func relayReviewInstruction(job relay.Job) string {
	return `[Bridge relay policy - trusted]
This is a review-only external player report. Analyze evidence and report recommended handling. Do not modify project files, game data, external services, or send a message. Record findings with the Bridge Work API and request human review.

` + job.Instruction
}

func shortRelayID(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:10])
}

func (h *Hub) completeRelayRun(requestID, status, reason, result string) {
	if h.relay == nil || requestID == "" {
		return
	}
	if _, changed, _ := h.relay.CompleteInboundByRequest(context.Background(), requestID, status, reason, result); changed {
		h.WakeRelayScheduler()
	}
}
