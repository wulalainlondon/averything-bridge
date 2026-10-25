package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"everything-go/internal/clientproto"
	"everything-go/internal/executor"
	"everything-go/internal/inbox"
	"everything-go/internal/protocol"
	rt "everything-go/internal/runtime"
	"everything-go/internal/search"
	"everything-go/internal/session"
)

// --- usage ------------------------------------------------------------------

// usageRouter is the subset of the executor (the Mux) that can report usage.
type usageRouter interface {
	UsageFor(s *session.Session) (executor.UsageProvider, bool)
	AllUsageProviders() []executor.UsageProvider
}

// sendUsage fetches usage for one session's backend (when session_id is given)
// or for every distinct backend, emitting a usage_report per backend.
func (h *Hub) sendUsage(c *Client, sessionID string) {
	if !c.live() {
		return
	}
	ur, ok := h.exec.(usageRouter)
	if !ok {
		return
	}
	ctx := context.Background()

	if s, found := h.registry.Get(sessionID); found && sessionID != "" {
		if up, ok := ur.UsageFor(s); ok {
			h.emitUsage(c, ctx, up)
		}
		return
	}
	for _, up := range ur.AllUsageProviders() {
		h.emitUsage(c, ctx, up)
	}
}

func (h *Hub) emitUsage(c *Client, ctx context.Context, up executor.UsageProvider) {
	rep, err := up.FetchUsage(ctx)
	if err != nil || rep == nil {
		return
	}
	if !c.live() {
		return // client replaced/gone during the usage fetch (#3)
	}
	c.enqueueEvent(h.client.UsageReport(*rep))
}

// --- tasks ------------------------------------------------------------------

// collectTasks lists AI sessions (with their live pid, if any) plus shells.
func (h *Hub) collectTasks() []protocol.Task {
	pi, _ := h.exec.(executor.ProcInspector)
	var tasks []protocol.Task
	for _, s := range h.registry.List() {
		var pid *int
		if pi != nil {
			if p, ok := pi.PID(s); ok {
				pid = &p
			}
		}
		snap := s.Snapshot()
		tasks = append(tasks, protocol.Task{
			ID: snap.ID, Name: snap.Name, Type: snap.Backend, PID: pid,
			IsStreaming: snap.Streaming, Cwd: snap.Cwd,
		})
	}
	tasks = append(tasks, h.shells.Tasks()...)
	return tasks
}

// killTask kills an AI session's subprocess or a shell by id.
func (h *Hub) killTask(id string) bool {
	if s, ok := h.registry.Get(id); ok {
		if pi, ok := h.exec.(executor.ProcInspector); ok {
			return pi.KillProc(s)
		}
		return false
	}
	if h.shells.Has(id) {
		return h.shells.KillTask(id)
	}
	return false
}

// --- browse_dir -------------------------------------------------------------

// sendDirListing answers browse_dir in two stages, mirroring file_ops.py:
// (1) filesystem entries + active sessions, (2) enriched with resumable sessions.
func (h *Hub) sendDirListing(c *Client, cmd clientproto.Command) {
	if !c.live() {
		return
	}
	path, err := h.scopedPath(cmd.Path)
	if err != nil {
		// Parent/home navigation stays at the jail root, so older clients
		// cannot navigate above it even if they show a parent button.
		path = realpath(rt.ExpandPath(h.cfg.RootDir))
	}
	entries := rt.ListEntries(path)
	if h.cfg.RootDir != "" {
		visible := entries[:0]
		for _, entry := range entries {
			if h.cwdInScope(filepath.Join(path, entry.Name)) {
				visible = append(visible, entry)
			}
		}
		entries = visible
	}
	hash := rt.DirHash(entries)
	unchanged := cmd.ClientHash != "" && cmd.ClientHash == hash

	sendEntries := entries
	if unchanged {
		sendEntries = nil
	}

	// Stage 1: active sessions rooted at this path (cheap).
	active := h.activeSessionsForPath(path)
	c.enqueueEvent(protocol.NewDirListing(path, sendEntries, active, hash, unchanged))

	// Stage 2: enrich with resumable sessions for this path (heavy scan, coalesced
	// + cached). Re-check liveness before sending — a stale client gets nothing (#3).
	resumable := h.resumableForPath(path)
	if !c.live() {
		return
	}
	merged := append(append([]protocol.DirSession{}, active...), resumable...)
	c.enqueueEvent(protocol.NewDirListing(path, sendEntries, merged, hash, unchanged))
}

const maxPreviewFileBytes = 256 * 1024
const maxSaveFileBytes = 512 * 1024

var previewTextExtensions = map[string]bool{
	".c": true, ".cc": true, ".cpp": true, ".css": true, ".go": true,
	".h": true, ".hpp": true, ".java": true, ".js": true,
	".json": true, ".jsx": true, ".kt": true, ".log": true, ".md": true, ".markdown": true,
	".py": true, ".rb": true, ".rs": true, ".sh": true, ".sql": true,
	".swift": true, ".toml": true, ".ts": true, ".tsx": true, ".txt": true,
	".xml": true, ".yaml": true, ".yml": true,
}

// previewURLTypes maps extensions that are served via HTTP URL (not raw content).
// Value is the MIME type sent to the client.
var previewURLTypes = map[string]string{
	// images
	".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
	".gif": "image/gif", ".webp": "image/webp",
	// video
	".mp4": "video/mp4", ".mov": "video/quicktime", ".m4v": "video/mp4",
	".webm": "video/webm", ".avi": "video/x-msvideo",
	// audio
	".mp3": "audio/mpeg", ".wav": "audio/wav", ".aac": "audio/aac",
	".ogg": "audio/ogg", ".m4a": "audio/mp4", ".flac": "audio/flac",
	// documents rendered by browser
	".pdf":  "application/pdf",
	".html": "text/html", ".htm": "text/html",
}

func (h *Hub) sendFileOpened(c *Client, cmd clientproto.Command) {
	path, scopeErr := h.scopedPath(cmd.Path)
	if scopeErr != nil {
		c.enqueueEvent(protocol.NewFileOpened(cmd.Path, filepath.Base(cmd.Path), "", "", 0, "text/plain", scopeErr.Error()))
		return
	}
	name := filepath.Base(path)
	info, err := os.Stat(path)
	if err != nil {
		c.enqueueEvent(protocol.NewFileOpened(path, name, "", "", 0, "text/plain", err.Error()))
		return
	}
	if info.IsDir() {
		c.enqueueEvent(protocol.NewFileOpened(path, name, "", "", 0, "text/plain", "path is a directory"))
		return
	}
	ext := strings.ToLower(filepath.Ext(path))
	if mimeType, ok := previewURLTypes[ext]; ok {
		mediaURL := h.mediaScan.LocalURL(path)
		c.enqueueEvent(protocol.NewFileOpenedWithModified(path, name, "", mediaURL, info.Size(), info.ModTime().Unix(), mimeType, ""))
		return
	}
	if info.Size() > maxPreviewFileBytes {
		c.enqueueEvent(protocol.NewFileOpened(path, name, "", "", info.Size(), "text/plain", "file is too large to preview"))
		return
	}
	if !previewTextExtensions[ext] {
		c.enqueueEvent(protocol.NewFileOpened(path, name, "", "", info.Size(), "application/octet-stream", "preview supports text files only"))
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		c.enqueueEvent(protocol.NewFileOpened(path, name, "", "", info.Size(), "text/plain", err.Error()))
		return
	}
	c.enqueueEvent(protocol.NewFileOpenedWithModified(path, name, string(data), "", info.Size(), info.ModTime().Unix(), "text/plain; charset=utf-8", ""))
}

func (h *Hub) sendMarkdownFilesListing(c *Client, cmd clientproto.Command) {
	roots := []string{}
	files := []protocol.MarkdownFileEntry{}
	errors := []protocol.MarkdownScanError{}
	limit := cmd.Limit
	if limit <= 0 || limit > rt.MaxMarkdownScanFiles {
		limit = rt.MaxMarkdownScanFiles
	}
	for _, raw := range cmd.Paths {
		if strings.TrimSpace(raw) == "" || len(files) >= limit {
			continue
		}
		root, scopeErr := h.scopedPath(raw)
		if scopeErr != nil {
			errors = append(errors, protocol.MarkdownScanError{Path: raw, Error: scopeErr.Error()})
			continue
		}
		roots = append(roots, root)
		found, err := rt.ScanMarkdownFiles(root, limit-len(files))
		if err != nil {
			errors = append(errors, protocol.MarkdownScanError{Path: raw, Error: err.Error()})
			continue
		}
		for _, file := range found {
			if h.cwdInScope(file.Path) {
				files = append(files, file)
			}
		}
	}
	sort.SliceStable(files, func(i, j int) bool {
		if files[i].Modified != files[j].Modified {
			return files[i].Modified > files[j].Modified
		}
		return strings.ToLower(files[i].RelativePath) < strings.ToLower(files[j].RelativePath)
	})
	c.enqueueEvent(protocol.NewMarkdownFilesListing(roots, files, errors))
}

func (h *Hub) saveFile(c *Client, cmd clientproto.Command) {
	path, scopeErr := h.scopedPath(cmd.Path)
	if scopeErr != nil {
		c.enqueueEvent(protocol.NewFileSaved(cmd.Path, filepath.Base(cmd.Path), "", 0, 0, "text/plain", scopeErr.Error()))
		return
	}
	name := filepath.Base(path)
	saveErr := func(content string, size, modified int64, msg string) {
		c.enqueueEvent(protocol.NewFileSaved(path, name, content, size, modified, "text/plain; charset=utf-8", msg))
	}
	if len([]byte(cmd.Content)) > maxSaveFileBytes {
		saveErr("", 0, 0, "file is too large to save from preview")
		return
	}
	info, err := os.Stat(path)
	if err != nil {
		saveErr(cmd.Content, 0, 0, err.Error())
		return
	}
	if info.IsDir() {
		saveErr(cmd.Content, 0, 0, "path is a directory")
		return
	}
	if !rt.IsMarkdownPath(path) {
		saveErr(cmd.Content, info.Size(), info.ModTime().Unix(), "editing supports markdown files only")
		return
	}
	modified := info.ModTime().Unix()
	if cmd.ExpectedModified != nil && *cmd.ExpectedModified > 0 && *cmd.ExpectedModified != modified {
		saveErr(cmd.Content, info.Size(), modified, "file changed on disk; reopen before saving")
		return
	}
	if err := rt.WriteTextFileAtomic(path, cmd.Content); err != nil {
		saveErr(cmd.Content, info.Size(), modified, err.Error())
		return
	}
	updated, err := os.Stat(path)
	if err != nil {
		saveErr(cmd.Content, 0, 0, err.Error())
		return
	}
	c.enqueueEvent(protocol.NewFileSaved(path, name, cmd.Content, updated.Size(), updated.ModTime().Unix(), "text/plain; charset=utf-8", ""))
}

func (h *Hub) activeSessionsForPath(path string) []protocol.DirSession {
	out := []protocol.DirSession{}
	for _, s := range h.registry.List() {
		snap := s.Snapshot()
		if realpath(snap.Cwd) != path {
			continue
		}
		last := snap.LastActivity
		if last == 0 {
			last = snap.CreatedAt
		}
		out = append(out, protocol.DirSession{
			ID: snap.ID, Name: snap.Name, ClaudeUUID: snap.ResumeID,
			LastUsed: int64(last), Backend: snap.Backend, IsActive: true,
		})
	}
	return out
}

func (h *Hub) resumableForPath(path string) []protocol.DirSession {
	// Exclude sessions already live (by resume id).
	activeUUIDs := map[string]bool{}
	for _, s := range h.registry.List() {
		if rid := s.ResumeID(); rid != "" {
			activeUUIDs[rid] = true
		}
	}
	// Reuse the coalesced+cached resumable scan (#6/#7): the heavy provider scan
	// is path-independent, so browse_dir bursts across paths share one scan.
	var out []protocol.DirSession
	for _, r := range h.coalescedResumable(500) {
		if realpath(r.Cwd) != path || activeUUIDs[r.ClaudeUUID] {
			continue
		}
		out = append(out, protocol.DirSession{
			ID: r.ID, Name: r.Name, ClaudeUUID: r.ClaudeUUID,
			LastUsed: r.LastUsed, Backend: r.Backend, IsActive: false,
		})
	}
	return out
}

// --- file push inbox (push_file / file_push_ack / get_inbox) ----------------

// handlePushFile stores a local file and broadcasts its download metadata to every connected
// device, persisting it for recovery on hello. The sender receives push_ack;
// file_push carries a file-specific download URL, never the base64 body.
func (h *Hub) handlePushFile(c *Client, reqPath string) {
	if h.inbox == nil {
		return
	}
	abs := rt.ExpandPath(reqPath)
	item, err := h.inbox.Push(abs, c.deviceID, h.connectedDeviceIDs(c.deviceID))
	if err != nil {
		c.enqueueEvent(protocol.NewError("", "", err.Error()))
		return
	}
	log.Printf("[push] file=%s id=%s size=%d → %d device(s)", item.Filename, item.FileID, item.Size, len(h.connectedDeviceIDs(c.deviceID)))
	c.enqueueEvent(protocol.NewPushAck(item.FileID, item.Filename, item.Size))
	prepared, err := h.inbox.DownloadItem(item)
	if err != nil {
		c.enqueueEvent(protocol.NewError("", "", "File download unavailable: "+err.Error()))
		return
	}
	h.mu.RLock()
	clients := make([]*Client, 0, len(h.clients))
	for client := range h.clients {
		clients = append(clients, client)
	}
	h.mu.RUnlock()
	for _, client := range clients {
		wire := h.inboxWireItem(client, prepared)
		client.enqueueEvent(protocol.NewFilePush(wire.FileID, wire.Filename, wire.URL, "", wire.Size, wire.MimeType))
	}
	if h.fcm != nil {
		go h.fcm.NotifyFilePush(item.FileID, item.Filename)
	}
}

// handleFilePushAck records a device's receipt of a pushed file; the inbox
// drops the entry once every target device has acked. Mirrors
// push_registry.handle_file_push_ack (minus the Storage blob delete).
func (h *Hub) handleFilePushAck(fileID, deviceID string) {
	if h.inbox == nil {
		return
	}
	if h.inbox.Ack(fileID, deviceID) {
		log.Printf("[push] file_push_ack id=%s device=%s → all targets acked, entry dropped", fileID, deviceID)
	}
}

// sendPendingPushes replays the un-acked file pushes targeted at this client's
// device as individual file_push frames, mirroring connection.py's hello path.
func (h *Hub) sendPendingPushes(c *Client) {
	if h.inbox == nil {
		return
	}
	for _, it := range h.inbox.Pending(c.deviceID) {
		if !c.live() {
			return
		}
		prepared, err := h.inbox.DownloadItem(it)
		if err != nil {
			c.enqueueEvent(protocol.NewError("", "", "File download unavailable: "+err.Error()))
			continue
		}
		wire := h.inboxWireItem(c, prepared)
		c.enqueueEvent(protocol.NewFilePush(wire.FileID, wire.Filename, wire.URL, "", wire.Size, wire.MimeType))
	}
}

// inboxItems builds the inbox_list reply (get_inbox), which—unlike the hello
// replay—includes pushed_at on each item.
func (h *Hub) inboxItems(c *Client) []protocol.InboxItem {
	if h.inbox == nil {
		return nil
	}
	pending := h.inbox.Pending(c.deviceID)
	out := make([]protocol.InboxItem, 0, len(pending))
	for _, it := range pending {
		prepared, err := h.inbox.DownloadItem(it)
		if err != nil {
			c.enqueueEvent(protocol.NewError("", "", "File download unavailable: "+err.Error()))
			continue
		}
		out = append(out, h.inboxWireItem(c, prepared))
	}
	return out
}

func (h *Hub) inboxWireItem(c *Client, item inbox.Item) protocol.InboxItem {
	remote := item.URL
	if strings.HasPrefix(remote, inbox.DownloadPrefix) {
		origin := c.downloadOrigin
		if origin == "" {
			host := h.cfg.LanIP
			if host == "" {
				host = "127.0.0.1"
			}
			origin = "http://" + net.JoinHostPort(host, fmt.Sprint(h.cfg.Port))
		}
		remote = strings.TrimRight(origin, "/") + remote
	}
	return protocol.InboxItem{FileID: item.FileID, Filename: item.Filename, URL: remote,
		Size: item.Size, MimeType: item.MimeType, PushedAt: item.PushedAt}
}

// Metadata can still accumulate for a long-lived desktop inbox. Keep every
// list message bounded; existing clients merge inbox_list items by file ID.
const inboxListMaxBytes = 48 * 1024 // also fits the WebRTC DataChannel message limit

func (h *Hub) sendInbox(c *Client) {
	items := h.inboxItems(c)
	batch := []protocol.InboxItem{}
	size := 64 // envelope, brackets and separators
	for _, item := range items {
		raw, err := json.Marshal(item)
		if err != nil || len(raw)+64 > inboxListMaxBytes {
			c.enqueueEvent(protocol.NewError("", "", "File metadata exceeds inbox message limit"))
			continue
		}
		if size+len(raw)+1 > inboxListMaxBytes {
			c.enqueueEvent(h.client.InboxListItems(batch))
			batch = []protocol.InboxItem{}
			size = 64
		}
		batch = append(batch, item)
		size += len(raw) + 1
	}
	if len(batch) > 0 || len(items) == 0 {
		c.enqueueEvent(h.client.InboxListItems(batch))
	}
}

func (h *Hub) ServeInboxDownload(w http.ResponseWriter, r *http.Request) {
	if h.inbox == nil {
		http.NotFound(w, r)
		return
	}
	h.inbox.DownloadHandler().ServeHTTP(w, r)
}

// realpath resolves symlinks like os.path.realpath; falls back to the absolute
// cleaned path so comparison still works when the dir doesn't exist.
func realpath(p string) string {
	if p == "" {
		return ""
	}
	if resolved, err := filepath.EvalSymlinks(p); err == nil {
		return resolved
	}
	if abs, err := filepath.Abs(p); err == nil {
		return abs
	}
	return filepath.Clean(p)
}

// --- search -----------------------------------------------------------------

// sendSearch maps the inbound request_search frame to a search.Search call.
func (h *Hub) sendSearch(c *Client, cmd clientproto.Command) {
	if h.search == nil || !c.live() {
		return
	}
	f := search.Filters{MaxPerSession: 3}
	if cmd.Filters != nil {
		f = search.Filters{
			ProjectDir:       cmd.Filters.ProjectDir,
			Since:            cmd.Filters.Since,
			Role:             cmd.Filters.Role,
			ExcludeSubagents: cmd.Filters.ExcludeSubagents,
			Source:           cmd.Filters.Source,
			MaxPerSession:    cmd.Filters.MaxPerSession,
		}
		if f.MaxPerSession <= 0 {
			f.MaxPerSession = 3
		}
	}
	limit := clampLimit(cmd.Limit, 50)
	offset := cmd.Offset
	if offset < 0 {
		offset = 0
	}
	res := h.search.Search(cmd.Query, f, limit, offset)
	if !c.live() {
		return // client replaced/gone during the query (#3)
	}
	c.enqueueEvent(res)
}

// clampLimit applies a default when limit is unset and bounds it to [1, 500].
func clampLimit(limit, def int) int {
	if limit <= 0 {
		return def
	}
	if limit > 500 {
		return 500
	}
	return limit
}

// --- status -----------------------------------------------------------------

func (h *Hub) statusResult(sessionID string) protocol.StatusResult {
	sessions := h.registry.List()
	streaming, queued := 0, 0
	for _, s := range sessions {
		if s.IsStreaming() {
			streaming++
		}
		queued += s.QueueLen()
	}
	status := map[string]any{
		"server_time_ms":     time.Now().UnixMilli(),
		"platform":           runtime.GOOS + "/" + runtime.GOARCH,
		"go_version":         runtime.Version(),
		"sessions_total":     len(sessions),
		"sessions_streaming": streaming,
		"queued_commands":    queued,
		"permission_mode":    "enforce",
	}
	if provider, ok := h.exec.(interface{ RuntimeDiagnostics() map[string]any }); ok {
		if diagnostics := provider.RuntimeDiagnostics(); len(diagnostics) > 0 {
			status["backend_runtimes"] = diagnostics
		}
	}
	return protocol.StatusResult{
		Type:      "status_result",
		SessionID: sessionID,
		Status:    status,
	}
}
