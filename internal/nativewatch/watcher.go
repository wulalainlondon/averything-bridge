// Package nativewatch discovers Claude/Codex sessions written by their native
// CLIs and reports lightweight metadata to the Go bridge registry.
package nativewatch

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"everything-go/internal/sourcepolicy"
	"github.com/fsnotify/fsnotify"
)

const (
	BackendClaude = "claude"
	BackendCodex  = "codex"
)

var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// NativeSession is the minimal metadata needed to surface an externally
// created native CLI session in the bridge dashboard.
type NativeSession struct {
	ID       string
	ResumeID string
	Backend  string
	Name     string
	Cwd      string
	LastUsed int64
	Path     string
}

type Options struct {
	ClaudeProjectsDir       string
	CodexSessionsDir        string
	CodexIgnoreCWDGlobs     []string
	CodexIgnoreNamePrefixes []string
	PollInterval            time.Duration
	Debounce                time.Duration
	InitialLookback         time.Duration
}

func DefaultOptions() Options {
	home, _ := os.UserHomeDir()
	claudeProjectsDir := strings.TrimSpace(os.Getenv("EVERYTHING_GO_CLAUDE_PROJECTS_DIR"))
	if claudeProjectsDir == "" {
		claudeProjectsDir = filepath.Join(home, ".claude", "projects")
	}
	return Options{
		ClaudeProjectsDir:       claudeProjectsDir,
		CodexSessionsDir:        sourcepolicy.CodexSessionsDir(),
		CodexIgnoreCWDGlobs:     sourcepolicy.CodexIgnoreCWDGlobs(),
		CodexIgnoreNamePrefixes: sourcepolicy.CodexIgnoreNamePrefixes(),
		PollInterval:            30 * time.Second,
		Debounce:                900 * time.Millisecond,
		InitialLookback:         30 * 24 * time.Hour,
	}
}

// Watch runs until ctx is cancelled. It scans recent sessions once at startup,
// then uses fsnotify for low-latency updates. If fsnotify cannot start, it
// falls back to polling.
func Watch(ctx context.Context, opts Options, onSession func(NativeSession)) {
	WatchChanges(ctx, opts, onSession, nil)
}

// WatchChanges is Watch with a notification that fires only for transcript
// changes observed after the initial import. This lets downstream indexers
// wake promptly without treating the startup catalog as thousands of edits.
func WatchChanges(ctx context.Context, opts Options, onSession func(NativeSession), onTranscriptChange func(string)) {
	if opts.PollInterval <= 0 {
		opts.PollInterval = 30 * time.Second
	}
	if opts.Debounce <= 0 {
		opts.Debounce = 900 * time.Millisecond
	}
	if opts.InitialLookback <= 0 {
		opts.InitialLookback = 30 * 24 * time.Hour
	}

	scanRecent(opts, onSession)
	onChange := func(session NativeSession) {
		onSession(session)
		if onTranscriptChange != nil {
			onTranscriptChange(session.Path)
		}
	}
	if usePolling() {
		// macOS fsnotify is backed by kqueue, which must hold an open file
		// descriptor for every file in every watched directory. Watching the
		// whole ~/.claude/projects + ~/.codex/sessions tree (thousands of
		// transcripts) opens and pins thousands of FDs for the process
		// lifetime — the dominant cause of the bridge's FD/RSS bloat. Polling
		// is stat-only and cheap; ~PollInterval latency is fine for surfacing
		// externally-created native sessions in the dashboard.
		poll(ctx, opts, onChange)
		return
	}
	if err := watchFS(ctx, opts, onChange); err != nil {
		log.Printf("[nativewatch] fsnotify unavailable (%v), falling back to polling", err)
		poll(ctx, opts, onChange)
	}
}

// usePolling reports whether to skip fsnotify and poll the trees instead.
// Defaults to polling on macOS — kqueue opens an FD per watched file, which
// exhausts and pins FDs on large history trees — and fsnotify elsewhere (Linux
// inotify watches per directory, not per file, so the tree is cheap to watch).
// Override either way with EVERYTHING_GO_NATIVEWATCH_MODE=poll|fsnotify.
func usePolling() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("EVERYTHING_GO_NATIVEWATCH_MODE"))) {
	case "poll", "polling":
		return true
	case "fsnotify", "watch", "kqueue", "inotify":
		return false
	}
	return runtime.GOOS == "darwin"
}

func watchFS(ctx context.Context, opts Options, onSession func(NativeSession)) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()

	addTree := func(root string) {
		if root == "" {
			return
		}
		_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err == nil && d.IsDir() {
				_ = w.Add(path)
			}
			return nil
		})
	}
	addTree(opts.ClaudeProjectsDir)
	addTree(opts.CodexSessionsDir)

	type pendingEntry struct {
		path string
		when time.Time
	}
	pending := map[string]pendingEntry{}
	var mu sync.Mutex
	flush := time.NewTicker(250 * time.Millisecond)
	defer flush.Stop()

	queue := func(path string) {
		if path == "" {
			return
		}
		if fi, err := os.Stat(path); err == nil && fi.IsDir() {
			_ = filepath.WalkDir(path, func(p string, d os.DirEntry, err error) error {
				if err == nil && d.IsDir() {
					_ = w.Add(p)
				}
				return nil
			})
			return
		}
		if !isCandidate(path) {
			return
		}
		mu.Lock()
		pending[path] = pendingEntry{path: path, when: time.Now().Add(opts.Debounce)}
		mu.Unlock()
	}

	log.Printf("[nativewatch] fsnotify started")
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-w.Events:
			if !ok {
				return nil
			}
			if ev.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) != 0 {
				queue(ev.Name)
			}
		case err, ok := <-w.Errors:
			if ok && err != nil {
				log.Printf("[nativewatch] fsnotify error: %v", err)
			}
		case <-flush.C:
			now := time.Now()
			var due []string
			mu.Lock()
			for path, ent := range pending {
				if now.After(ent.when) {
					due = append(due, ent.path)
					delete(pending, path)
				}
			}
			mu.Unlock()
			sort.Strings(due)
			for _, path := range due {
				if ns, ok := ParsePath(path, opts); ok {
					onSession(ns)
				}
			}
		}
	}
}

func poll(ctx context.Context, opts Options, onSession func(NativeSession)) {
	seen := map[string]int64{}
	primeSeen(opts, seen)
	t := time.NewTicker(opts.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, path := range discover(opts, 0) {
				fi, err := os.Stat(path)
				if err != nil {
					continue
				}
				mt := fi.ModTime().UnixNano()
				if seen[path] == mt {
					continue
				}
				seen[path] = mt
				if ns, ok := ParsePath(path, opts); ok {
					onSession(ns)
				}
			}
		}
	}
}

func scanRecent(opts Options, onSession func(NativeSession)) {
	cutoff := time.Now().Add(-opts.InitialLookback)
	paths := discover(opts, cutoff.Unix())
	sort.Slice(paths, func(i, j int) bool {
		ai, _ := os.Stat(paths[i])
		aj, _ := os.Stat(paths[j])
		var ti, tj time.Time
		if ai != nil {
			ti = ai.ModTime()
		}
		if aj != nil {
			tj = aj.ModTime()
		}
		return ti.After(tj)
	})
	for _, path := range paths {
		if ns, ok := ParsePath(path, opts); ok {
			onSession(ns)
		}
	}
}

func primeSeen(opts Options, seen map[string]int64) {
	for _, path := range discover(opts, 0) {
		if fi, err := os.Stat(path); err == nil {
			seen[path] = fi.ModTime().UnixNano()
		}
	}
}

func discover(opts Options, modifiedAfterUnix int64) []string {
	var paths []string
	add := func(path string) {
		fi, err := os.Stat(path)
		if err != nil || fi.IsDir() {
			return
		}
		if modifiedAfterUnix > 0 && fi.ModTime().Unix() < modifiedAfterUnix {
			return
		}
		paths = append(paths, path)
	}
	if opts.ClaudeProjectsDir != "" {
		_ = filepath.WalkDir(opts.ClaudeProjectsDir, func(path string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(d.Name(), ".jsonl") {
				add(path)
			}
			return nil
		})
	}
	if opts.CodexSessionsDir != "" {
		_ = filepath.WalkDir(opts.CodexSessionsDir, func(path string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && isCodexFileName(d.Name()) {
				add(path)
			}
			return nil
		})
	}
	return paths
}

func isCandidate(path string) bool {
	name := filepath.Base(path)
	return strings.HasSuffix(name, ".jsonl") || strings.HasSuffix(name, ".jsonl.gz")
}

// ParsePath extracts NativeSession metadata from a Claude/Codex transcript path.
func ParsePath(path string, opts Options) (NativeSession, bool) {
	name := filepath.Base(path)
	if isCodexFileName(name) && opts.CodexSessionsDir != "" && inside(path, opts.CodexSessionsDir) {
		session, ok := parseCodex(path)
		if ok && sourcepolicy.IgnoreCodexSession(
			session.Cwd,
			session.Name,
			opts.CodexIgnoreCWDGlobs,
			opts.CodexIgnoreNamePrefixes,
		) {
			return NativeSession{}, false
		}
		return session, ok
	}
	if strings.HasSuffix(name, ".jsonl") && opts.ClaudeProjectsDir != "" && inside(path, opts.ClaudeProjectsDir) {
		return parseClaude(path)
	}
	return NativeSession{}, false
}

func parseClaude(path string) (NativeSession, bool) {
	stem := strings.TrimSuffix(filepath.Base(path), ".jsonl")
	if !uuidRE.MatchString(stem) {
		return NativeSession{}, false
	}
	cwd, title := scanClaudeCwdTitle(path)
	if title == "" {
		title = shortID(stem)
	}
	return NativeSession{
		ID:       "jl_c_" + shortN(stem, 12),
		ResumeID: stem,
		Backend:  BackendClaude,
		Name:     title,
		Cwd:      cwd,
		LastUsed: modUnix(path),
		Path:     path,
	}, true
}

func parseCodex(path string) (NativeSession, bool) {
	uid := codexRolloutUID(filepath.Base(path))
	if !uuidRE.MatchString(uid) {
		return NativeSession{}, false
	}
	cwd, title, isSubagent := scanCodexCwdTitle(path)
	if isSubagent {
		return NativeSession{}, false
	}
	if title == "" {
		title = shortID(uid)
	}
	// A rollout filename records thread creation, not its latest activity. A
	// long-running Codex thread can be appended for months, so mtime is the only
	// useful discovery cursor. Filename time is merely a fallback for odd files.
	last := modUnix(path)
	if last == 0 {
		last = codexTimestampFromFilename(filepath.Base(path))
	}
	return NativeSession{
		ID:       "jl_x_" + shortN(uid, 12),
		ResumeID: uid,
		Backend:  BackendCodex,
		Name:     title,
		Cwd:      cwd,
		LastUsed: last,
		Path:     path,
	}, true
}

func scanClaudeCwdTitle(path string) (string, string) {
	f, err := os.Open(path)
	if err != nil {
		return "", ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	cwd := ""
	title := ""
	for i := 0; i < 200 && sc.Scan(); i++ {
		var row struct {
			Type    string `json:"type"`
			Cwd     string `json:"cwd"`
			Message struct {
				Content any `json:"content"`
			} `json:"message"`
		}
		if json.Unmarshal(sc.Bytes(), &row) != nil {
			continue
		}
		if cwd == "" {
			cwd = strings.TrimSpace(row.Cwd)
		}
		if row.Type == "user" {
			title = firstText(row.Message.Content)
			if strings.HasPrefix(title, "<") {
				title = ""
			}
		}
		if cwd != "" && title != "" {
			return cwd, trimTitle(title)
		}
	}
	return cwd, trimTitle(title)
}

func scanCodexCwdTitle(path string) (string, string, bool) {
	r, closeFn, err := openText(path)
	if err != nil {
		return "", "", false
	}
	defer closeFn()
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	cwd := ""
	for i := 0; i < 300 && sc.Scan(); i++ {
		var row struct {
			Type    string         `json:"type"`
			Payload map[string]any `json:"payload"`
		}
		if json.Unmarshal(sc.Bytes(), &row) != nil {
			continue
		}
		if row.Type == "session_meta" {
			if codexMetaIsSubagent(row.Payload) {
				return "", "", true
			}
			if cwd == "" {
				if v, ok := row.Payload["cwd"].(string); ok {
					cwd = strings.TrimSpace(v)
				} else if v, ok := row.Payload["workingDirectory"].(string); ok {
					cwd = strings.TrimSpace(v)
				}
			}
		}
		if row.Type == "event_msg" {
			if row.Payload["type"] == "user_message" {
				if msg, ok := row.Payload["message"].(string); ok && !isTitleNoise(msg) {
					return cwd, trimTitle(msg), false
				}
			}
		}
		if row.Type == "response_item" && row.Payload["role"] == "user" {
			title := firstText(row.Payload["content"])
			if title != "" && !isTitleNoise(title) {
				return cwd, trimTitle(title), false
			}
		}
	}
	return cwd, "", false
}

func codexMetaIsSubagent(payload map[string]any) bool {
	if payload["thread_source"] == "subagent" {
		return true
	}
	if parent, ok := payload["parent_thread_id"].(string); ok && strings.TrimSpace(parent) != "" {
		return true
	}
	source, ok := payload["source"].(map[string]any)
	if !ok {
		return false
	}
	_, ok = source["subagent"]
	return ok
}

func openText(path string) (*bufio.Reader, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, func() {}, err
	}
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, func() {}, err
		}
		return bufio.NewReader(gz), func() { _ = gz.Close(); _ = f.Close() }, nil
	}
	return bufio.NewReader(f), func() { _ = f.Close() }, nil
}

func firstText(content any) string {
	switch v := content.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		var parts []string
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			if t, ok := m["text"].(string); ok && strings.TrimSpace(t) != "" {
				parts = append(parts, strings.TrimSpace(t))
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func isCodexFileName(name string) bool {
	return strings.HasPrefix(name, "rollout-") &&
		(strings.HasSuffix(name, ".jsonl") || strings.HasSuffix(name, ".jsonl.gz"))
}

func codexRolloutUID(name string) string {
	stem := strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".jsonl")
	if len(stem) < 36 {
		return ""
	}
	return stem[len(stem)-36:]
}

func codexTimestampFromFilename(name string) int64 {
	// rollout-2026-06-06T20-01-57-<uuid>.jsonl
	if len(name) < len("rollout-2006-01-02T15-04-05-")+36 {
		return 0
	}
	datePart := name[8:18]
	timePart := strings.ReplaceAll(name[19:27], "-", ":")
	t, err := time.Parse("2006-01-02T15:04:05Z", datePart+"T"+timePart+"Z")
	if err != nil {
		return 0
	}
	return t.Unix()
}

func trimTitle(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 50 {
		s = s[:50]
	}
	return strings.TrimSpace(s)
}

func isTitleNoise(text string) bool {
	s := strings.TrimSpace(text)
	return s == "" ||
		strings.HasPrefix(s, "<turn_aborted>") ||
		strings.HasPrefix(s, "# AGENTS.md instructions") ||
		strings.HasPrefix(s, "<permissions instructions>") ||
		strings.HasPrefix(s, "<environment_context>")
}

func shortID(id string) string { return shortN(id, 8) }

func shortN(id string, n int) string {
	if len(id) <= n {
		return id
	}
	return id[:n]
}

func modUnix(path string) int64 {
	if fi, err := os.Stat(path); err == nil {
		return fi.ModTime().Unix()
	}
	return time.Now().Unix()
}

func inside(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".."
}
