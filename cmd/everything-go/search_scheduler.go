package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"everything-go/internal/search"
)

const (
	defaultDirtyPathLimit = 2048
	defaultIndexDebounce  = 1200 * time.Millisecond
	minimumIndexInterval  = 10 * time.Second
	defaultFullReconcile  = 6 * time.Hour
	failedIndexRetryDelay = time.Minute
)

// dirtyPathQueue bounds and deduplicates watcher notifications. Overflow is
// represented explicitly so the scheduler can fall back to a complete scan
// instead of silently losing search updates.
type dirtyPathQueue struct {
	mu       sync.Mutex
	paths    map[string]struct{}
	overflow bool
	limit    int
	notify   chan struct{}
}

func newDirtyPathQueue(limit int) *dirtyPathQueue {
	if limit <= 0 {
		limit = defaultDirtyPathLimit
	}
	return &dirtyPathQueue{paths: make(map[string]struct{}), limit: limit, notify: make(chan struct{}, 1)}
}

func (q *dirtyPathQueue) Add(path string) {
	path = strings.TrimSpace(path)
	q.mu.Lock()
	if path == "" {
		q.overflow = true
	} else if _, exists := q.paths[path]; !exists {
		if len(q.paths) >= q.limit {
			q.overflow = true
		} else {
			q.paths[path] = struct{}{}
		}
	}
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *dirtyPathQueue) Notify() <-chan struct{} { return q.notify }

func (q *dirtyPathQueue) Drain() ([]string, bool) {
	q.mu.Lock()
	paths := make([]string, 0, len(q.paths))
	for path := range q.paths {
		paths = append(paths, path)
	}
	q.paths = make(map[string]struct{})
	overflow := q.overflow
	q.overflow = false
	q.mu.Unlock()
	sort.Strings(paths)
	return paths, overflow
}

func readIndexPaths(r io.Reader, limit int) ([]string, error) {
	if limit <= 0 {
		limit = defaultDirtyPathLimit
	}
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	seen := make(map[string]struct{})
	for scanner.Scan() {
		path := strings.TrimSpace(scanner.Text())
		if path == "" {
			continue
		}
		if _, duplicate := seen[path]; duplicate {
			continue
		}
		if len(seen) >= limit {
			return nil, errors.New("dirty path limit exceeded")
		}
		seen[path] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(seen))
	for path := range seen {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

func incrementalSearchEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("EVERYTHING_GO_SEARCH_INCREMENTAL")))
	return value != "0" && value != "false" && value != "off" && value != "legacy"
}

func searchFullReconcileInterval() time.Duration {
	if value := strings.TrimSpace(os.Getenv("EVERYTHING_GO_SEARCH_FULL_INTERVAL")); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed >= time.Minute {
			return parsed
		}
	}
	return defaultFullReconcile
}

func runSearchIndexerLoop(ctx context.Context, idx *search.Index, exePath, dataDir string, dirty *dirtyPathQueue, incremental bool, onIndexed func()) {
	if !incremental {
		log.Printf("[search] incremental path indexing disabled; using legacy full reconciliation")
		runLegacySearchIndexerLoop(ctx, idx, exePath, dataDir, dirty, time.Minute, 15*time.Minute, onIndexed)
		return
	}
	log.Printf("[search] incremental path indexing enabled (full_reconcile=%s)", searchFullReconcileInterval())
	runIncrementalSearchIndexerLoop(ctx, idx, exePath, dataDir, dirty, defaultIndexDebounce, searchFullReconcileInterval(), onIndexed)
}

func runIncrementalSearchIndexerLoop(ctx context.Context, idx *search.Index, exePath, dataDir string, dirty *dirtyPathQueue, debounce, fullInterval time.Duration, onIndexed func()) {
	if debounce <= 0 {
		debounce = defaultIndexDebounce
	}
	if fullInterval <= 0 {
		fullInterval = defaultFullReconcile
	}

	requestFull := true
	var requestPaths []string
	lastFull := time.Time{}
	lastChildStarted := time.Time{}
	for {
		if !requestFull && !lastChildStarted.IsZero() {
			remaining := minimumIndexInterval - time.Since(lastChildStarted)
			if remaining > 0 && !waitContext(ctx, remaining) {
				return
			}
			extra, overflow := dirty.Drain()
			if overflow {
				requestFull = true
				requestPaths = nil
			} else {
				requestPaths = mergeDirtyPaths(requestPaths, extra)
			}
		}
		lastChildStarted = time.Now()
		metrics, metricsOK, err := runIndexerChild(ctx, idx, exePath, dataDir, requestPaths, !requestFull)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[search] indexer child failed: %v; full retry in %s", err, failedIndexRetryDelay)
			if !waitContext(ctx, failedIndexRetryDelay) {
				return
			}
			requestFull = true
			requestPaths = nil
			continue
		}
		idx.MarkReady()
		idx.RecordIngestMetrics(metrics)
		if requestFull {
			lastFull = time.Now()
		}
		if !metricsOK {
			log.Printf("[search] indexer child returned no metrics; scheduling safe full reconciliation")
			requestFull = true
			requestPaths = nil
			continue
		}
		log.Printf("[search] indexed mode=%s files_seen=%d files_changed=%d messages=%d; next full reconciliation by %s",
			metrics.Mode, metrics.FilesSeen, metrics.FilesChanged, metrics.MessagesAdded, lastFull.Add(fullInterval).Format(time.RFC3339))
		if shouldRefreshSessionSummaries(metrics, metricsOK) && onIndexed != nil {
			onIndexed()
		}

		for {
			untilFull := time.Until(lastFull.Add(fullInterval))
			if untilFull <= 0 {
				requestFull = true
				requestPaths = nil
				break
			}
			timer := time.NewTimer(untilFull)
			select {
			case <-ctx.Done():
				stopTimer(timer)
				return
			case <-timer.C:
				requestFull = true
				requestPaths = nil
			case <-dirty.Notify():
				stopTimer(timer)
				if !waitContext(ctx, debounce) {
					return
				}
				paths, overflow := dirty.Drain()
				if overflow {
					log.Printf("[search] dirty path queue overflowed; using full reconciliation")
					requestFull = true
					requestPaths = nil
				} else if len(paths) > 0 {
					requestFull = false
					requestPaths = paths
				} else {
					continue
				}
			}
			break
		}
	}
}

func shouldRefreshSessionSummaries(metrics search.IngestMetrics, metricsOK bool) bool {
	return metricsOK && (metrics.MessagesAdded > 0 || metrics.MaintenanceRows > 0)
}

func mergeDirtyPaths(left, right []string) []string {
	seen := make(map[string]struct{}, len(left)+len(right))
	for _, path := range left {
		if path != "" {
			seen[path] = struct{}{}
		}
	}
	for _, path := range right {
		if path != "" {
			seen[path] = struct{}{}
		}
	}
	merged := make([]string, 0, len(seen))
	for path := range seen {
		merged = append(merged, path)
	}
	sort.Strings(merged)
	return merged
}

func runIndexerChild(ctx context.Context, idx *search.Index, exePath, dataDir string, paths []string, explicitPaths bool) (search.IngestMetrics, bool, error) {
	idx.SetIndexing(true)
	defer idx.SetIndexing(false)
	args := []string{"--mode=index", "--data-dir", dataDir}
	if explicitPaths {
		args = append(args, "--index-paths-stdin")
	}
	cmd := exec.CommandContext(ctx, exePath, args...)
	if explicitPaths {
		cmd.Stdin = strings.NewReader(strings.Join(paths, "\n") + "\n")
	}
	var childOut bytes.Buffer
	cmd.Stdout = io.MultiWriter(os.Stdout, &childOut)
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	metrics, ok := parseIndexMetrics(childOut.String())
	return metrics, ok, err
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer stopTimer(timer)
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
