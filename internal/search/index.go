package search

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"everything-go/internal/sourcepolicy"
)

// Index owns the search database. One Index is shared by the whole bridge and
// queries run concurrently against the WAL DB. Ingestion does NOT run in this
// process — it happens in a short-lived `--mode=index` child (see RunOnce), so
// the resident bridge only ever reads.
type Index struct {
	db      *sql.DB
	path    string
	sources []source

	writeMu sync.Mutex

	mu       sync.Mutex
	ready    bool
	progress ingestProgress
}

type ingestProgress struct {
	status        string
	filesTotal    int
	filesDone     int
	currentFile   string
	currentSource string
	lastAdded     int
	lastError     string
	cycleStarted  time.Time
	cycleDone     time.Time
	lastMetrics   IngestMetrics
}

// IngestMetrics describes one complete reconciliation pass. Byte counters are
// transcript bytes consumed by incremental readers; DBBytesDelta is the signed
// change in the database + WAL footprint, not an estimate of physical SSD I/O.
type IngestMetrics struct {
	Mode             string `json:"mode,omitempty"`
	FilesSeen        int    `json:"files_seen"`
	FilesChanged     int    `json:"files_changed"`
	FilesQueued      int    `json:"files_queued"`
	MessagesAdded    int    `json:"messages_added"`
	MessageConflicts int    `json:"message_conflicts"`
	BytesRead        int64  `json:"bytes_read"`
	DBBytesDelta     int64  `json:"db_bytes_delta"`
	DurationMS       int64  `json:"duration_ms"`
	MaintenanceRows  int64  `json:"maintenance_rows,omitempty"`
	WALBytesBefore   int64  `json:"wal_bytes_before,omitempty"`
	WALBytesAfter    int64  `json:"wal_bytes_after,omitempty"`
	CheckpointBusy   int    `json:"checkpoint_busy,omitempty"`
	CheckpointLog    int    `json:"checkpoint_log_pages,omitempty"`
	CheckpointDone   int    `json:"checkpointed_pages,omitempty"`
}

// New opens (creating if needed) the search index at dbPath and registers the
// Claude + Codex sources. It does not ingest — the bridge issues read-only
// queries while the `--mode=index` child calls RunOnce.
func New(dbPath string) (*Index, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, err
	}
	return &Index{
		db:      db,
		path:    dbPath,
		sources: []source{newClaudeSource(), newCodexSource()},
	}, nil
}

// RunOnce ingests every source's new content to completion and returns the
// number of messages added. It is the body of the `--mode=index` child: a
// short-lived process that does the heap-heavy parse and then exits, handing all
// of its memory back to the OS so the resident bridge stays lightweight.
func (idx *Index) RunOnce() int {
	return idx.RunOnceMetrics().MessagesAdded
}

// RunOnceMetrics is RunOnce with operational counters used by the resident
// scheduler to distinguish a genuinely idle pass from useful indexing work.
func (idx *Index) RunOnceMetrics() IngestMetrics {
	started := time.Now()
	before := databaseFootprint(idx.path)
	walBefore := fileSize(idx.path + "-wal")
	maintenanceRows := idx.runVersionedMaintenance()
	metrics := idx.ingestAllMetrics()
	metrics.Mode = "full"
	metrics.MaintenanceRows = maintenanceRows
	metrics.WALBytesBefore = walBefore
	metrics.CheckpointBusy, metrics.CheckpointLog, metrics.CheckpointDone = idx.maybeCheckpointWAL()
	metrics.WALBytesAfter = fileSize(idx.path + "-wal")
	metrics.DBBytesDelta = databaseFootprint(idx.path) - before
	metrics.DurationMS = time.Since(started).Milliseconds()
	return metrics
}

// RunPathsMetrics indexes only validated dirty transcript paths. A periodic
// RunOnceMetrics remains the correctness backstop for missed notifications,
// deletions and policy changes.
func (idx *Index) RunPathsMetrics(paths []string) IngestMetrics {
	started := time.Now()
	before := databaseFootprint(idx.path)
	maintenanceRows := idx.runVersionedMaintenance()
	metrics := idx.ingestPathsMetrics(paths)
	metrics.MaintenanceRows = maintenanceRows
	metrics.DBBytesDelta = databaseFootprint(idx.path) - before
	metrics.DurationMS = time.Since(started).Milliseconds()
	return metrics
}

const frameworkNoiseMaintenanceVersion = "1"
const codexMessageIDVersion = "2-offset"

func (idx *Index) runVersionedMaintenance() int64 {
	var changed int64
	if rebuilt, err := idx.refreshCodexMessageIDVersion(); err != nil {
		log.Printf("[search] Codex message ID migration: %v", err)
	} else if rebuilt > 0 {
		changed += rebuilt
		log.Printf("[search] reset %d stale Codex index row(s) for message ID v%s", rebuilt, codexMessageIDVersion)
	}
	policyChanged, err := idx.refreshSourcePolicyFingerprint()
	if err != nil {
		log.Printf("[search] source policy fingerprint: %v", err)
	}
	if policyChanged {
		if removed, pruneErr := idx.pruneExcludedCodexSessions(); pruneErr != nil {
			log.Printf("[search] prune excluded Codex sessions: %v", pruneErr)
		} else if removed > 0 {
			changed += int64(removed)
			log.Printf("[search] pruned %d excluded Codex session(s)", removed)
		}
	}

	const maintenanceKey = "framework_noise_maintenance_version"
	var current string
	err = idx.db.QueryRow("SELECT value FROM schema_meta WHERE key=?", maintenanceKey).Scan(&current)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("[search] framework-noise maintenance version: %v", err)
		return changed
	}
	if current != frameworkNoiseMaintenanceVersion {
		removed, pruneErr := idx.pruneFrameworkNoiseMessages()
		if pruneErr != nil {
			log.Printf("[search] prune framework-noise messages: %v", pruneErr)
			return changed
		}
		changed += removed
		if removed > 0 {
			log.Printf("[search] pruned %d framework-noise message(s)", removed)
		}
		if _, writeErr := idx.db.Exec(`INSERT INTO schema_meta(key,value) VALUES(?,?)
			ON CONFLICT(key) DO UPDATE SET value=excluded.value`, maintenanceKey, frameworkNoiseMaintenanceVersion); writeErr != nil {
			log.Printf("[search] persist framework-noise maintenance version: %v", writeErr)
		}
	}
	return changed
}

// refreshCodexMessageIDVersion performs a one-time rebuild when the synthetic
// identity for UUID-less Codex messages changes. Keeping old line-based IDs
// alongside offset-based IDs would duplicate historical messages, so Codex
// rows and cursors are reset atomically and normal ingestion repopulates them.
func (idx *Index) refreshCodexMessageIDVersion() (int64, error) {
	const key = "codex_message_id_version"
	var current string
	err := idx.db.QueryRow("SELECT value FROM schema_meta WHERE key=?", key).Scan(&current)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	if current == codexMessageIDVersion {
		return 0, nil
	}

	tx, err := idx.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var changed int64
	result, err := tx.Exec(`DELETE FROM messages WHERE session_id IN (
		SELECT session_id FROM sessions WHERE source='codex'
	)`)
	if err != nil {
		return 0, err
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr == nil {
		changed += rows
	}
	result, err = tx.Exec("DELETE FROM sessions WHERE source='codex'")
	if err != nil {
		return 0, err
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr == nil {
		changed += rows
	}
	root := filepath.Clean(sourcepolicy.CodexSessionsDir()) + string(os.PathSeparator) + "%"
	if _, err := tx.Exec("DELETE FROM ingest_state WHERE source_path LIKE ?", root); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(`INSERT INTO schema_meta(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, codexMessageIDVersion); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return changed, nil
}

func databaseFootprint(path string) int64 {
	var total int64
	for _, candidate := range []string{path, path + "-wal"} {
		if info, err := os.Stat(candidate); err == nil {
			total += info.Size()
		}
	}
	return total
}

func fileSize(path string) int64 {
	if info, err := os.Stat(path); err == nil {
		return info.Size()
	}
	return 0
}

const passiveCheckpointThreshold = 32 << 20

// maybeCheckpointWAL copies committed frames back to the main database only
// during low-frequency full reconciliation. PASSIVE never waits for readers
// and deliberately leaves the WAL allocated for reuse; it avoids the I/O and
// coordination spikes of TRUNCATE/VACUUM on an actively queried index.
func (idx *Index) maybeCheckpointWAL() (busy, logPages, checkpointed int) {
	if fileSize(idx.path+"-wal") < passiveCheckpointThreshold {
		return 0, 0, 0
	}
	if err := idx.db.QueryRow("PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &logPages, &checkpointed); err != nil {
		log.Printf("[search] passive WAL checkpoint: %v", err)
		return 0, 0, 0
	}
	log.Printf("[search] passive WAL checkpoint busy=%d log_pages=%d checkpointed=%d", busy, logPages, checkpointed)
	return busy, logPages, checkpointed
}

// refreshSourcePolicyFingerprint invalidates only Codex ingest cursors when
// ignore rules actually change. Ignored files can therefore keep a lightweight
// stat cursor during normal cycles without becoming permanently invisible if a
// later policy starts including them.
func (idx *Index) refreshSourcePolicyFingerprint() (bool, error) {
	parts := append([]string(nil), sourcepolicy.CodexIgnoreCWDGlobs()...)
	parts = append(parts, "\x00")
	parts = append(parts, sourcepolicy.CodexIgnoreNamePrefixes()...)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	fingerprint := hex.EncodeToString(sum[:])
	const key = "codex_source_policy_fingerprint"
	var previous string
	err := idx.db.QueryRow("SELECT value FROM schema_meta WHERE key=?", key).Scan(&previous)
	if err == sql.ErrNoRows {
		_, err = idx.db.Exec("INSERT INTO schema_meta(key,value) VALUES(?,?)", key, fingerprint)
		return err == nil, err
	}
	if err != nil || previous == fingerprint {
		return false, err
	}
	root := filepath.Clean(sourcepolicy.CodexSessionsDir()) + string(os.PathSeparator) + "%"
	tx, err := idx.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err := tx.Exec("DELETE FROM ingest_state WHERE source_path LIKE ?", root); err != nil {
		return false, err
	}
	if _, err := tx.Exec("UPDATE schema_meta SET value=? WHERE key=?", fingerprint, key); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (idx *Index) pruneFrameworkNoiseMessages() (int64, error) {
	tx, err := idx.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	result, err := tx.Exec(`
		DELETE FROM messages
		WHERE role='user'
		  AND session_id IN (SELECT session_id FROM sessions WHERE source='codex')
		  AND lower(ltrim(content)) LIKE '<recommended_plugins>%'`)
	if err != nil {
		return 0, err
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if removed > 0 {
		if _, err := tx.Exec(`
		UPDATE sessions
		SET msg_count=(SELECT COUNT(*) FROM messages WHERE messages.session_id=sessions.session_id)
		WHERE source='codex'`); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return removed, nil
}

func (idx *Index) pruneExcludedCodexSessions() (int, error) {
	cwdGlobs := sourcepolicy.CodexIgnoreCWDGlobs()
	namePrefixes := sourcepolicy.CodexIgnoreNamePrefixes()
	if len(cwdGlobs) == 0 && len(namePrefixes) == 0 {
		return 0, nil
	}
	rows, err := idx.db.Query(
		"SELECT session_id, source_path, COALESCE(cwd,''), COALESCE(display_name,'') FROM sessions WHERE source='codex'",
	)
	if err != nil {
		return 0, err
	}
	type excludedRow struct {
		sessionID, sourcePath string
	}
	var excluded []excludedRow
	for rows.Next() {
		var sessionID, sourcePath, cwd, name string
		if err := rows.Scan(&sessionID, &sourcePath, &cwd, &name); err != nil {
			rows.Close()
			return 0, err
		}
		if sourcepolicy.IgnoreCodexSession(cwd, name, cwdGlobs, namePrefixes) {
			excluded = append(excluded, excludedRow{sessionID: sessionID, sourcePath: sourcePath})
		}
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if len(excluded) == 0 {
		return 0, nil
	}
	tx, err := idx.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, row := range excluded {
		if _, err := tx.Exec("DELETE FROM messages WHERE session_id=?", row.sessionID); err != nil {
			return 0, err
		}
		if _, err := tx.Exec("DELETE FROM sessions WHERE session_id=?", row.sessionID); err != nil {
			return 0, err
		}
		if row.sourcePath != "" {
			if _, err := tx.Exec("DELETE FROM ingest_state WHERE source_path=?", row.sourcePath); err != nil {
				return 0, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(excluded), nil
}

// MarkReady marks the index queryable. The bridge calls it after the first
// successful child indexer run; Health also derives readiness from the DB.
func (idx *Index) MarkReady() {
	idx.mu.Lock()
	idx.ready = true
	idx.mu.Unlock()
}

// RecordIngestMetrics copies child-process counters into the resident search
// health view. The paths themselves never cross into diagnostics.
func (idx *Index) RecordIngestMetrics(metrics IngestMetrics) {
	idx.setProgress(func(p *ingestProgress) {
		p.lastMetrics = metrics
		p.lastAdded = metrics.MessagesAdded
	})
}

// SetIndexing records whether a child indexer is currently running so Health()
// can surface ingest activity to the app.
func (idx *Index) SetIndexing(on bool) {
	idx.setProgress(func(p *ingestProgress) {
		if on {
			p.status = "ingesting"
			p.cycleStarted = time.Now()
			p.cycleDone = time.Time{}
		} else {
			p.status = "ready"
			p.cycleDone = time.Now()
		}
	})
}

func (idx *Index) isReady() bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.ready
}

func (idx *Index) snapshotProgress() ingestProgress {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.progress
}

func (idx *Index) setProgress(fn func(*ingestProgress)) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	fn(&idx.progress)
}

func (idx *Index) Close() error { return idx.db.Close() }
