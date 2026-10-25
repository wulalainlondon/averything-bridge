package search

import (
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type ingestJob struct {
	src   source
	path  string
	mtime time.Time
	meta  sessionMeta
}

// ingestFile brings one file's messages into the index incrementally. It honors
// the stored byte offset, detects rotation/truncation via head signature + size,
// upserts the session row, inserts new messages, and records ingest_state.
// Mirrors bridge/search/ingest/single_file.py (condensed).
func (idx *Index) ingestFile(job ingestJob) (inserted int, conflicts int, bytesRead int64, err error) {
	src, path := job.src, job.path
	info, statErr := os.Stat(path)
	if statErr != nil {
		return 0, 0, 0, statErr
	}
	size := info.Size()
	mtime := float64(info.ModTime().UnixNano()) / 1e9
	headSHA := src.headSignature(path)

	// Read prior ingest state.
	var startOffset int64
	var prevSHA string
	row := idx.db.QueryRow(
		"SELECT last_offset, head_sha256 FROM ingest_state WHERE source_path = ?", path)
	if scanErr := row.Scan(&startOffset, &prevSHA); scanErr == sql.ErrNoRows {
		startOffset, prevSHA = 0, ""
	} else if scanErr != nil {
		return 0, 0, 0, scanErr
	}

	rotated := prevSHA != "" && prevSHA != headSHA
	truncated := startOffset > size
	if rotated || truncated {
		startOffset = 0 // re-ingest from the top
	}
	if startOffset == size && !rotated {
		idx.refreshUnchangedCursor(path, size, mtime, headSHA)
		return 0, 0, 0, nil // nothing new
	}

	msgs, finalOffset := src.iterMessages(path, startOffset)
	bytesRead = finalOffset - startOffset
	if bytesRead < 0 {
		bytesRead = 0
	}
	meta := job.meta
	sid := src.sessionIDFor(path)

	idx.writeMu.Lock()
	defer idx.writeMu.Unlock()

	tx, err := idx.db.Begin()
	if err != nil {
		return 0, 0, bytesRead, err
	}
	defer tx.Rollback()

	if len(msgs) > 0 {
		lastTS := meta.FirstTS
		for _, m := range msgs {
			if m.Timestamp > lastTS {
				lastTS = m.Timestamp
			}
		}
		// On rotation, reset the running msg_count to this batch's size.
		if rotated || startOffset == 0 {
			_, _ = tx.Exec(`
				INSERT INTO sessions(session_id, source, source_path, project_dir, cwd,
					display_name, first_ts, last_ts, msg_count, backend)
				VALUES(?,?,?,?,?,?,?,?,?,?)
				ON CONFLICT(session_id) DO UPDATE SET
					source_path = excluded.source_path,
					project_dir = excluded.project_dir,
					cwd = COALESCE(NULLIF(excluded.cwd,''), sessions.cwd),
					display_name = CASE WHEN sessions.display_name IS NULL OR sessions.display_name=''
						THEN excluded.display_name ELSE sessions.display_name END,
					first_ts = CASE WHEN sessions.first_ts IS NULL OR sessions.first_ts=''
						THEN excluded.first_ts ELSE sessions.first_ts END,
					last_ts = CASE WHEN excluded.last_ts > sessions.last_ts
						THEN excluded.last_ts ELSE sessions.last_ts END,
					msg_count = excluded.msg_count,
					backend = excluded.backend`,
				sid, src.name(), path, meta.ProjectDir, meta.Cwd,
				meta.DisplayName, meta.FirstTS, lastTS, len(msgs), src.name())
		} else {
			_, _ = tx.Exec(`
				INSERT INTO sessions(session_id, source, source_path, project_dir, cwd,
					display_name, first_ts, last_ts, msg_count, backend)
				VALUES(?,?,?,?,?,?,?,?,?,?)
				ON CONFLICT(session_id) DO UPDATE SET
					cwd = COALESCE(NULLIF(excluded.cwd,''), sessions.cwd),
					display_name = CASE WHEN sessions.display_name IS NULL OR sessions.display_name=''
						THEN excluded.display_name ELSE sessions.display_name END,
					last_ts = CASE WHEN excluded.last_ts > sessions.last_ts
						THEN excluded.last_ts ELSE sessions.last_ts END,
					msg_count = sessions.msg_count + excluded.msg_count,
					backend = excluded.backend`,
				sid, src.name(), path, meta.ProjectDir, meta.Cwd,
				meta.DisplayName, meta.FirstTS, lastTS, len(msgs), src.name())
		}

		stmt, perr := tx.Prepare(`
			INSERT INTO messages(session_id, msg_uuid, parent_uuid, role, ts, is_subagent, content)
			VALUES(?,?,?,?,?,?,?)
			ON CONFLICT(session_id, msg_uuid) DO NOTHING`)
		if perr != nil {
			return 0, 0, bytesRead, perr
		}
		for _, m := range msgs {
			sub := 0
			if m.IsSubagent {
				sub = 1
			}
			result, e := stmt.Exec(m.SessionID, m.MsgUUID, nullStr(m.ParentUUID),
				m.Role, m.Timestamp, sub, m.Text)
			if e != nil {
				stmt.Close()
				return 0, 0, bytesRead, e
			}
			if affected, affectedErr := result.RowsAffected(); affectedErr == nil && affected == 1 {
				inserted++
			} else {
				conflicts++
			}
		}
		stmt.Close()
	}

	now := float64(time.Now().UnixNano()) / 1e9
	_, _ = tx.Exec(`
		INSERT INTO ingest_state(source_path, file_size, last_mtime, last_offset,
			head_sha256, last_ingest_at, msg_extracted, errors)
		VALUES(?,?,?,?,?,?,?,0)
		ON CONFLICT(source_path) DO UPDATE SET
			file_size = excluded.file_size,
			last_mtime = excluded.last_mtime,
			last_offset = excluded.last_offset,
			head_sha256 = excluded.head_sha256,
			last_ingest_at = excluded.last_ingest_at,
			msg_extracted = ingest_state.msg_extracted + excluded.msg_extracted`,
		path, size, mtime, finalOffset, headSHA, now, len(msgs))

	if err := tx.Commit(); err != nil {
		return 0, 0, bytesRead, err
	}
	return inserted, conflicts, bytesRead, nil
}

// refreshUnchangedCursor records metadata-only touches. Without this update a
// file whose mtime changed but whose size/head did not would be rediscovered on
// every reconciliation forever, preventing the idle scheduler from backing off.
func (idx *Index) refreshUnchangedCursor(path string, size int64, mtime float64, headSHA string) {
	now := float64(time.Now().UnixNano()) / 1e9
	_, _ = idx.db.Exec(`
		INSERT INTO ingest_state(source_path, file_size, last_mtime, last_offset,
			head_sha256, last_ingest_at, msg_extracted, errors)
		VALUES(?,?,?,?,?,?,0,0)
		ON CONFLICT(source_path) DO UPDATE SET
			file_size=excluded.file_size,
			last_mtime=excluded.last_mtime,
			last_offset=excluded.last_offset,
			head_sha256=excluded.head_sha256,
			last_ingest_at=excluded.last_ingest_at`,
		path, size, mtime, size, headSHA, now,
	)
}

type cachedState struct {
	size  int64
	mtime float64
}

// discoverJobs scans all configured sources and sorts newest files first.
// It loads the entire ingest_state table in one query so unchanged files can
// be filtered out before any file I/O, keeping incremental cycles fast.
func (idx *Index) discoverJobs() []ingestJob {
	jobs, _, _ := idx.discoverJobsMetrics()
	return jobs
}

func (idx *Index) discoverJobsMetrics() (jobs []ingestJob, filesSeen, filesChanged int) {
	// Bulk-load ingest_state in a single query (~3000+ rows but one round-trip).
	known := make(map[string]cachedState)
	if rows, err := idx.db.Query("SELECT source_path, file_size, last_mtime FROM ingest_state"); err == nil {
		for rows.Next() {
			var path string
			var s cachedState
			if rows.Scan(&path, &s.size, &s.mtime) == nil {
				known[path] = s
			}
		}
		rows.Close()
	}

	for _, src := range idx.sources {
		if !src.enabled() {
			continue
		}
		for _, path := range src.discover() {
			info, err := os.Stat(path)
			if err != nil {
				continue
			}
			filesSeen++
			// Skip files whose size and mtime are unchanged — nothing to ingest.
			if s, ok := known[path]; ok {
				mtime := float64(info.ModTime().UnixNano()) / 1e9
				if info.Size() == s.size && mtime == s.mtime {
					continue
				}
			}
			filesChanged++
			// Metadata can require opening and parsing the rollout. Do it only
			// after the stat-based unchanged-file gate; the previous ordering
			// reread every Codex file on every nominally incremental cycle.
			meta := src.sessionMeta(path)
			if filter, ok := src.(interface{ includeMeta(sessionMeta) bool }); ok && !filter.includeMeta(meta) {
				idx.markSkippedFile(src, path, info)
				continue
			}
			jobs = append(jobs, ingestJob{src: src, path: path, mtime: info.ModTime(), meta: meta})
		}
	}
	sort.SliceStable(jobs, func(i, j int) bool {
		return jobs[i].mtime.After(jobs[j].mtime)
	})
	return jobs, filesSeen, filesChanged
}

// discoverPathJobsMetrics performs the same cursor and source-policy checks as
// full reconciliation, but touches only the explicitly dirty transcript paths.
// Paths are canonicalized, source-root validated and deduplicated here so both
// scheduler bugs and hostile child input fail closed.
func (idx *Index) discoverPathJobsMetrics(paths []string) (jobs []ingestJob, filesSeen, filesChanged int) {
	known := make(map[string]cachedState)
	if rows, err := idx.db.Query("SELECT source_path, file_size, last_mtime FROM ingest_state"); err == nil {
		for rows.Next() {
			var path string
			var state cachedState
			if rows.Scan(&path, &state.size, &state.mtime) == nil {
				known[path] = state
			}
		}
		rows.Close()
	}

	seen := make(map[string]struct{}, len(paths))
	for _, candidate := range paths {
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			continue
		}
		resolved, err = filepath.Abs(resolved)
		if err != nil {
			continue
		}
		if _, duplicate := seen[resolved]; duplicate {
			continue
		}
		seen[resolved] = struct{}{}

		var owner source
		for _, src := range idx.sources {
			if src.enabled() && src.owns(resolved) {
				owner = src
				break
			}
		}
		if owner == nil {
			continue
		}
		info, err := os.Stat(resolved)
		if err != nil || info.IsDir() {
			continue
		}
		filesSeen++
		mtime := float64(info.ModTime().UnixNano()) / 1e9
		if state, ok := known[resolved]; ok && info.Size() == state.size && mtime == state.mtime {
			continue
		}
		filesChanged++
		meta := owner.sessionMeta(resolved)
		if filter, ok := owner.(interface{ includeMeta(sessionMeta) bool }); ok && !filter.includeMeta(meta) {
			idx.markSkippedFile(owner, resolved, info)
			continue
		}
		jobs = append(jobs, ingestJob{src: owner, path: resolved, mtime: info.ModTime(), meta: meta})
	}
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].mtime.After(jobs[j].mtime) })
	return jobs, filesSeen, filesChanged
}

func (idx *Index) markSkippedFile(src source, path string, info os.FileInfo) {
	mtime := float64(info.ModTime().UnixNano()) / 1e9
	now := float64(time.Now().UnixNano()) / 1e9
	_, _ = idx.db.Exec(
		`INSERT INTO ingest_state(source_path,file_size,last_mtime,last_offset,head_sha256,last_ingest_at,msg_extracted,errors)
		 VALUES(?,?,?,?,?,?,0,0)
		 ON CONFLICT(source_path) DO UPDATE SET
		  file_size=excluded.file_size,
		  last_mtime=excluded.last_mtime,
		  last_offset=excluded.last_offset,
		  head_sha256=excluded.head_sha256,
		  last_ingest_at=excluded.last_ingest_at`,
		path, info.Size(), mtime, info.Size(), src.headSignature(path), now,
	)
}

// ingestBatch processes a bounded slice of work. It returns the remaining jobs
// and the number of messages added in this batch.
func (idx *Index) ingestBatch(jobs []ingestJob, maxFiles int, maxDuration time.Duration) ([]ingestJob, int, int, int64) {
	if maxFiles <= 0 {
		maxFiles = 1
	}
	deadline := time.Now().Add(maxDuration)
	total := 0
	conflicts := 0
	var bytesRead int64
	done := 0
	for done < len(jobs) && done < maxFiles {
		job := jobs[done]
		idx.setProgress(func(p *ingestProgress) {
			p.currentFile = job.path
			p.currentSource = job.src.name()
		})

		n, collided, read, err := idx.ingestFile(job)
		bytesRead += read
		if err != nil {
			log.Printf("[search] ingest %s: %v", job.path, err)
			idx.setProgress(func(p *ingestProgress) {
				p.lastError = err.Error()
			})
		} else {
			total += n
			conflicts += collided
		}
		done++
		idx.setProgress(func(p *ingestProgress) {
			p.filesDone++
		})
		if done >= maxFiles || time.Now().After(deadline) {
			break
		}
	}
	return jobs[done:], total, conflicts, bytesRead
}

// ingestAll scans every source's files once. Returns total messages added.
func (idx *Index) ingestAll() int {
	return idx.ingestAllMetrics().MessagesAdded
}

func (idx *Index) ingestAllMetrics() IngestMetrics {
	jobs, filesSeen, filesChanged := idx.discoverJobsMetrics()
	metrics := IngestMetrics{FilesSeen: filesSeen, FilesChanged: filesChanged, FilesQueued: len(jobs)}
	for len(jobs) > 0 {
		var added, conflicts int
		var bytesRead int64
		jobs, added, conflicts, bytesRead = idx.ingestBatch(jobs, len(jobs), 24*time.Hour)
		metrics.MessagesAdded += added
		metrics.MessageConflicts += conflicts
		metrics.BytesRead += bytesRead
	}
	return metrics
}

func (idx *Index) ingestPathsMetrics(paths []string) IngestMetrics {
	jobs, filesSeen, filesChanged := idx.discoverPathJobsMetrics(paths)
	metrics := IngestMetrics{Mode: "incremental", FilesSeen: filesSeen, FilesChanged: filesChanged, FilesQueued: len(jobs)}
	for len(jobs) > 0 {
		var added, conflicts int
		var bytesRead int64
		jobs, added, conflicts, bytesRead = idx.ingestBatch(jobs, len(jobs), 24*time.Hour)
		metrics.MessagesAdded += added
		metrics.MessageConflicts += conflicts
		metrics.BytesRead += bytesRead
	}
	return metrics
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
