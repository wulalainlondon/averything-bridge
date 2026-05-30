"""
single_file.py — ingest a single JSONL file into the search DB.

Handles:
- Resuming from last_offset (incremental)
- File rotation / truncation detection via head_sha256
- Batch INSERT (200 messages or 1 MB, whichever comes first)
- PRAGMA wal_checkpoint(PASSIVE) after completion
- Advances offset past bad lines (no stuck-on-bad-line)
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..sources.base import SearchSource
    from ..db.sqlite_adapter import sqlite3 as Sqlite3Connection

from ..db.sqlite_adapter import sqlite3
from .display_name import is_framework_noise

log = logging.getLogger(__name__)

_BATCH_SIZE = 2000         # max messages before commit
_BATCH_MAX_BYTES = 8 << 20  # 8 MB content before commit

# For files smaller than this threshold, use a full-file sha to detect rotation.
# For larger files, use the head-bytes sha (fast append detection).
_FULL_FILE_SHA_THRESHOLD = 64 * 1024  # 64 KB


@dataclass
class IngestResult:
    path: Path
    messages_added: int
    errors: int
    bytes_read: int
    elapsed_sec: float
    rotated: bool


# ---------------------------------------------------------------------------
# SQL templates
# ---------------------------------------------------------------------------

_INSERT_SESSION = """
INSERT INTO sessions(
    session_id, source, source_path, project_dir,
    cwd, display_name, first_ts, last_ts,
    msg_count, backend
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET
    cwd          = COALESCE(excluded.cwd, sessions.cwd),
    display_name = COALESCE(NULLIF(sessions.display_name, ''), excluded.display_name),
    last_ts      = CASE WHEN excluded.last_ts > sessions.last_ts
                        THEN excluded.last_ts ELSE sessions.last_ts END,
    first_ts     = CASE WHEN sessions.first_ts = '' OR sessions.first_ts IS NULL
                        THEN excluded.first_ts ELSE sessions.first_ts END,
    msg_count    = sessions.msg_count + excluded.msg_count,
    source_path  = excluded.source_path
"""

# For the rotation reset case we issue a separate UPDATE after the UPSERT.
_RESET_SESSION_MSG_COUNT = """
UPDATE sessions SET msg_count = ? WHERE session_id = ?
"""

_INSERT_MESSAGE = """
INSERT INTO messages(session_id, msg_uuid, parent_uuid, role, ts, is_subagent, content)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id, msg_uuid) DO NOTHING
"""

_INSERT_INGEST_STATE = """
INSERT INTO ingest_state(
    source_path, file_size, last_mtime, last_offset,
    head_sha256, last_ingest_at, msg_extracted, errors
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(source_path) DO UPDATE SET
    file_size      = excluded.file_size,
    last_mtime     = excluded.last_mtime,
    last_offset    = excluded.last_offset,
    head_sha256    = excluded.head_sha256,
    last_ingest_at = excluded.last_ingest_at,
    msg_extracted  = ingest_state.msg_extracted + excluded.msg_extracted,
    errors         = ingest_state.errors + excluded.errors
"""

_RESET_INGEST_STATE_MSG_EXTRACTED = """
UPDATE ingest_state SET msg_extracted = ? WHERE source_path = ?
"""

_SELECT_INGEST_STATE = """
SELECT last_offset, head_sha256, file_size, last_mtime
FROM ingest_state
WHERE source_path = ?
"""

_DELETE_MESSAGES_BY_SESSION = """
DELETE FROM messages WHERE session_id = ?
"""

# Update display_name only when the stored value is still framework noise.
# Clean values (set by prior ingest or by the user) are left untouched.
_UPDATE_DISPLAY_NAME_IF_NOISE = """
UPDATE sessions SET display_name = ?
WHERE session_id = ?
  AND (
    display_name IS NULL
    OR display_name = ''
    OR display_name LIKE '<%'
    OR display_name LIKE '#%'
    OR display_name LIKE 'This session is being continued%'
    OR display_name LIKE 'Caveat:%'
  )
"""


# ---------------------------------------------------------------------------
# Sync DB helpers — each called via asyncio.to_thread() from ingest_file()
# ---------------------------------------------------------------------------

def _sync_read_ingest_state(conn, path_str: str):
    """SELECT ingest_state row for path_str; returns fetchone() result."""
    return conn.execute(_SELECT_INGEST_STATE, (path_str,)).fetchone()


def _sync_get_session_display_name(conn, session_id: str) -> Optional[str]:
    """Return the stored display_name for session_id, or None if not found."""
    row = conn.execute(
        "SELECT display_name FROM sessions WHERE session_id = ?", (session_id,)
    ).fetchone()
    return row[0] if row and row[0] else None


def _sync_delete_and_commit(conn, session_id: str) -> None:
    """DELETE all messages for session_id and commit (rotation reset)."""
    conn.execute(_DELETE_MESSAGES_BY_SESSION, (session_id,))
    conn.commit()


def _flush_batch(conn, batch: list) -> int:
    """INSERT a batch of message rows and commit; returns row count."""
    if not batch:
        return 0
    conn.executemany(_INSERT_MESSAGE, batch)
    conn.commit()
    return len(batch)


def _sync_upsert_session(
    conn,
    session_id: str,
    source_name: str,
    path_str: str,
    project_dir: str,
    cwd,
    display_name: str,
    first_ts: str,
    last_ts: str,
    messages_added: int,
    rotated: bool,
) -> None:
    """UPSERT sessions row; on rotation also resets msg_count."""
    conn.execute(
        _INSERT_SESSION,
        (
            session_id,
            source_name,
            path_str,
            project_dir,
            cwd,
            display_name,
            first_ts,
            last_ts,
            messages_added,
            source_name,
        ),
    )
    if rotated:
        conn.execute(_RESET_SESSION_MSG_COUNT, (messages_added, session_id))
    conn.commit()


def _sync_upsert_ingest_state(
    conn,
    path_str: str,
    file_size: int,
    mtime: float,
    last_offset: int,
    current_sha: str,
    now: float,
    messages_added: int,
    errors: int,
    rotated: bool,
) -> None:
    """UPSERT ingest_state row; on rotation also resets msg_extracted."""
    conn.execute(
        _INSERT_INGEST_STATE,
        (
            path_str,
            file_size,
            mtime,
            last_offset,
            current_sha,
            now,
            messages_added,
            errors,
        ),
    )
    if rotated:
        conn.execute(_RESET_INGEST_STATE_MSG_EXTRACTED, (messages_added, path_str))
    conn.commit()


def _sync_update_display_name_if_noise(
    conn, session_id: str, display_name: str
) -> None:
    """UPDATE display_name only when the existing value is framework noise."""
    conn.execute(_UPDATE_DISPLAY_NAME_IF_NOISE, (display_name, session_id))
    conn.commit()


def _sync_checkpoint(conn) -> None:
    """Run PRAGMA wal_checkpoint(PASSIVE); ignore errors."""
    try:
        conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def ingest_file(
    conn,
    source,
    path: Path,
    *,
    checkpoint: bool = True,
    full_index: bool = True,
) -> IngestResult:
    """
    Process a single JSONL file:

    1. Read ingest_state for this path.
    2. Detect rotation (head_sha256 mismatch) or truncation (file_size shrink).
       On rotation/truncation: DELETE messages for this session + reset offset=0.
    3. iter_messages from last_offset.
    4. Batch INSERT 200 messages or 1 MB, whichever comes first.
    5. Update sessions table with accumulated metadata.
    6. Upsert ingest_state (additive counters per §4.1).
    7. PRAGMA wal_checkpoint(PASSIVE).
    8. Return IngestResult.
    """
    t0 = time.monotonic()
    path = path.resolve()
    path_str = str(path)

    # ---- stat the file ----
    try:
        stat = os.stat(path)
        current_size = stat.st_size
        current_mtime = stat.st_mtime
    except OSError as exc:
        log.error("ingest_file: cannot stat %s: %s", path, exc)
        return IngestResult(
            path=path,
            messages_added=0,
            errors=1,
            bytes_read=0,
            elapsed_sec=time.monotonic() - t0,
            rotated=False,
        )

    # ---- read existing ingest_state ----
    row = await asyncio.to_thread(_sync_read_ingest_state, conn, path_str)
    last_offset: int = 0
    stored_sha: Optional[str] = None
    stored_size: int = 0
    stored_mtime: float = 0.0
    rotated = False

    start_offset_before: int = 0  # for bytes_read calculation
    if row is not None:
        last_offset, stored_sha, stored_size, stored_mtime = row[0], row[1], row[2], row[3]
        start_offset_before = last_offset
        # Fast-path: file size equals our last_offset AND mtime unchanged → nothing new
        if current_size == last_offset and current_mtime == stored_mtime:
            # Still attempt auto-register so already-ingested sessions that were
            # not yet in saved_sessions.json (e.g. ingested before this feature
            # was deployed) get picked up on the next watcher ping.
            _fp_session_id = source.session_id_for(path)
            if ':subagent:' not in _fp_session_id and source.name in ('claude', 'codex'):
                try:
                    _fp_name = await asyncio.to_thread(
                        _sync_get_session_display_name, conn, _fp_session_id
                    )
                    if _fp_name:
                        from auto_register import auto_register_session, parse_iso8601_to_epoch
                        _saved_path = Path.home() / '.claude-bridge-runtime' / 'saved_sessions.json'
                        # Prefer the real last message timestamp stored in the
                        # sessions table (last_ts column, ISO8601) over file mtime.
                        _fp_last_ts_row = await asyncio.to_thread(
                            lambda: conn.execute(
                                "SELECT last_ts FROM sessions WHERE session_id = ?",
                                (_fp_session_id,),
                            ).fetchone()
                        )
                        _fp_last_ts_str = (_fp_last_ts_row[0] if _fp_last_ts_row else None) or ''
                        _fp_epoch = parse_iso8601_to_epoch(_fp_last_ts_str)
                        _fp_last_used = _fp_epoch if _fp_epoch is not None else current_mtime
                        auto_register_session(
                            _saved_path,
                            claude_uuid=path.stem,
                            name=_fp_name,
                            cwd=None,
                            backend=source.name,
                            last_used=_fp_last_used,
                        )
                except Exception:
                    pass
            return IngestResult(
                path=path,
                messages_added=0,
                errors=0,
                bytes_read=0,
                elapsed_sec=time.monotonic() - t0,
                rotated=False,
            )

    # ---- compute current sha for rotation detection ----
    # The sha stored in the DB always represents the content at the time of last
    # ingest: full-file sha for small files, head-4KB sha for large files.
    # On the next ingest we need to compare apples to apples.
    #
    # For large files (>= _FULL_FILE_SHA_THRESHOLD): use head-4KB sha.
    #   - Appending beyond 4KB does not change the head sha → no false rotation.
    #   - Overwriting the start of the file does change it → correct rotation.
    #
    # For small files (< _FULL_FILE_SHA_THRESHOLD): compare to the stored sha by
    # reading exactly `stored_size` bytes from the current file.
    #   - If current_size > stored_size (append): sha of first stored_size bytes
    #     should match stored sha; mismatch means content was changed → rotation.
    #   - If current_size == stored_size (same or overwrite): direct sha compare.
    #   - If current_size < stored_size (shrink): handled by truncation check.
    # For new files (row is None), we store the full-file sha for small files.

    _HEAD_BYTES = 4096  # bytes read by source.head_signature

    if current_size < _FULL_FILE_SHA_THRESHOLD:
        # Compute full-file sha (stored for new files; used for comparison in rotation check)
        # Use asyncio.to_thread so large-file reads don't block the event loop.
        try:
            def _read_full() -> str:
                with open(path, 'rb') as _fh:
                    return hashlib.sha256(_fh.read()).hexdigest()
            current_sha = await asyncio.to_thread(_read_full)
        except OSError:
            current_sha = source.head_signature(path)
    else:
        current_sha = source.head_signature(path)

    # ---- rotation / truncation detection ----
    session_id = source.session_id_for(path)

    if row is not None:
        truncated = current_size < last_offset

        # sig_mismatch logic depends on file size category:
        if stored_size < _FULL_FILE_SHA_THRESHOLD:
            # stored_sha is the full-file sha of the file at previous ingest.
            # Compare it against the sha of the first stored_size bytes of the
            # current file to detect rotation vs. append.
            if stored_sha and current_size >= stored_size and stored_size > 0:
                try:
                    _n = stored_size
                    def _read_prefix() -> str:
                        with open(path, 'rb') as _fh:
                            return hashlib.sha256(_fh.read(_n)).hexdigest()
                    _prefix_sha = await asyncio.to_thread(_read_prefix)
                    sig_mismatch = (_prefix_sha != stored_sha)
                except OSError:
                    sig_mismatch = False
            elif stored_sha and current_size == stored_size:
                # Same size: direct sha compare (current_sha is full-file sha)
                sig_mismatch = (current_sha != stored_sha)
            else:
                sig_mismatch = False  # shrink handled by truncated
        else:
            # Large file: stored_sha is head-4KB sha.
            # A sha change here means the beginning was overwritten → rotation.
            sig_mismatch = (
                stored_sha
                and current_sha
                and current_sha != stored_sha
                and stored_size >= _HEAD_BYTES
            )

        if truncated or sig_mismatch:
            reason = "truncation" if truncated else "rotation"
            log.info("ingest_file: %s detected for %s — re-ingesting from offset 0", reason, path)
            rotated = True
            # Delete existing messages for this session
            await asyncio.to_thread(_sync_delete_and_commit, conn, session_id)
            last_offset = 0

    # ---- accumulate metadata for session upsert ----
    session_meta = source.get_session_meta(path)
    source_name = source.name  # 'claude' | 'codex' | 'ollama'

    display_name = session_meta.get('display_name') or path.name
    cwd = session_meta.get('cwd')
    project_dir = session_meta.get('project_dir') or str(path.parent)
    first_ts = session_meta.get('first_ts') or ''

    # ---- iterate messages ----
    messages_added = 0
    errors = 0
    batch: list = []
    batch_content_bytes = 0
    last_ts = first_ts
    first_user_text: Optional[str] = None  # for display_name override from actual msg

    try:
        for msg, next_offset in source.iter_messages(path, start_offset=last_offset):
            # Track first non-noise user message text for display_name
            if first_user_text is None and msg.role == 'user' and not is_framework_noise(msg.text):
                first_user_text = ' '.join(msg.text.split())[:80]

            # Track latest timestamp
            if msg.timestamp and msg.timestamp > last_ts:
                last_ts = msg.timestamp

            last_offset = next_offset

            if not full_index:
                # Metadata-only mode (BRIDGE_SEARCH_FULL_INDEX=0):
                # Skip inserting message body rows — only session metadata is indexed.
                # App-side SQLite FTS handles local message search.
                continue

            row_tuple = (
                msg.session_id,
                msg.msg_uuid,
                msg.parent_uuid,
                msg.role,
                msg.timestamp or '',
                1 if msg.is_subagent else 0,
                msg.text,
            )
            batch.append(row_tuple)
            batch_content_bytes += len(msg.text.encode('utf-8'))

            # Flush batch if size threshold reached
            if len(batch) >= _BATCH_SIZE or batch_content_bytes >= _BATCH_MAX_BYTES:
                try:
                    await asyncio.to_thread(_flush_batch, conn, batch)
                    messages_added += len(batch)
                except sqlite3.Error as db_err:
                    log.error("ingest_file: DB write error for %s: %s", path, db_err)
                    # Do NOT modify last_offset.  Committed batches already advanced it;
                    # this uncommitted batch is dropped.  On the next ingest cycle the
                    # rows will be re-read and silently skipped by ON CONFLICT DO NOTHING
                    # — idempotent by design.
                    errors += 1
                    break
                batch = []
                batch_content_bytes = 0

    except OSError as exc:
        log.error("ingest_file: read error on %s: %s", path, exc)
        errors += 1

    # ---- flush remaining batch ----
    if batch:
        try:
            await asyncio.to_thread(_flush_batch, conn, batch)
            messages_added += len(batch)
        except sqlite3.Error as db_err:
            log.error("ingest_file: DB write error (final batch) for %s: %s", path, db_err)
            errors += 1

    # ---- resolve final display_name (noise-filtered) ----
    # Priority: first_user_text from this ingest batch > session_meta > path stem
    if first_user_text:
        display_name = first_user_text
    elif not display_name:
        display_name = path.stem

    # ---- upsert sessions ----
    try:
        await asyncio.to_thread(
            _sync_upsert_session,
            conn,
            session_id,
            source_name,
            path_str,
            project_dir,
            cwd,
            display_name,
            first_ts,
            last_ts,
            messages_added,  # additive — DO UPDATE adds to existing msg_count
            rotated,
        )
    except sqlite3.Error as exc:
        log.error("ingest_file: sessions upsert failed for %s: %s", path, exc)
        errors += 1

    # ---- patch existing noise display_names from prior ingests ----
    # The UPSERT above uses COALESCE which keeps an existing non-empty value.
    # If the DB already holds a noise value (from a previous ingest run before
    # the noise filter was added), overwrite it now — but only when the stored
    # value still looks like framework noise.
    if display_name and display_name != path.stem:
        try:
            await asyncio.to_thread(
                _sync_update_display_name_if_noise,
                conn,
                session_id,
                display_name,
            )
        except sqlite3.Error as exc:
            log.error("ingest_file: display_name patch failed for %s: %s", path, exc)
            errors += 1

    # ---- auto-register into saved_sessions.json (CLI-discovered sessions) ----
    # Only for main (non-subagent) sessions so CLI conversations appear in the
    # dashboard.  Subagents, ollama, and invalid-UUID stems are skipped inside
    # auto_register_session().
    is_subagent = ':subagent:' in session_id
    if not is_subagent and display_name and source_name in ('claude', 'codex'):
        try:
            from auto_register import auto_register_session, parse_iso8601_to_epoch
            _saved_path = Path.home() / '.claude-bridge-runtime' / 'saved_sessions.json'
            # Use the real last message timestamp from the ingested batch rather
            # than file mtime or wall clock, so the sort order in the dashboard
            # reflects when users actually last interacted with the session.
            # last_ts accumulates the max message.timestamp seen during iteration;
            # fall back to current_mtime only when no timestamp was found.
            _last_msg_epoch = parse_iso8601_to_epoch(last_ts) if last_ts else None
            _last_used = _last_msg_epoch if _last_msg_epoch is not None else current_mtime
            added = auto_register_session(
                _saved_path,
                claude_uuid=path.stem,
                name=display_name,
                cwd=cwd,
                backend=source_name,
                last_used=_last_used,
            )
            if added:
                log.debug(
                    'auto_register: added new session %s (%s)',
                    path.stem[:11], display_name[:40],
                )
        except Exception as exc:
            log.warning('auto_register failed for %s: %s', path, exc)

    # ---- upsert ingest_state ----
    try:
        stat_now = os.stat(path)
        await asyncio.to_thread(
            _sync_upsert_ingest_state,
            conn,
            path_str,
            stat_now.st_size,
            stat_now.st_mtime,
            last_offset,
            current_sha,
            time.time(),
            messages_added,
            errors,
            rotated,
        )
    except (sqlite3.Error, OSError) as exc:
        log.error("ingest_file: ingest_state upsert failed for %s: %s", path, exc)

    # ---- checkpoint (per §5.4; can be skipped during bulk for performance) ----
    if checkpoint:
        await asyncio.to_thread(_sync_checkpoint, conn)

    elapsed = time.monotonic() - t0
    if messages_added or errors or rotated:
        log.debug(
            "ingest_file: %s — %d msgs added, %d errors, rotated=%s, %.2fs",
            path.name, messages_added, errors, rotated, elapsed,
        )

    # Recompute bytes_read as total bytes processed this run.
    _prior_offset = 0 if (row is None or rotated) else start_offset_before
    bytes_read = max(last_offset - _prior_offset, 0)

    return IngestResult(
        path=path,
        messages_added=messages_added,
        errors=errors,
        bytes_read=bytes_read,
        elapsed_sec=elapsed,
        rotated=rotated,
    )
