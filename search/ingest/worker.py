"""
worker.py — IngestWorker: owns the writer DB connection, orchestrates bulk + incremental ingest.

Per SCHEMA_REVIEW §5.2: writer connection is exclusive to IngestWorker.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from config.schema import BridgeConfig

from config import get_config
from ..db import open_connection, init_schema, migrate
from .bulk import BulkResult, bulk_ingest
from .single_file import IngestResult, ingest_file
from .watcher import WatchdogWatcher

# Feature flag: when False, only session metadata is indexed (message body skipped).
# Controlled by BRIDGE_SEARCH_FULL_INDEX env var (0 = metadata-only, 1/unset = full).
import os as _os
_FULL_INDEX: bool = _os.environ.get("BRIDGE_SEARCH_FULL_INDEX", "1").strip() not in ("0", "false", "no")

log = logging.getLogger(__name__)


class IngestWorker:
    """
    Owns the writer SQLite connection (singleton per process).
    Orchestrates:
      - Startup bulk ingest (background asyncio.Task)
      - Watchdog file watching → incremental ingest
      - Progress reporting for health endpoint

    Thread-safety: all DB access runs in the asyncio event loop thread.
    Per SCHEMA_REVIEW §5.2, this connection must NOT be shared with readers.
    """

    def __init__(
        self,
        config: 'BridgeConfig | None' = None,
        activity_probe: Optional[Callable[[], bool]] = None,
    ):
        if config is None:
            config = get_config()
        self._config = config
        from ..sources import registered_sources
        self._sources: List = registered_sources(config)
        self._conn = None  # opened in start()
        self._queue: asyncio.Queue[Path] = asyncio.Queue(maxsize=10_000)
        self._watcher: Optional[WatchdogWatcher] = None
        self._bulk_task: Optional[asyncio.Task] = None
        self._consumer_task: Optional[asyncio.Task] = None
        self._ready_event = asyncio.Event()
        self._bulk_failed = False
        self._progress: Dict = {'total': 0, 'done': 0, 'errors': 0}
        self._stopped = False
        self._activity_probe = activity_probe
        # full_index from config takes precedence over module-level env var
        self._full_index: bool = getattr(config.search, 'full_index', _FULL_INDEX)
        # Event that is set when no ingest_file call is in progress (idle).
        # Starts set (idle); cleared while an incremental ingest is running.
        self._inflight_event: asyncio.Event = asyncio.Event()
        self._inflight_event.set()

    # ----------------------------------------------------------------
    # Lifecycle
    # ----------------------------------------------------------------

    async def start(self) -> None:
        """
        1. Open + init + migrate DB.
        2. Start background bulk_ingest task.
        3. Start watchdog watcher.
        4. Start queue consumer task.
        """
        if self._conn is not None:
            log.warning("IngestWorker.start() called but already running")
            return

        db_path = self._config.search.index_path
        log.info("IngestWorker: opening DB at %s", db_path)
        self._conn = open_connection(db_path)
        init_schema(self._conn)
        migrate(self._conn)

        # Bulk ingest (background, non-blocking)
        if self._config.search.ingest_on_startup:
            self._bulk_task = asyncio.create_task(
                self._run_bulk_after_startup_delay(), name="search-bulk-ingest"
            )
        else:
            log.info("IngestWorker: ingest_on_startup=False, skipping bulk")
            self._ready_event.set()

        # Watchdog
        if self._config.search.watch_enabled:
            self._watcher = WatchdogWatcher(
                sources=self._sources,
                queue=self._queue,
                poll_interval=self._config.search.watch_interval_sec,
            )
            self._watcher.start()

        # Consumer
        self._consumer_task = asyncio.create_task(
            self._consume_queue(), name="search-ingest-consumer"
        )

    async def stop(self) -> None:
        """Graceful shutdown: stop watcher, cancel tasks, close DB.

        Tasks are cancelled directly (no asyncio.shield) so cancellation
        propagates immediately.  The DB connection is closed only after the
        tasks have fully stopped, preventing in-flight writes from being
        interrupted by a connection close.
        """
        if self._stopped:
            return
        self._stopped = True
        log.info("IngestWorker: stopping")

        if self._watcher is not None:
            self._watcher.stop()
            self._watcher = None

        # Wait for any in-flight incremental ingest_file call to finish before
        # cancelling tasks and closing the connection.  This prevents SQLite
        # "closed database" errors when stop() races with an active write.
        try:
            await asyncio.wait_for(self._inflight_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            log.warning(
                "IngestWorker: timed out waiting for in-flight ingest to finish; "
                "proceeding with shutdown"
            )

        for task in (self._consumer_task, self._bulk_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task  # no shield, no timeout — cancel propagates promptly
                except (asyncio.CancelledError, Exception):
                    pass

        conn = self._conn
        self._conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

        log.info("IngestWorker: stopped")

    # ----------------------------------------------------------------
    # State queries (for health endpoint)
    # ----------------------------------------------------------------

    def is_ready(self) -> bool:
        return self._ready_event.is_set() and not self._bulk_failed

    def get_progress(self) -> dict:
        d = {
            'total': self._progress['total'],
            'done': self._progress['done'],
            'errors': self._progress['errors'],
            'ready': self._ready_event.is_set() and not self._bulk_failed,
        }
        if self._bulk_failed:
            d['error'] = 'bulk_ingest_failed'
        return d

    # ----------------------------------------------------------------
    # Internal: bulk ingest
    # ----------------------------------------------------------------

    async def _run_bulk_after_startup_delay(self) -> None:
        delay = max(0.0, float(getattr(self._config.search, 'ingest_startup_delay_sec', 0.0)))
        if delay > 0:
            log.info("IngestWorker: delaying startup bulk ingest by %.1fs", delay)
            await asyncio.sleep(delay)
        await self._run_bulk()

    async def _run_bulk(self) -> None:
        log.info("IngestWorker: starting bulk ingest")
        self._progress['done'] = 0
        self._progress['errors'] = 0

        def _progress_cb(path_str: str, done: int, total: int) -> None:
            self._progress['done'] = done
            self._progress['total'] = total

        try:
            result: BulkResult = await bulk_ingest(
                self._conn,
                self._sources,
                progress_callback=_progress_cb,
                pause_every_files=max(
                    0,
                    int(getattr(self._config.search, 'ingest_bulk_pause_every_files', 0)),
                ),
                pause_sec=max(0.0, float(getattr(self._config.search, 'ingest_bulk_pause_sec', 0.0))),
                activity_probe=self._activity_probe,
                activity_pause_sec=max(
                    0.0,
                    float(getattr(self._config.search, 'ingest_idle_pause_sec', 0.0)),
                ),
            )
            self._progress['errors'] = result.total_errors
            log.info(
                "IngestWorker: bulk complete — %d files, %d messages, %d errors, %.1fs",
                result.total_files, result.total_messages, result.total_errors, result.elapsed_sec,
            )
        except asyncio.CancelledError:
            log.info("IngestWorker: bulk ingest cancelled")
            raise
        except Exception as exc:
            log.error("IngestWorker: bulk ingest failed: %s", exc)
            self._bulk_failed = True
        else:
            self._ready_event.set()

    # ----------------------------------------------------------------
    # Internal: queue consumer (incremental ingest)
    # ----------------------------------------------------------------

    async def _consume_queue(self) -> None:
        """Pull paths from queue and ingest them, waiting for bulk to finish first."""
        log.info("IngestWorker: consumer task started")
        # Wait until bulk is done so we don't fight for the writer connection
        # during heavy startup I/O — bulk already holds it, consumer serialises behind
        await self._ready_event.wait()

        while not self._stopped:
            try:
                path = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            conn = self._conn
            if conn is None:
                break

            source = self._find_source_for(path)
            if source is None:
                self._queue.task_done()
                continue

            self._inflight_event.clear()
            try:
                result: IngestResult = await ingest_file(
                    conn, source, path, full_index=self._full_index
                )
                if result.messages_added:
                    log.debug(
                        "IngestWorker: incremental — %s: +%d msgs",
                        path.name, result.messages_added,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("IngestWorker: incremental ingest error for %s: %s", path, exc)
            finally:
                self._inflight_event.set()
                self._queue.task_done()

    def upsert_session_metadata(
        self,
        *,
        session_id: str,
        source: str,
        cwd: str,
        display_name: str,
    ) -> None:
        """Immediately upsert a sessions row so FTS5 search finds new sessions
        before the file-watcher runs.  Called synchronously from the bridge's
        new_session handler (already on the asyncio event-loop thread); safe
        because the writer connection is always owned by this worker.

        Does nothing when the DB connection is not yet open.
        """
        if self._conn is None:
            return
        try:
            import time as _t
            now_iso = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            self._conn.execute(
                """
                INSERT INTO sessions(
                    session_id, source, source_path, project_dir,
                    cwd, display_name, first_ts, last_ts,
                    msg_count, backend
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    display_name = CASE
                        WHEN sessions.display_name IS NULL OR sessions.display_name = ''
                        THEN excluded.display_name
                        ELSE sessions.display_name
                    END,
                    cwd = COALESCE(sessions.cwd, excluded.cwd)
                """,
                (
                    session_id,
                    source,
                    "",         # source_path unknown at creation time
                    "",         # project_dir unknown at creation time
                    cwd,
                    display_name,
                    now_iso,
                    now_iso,
                    source,
                ),
            )
            self._conn.commit()
            log.debug("upsert_session_metadata: inserted %s (%s)", session_id[:11], display_name[:40])
        except Exception as exc:
            log.warning("upsert_session_metadata failed for %s: %s", session_id, exc)

    def upsert_first_user_message(
        self,
        *,
        session_id: str,
        content: str,
        msg_uuid: str,
        ts: str,
    ) -> None:
        """Insert the first user message into messages (and FTS5 via trigger) so
        the session body is immediately searchable without waiting for the
        file-watcher ingest cycle.

        - Idempotent: ON CONFLICT(session_id, msg_uuid) DO NOTHING.
        - Does nothing when the DB connection is not yet open.
        - Failures are logged as warnings and never re-raised.
        """
        if self._conn is None:
            return
        try:
            self._conn.execute(
                """
                INSERT INTO messages(session_id, msg_uuid, parent_uuid, role, ts, is_subagent, content)
                VALUES (?, ?, NULL, 'user', ?, 0, ?)
                ON CONFLICT(session_id, msg_uuid) DO NOTHING
                """,
                (session_id, msg_uuid, ts, content),
            )
            self._conn.commit()
            log.debug("upsert_first_user_message: indexed msg %s for session %s", msg_uuid[:11], session_id[:11])
        except Exception as exc:
            log.warning("upsert_first_user_message failed for %s: %s", session_id, exc)

    def _find_source_for(self, path: Path) -> object | None:
        """Return the matching SearchSource for a given path, or None."""
        for source in self._sources:
            if not source.is_enabled():
                continue
            # Use the source's own watch_root to test path membership,
            # respecting any config-driven overrides.
            root = getattr(source, 'watch_root', None)
            if root is not None:
                try:
                    path.relative_to(root)
                    return source
                except ValueError:
                    continue
        return None
