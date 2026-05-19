"""
bulk.py — startup bulk ingest across all enabled sources.

Iterates all paths discovered by each SearchSource, calls ingest_file()
for each, and yields progress via an optional callback.
Uses asyncio.sleep(0) between files to stay cooperative.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..sources.base import SearchSource

from .single_file import IngestResult, ingest_file

log = logging.getLogger(__name__)


@dataclass
class BulkResult:
    total_files: int
    total_messages: int
    total_errors: int
    total_bytes: int
    elapsed_sec: float
    results: List[IngestResult] = field(default_factory=list)


async def bulk_ingest(
    conn,
    sources: List,
    progress_callback: Optional[Callable[[str, int, int], None]] = None,
    pause_every_files: int = 0,
    pause_sec: float = 0.0,
    activity_probe: Optional[Callable[[], bool]] = None,
    activity_pause_sec: float = 0.0,
) -> BulkResult:
    """
    Bulk-ingest all files from all enabled sources.

    Args:
        conn: SQLite writer connection (must already have schema initialised).
        sources: List of SearchSource instances.
        progress_callback: Optional callable(path_str, done_count, total_count).
                           Called after every file processed.
        pause_every_files: If > 0, sleep for pause_sec after every N files.
        pause_sec: Background throttle sleep duration.
        activity_probe: Optional callback returning True when foreground traffic is recent.
        activity_pause_sec: Longer background sleep while foreground traffic is recent.

    Returns:
        BulkResult with aggregate counts.

    Performance target: < 30 s for ~3,378 files / 26 MB signal
    (per Phase 1 benchmark: 179 MB/s Python parse throughput).
    """
    t0 = time.monotonic()

    # ---- discover all paths ----
    all_paths: List[tuple] = []  # (source, path)
    for source in sources:
        if not source.is_enabled():
            log.info("bulk_ingest: source '%s' not enabled, skipping", source.name)
            continue
        try:
            for path in source.discover():
                all_paths.append((source, path))
        except Exception as exc:
            log.error("bulk_ingest: discover() failed for source '%s': %s", source.name, exc)

    total = len(all_paths)
    log.info("bulk_ingest: discovered %d files across %d sources", total, len(sources))

    total_messages = 0
    total_errors = 0
    total_bytes = 0
    results: List[IngestResult] = []

    _CHECKPOINT_INTERVAL = 500  # checkpoint WAL every N files during bulk

    for done, (source, path) in enumerate(all_paths, start=1):
        # Skip checkpoint inside ingest_file during bulk; we checkpoint periodically here
        do_checkpoint = (done % _CHECKPOINT_INTERVAL == 0) or (done == total)
        try:
            result = await ingest_file(conn, source, path, checkpoint=do_checkpoint)
            results.append(result)
            total_messages += result.messages_added
            total_errors += result.errors
            total_bytes += result.bytes_read
        except Exception as exc:
            log.error("bulk_ingest: unexpected error for %s: %s", path, exc)
            total_errors += 1

        if progress_callback is not None:
            try:
                progress_callback(str(path), done, total)
            except Exception:
                pass

        if pause_every_files > 0 and done % pause_every_files == 0:
            sleep_for = pause_sec if pause_sec > 0 else 0.0
            if activity_probe is not None:
                try:
                    if activity_probe():
                        sleep_for = max(sleep_for, activity_pause_sec)
                except Exception:
                    pass
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            else:
                await asyncio.sleep(0)
        else:
            # Yield to event loop every file. to_thread() already yields during each
            # sqlite call; sleep(0) here gives an extra cooperative yield between files
            # at zero wall-time cost.
            await asyncio.sleep(0)

    elapsed = time.monotonic() - t0
    log.info(
        "bulk_ingest: done — %d files, %d messages, %d errors, %.1fs",
        total, total_messages, total_errors, elapsed,
    )

    return BulkResult(
        total_files=total,
        total_messages=total_messages,
        total_errors=total_errors,
        total_bytes=total_bytes,
        elapsed_sec=elapsed,
        results=results,
    )
