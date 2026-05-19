"""
Search index health reporting.

Queries sessions, messages, and ingest_state tables for statistics.
Optionally queries the ingest worker for live progress.
"""
from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field

from .connection_pool import ConnectionPool


@dataclass
class SearchHealth:
    indexed_sessions: int
    indexed_messages: int
    db_size_mb: float
    ingest_lag_seconds: float | None    # max(now - ingest_state.last_ingest_at)
    last_full_rebuild_at: str | None
    errors_last_24h: int
    ready: bool                          # ingest worker ready?
    ingest_progress: dict                # from worker.get_progress() or fallback


def _run_health_queries(conn) -> dict:
    """Synchronous SQL queries for health stats; run inside asyncio.to_thread."""
    result: dict = {}

    row = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()
    result["indexed_sessions"] = row[0] if row else 0

    row = conn.execute("SELECT COUNT(*) FROM messages").fetchone()
    result["indexed_messages"] = row[0] if row else 0

    # Ingest lag: how long since any file was last ingested
    row = conn.execute(
        "SELECT MAX(last_ingest_at) FROM ingest_state"
    ).fetchone()
    last_ingest_at = row[0] if row and row[0] is not None else None
    if last_ingest_at is not None:
        result["ingest_lag_seconds"] = time.time() - last_ingest_at
    else:
        result["ingest_lag_seconds"] = None

    # Last full rebuild timestamp from schema_meta
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'last_full_rebuild_at'"
    ).fetchone()
    result["last_full_rebuild_at"] = row[0] if row else None

    # Errors in last 24h from ingest_state (errors column is cumulative per file;
    # we count files that had any errors recently as a proxy)
    cutoff = time.time() - 86400
    row = conn.execute(
        "SELECT COALESCE(SUM(errors), 0) FROM ingest_state WHERE last_ingest_at >= ?",
        (cutoff,),
    ).fetchone()
    result["errors_last_24h"] = row[0] if row else 0

    return result


async def health(conn_pool: ConnectionPool) -> SearchHealth:
    """Return current search index health stats."""
    async with conn_pool.borrow() as conn:
        stats = await asyncio.to_thread(_run_health_queries, conn)

    # Try to get live ingest worker progress
    ingest_progress: dict = {}
    ready = False
    try:
        from search.ingest import get_worker  # type: ignore[import]
        worker = get_worker()
        if worker is None:
            ingest_progress = {"status": "unavailable"}
            ready = False
        else:
            ingest_progress = worker.get_progress()
            ready = worker.is_ready()  # respects _bulk_failed flag (Phase 5 F2)
    except ImportError:
        # Ingest worker module not yet available — return fallback
        ingest_progress = {"status": "unavailable", "note": "ingest worker not yet initialised"}
        ready = False
    except Exception as exc:
        ingest_progress = {"status": "error", "message": str(exc)}
        ready = False

    return SearchHealth(
        indexed_sessions=stats["indexed_sessions"],
        indexed_messages=stats["indexed_messages"],
        db_size_mb=_db_size_mb(conn_pool),
        ingest_lag_seconds=stats["ingest_lag_seconds"],
        last_full_rebuild_at=stats["last_full_rebuild_at"],
        errors_last_24h=stats["errors_last_24h"],
        ready=ready,
        ingest_progress=ingest_progress,
    )


def _db_size_mb(conn_pool: ConnectionPool) -> float:
    """Return the DB file size in megabytes, or 0.0 if not accessible."""
    try:
        path = conn_pool._path
        size_bytes = os.path.getsize(path)
        return round(size_bytes / (1024 * 1024), 3)
    except OSError:
        return 0.0
