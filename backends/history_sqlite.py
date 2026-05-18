"""Persistent SQLite cache for JSONL history indexes.

Survives bridge restarts and Mac sleep/wake cycles.  Cache key =
(path, mtime_ns, file_size) — same invalidation as in-memory cache,
so stale entries are transparently bypassed without needing TTL logic.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Optional

_DB_PATH: str = os.path.expanduser(
    os.environ.get(
        "BRIDGE_HISTORY_CACHE_DB",
        "~/.claude-bridge-runtime/bridge_history_cache.db",
    )
)

_lock = threading.Lock()
_conn: Optional[sqlite3.Connection] = None


def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is not None:
        return _conn
    with _lock:
        if _conn is not None:
            return _conn
        os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
        conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-8000")  # 8 MB page cache
        conn.execute("""
            CREATE TABLE IF NOT EXISTS history_cache (
                cache_name  TEXT    PRIMARY KEY,
                mtime_ns    INTEGER NOT NULL,
                file_size   INTEGER NOT NULL,
                cached_at   REAL    NOT NULL,
                messages_json TEXT  NOT NULL
            )
        """)
        conn.commit()
        _conn = conn
    return _conn


def init_cache_db() -> None:
    """Open the DB and prune entries older than 30 days.  Call once at startup."""
    try:
        conn = _get_conn()
        cutoff = time.time() - 30 * 86400
        with _lock:
            conn.execute("DELETE FROM history_cache WHERE cached_at < ?", (cutoff,))
            conn.commit()
    except Exception:
        pass


def sqlite_load(cache_name: str, key: tuple) -> Optional[list]:
    """Return cached messages list if key matches current file state, else None."""
    try:
        _, mtime_ns, file_size = key
        conn = _get_conn()
        row = conn.execute(
            "SELECT mtime_ns, file_size, messages_json FROM history_cache WHERE cache_name = ?",
            (cache_name,),
        ).fetchone()
        if row and row[0] == mtime_ns and row[1] == file_size:
            return json.loads(row[2])
    except Exception:
        pass
    return None


def sqlite_save_background(cache_name: str, key: tuple, messages: list) -> None:
    """Persist messages to SQLite in a daemon thread (non-blocking)."""
    threading.Thread(
        target=_sqlite_save,
        args=(cache_name, key, messages),
        daemon=True,
    ).start()


def _sqlite_save(cache_name: str, key: tuple, messages: list) -> None:
    try:
        _, mtime_ns, file_size = key
        conn = _get_conn()
        with _lock:
            conn.execute(
                """INSERT OR REPLACE INTO history_cache
                   (cache_name, mtime_ns, file_size, cached_at, messages_json)
                   VALUES (?, ?, ?, ?, ?)""",
                (cache_name, mtime_ns, file_size, time.time(), json.dumps(messages)),
            )
            conn.commit()
    except Exception:
        pass
