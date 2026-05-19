"""
open_connection() — opens a SQLite connection and applies the full PRAGMA suite.

Per SCHEMA_REVIEW §5.3.
"""
from __future__ import annotations

from pathlib import Path

from .sqlite_adapter import sqlite3

# PRAGMAs safe on both read-only and read-write connections.
_SHARED_PRAGMAS = [
    "PRAGMA cache_size = -65536",        # 64 MB (negative = KB)
    "PRAGMA temp_store = MEMORY",
    "PRAGMA mmap_size = 268435456",      # 256 MB
    "PRAGMA busy_timeout = 5000",        # 5 s auto-retry on writer lock
    "PRAGMA foreign_keys = ON",
]

# PRAGMAs that require write access — must NOT run on read-only connections.
_WRITE_PRAGMAS = [
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA wal_autocheckpoint = 1000",
]

# PRAGMAs that only make sense on read-only connections.
_READONLY_ONLY_PRAGMAS = [
    "PRAGMA query_only = 1",
]


def open_connection(db_path: Path | str, *, read_only: bool = False) -> sqlite3.Connection:
    """Open a SQLite connection, apply all PRAGMAs, and return it.

    Args:
        db_path: Path to the .db file. Will be created if it does not exist
                 (unless read_only=True).
        read_only: Open in URI read-only mode. Raises if the file does not exist.
                   Write-requiring PRAGMAs (journal_mode, synchronous,
                   wal_autocheckpoint) are skipped; query_only=1 is set instead.

    Returns:
        sqlite3.Connection with check_same_thread=False (caller is responsible
        for not sharing across threads/coroutines without locking).
    """
    db_path = Path(db_path)

    if read_only:
        uri = db_path.as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
        for pragma in _SHARED_PRAGMAS + _READONLY_ONLY_PRAGMAS:
            conn.execute(pragma)
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        for pragma in _SHARED_PRAGMAS + _WRITE_PRAGMAS:
            conn.execute(pragma)

    return conn
