"""
SimpleConnectionPool — read-only sqlite3 connection pool for query layer.

Per SCHEMA_REVIEW §5.2:
- Writer (ingest worker) owns a dedicated connection.
- Readers use a pool; one connection per coroutine/thread.
- sqlite3.Connection is NOT safe to share across coroutines/threads.
- Queries run in asyncio.to_thread with per-task connection from pool.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from search.db.sqlite_adapter import sqlite3
from search.db.connection import open_connection


class ConnectionPool:
    """
    Pool of read-only sqlite3 connections.

    Opens connections lazily up to max_size. Excess acquire() calls wait
    until a connection is returned via release().

    A BoundedSemaphore(max_size) gates both lazy creation and waiting, which
    eliminates the TOCTOU race where concurrent coroutines could each pass the
    _total_created < max_size check and over-allocate beyond the pool limit.
    """

    def __init__(self, db_path: Path, max_size: int = 4) -> None:
        self._path = Path(db_path)
        self._max_size = max_size
        self._available: asyncio.Queue[sqlite3.Connection] = asyncio.Queue()
        self._total_created = 0
        self._lock = asyncio.Lock()
        # Semaphore starts at max_size; each "slot" is one connection ticket.
        # acquire() takes a ticket (blocking when all are in use);
        # release() returns the ticket when the connection goes back to the pool.
        self._semaphore = asyncio.BoundedSemaphore(max_size)

    async def acquire(self) -> sqlite3.Connection:
        """Return a connection from the pool, creating one if under max_size.

        Blocks until a slot is available (bounded by max_size), preventing
        over-allocation even under concurrent callers.
        """
        # Gate: blocks when all max_size tickets are in use.
        await self._semaphore.acquire()

        # A ticket is now ours.  Try the queue first (reuse idle connection).
        try:
            return self._available.get_nowait()
        except asyncio.QueueEmpty:
            pass

        # No idle connection — create a new one (safely within the ticket).
        conn = open_connection(self._path, read_only=True)
        self._total_created += 1
        return conn

    async def release(self, conn: sqlite3.Connection) -> None:
        """Return a connection to the pool and release its semaphore ticket."""
        await self._available.put(conn)
        self._semaphore.release()

    @asynccontextmanager
    async def borrow(self):
        """Context manager: acquire a connection, yield it, then release."""
        conn = await self.acquire()
        try:
            yield conn
        finally:
            await self.release(conn)

    async def close_all(self) -> None:
        """Close all currently available connections. Does not wait for borrowed ones."""
        closed = 0
        while True:
            try:
                conn = self._available.get_nowait()
                conn.close()
                closed += 1
            except asyncio.QueueEmpty:
                break
        self._total_created = max(0, self._total_created - closed)
