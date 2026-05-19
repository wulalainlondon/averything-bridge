"""
bridge.search.ingest — public API for the search ingest subsystem.

Usage:
    from search.ingest import start_worker, stop_worker, get_worker

    # At bridge startup:
    worker = await start_worker()

    # At bridge shutdown:
    await stop_worker()

    # From health endpoint:
    worker = get_worker()
    progress = worker.get_progress() if worker else {}
"""
from __future__ import annotations

import asyncio
import logging
from typing import Callable, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ...config.schema import BridgeConfig

from .worker import IngestWorker

log = logging.getLogger(__name__)

__all__ = [
    "IngestWorker",
    "start_worker",
    "stop_worker",
    "get_worker",
]

_worker_singleton: Optional[IngestWorker] = None
_singleton_lock: Optional[asyncio.Lock] = None


def _get_singleton_lock() -> asyncio.Lock:
    """Return (creating if needed) the module-level asyncio.Lock for singleton guard."""
    global _singleton_lock
    if _singleton_lock is None:
        _singleton_lock = asyncio.Lock()
    return _singleton_lock


async def start_worker(
    config: 'BridgeConfig | None' = None,
    activity_probe: Optional[Callable[[], bool]] = None,
) -> IngestWorker:
    """
    Start the ingest worker (idempotent — returns existing worker if already running).

    Args:
        config: Optional BridgeConfig. If None, loads from get_config().

    Returns:
        The running IngestWorker instance.
    """
    global _worker_singleton
    # Fast-path: avoid lock acquisition when already started
    if _worker_singleton is not None:
        log.debug("start_worker: worker already running, returning existing instance")
        return _worker_singleton

    async with _get_singleton_lock():
        # Re-check after acquiring lock (double-checked locking pattern)
        if _worker_singleton is not None:
            log.debug("start_worker: worker already running (post-lock check), returning existing instance")
            return _worker_singleton

        worker = IngestWorker(config=config, activity_probe=activity_probe)
        await worker.start()
        _worker_singleton = worker
        log.info("start_worker: IngestWorker started")
        return worker


async def stop_worker() -> None:
    """Stop and tear down the singleton IngestWorker."""
    global _worker_singleton, _singleton_lock
    if _worker_singleton is None:
        return
    worker = _worker_singleton
    _worker_singleton = None
    _singleton_lock = None  # Reset so next start_worker() creates a fresh lock
    await worker.stop()
    log.info("stop_worker: IngestWorker stopped")


def get_worker() -> Optional[IngestWorker]:
    """Return the current IngestWorker, or None if not started."""
    return _worker_singleton
