"""Small asyncio task registry with centralized exception logging."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any


log = logging.getLogger("bridge_v2")
_TASKS: set[asyncio.Task[Any]] = set()


def spawn(name: str, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
    """Create a task, retain it until done, and log unhandled exceptions."""
    task = asyncio.create_task(coro, name=name)
    _TASKS.add(task)

    def _done(done: asyncio.Task[Any]) -> None:
        _TASKS.discard(done)
        try:
            done.result()
        except asyncio.CancelledError:
            log.debug("[task] cancelled name=%s", done.get_name())
        except Exception:
            log.exception("[task] failed name=%s", done.get_name())

    task.add_done_callback(_done)
    return task


async def cancel_all() -> None:
    """Cancel all tracked tasks and wait for their callbacks to observe results."""
    tasks = list(_TASKS)
    if not tasks:
        return
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


def active_count() -> int:
    return len(_TASKS)
