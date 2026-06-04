"""Small asyncio task registry with centralized exception logging."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any


log = logging.getLogger("bridge_v2")
_TASKS: set[asyncio.Task[Any]] = set()
_TASK_OWNERS: dict[asyncio.Task[Any], str] = {}


def spawn(name: str, coro: Coroutine[Any, Any, Any], *, owner: str | None = None) -> asyncio.Task[Any]:
    """Create a task, retain it until done, and log unhandled exceptions."""
    task = asyncio.create_task(coro, name=name)
    _TASKS.add(task)
    if owner:
        _TASK_OWNERS[task] = owner

    def _done(done: asyncio.Task[Any]) -> None:
        _TASKS.discard(done)
        _TASK_OWNERS.pop(done, None)
        try:
            done.result()
        except asyncio.CancelledError:
            log.debug("[task] cancelled name=%s", done.get_name())
        except Exception:
            log.exception("[task] failed name=%s", done.get_name())

    task.add_done_callback(_done)
    return task


def cancel_owner(owner: str) -> int:
    """Cancel tracked tasks owned by a disconnected client."""
    tasks = [task for task, task_owner in list(_TASK_OWNERS.items()) if task_owner == owner]
    for task in tasks:
        task.cancel()
    return len(tasks)


async def cancel_all() -> None:
    """Cancel all tracked tasks and wait for their callbacks to observe results."""
    tasks = list(_TASKS)
    if not tasks:
        return
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    _TASK_OWNERS.clear()


def active_count() -> int:
    return len(_TASKS)
