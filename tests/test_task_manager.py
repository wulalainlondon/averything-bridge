from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path


_BRIDGE_ROOT = Path(__file__).parent.parent
_REPO_ROOT = _BRIDGE_ROOT.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_BRIDGE_ROOT))


def test_spawn_removes_successful_task():
    import task_manager

    async def run():
        await task_manager.cancel_all()

        async def ok():
            return "done"

        task = task_manager.spawn("unit-ok", ok())
        await task
        await asyncio.sleep(0)
        return task_manager.active_count()

    assert asyncio.run(run()) == 0


def test_spawn_logs_failed_task(caplog):
    import task_manager

    async def run():
        await task_manager.cancel_all()

        async def boom():
            raise RuntimeError("task exploded")

        task_manager.spawn("unit-boom", boom())
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    with caplog.at_level(logging.ERROR, logger="bridge_v2"):
        asyncio.run(run())

    assert "[task] failed name=unit-boom" in caplog.text
    assert "RuntimeError: task exploded" in caplog.text
    assert task_manager.active_count() == 0


def test_cancel_all_clears_tracked_tasks():
    import task_manager

    async def run():
        await task_manager.cancel_all()
        started = asyncio.Event()

        async def wait_forever():
            started.set()
            await asyncio.Future()

        task_manager.spawn("unit-cancel", wait_forever())
        await started.wait()
        assert task_manager.active_count() == 1
        await task_manager.cancel_all()
        await asyncio.sleep(0)
        return task_manager.active_count()

    assert asyncio.run(run()) == 0
