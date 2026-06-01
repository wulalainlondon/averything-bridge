"""Tests for handlers/instance_ops.py — 5 WebSocket instance management commands."""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_BRIDGE_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_BRIDGE_ROOT))


class _Ws:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))


class _Client:
    client_id = "client_a"
    device_id = "device_a"


def _make_ctx(*, root_dir: str = "", instance_name: str = "master"):
    broadcasts: list[dict] = []
    spawned: list[tuple[str, object]] = []

    async def broadcast(payload: dict) -> int:
        broadcasts.append(payload)
        return 1

    def spawn(name: str, coro) -> None:
        spawned.append((name, coro))
        # Close the coroutine to avoid ResourceWarning
        coro.close()

    ctx = MagicMock()
    ctx.root_dir = root_dir
    ctx.instance_name = instance_name
    ctx.broadcast_json = broadcast
    ctx.spawn_task = spawn
    ctx._broadcasts = broadcasts
    ctx._spawned = spawned
    return ctx


# ---------------------------------------------------------------------------
# Master-only guard
# ---------------------------------------------------------------------------

def test_child_bridge_rejects_all_instance_commands():
    """Any instance command on a child bridge (root_dir non-empty) returns not_master."""
    from handlers.instance_ops import handle_instance_msg

    async def run():
        results = []
        for mtype in ["list_instances", "start_instance", "stop_instance",
                      "upsert_instance", "delete_instance"]:
            ws = _Ws()
            ctx = _make_ctx(root_dir="/some/jail")
            msg = {"type": mtype, "name": "test", "port": 9000, "root_dir": ""}
            handled = await handle_instance_msg(
                mtype=mtype, msg=msg, ws=ws, client=_Client(), ctx=ctx,
            )
            results.append((handled, ws.sent[0] if ws.sent else None))
        return results

    results = asyncio.run(run())
    for handled, sent in results:
        assert handled is True
        assert sent is not None
        assert sent["type"] == "instance_error"
        assert sent["code"] == "not_master"


# ---------------------------------------------------------------------------
# list_instances
# ---------------------------------------------------------------------------

def test_list_instances_returns_correct_shape():
    from handlers.instance_ops import handle_instance_msg

    fake_items = [{"name": "alpha", "port": 9001, "root_dir": "", "data_dir": "/tmp/alpha"}]
    fake_statuses = [{"name": "alpha", "port": 9001, "state": "running"}]

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        with patch("handlers.instance_ops.instances_store.load_instances", return_value=fake_items), \
             patch("handlers.instance_ops.instance_lifecycle.list_status", return_value=fake_statuses):
            handled = await handle_instance_msg(
                mtype="list_instances", msg={"type": "list_instances"},
                ws=ws, client=_Client(), ctx=ctx,
            )
        return handled, ws.sent

    handled, sent = asyncio.run(run())
    assert handled is True
    assert len(sent) == 1
    assert sent[0]["type"] == "instances_list"
    assert sent[0]["instances"] == fake_statuses


# ---------------------------------------------------------------------------
# start_instance
# ---------------------------------------------------------------------------

def test_start_instance_not_found_sends_error():
    from handlers.instance_ops import handle_instance_msg

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        with patch("handlers.instance_ops.instances_store.load_instances", return_value=[]):
            handled = await handle_instance_msg(
                mtype="start_instance",
                msg={"type": "start_instance", "name": "nonexistent"},
                ws=ws, client=_Client(), ctx=ctx,
            )
        return handled, ws.sent

    handled, sent = asyncio.run(run())
    assert handled is True
    assert sent[0]["type"] == "instance_error"
    assert sent[0]["code"] == "not_found"
    assert sent[0]["command"] == "start_instance"


def test_start_instance_success_sends_started_and_schedules_broadcast():
    from handlers.instance_ops import handle_instance_msg

    fake_item = {"name": "beta", "port": 9002, "root_dir": "", "data_dir": "/tmp/beta"}

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        with patch("handlers.instance_ops.instances_store.load_instances", return_value=[fake_item]), \
             patch("handlers.instance_ops.instance_lifecycle.start_instance", return_value=(True, None)):
            handled = await handle_instance_msg(
                mtype="start_instance",
                msg={"type": "start_instance", "name": "beta"},
                ws=ws, client=_Client(), ctx=ctx,
            )
        return handled, ws.sent, ctx._spawned

    handled, sent, spawned = asyncio.run(run())
    assert handled is True
    assert sent[0] == {"type": "instance_started", "name": "beta", "ok": True, "code": None}
    assert len(spawned) == 1
    assert spawned[0][0] == "instances-broadcast"


# ---------------------------------------------------------------------------
# stop_instance
# ---------------------------------------------------------------------------

def test_stop_instance_default_rejected():
    from handlers.instance_ops import handle_instance_msg

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        handled = await handle_instance_msg(
            mtype="stop_instance",
            msg={"type": "stop_instance", "name": "default"},
            ws=ws, client=_Client(), ctx=ctx,
        )
        return handled, ws.sent

    handled, sent = asyncio.run(run())
    assert handled is True
    assert sent[0]["type"] == "instance_error"
    assert sent[0]["code"] == "default_immutable"


def test_stop_instance_not_found_sends_error():
    from handlers.instance_ops import handle_instance_msg

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        with patch("handlers.instance_ops.instances_store.load_instances", return_value=[]):
            handled = await handle_instance_msg(
                mtype="stop_instance",
                msg={"type": "stop_instance", "name": "ghost"},
                ws=ws, client=_Client(), ctx=ctx,
            )
        return handled, ws.sent

    handled, sent = asyncio.run(run())
    assert handled is True
    assert sent[0]["type"] == "instance_error"
    assert sent[0]["code"] == "not_found"


def test_stop_instance_success_schedules_broadcast():
    from handlers.instance_ops import handle_instance_msg

    fake_item = {"name": "gamma", "port": 9003, "root_dir": "", "data_dir": "/tmp/gamma"}

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        with patch("handlers.instance_ops.instances_store.load_instances", return_value=[fake_item]), \
             patch("handlers.instance_ops.instance_lifecycle.stop_instance", return_value=(True, None)):
            handled = await handle_instance_msg(
                mtype="stop_instance",
                msg={"type": "stop_instance", "name": "gamma"},
                ws=ws, client=_Client(), ctx=ctx,
            )
        return handled, ws.sent, ctx._spawned

    handled, sent, spawned = asyncio.run(run())
    assert handled is True
    assert sent[0] == {"type": "instance_stopped", "name": "gamma", "ok": True, "code": None}
    assert len(spawned) == 1
    assert spawned[0][0] == "instances-broadcast"


# ---------------------------------------------------------------------------
# upsert_instance
# ---------------------------------------------------------------------------

def test_upsert_instance_port_in_use_returns_error():
    from handlers.instance_ops import handle_instance_msg

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        with patch("handlers.instance_ops.instances_store.upsert_instance", return_value=(False, "port_in_use")):
            handled = await handle_instance_msg(
                mtype="upsert_instance",
                msg={"type": "upsert_instance", "name": "delta", "port": 9004, "root_dir": ""},
                ws=ws, client=_Client(), ctx=ctx,
            )
        return handled, ws.sent

    handled, sent = asyncio.run(run())
    assert handled is True
    assert sent[0] == {
        "type": "instance_upserted",
        "ok": False,
        "code": "port_in_use",
        "instance": None,
        "name": "delta",
    }


def test_upsert_instance_success_broadcasts_list():
    from handlers.instance_ops import handle_instance_msg

    fake_item = {"name": "epsilon", "port": 9005, "root_dir": "", "data_dir": "/tmp/epsilon",
                 "backend": "", "model": ""}
    fake_statuses = [{"name": "epsilon", "port": 9005, "state": "stopped"}]

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        with patch("handlers.instance_ops.instances_store.upsert_instance", return_value=(True, None)), \
             patch("handlers.instance_ops.instances_store.load_instances", return_value=[fake_item]), \
             patch("handlers.instance_ops.instance_lifecycle.list_status", return_value=fake_statuses):
            handled = await handle_instance_msg(
                mtype="upsert_instance",
                msg={"type": "upsert_instance", "name": "epsilon", "port": 9005, "root_dir": ""},
                ws=ws, client=_Client(), ctx=ctx,
            )
        return handled, ws.sent, ctx._broadcasts

    handled, sent, broadcasts = asyncio.run(run())
    assert handled is True
    assert sent[0]["type"] == "instance_upserted"
    assert sent[0]["ok"] is True
    assert sent[0]["code"] is None
    assert sent[0]["instance"] is not None
    assert broadcasts == [{"type": "instances_list", "instances": fake_statuses}]


def test_upsert_instance_default_data_dir_when_omitted():
    """When data_dir is not in msg, it defaults to ~/.bridge-instances/<name>."""
    from handlers.instance_ops import handle_instance_msg

    captured: list[dict] = []

    async def run():
        ws = _Ws()
        ctx = _make_ctx()

        def fake_upsert(item, path=None):
            captured.append(item)
            return (True, None)

        with patch("handlers.instance_ops.instances_store.upsert_instance", side_effect=fake_upsert), \
             patch("handlers.instance_ops.instances_store.load_instances", return_value=[]), \
             patch("handlers.instance_ops.instance_lifecycle.list_status", return_value=[]):
            await handle_instance_msg(
                mtype="upsert_instance",
                msg={"type": "upsert_instance", "name": "zeta", "port": 9006, "root_dir": ""},
                ws=ws, client=_Client(), ctx=ctx,
            )

    asyncio.run(run())
    assert len(captured) == 1
    expected_data_dir = os.path.expanduser("~/.bridge-instances/zeta")
    assert captured[0]["data_dir"] == expected_data_dir


# ---------------------------------------------------------------------------
# delete_instance
# ---------------------------------------------------------------------------

def test_delete_instance_default_rejected():
    from handlers.instance_ops import handle_instance_msg

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        handled = await handle_instance_msg(
            mtype="delete_instance",
            msg={"type": "delete_instance", "name": "default"},
            ws=ws, client=_Client(), ctx=ctx,
        )
        return handled, ws.sent

    handled, sent = asyncio.run(run())
    assert handled is True
    assert sent[0]["type"] == "instance_error"
    assert sent[0]["code"] == "default_immutable"


def test_delete_instance_success_stops_and_broadcasts():
    from handlers.instance_ops import handle_instance_msg

    fake_item = {"name": "eta", "port": 9007, "root_dir": "", "data_dir": "/tmp/eta"}
    fake_statuses: list[dict] = []

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        with patch("handlers.instance_ops.instances_store.load_instances", return_value=[fake_item]), \
             patch("handlers.instance_ops.instance_lifecycle.stop_instance", return_value=(True, None)) as mock_stop, \
             patch("handlers.instance_ops.instances_store.delete_instance", return_value=(True, None)), \
             patch("handlers.instance_ops.instance_lifecycle.list_status", return_value=fake_statuses):
            handled = await handle_instance_msg(
                mtype="delete_instance",
                msg={"type": "delete_instance", "name": "eta"},
                ws=ws, client=_Client(), ctx=ctx,
            )
        return handled, ws.sent, ctx._broadcasts, mock_stop.call_count

    handled, sent, broadcasts, stop_calls = asyncio.run(run())
    assert handled is True
    assert sent[0] == {"type": "instance_deleted", "name": "eta", "ok": True, "code": None}
    assert stop_calls == 1
    assert broadcasts == [{"type": "instances_list", "instances": fake_statuses}]


def test_delete_instance_not_found_still_returns_result():
    """delete_instance with unknown name: stop is skipped, delete_instance returns not_found."""
    from handlers.instance_ops import handle_instance_msg

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        with patch("handlers.instance_ops.instances_store.load_instances", return_value=[]), \
             patch("handlers.instance_ops.instances_store.delete_instance", return_value=(False, "not_found")), \
             patch("handlers.instance_ops.instance_lifecycle.list_status", return_value=[]):
            handled = await handle_instance_msg(
                mtype="delete_instance",
                msg={"type": "delete_instance", "name": "phantom"},
                ws=ws, client=_Client(), ctx=ctx,
            )
        return handled, ws.sent

    handled, sent = asyncio.run(run())
    assert handled is True
    assert sent[0] == {"type": "instance_deleted", "name": "phantom", "ok": False, "code": "not_found"}


# ---------------------------------------------------------------------------
# Non-instance type passthrough
# ---------------------------------------------------------------------------

def test_handle_instance_msg_returns_false_for_unrelated_type():
    from handlers.instance_ops import handle_instance_msg

    async def run():
        ws = _Ws()
        ctx = _make_ctx()
        return await handle_instance_msg(
            mtype="ping", msg={"type": "ping"},
            ws=ws, client=_Client(), ctx=ctx,
        )

    assert asyncio.run(run()) is False
