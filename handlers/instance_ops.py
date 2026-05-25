"""Instance management WebSocket commands (master bridge only)."""
from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import instance_lifecycle
import instances_store

_INSTANCE_TYPES = frozenset({
    "list_instances",
    "start_instance",
    "stop_instance",
    "upsert_instance",
    "delete_instance",
})


async def _safe_send(ws: Any, payload: dict) -> None:
    try:
        await ws.send(json.dumps(payload))
    except Exception:
        pass


async def _delayed_broadcast(ctx: Any, delay: float) -> None:
    await asyncio.sleep(delay)
    items = instances_store.load_instances()
    statuses = instance_lifecycle.list_status(items)
    await ctx.broadcast_json({"type": "instances_list", "instances": statuses})


async def handle_instance_msg(
    *,
    mtype: str,
    msg: dict,
    ws: Any,
    client: Any,
    ctx: Any,
) -> bool:
    if mtype not in _INSTANCE_TYPES:
        return False

    # Master-only guard: child bridges (root_dir non-empty) reject all instance ops.
    if ctx.root_dir:
        await _safe_send(ws, {
            "type": "instance_error",
            "command": mtype,
            "name": str(msg.get("name") or ""),
            "code": "not_master",
            "message": "Instance management is only available on the master bridge",
        })
        return True

    if mtype == "list_instances":
        items = instances_store.load_instances()
        statuses = instance_lifecycle.list_status(items)
        await _safe_send(ws, {"type": "instances_list", "instances": statuses})
        return True

    if mtype == "start_instance":
        name: str = msg["name"]
        items = instances_store.load_instances()
        item = next((i for i in items if i["name"] == name), None)
        if not item:
            await _safe_send(ws, {
                "type": "instance_error",
                "command": mtype,
                "name": name,
                "code": "not_found",
                "message": f"Instance '{name}' not found",
            })
            return True
        ok, code = instance_lifecycle.start_instance(item)
        await _safe_send(ws, {
            "type": "instance_started",
            "name": name,
            "ok": ok,
            "code": code,
        })
        ctx.spawn_task(
            "instances-broadcast",
            _delayed_broadcast(ctx, 2.0),
        )
        return True

    if mtype == "stop_instance":
        name = msg["name"]
        if name == "default":
            await _safe_send(ws, {
                "type": "instance_error",
                "command": mtype,
                "name": name,
                "code": "default_immutable",
                "message": "The default instance cannot be stopped",
            })
            return True
        items = instances_store.load_instances()
        item = next((i for i in items if i["name"] == name), None)
        if not item:
            await _safe_send(ws, {
                "type": "instance_error",
                "command": mtype,
                "name": name,
                "code": "not_found",
                "message": f"Instance '{name}' not found",
            })
            return True
        ok, code = instance_lifecycle.stop_instance(name, item)
        await _safe_send(ws, {
            "type": "instance_stopped",
            "name": name,
            "ok": ok,
            "code": code,
        })
        ctx.spawn_task(
            "instances-broadcast",
            _delayed_broadcast(ctx, 2.0),
        )
        return True

    if mtype == "upsert_instance":
        name = str(msg["name"])
        port: int = msg["port"]
        root_dir: str = str(msg["root_dir"])
        data_dir: str = str(
            msg.get("data_dir") or
            os.path.expanduser(f"~/.bridge-instances/{name}")
        )
        backend: str = str(msg.get("backend") or "")
        model: str = str(msg.get("model") or "")

        item = {
            "name": name,
            "port": port,
            "root_dir": root_dir,
            "data_dir": data_dir,
            "backend": backend,
            "model": model,
        }
        ok, code = instances_store.upsert_instance(item)
        if ok:
            await _safe_send(ws, {
                "type": "instance_upserted",
                "ok": True,
                "name": name,
                "code": None,
                "instance": item,
            })
            items = instances_store.load_instances()
            statuses = instance_lifecycle.list_status(items)
            await ctx.broadcast_json({"type": "instances_list", "instances": statuses})
        else:
            await _safe_send(ws, {
                "type": "instance_upserted",
                "ok": False,
                "name": name,
                "code": code,
                "instance": None,
            })
        return True

    if mtype == "delete_instance":
        name = msg["name"]
        if name == "default":
            await _safe_send(ws, {
                "type": "instance_error",
                "command": mtype,
                "name": name,
                "code": "default_immutable",
                "message": "The default instance cannot be deleted",
            })
            return True
        items = instances_store.load_instances()
        item = next((i for i in items if i["name"] == name), None)
        if item:
            ok_stop, _ = instance_lifecycle.stop_instance(name, item)
        ok, code = instances_store.delete_instance(name)
        await _safe_send(ws, {
            "type": "instance_deleted",
            "name": name,
            "ok": ok,
            "code": code,
        })
        items = instances_store.load_instances()
        statuses = instance_lifecycle.list_status(items)
        await ctx.broadcast_json({"type": "instances_list", "instances": statuses})
        return True

    return False  # unreachable but satisfies type checker
