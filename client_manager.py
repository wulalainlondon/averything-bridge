"""WebSocket client registry and broadcast helpers."""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable


@dataclass
class ClientConn:
    client_id: str
    device_id: str
    device_name: str
    ws: Any
    connected_at: float
    last_seen: float


CLIENTS: dict[Any, ClientConn] = {}


def register(ws: Any, client: ClientConn) -> None:
    CLIENTS[ws] = client


def remove(ws: Any) -> ClientConn | None:
    return CLIENTS.pop(ws, None)


def values() -> Iterable[ClientConn]:
    return CLIENTS.values()


def items() -> Iterable[tuple[Any, ClientConn]]:
    return CLIENTS.items()


def has_clients() -> bool:
    return bool(CLIENTS)


def connected_device_ids(*extra_device_ids: str) -> list[str]:
    ids = {
        client.device_id
        for client in CLIENTS.values()
        if getattr(client, "device_id", "")
    }
    ids.update(device_id for device_id in extra_device_ids if device_id)
    return sorted(ids)


async def close_duplicate_device_clients(current_ws: Any, device_id: str) -> int:
    stale: list[Any] = []
    for other_ws, other_client in list(CLIENTS.items()):
        if other_ws is current_ws:
            continue
        if other_client.device_id != device_id:
            continue
        stale.append(other_ws)

    closed = 0
    for old_ws in stale:
        CLIENTS.pop(old_ws, None)
        try:
            await old_ws.close()
        except Exception:
            pass
        closed += 1
    return closed


async def broadcast_json(payload: dict) -> int:
    raw = json.dumps(payload)
    clients = list(CLIENTS.items())
    if not clients:
        return 0

    async def _send_one(ws: Any, client: ClientConn) -> bool:
        try:
            await ws.send(raw)
            client.last_seen = time.time()
            return True
        except Exception:
            return False

    results = await asyncio.gather(*[_send_one(ws, c) for ws, c in clients], return_exceptions=True)
    delivered = sum(1 for r in results if r is True)

    # clean up dead connections
    for (ws, _), ok in zip(clients, results):
        if ok is not True:
            CLIENTS.pop(ws, None)

    return delivered


async def send_unread_for_session(session: Any, unread_for: Callable[[Any, str], int]) -> None:
    dead: list[Any] = []
    for ws, client in list(CLIENTS.items()):
        unread = unread_for(session, client.device_id)
        payload = {"type": "session_unread", "session_id": session.session_id, "unread": unread}
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            dead.append(ws)
    for ws in dead:
        CLIENTS.pop(ws, None)


async def send_unread_snapshot(
    ws: Any,
    client: ClientConn,
    sessions: Iterable[Any],
    unread_for: Callable[[Any, str], int],
    batch_size: int = 500,
) -> None:
    batch_size = max(1, int(batch_size or 500))
    items: list[dict[str, Any]] = []
    for session in list(sessions):
        items.append({
            "session_id": session.session_id,
            "unread": unread_for(session, client.device_id),
        })
        if len(items) < batch_size:
            continue
        payload = {"type": "session_unread_snapshot", "items": items}
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            return
        items = []
    if not items:
        return
    payload = {"type": "session_unread_snapshot", "items": items}
    try:
        await ws.send(json.dumps(payload))
    except Exception:
        return


async def send_unread_for_client_session(
    ws: Any,
    client: ClientConn,
    session: Any,
    unread_for: Callable[[Any, str], int],
) -> None:
    payload = {"type": "session_unread", "session_id": session.session_id, "unread": unread_for(session, client.device_id)}
    try:
        await ws.send(json.dumps(payload))
    except Exception:
        pass
