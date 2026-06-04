"""WebSocket client registry and broadcast helpers."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable

try:
    from websockets.exceptions import ConnectionClosed as _WsConnectionClosed
except ImportError:
    _WsConnectionClosed = OSError  # type: ignore[assignment,misc]

log = logging.getLogger(__name__)


@dataclass
class ClientConn:
    client_id: str
    device_id: str
    device_name: str
    ws: Any
    connected_at: float
    last_seen: float
    generation: int = 0


CLIENTS: dict[Any, ClientConn] = {}
_LATEST_BY_DEVICE: dict[str, Any] = {}
_DEVICE_GENERATIONS: dict[str, int] = {}


def mark_latest(ws: Any, device_id: str) -> int:
    if not device_id:
        return 0
    for old_device_id, old_ws in list(_LATEST_BY_DEVICE.items()):
        if old_ws is ws and old_device_id != device_id:
            _LATEST_BY_DEVICE.pop(old_device_id, None)
    generation = _DEVICE_GENERATIONS.get(device_id, 0) + 1
    _DEVICE_GENERATIONS[device_id] = generation
    _LATEST_BY_DEVICE[device_id] = ws
    client = CLIENTS.get(ws)
    if client is not None:
        client.generation = generation
    return generation


def register(ws: Any, client: ClientConn) -> None:
    CLIENTS[ws] = client
    client.generation = mark_latest(ws, client.device_id)


def remove(ws: Any) -> ClientConn | None:
    # Also drop any broadcast fail-counter so the dict can't accumulate orphan
    # keys for clients that disconnect normally before hitting the eviction limit.
    _BROADCAST_FAIL_COUNTS.pop(ws, None)
    client = CLIENTS.pop(ws, None)
    if client is not None and _LATEST_BY_DEVICE.get(client.device_id) is ws:
        _LATEST_BY_DEVICE.pop(client.device_id, None)
    return client


def values() -> Iterable[ClientConn]:
    return CLIENTS.values()


def items() -> Iterable[tuple[Any, ClientConn]]:
    return CLIENTS.items()


def has_clients() -> bool:
    return bool(CLIENTS)


def is_current(ws: Any, client: ClientConn | None = None) -> bool:
    current = CLIENTS.get(ws)
    if current is None:
        return False
    if client is not None and current is not client:
        return False
    return _LATEST_BY_DEVICE.get(current.device_id) is ws


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
        old_client = remove(old_ws)
        if old_client is not None:
            try:
                from task_manager import cancel_owner
                cancel_owner(old_client.client_id)
            except Exception:
                pass
        try:
            await old_ws.close()
        except Exception:
            pass
        closed += 1
    return closed


# Per-client consecutive send-failure counter (transient errors).
# Reset to 0 on success; client evicted when this reaches _BROADCAST_FAIL_LIMIT.
_BROADCAST_FAIL_COUNTS: dict[Any, int] = {}
_BROADCAST_FAIL_LIMIT = 5


def _is_closed_send_error(exc: Exception) -> bool:
    return "closed" in str(exc).lower()


async def broadcast_json(payload: dict) -> int:
    raw = json.dumps(payload)
    clients = list(CLIENTS.items())
    if not clients:
        return 0

    async def _send_one(ws: Any, client: ClientConn) -> "str":
        """Return 'ok', 'closed', or 'error'."""
        try:
            await ws.send(raw)
            client.last_seen = time.time()
            return "ok"
        except _WsConnectionClosed:
            return "closed"
        except Exception as exc:
            if _is_closed_send_error(exc):
                return "closed"
            log.debug("broadcast_json transient error for %s: %s", client.client_id, exc)
            return "error"

    results = await asyncio.gather(*[_send_one(ws, c) for ws, c in clients], return_exceptions=True)
    delivered = 0
    to_remove: list[Any] = []

    for (ws, client), result in zip(clients, results):
        if result == "ok":
            delivered += 1
            _BROADCAST_FAIL_COUNTS.pop(ws, None)
        elif result == "closed" or isinstance(result, BaseException):
            # ConnectionClosed or gather exception — remove immediately
            to_remove.append(ws)
            _BROADCAST_FAIL_COUNTS.pop(ws, None)
        else:
            # Transient error — increment counter, evict only after N consecutive failures
            count = _BROADCAST_FAIL_COUNTS.get(ws, 0) + 1
            _BROADCAST_FAIL_COUNTS[ws] = count
            if count >= _BROADCAST_FAIL_LIMIT:
                log.warning(
                    "broadcast_json: evicting client %s after %d consecutive errors",
                    client.client_id, count,
                )
                to_remove.append(ws)
                _BROADCAST_FAIL_COUNTS.pop(ws, None)

    for ws in to_remove:
        CLIENTS.pop(ws, None)

    return delivered


async def send_unread_for_session(session: Any, unread_for: Callable[[Any, str], int]) -> None:
    dead: list[Any] = []
    for ws, client in list(CLIENTS.items()):
        unread = unread_for(session, client.device_id)
        payload = {"type": "session_unread", "session_id": session.session_id, "unread": unread}
        try:
            await ws.send(json.dumps(payload))
        except _WsConnectionClosed:
            dead.append(ws)
        except Exception as exc:
            if _is_closed_send_error(exc):
                dead.append(ws)
                continue
            log.debug("send_unread_for_session transient error for %s: %s", client.client_id, exc)
    for ws in dead:
        remove(ws)


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
