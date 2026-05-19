from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path


_BRIDGE_ROOT = Path(__file__).parent.parent
_REPO_ROOT = _BRIDGE_ROOT.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_BRIDGE_ROOT))


class _Ws:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list[dict] = []
        self.closed = False

    async def send(self, raw: str) -> None:
        if self.fail:
            raise RuntimeError("closed")
        self.sent.append(json.loads(raw))

    async def close(self) -> None:
        self.closed = True


class _Session:
    def __init__(self, session_id: str, seq: int = 0) -> None:
        self.session_id = session_id
        self.message_seq = seq


def _client(ws, device_id: str = "d1"):
    import client_manager

    return client_manager.ClientConn(
        client_id=f"c_{device_id}",
        device_id=device_id,
        device_name="Device",
        ws=ws,
        connected_at=time.time(),
        last_seen=0,
    )


def test_broadcast_json_counts_delivered_and_removes_dead_clients():
    import client_manager

    async def run():
        client_manager.CLIENTS.clear()
        good = _Ws()
        dead = _Ws(fail=True)
        client_manager.register(good, _client(good, "good"))
        client_manager.register(dead, _client(dead, "dead"))

        delivered = await client_manager.broadcast_json({"type": "pong"})
        return delivered, good.sent, dict(client_manager.CLIENTS)

    delivered, sent, clients = asyncio.run(run())

    assert delivered == 1
    assert sent == [{"type": "pong"}]
    assert list(clients.keys())[0].sent == [{"type": "pong"}]
    assert len(clients) == 1


def test_send_unread_for_session_uses_per_client_device_id_and_removes_dead_clients():
    import client_manager

    async def run():
        client_manager.CLIENTS.clear()
        good = _Ws()
        dead = _Ws(fail=True)
        client_manager.register(good, _client(good, "good"))
        client_manager.register(dead, _client(dead, "dead"))
        session = _Session("s1", seq=7)

        def unread_for(session_arg, device_id: str) -> int:
            return 3 if device_id == "good" else 9

        await client_manager.send_unread_for_session(session, unread_for)
        return good.sent, dict(client_manager.CLIENTS)

    sent, clients = asyncio.run(run())

    assert sent == [{"type": "session_unread", "session_id": "s1", "unread": 3}]
    assert len(clients) == 1


def test_send_unread_snapshot_sends_zero_counts_in_batches_for_all_sessions():
    import client_manager

    async def run():
        ws = _Ws()
        client = _client(ws, "device")
        sessions = [_Session("s1"), _Session("s2"), _Session("s3")]

        await client_manager.send_unread_snapshot(
            ws,
            client,
            sessions,
            lambda session, device_id: 0 if session.session_id != "s2" else 2,
            batch_size=2,
        )
        return ws.sent

    sent = asyncio.run(run())

    assert sent == [
        {
            "type": "session_unread_snapshot",
            "items": [
                {"session_id": "s1", "unread": 0},
                {"session_id": "s2", "unread": 2},
            ],
        },
        {
            "type": "session_unread_snapshot",
            "items": [
                {"session_id": "s3", "unread": 0},
            ],
        },
    ]


def test_connected_device_ids_includes_registered_and_extra_ids():
    import client_manager

    client_manager.CLIENTS.clear()
    ws = _Ws()
    client_manager.register(ws, _client(ws, "phone"))

    assert client_manager.connected_device_ids("sender") == ["phone", "sender"]


def test_close_duplicate_device_clients_removes_and_closes_stale_socket():
    import client_manager

    async def run():
        client_manager.CLIENTS.clear()
        current = _Ws()
        stale_same_device = _Ws()
        other_device = _Ws()
        client_manager.register(current, _client(current, "phone"))
        client_manager.register(stale_same_device, _client(stale_same_device, "phone"))
        client_manager.register(other_device, _client(other_device, "tablet"))

        closed = await client_manager.close_duplicate_device_clients(current, "phone")
        return closed, stale_same_device.closed, dict(client_manager.CLIENTS)

    closed, stale_closed, clients = asyncio.run(run())

    assert closed == 1
    assert stale_closed is True
    assert len(clients) == 2
