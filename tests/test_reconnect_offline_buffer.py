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
    def __init__(self, *, fail_at: int | None = None) -> None:
        self.fail_at = fail_at
        self.sent: list[dict] = []
        self._send_count = 0

    async def send(self, raw: str) -> None:
        self._send_count += 1
        if self.fail_at is not None and self._send_count >= self.fail_at:
            raise RuntimeError("socket closed")
        self.sent.append(json.loads(raw))


def _session(session_id: str):
    from bridge_v2 import Session

    return Session(
        session_id=session_id,
        name=session_id,
        created_at=time.time(),
        backend_name="claude",
    )


def test_replay_offline_buffers_preserves_session_order_and_clears_sent_events():
    import offline_replay

    async def run():
        s1 = _session("s1")
        s2 = _session("s2")
        s1.offline_buffer = [
            {"type": "text_chunk", "session_id": "s1", "content": "a"},
            {"type": "done", "session_id": "s1"},
        ]
        s2.offline_buffer = [
            {"type": "error", "session_id": "s2", "message": "boom"},
        ]
        ws = _Ws()

        replayed = await offline_replay.replay_offline_buffers(ws, [s1, s2])
        return replayed, ws.sent, s1.offline_buffer, s2.offline_buffer

    replayed, sent, s1_buf, s2_buf = asyncio.run(run())

    assert replayed == 3
    assert [evt["session_id"] for evt in sent] == ["s1", "s1", "s2"]
    assert [evt["type"] for evt in sent] == ["text_chunk", "done", "error"]
    assert s1_buf == []
    assert s2_buf == []


def test_replay_offline_buffers_restores_unsent_tail_on_send_failure():
    import offline_replay

    async def run():
        s1 = _session("s1")
        s2 = _session("s2")
        s1.offline_buffer = [
            {"type": "text_chunk", "session_id": "s1", "content": "sent"},
            {"type": "done", "session_id": "s1"},
        ]
        s2.offline_buffer = [
            {"type": "done", "session_id": "s2"},
        ]
        ws = _Ws(fail_at=2)

        replayed = await offline_replay.replay_offline_buffers(ws, [s1, s2])
        return replayed, ws.sent, s1.offline_buffer, s2.offline_buffer

    replayed, sent, s1_buf, s2_buf = asyncio.run(run())

    assert replayed == 1
    assert sent == [{"type": "text_chunk", "session_id": "s1", "content": "sent"}]
    assert s1_buf == [{"type": "done", "session_id": "s1"}]
    assert s2_buf == [{"type": "done", "session_id": "s2"}]


def test_dispatch_event_returns_false_when_all_registered_clients_are_dead(monkeypatch):
    import client_manager
    import bridge_v2 as bv2

    async def run():
        client_manager.CLIENTS.clear()
        session = _session("s1")
        dead_ws = _Ws(fail_at=1)
        client = bv2.ClientConn(
            client_id="c1",
            device_id="d1",
            device_name="Device",
            ws=dead_ws,
            connected_at=time.time(),
            last_seen=time.time(),
        )
        client_manager.register(dead_ws, client)

        delivered = await bv2._dispatch_event({"type": "text_chunk", "session_id": "s1", "content": "lost"}, session)
        return delivered, dict(client_manager.CLIENTS)

    delivered, clients = asyncio.run(run())

    assert delivered is False
    assert clients == {}


def test_send_event_buffers_when_only_stale_clients_exist(monkeypatch):
    import client_manager
    import bridge_v2 as bv2
    from backends.events import send_event, set_event_dispatcher

    async def run():
        client_manager.CLIENTS.clear()
        session = _session("s1")
        dead_ws = _Ws(fail_at=1)
        client = bv2.ClientConn(
            client_id="c1",
            device_id="d1",
            device_name="Device",
            ws=dead_ws,
            connected_at=time.time(),
            last_seen=time.time(),
        )
        client_manager.register(dead_ws, client)
        set_event_dispatcher(bv2._dispatch_event)
        try:
            await send_event(session, {"type": "text_chunk", "content": "buffer me"})
        finally:
            set_event_dispatcher(None)
        return session.offline_buffer, dict(client_manager.CLIENTS)

    offline_buffer, clients = asyncio.run(run())

    # send_event now also stamps a per-session `seq` and per-boot `gen` (used by
    # the client to detect dropped events); assert on the stable fields plus the
    # presence of the new ones.
    assert len(offline_buffer) == 1
    evt = offline_buffer[0]
    assert {k: evt[k] for k in ("type", "content", "session_id")} == {
        "type": "text_chunk", "content": "buffer me", "session_id": "s1"
    }
    assert evt["seq"] == 1 and isinstance(evt["gen"], str) and evt["gen"]
    assert clients == {}


def test_file_push_ack_deletes_when_no_original_targets(monkeypatch):
    import push_registry

    saves = []
    monkeypatch.setattr(push_registry, "save_inbox", lambda: saves.append(dict(push_registry._PUSH_FILE_REGISTRY)))
    monkeypatch.setattr(push_registry, "_firebase_storage_app", None)
    push_registry._PUSH_FILE_REGISTRY = {
        "file_1": {
            "blob_path": None,
            "filename": "a.txt",
            "target_device_ids": [],
            "acked_device_ids": [],
        }
    }

    asyncio.run(push_registry.handle_file_push_ack("file_1", "phone_1"))

    assert "file_1" not in push_registry._PUSH_FILE_REGISTRY
    assert len(saves) == 2


def test_file_push_ack_persists_partial_ack_until_all_targets(monkeypatch):
    import push_registry

    saves = []
    monkeypatch.setattr(push_registry, "save_inbox", lambda: saves.append(dict(push_registry._PUSH_FILE_REGISTRY)))
    monkeypatch.setattr(push_registry, "_firebase_storage_app", None)
    push_registry._PUSH_FILE_REGISTRY = {
        "file_1": {
            "blob_path": None,
            "filename": "a.txt",
            "target_device_ids": ["phone_1", "phone_2"],
            "acked_device_ids": [],
        }
    }

    asyncio.run(push_registry.handle_file_push_ack("file_1", "phone_1"))

    assert push_registry._PUSH_FILE_REGISTRY["file_1"]["acked_device_ids"] == ["phone_1"]
    assert saves
