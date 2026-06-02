from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

import bridge_v2
import client_manager


class FakeRequest:
    headers: dict[str, str] = {}


class FakeWebSocket:
    request = FakeRequest()
    remote_address = ("127.0.0.1", 50123)

    def __init__(self, messages: list[dict | str]):
        self._messages = [json.dumps(m) if isinstance(m, dict) else m for m in messages]
        self.sent: list[dict] = []

    async def recv(self) -> str:
        if not self._messages:
            raise RuntimeError("no handshake message")
        return self._messages.pop(0)

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def send(self, payload: str) -> None:
        self.sent.append(json.loads(payload))


@pytest.fixture(autouse=True)
def clean_bridge_state(monkeypatch, tmp_path):
    bridge_v2._SESSIONS.clear()
    bridge_v2._BACKENDS.clear()
    bridge_v2._SHELL_SESSIONS.clear()
    bridge_v2._PAIRING.clear()
    monkeypatch.setattr(bridge_v2, "_INSTANCE_ID", "b_test")
    monkeypatch.setattr(bridge_v2, "_INSTANCE_NAME", "Test Bridge")
    monkeypatch.setattr(bridge_v2, "_ROOT_DIR", "/work")
    monkeypatch.setattr(bridge_v2, "_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(bridge_v2, "_LAN_IP", "192.168.1.10")
    monkeypatch.setattr(bridge_v2, "PAIRING_FILE", str(tmp_path / "pairing.json"))
    client_manager.CLIENTS.clear()

    monkeypatch.delenv("BRIDGE_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("BRIDGE_AUTO_TUNNEL", raising=False)
    monkeypatch.setattr(bridge_v2, "_AUTO_TUNNEL_TASK", None, raising=False)
    monkeypatch.setattr(bridge_v2, "_TUNNEL_URL_FILE", "", raising=False)
    monkeypatch.setattr(bridge_v2, "_is_cloudflared_running", lambda: True)
    monkeypatch.setattr(bridge_v2, "get_current_tunnel_url", lambda: "https://tunnel.example")
    monkeypatch.setattr(bridge_v2, "build_sessions_list", lambda: {"type": "sessions_list", "sessions": []})
    monkeypatch.setattr(bridge_v2, "replay_offline_buffers", lambda ws, sessions: _async_none())
    monkeypatch.setattr(bridge_v2, "pending_file_push_items", lambda *args, **kwargs: [])
    monkeypatch.setattr(bridge_v2, "_send_unread_snapshot_deferred", lambda ws, client: _async_none())
    monkeypatch.setattr(bridge_v2, "_spawn_task", lambda name, coro: coro.close())
    monkeypatch.setattr(bridge_v2, "_webrtc_cleanup_for_ws", lambda ws: None)
    monkeypatch.setattr(bridge_v2, "mark_tunnel_url_delivered", lambda: None)
    monkeypatch.setattr(bridge_v2, "send_pending_interactions", lambda ws: _async_none())
    monkeypatch.setattr(bridge_v2, "handle_interaction_message", lambda **kwargs: _async_false())
    monkeypatch.setattr(bridge_v2, "_dispatch_ws_message", lambda ws, msg: _async_false())
    monkeypatch.setattr(bridge_v2, "handle_system_msg", lambda mtype, msg, ws, ctx: _async_false())
    monkeypatch.setattr(bridge_v2, "handle_runtime_msg", lambda mtype, msg, ws, ctx: _async_false())
    monkeypatch.setattr(bridge_v2, "handle_file_msg", lambda mtype, msg, ws, ctx: _async_false())
    monkeypatch.setattr(bridge_v2._PERF, "record", lambda *args, **kwargs: None)
    yield
    client_manager.CLIENTS.clear()


async def _async_none():
    return None


async def _async_false():
    return False


@pytest.mark.asyncio
async def test_handler_rejects_invalid_first_frame():
    ws = FakeWebSocket(["not-json"])

    await bridge_v2.handler(ws)

    assert ws.sent == [{"type": "error", "message": "Handshake failed: invalid JSON"}]
    assert client_manager.CLIENTS == {}


@pytest.mark.asyncio
async def test_handler_requires_hello_as_first_protocol_message():
    ws = FakeWebSocket([{"type": "ping"}])

    await bridge_v2.handler(ws)

    assert ws.sent == [{"type": "error", "message": "Protocol error: first message must be hello"}]
    assert client_manager.CLIENTS == {}


@pytest.mark.asyncio
async def test_handler_sends_hello_ack_sessions_list_and_disconnects_cleanly():
    ws = FakeWebSocket([
        {"type": "hello", "device_id": "dev1", "device_name": "Phone", "auth_token": "tok"},
    ])

    await bridge_v2.handler(ws)

    assert ws.sent[0] == {
        "type": "hello_ack",
        "instance_id": "b_test",
        "gen": ws.sent[0]["gen"],
        "client_id": ws.sent[0]["client_id"],
        "device_id": "dev1",
        "device_name": "Phone",
        "is_locked": False,
        "locked_to_me": False,
        "instance_name": "Test Bridge",
        "root_dir": "/work",
        "data_dir": ws.sent[0]["data_dir"],
        "lan_ip": "192.168.1.10",
        "tunnel_url": "https://tunnel.example",
    }
    assert ws.sent[1] == {"type": "sessions_list", "sessions": []}
    assert client_manager.CLIENTS == {}


@pytest.mark.asyncio
async def test_handler_claims_rejects_competing_claim_and_unclaims(tmp_path):
    ws = FakeWebSocket([
        {"type": "hello", "device_id": "dev1", "auth_token": "tok"},
        {"type": "claim_bridge", "device_id": "dev1", "auth_token": "tok"},
        {"type": "claim_bridge", "device_id": "dev2", "auth_token": "other"},
        {"type": "unclaim_bridge", "auth_token": "wrong"},
        {"type": "unclaim_bridge", "auth_token": "tok"},
    ])

    await bridge_v2.handler(ws)

    assert {"type": "claim_ack", "is_locked": True, "locked_to_me": True} in ws.sent
    assert {"type": "error", "message": "Bridge already claimed by another device"} in ws.sent
    assert {"type": "error", "message": "Unauthorized: token mismatch"} in ws.sent
    assert {"type": "unclaim_ack", "is_locked": False} in ws.sent
    assert bridge_v2._PAIRING == {}
    assert not Path(bridge_v2.PAIRING_FILE).exists()


@pytest.mark.asyncio
async def test_handler_rejects_hello_when_pairing_token_does_not_match():
    bridge_v2._PAIRING.update({"paired_token": "expected"})
    ws = FakeWebSocket([
        {"type": "hello", "device_id": "dev1", "auth_token": "wrong"},
    ])

    await bridge_v2.handler(ws)

    assert ws.sent == [{"type": "error", "message": "Unauthorized: invalid auth token"}]
    assert client_manager.CLIENTS == {}


@pytest.mark.asyncio
async def test_handler_returns_device_scoped_inbox(monkeypatch):
    seen: list[tuple[str, bool]] = []

    def fake_pending(device_id: str, include_pushed_at: bool = False):
        seen.append((device_id, include_pushed_at))
        return [{"id": "file1", "name": "notes.md"}]

    monkeypatch.setattr(bridge_v2, "pending_file_push_items", fake_pending)
    ws = FakeWebSocket([
        {"type": "hello", "device_id": "dev1"},
        {"type": "get_inbox"},
    ])

    await bridge_v2.handler(ws)

    assert seen == [("dev1", False), ("dev1", True)]
    assert {"type": "inbox_list", "items": [{"id": "file1", "name": "notes.md"}]} in ws.sent


@pytest.mark.asyncio
async def test_handler_reports_validation_errors_after_handshake():
    ws = FakeWebSocket([
        {"type": "hello", "device_id": "dev1"},
        {"type": "message"},
    ])

    await bridge_v2.handler(ws)

    assert {"type": "error", "message": "Protocol error: 'message' missing required field 'session_id'"} in ws.sent


@pytest.mark.asyncio
async def test_handler_delegates_unmatched_messages_to_low_coupling_router(monkeypatch):
    calls: list[tuple[str, dict]] = []

    async def fake_low_coupling(mtype, msg, ws, client, ctx):
        calls.append((mtype, msg))
        await ws.send(json.dumps({"type": "handled", "mtype": mtype, "client": client.device_id}))
        return True

    monkeypatch.setattr(bridge_v2, "handle_low_coupling_message", fake_low_coupling)
    ws = FakeWebSocket([
        {"type": "hello", "device_id": "dev1"},
        {"type": "ping"},
    ])

    await bridge_v2.handler(ws)

    assert calls == [("ping", {"type": "ping"})]
    assert {"type": "handled", "mtype": "ping", "client": "dev1"} in ws.sent
