"""
Integration tests: bridge_v2 search subsystem wiring.

Tests verify that _init_search / _shutdown_search / _dispatch_ws_message
behave correctly without starting a real WebSocket server or touching disk.

Run: pytest bridge/tests/test_search_integration.py -v
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Make 'bridge' package and 'bridge_v2' importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeWs:
    """Minimal fake WebSocket that records sent messages."""

    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)


def _make_disabled_cfg():
    from bridge.config.schema import BridgeConfig, SearchConfig
    cfg = BridgeConfig()
    cfg.search = SearchConfig(enabled=False)
    return cfg


def _make_enabled_cfg(tmp_path: Path):
    from bridge.config.schema import BridgeConfig, SearchConfig
    cfg = BridgeConfig()
    cfg.search = SearchConfig(enabled=True, index_path=tmp_path / "search.db")
    return cfg


# ---------------------------------------------------------------------------
# test_bridge_starts_without_search_when_config_disabled
# ---------------------------------------------------------------------------

def test_bridge_starts_without_search_when_config_disabled():
    """When config.search.enabled=False, _init_search sets _search_enabled=False."""
    import bridge_v2 as b2

    with patch("bridge_v2.get_config", return_value=_make_disabled_cfg()), \
         patch("bridge_v2._SEARCH_AVAILABLE", True):
        b2._search_enabled = False
        b2._search_pool = None

        asyncio.run(b2._init_search())

        assert b2._search_enabled is False
        assert b2._search_pool is None


# ---------------------------------------------------------------------------
# test_bridge_starts_with_search_enabled_via_env
# ---------------------------------------------------------------------------

def test_bridge_starts_with_search_enabled_via_env(tmp_path):
    """When search.enabled=True, _init_search starts worker and creates pool."""
    import bridge_v2 as b2

    fake_pool = MagicMock()
    cfg = _make_enabled_cfg(tmp_path)

    async def _run():
        with patch("bridge_v2.get_config", return_value=cfg), \
             patch("bridge_v2._SEARCH_AVAILABLE", True), \
             patch("bridge_v2.start_worker", new=AsyncMock(return_value=MagicMock())), \
             patch("bridge_v2.ConnectionPool", return_value=fake_pool):

            b2._search_enabled = False
            b2._search_pool = None

            await b2._init_search()

            assert b2._search_enabled is True
            assert b2._search_pool is fake_pool

    asyncio.run(_run())


def test_client_message_summary_does_not_log_content():
    """Inbound debug summaries must keep payload text out of logs."""
    import bridge_v2 as b2

    summary = b2._summarize_client_msg(
        {
            "type": "message",
            "session_id": "s_abcdefghijklmnopqrstuvwxyz",
            "request_id": "r_1234567890",
            "content": "secret prompt text",
            "files": [{"name": "a.txt"}],
        },
        123,
    )

    assert "type=message" in summary
    assert "bytes=123" in summary
    assert "content_len=18" in summary
    assert "files_count=1" in summary
    assert "secret prompt text" not in summary


# ---------------------------------------------------------------------------
# test_bridge_shutdown_cleans_up_search_resources
# ---------------------------------------------------------------------------

def test_bridge_shutdown_cleans_up_search_resources():
    """_shutdown_search closes pool and calls stop_worker."""
    import bridge_v2 as b2

    async def _run():
        fake_pool = AsyncMock()
        fake_pool.close_all = AsyncMock()

        b2._search_pool = fake_pool
        b2._search_enabled = True

        with patch("bridge_v2._SEARCH_AVAILABLE", True), \
             patch("bridge_v2.stop_worker", new=AsyncMock()) as mock_stop:

            await b2._shutdown_search()

            fake_pool.close_all.assert_awaited_once()
            mock_stop.assert_awaited_once()
            assert b2._search_pool is None
            assert b2._search_enabled is False

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# test_ws_dispatcher_routes_request_search_to_handler
# ---------------------------------------------------------------------------

def test_ws_dispatcher_routes_request_search_to_handler():
    """_dispatch_ws_message calls handle_search_message for search types."""
    import bridge_v2 as b2

    async def _run():
        ws = _FakeWs()
        msg = {"type": "request_search", "query": "hello"}

        fake_pool = MagicMock()
        b2._search_enabled = True
        b2._search_pool = fake_pool

        with patch("bridge_v2.handle_search_message", new=AsyncMock()) as mock_handler:
            result = await b2._dispatch_ws_message(ws, msg)

            assert result is True
            mock_handler.assert_awaited_once_with(ws, msg, pool=fake_pool)

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# test_ws_dispatcher_bypasses_when_search_disabled
# ---------------------------------------------------------------------------

def test_ws_dispatcher_bypasses_when_search_disabled():
    """When _search_enabled=False, _dispatch_ws_message returns False."""
    import bridge_v2 as b2

    async def _run():
        ws = _FakeWs()
        msg = {"type": "request_search", "query": "hello"}

        b2._search_enabled = False
        b2._search_pool = None

        with patch("bridge_v2.handle_search_message", new=AsyncMock()) as mock_handler:
            result = await b2._dispatch_ws_message(ws, msg)

            assert result is False
            mock_handler.assert_not_awaited()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# test_ws_dispatcher_does_not_crash_on_handler_exception
# ---------------------------------------------------------------------------

def test_ws_dispatcher_does_not_crash_on_handler_exception():
    """If handle_search_message raises, _dispatch_ws_message catches, sends error, returns True."""
    import bridge_v2 as b2

    async def _run():
        ws = _FakeWs()
        msg = {"type": "request_search_health"}

        fake_pool = MagicMock()
        b2._search_enabled = True
        b2._search_pool = fake_pool

        async def _boom(ws, msg, *, pool):
            raise RuntimeError("db gone")

        with patch("bridge_v2.handle_search_message", new=_boom):
            result = await b2._dispatch_ws_message(ws, msg)

        assert result is True
        assert len(ws.sent) == 1
        assert ws.sent[0]["type"] == "request_search_health_error"
        assert "db gone" in ws.sent[0]["message"]

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# test_bridge_survives_search_init_failure
# ---------------------------------------------------------------------------

def test_bridge_survives_search_init_failure(tmp_path):
    """If start_worker raises, _init_search sets _search_enabled=False and does not re-raise."""
    import bridge_v2 as b2

    cfg = _make_enabled_cfg(tmp_path)

    async def _bad_start(config):
        raise OSError("disk full")

    async def _run():
        with patch("bridge_v2.get_config", return_value=cfg), \
             patch("bridge_v2._SEARCH_AVAILABLE", True), \
             patch("bridge_v2.start_worker", new=_bad_start):

            b2._search_enabled = False
            b2._search_pool = None

            await b2._init_search()

            assert b2._search_enabled is False
            assert b2._search_pool is None

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# test_known_msg_types_includes_search_types
# ---------------------------------------------------------------------------

def test_known_msg_types_includes_search_types():
    """All three search message types are in _KNOWN_MSG_TYPES to pass validation."""
    import bridge_v2 as b2

    assert "request_search" in b2._KNOWN_MSG_TYPES
    assert "request_search_health" in b2._KNOWN_MSG_TYPES
    assert "request_session_list" in b2._KNOWN_MSG_TYPES


# ---------------------------------------------------------------------------
# test_dispatcher_ignores_non_search_types
# ---------------------------------------------------------------------------

def test_dispatcher_ignores_non_search_types():
    """_dispatch_ws_message returns False for non-search message types."""
    import bridge_v2 as b2

    async def _run():
        ws = _FakeWs()
        msg = {"type": "ping"}

        fake_pool = MagicMock()
        b2._search_enabled = True
        b2._search_pool = fake_pool

        with patch("bridge_v2.handle_search_message", new=AsyncMock()) as mock_handler:
            result = await b2._dispatch_ws_message(ws, msg)

        assert result is False
        mock_handler.assert_not_awaited()

    asyncio.run(_run())
