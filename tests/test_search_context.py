"""
Unit tests for bridge.search.query.context — get_search_context() and
the WebSocket handler dispatch for 'request_search_context'.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from bridge.search.db import open_connection, init_schema
from bridge.search.query import ConnectionPool
from bridge.search.query.context import get_search_context, SearchContextResponse
from bridge.handlers.search_ws import handle_search_message


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "test_ctx.db"
    conn = open_connection(path)
    init_schema(conn)
    conn.commit()
    conn.close()
    return path


@pytest.fixture()
def pool(db_path: Path) -> ConnectionPool:
    return ConnectionPool(db_path, max_size=2)


def _insert_session(conn, session_id: str = "sess1") -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO sessions
          (session_id, source, source_path, project_dir, display_name,
           backend, first_ts, last_ts, msg_count, is_pinned, is_hidden, cwd)
        VALUES (?, 'claude', ?, '/proj', 'Test Session', 'claude',
                '2026-01-01T10:00:00Z', '2026-01-01T12:00:00Z', 0, 0, 0, '/home/user')
        """,
        (session_id, f"/src/{session_id}"),
    )
    conn.commit()


def _insert_message(
    conn,
    session_id: str = "sess1",
    msg_uuid: str = "msg1",
    content: str = "hello world",
    role: str = "user",
    ts: str = "2026-01-01T12:00:00Z",
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO messages
          (session_id, msg_uuid, role, ts, is_subagent, content)
        VALUES (?, ?, ?, ?, 0, ?)
        """,
        (session_id, msg_uuid, role, ts, content),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_get_search_context_returns_target_plus_around(db_path: Path) -> None:
    """
    With around=2, requesting context for msg_uuid='m3' should return
    m1..m5 (rowids 1..5), with m3 marked is_target=True.
    """
    conn = open_connection(db_path)
    _insert_session(conn, "sess1")
    msgs = [
        ("m1", "message one",   "user",      "2026-01-01T10:00:00Z"),
        ("m2", "message two",   "assistant",  "2026-01-01T10:01:00Z"),
        ("m3", "message three", "user",       "2026-01-01T10:02:00Z"),
        ("m4", "message four",  "assistant",  "2026-01-01T10:03:00Z"),
        ("m5", "message five",  "user",       "2026-01-01T10:04:00Z"),
        ("m6", "message six",   "assistant",  "2026-01-01T10:05:00Z"),
        ("m7", "message seven", "user",       "2026-01-01T10:06:00Z"),
    ]
    for uuid, content, role, ts in msgs:
        _insert_message(conn, "sess1", uuid, content, role, ts)
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(get_search_context(pool, "sess1", "m3", around=2))

    assert isinstance(result, SearchContextResponse)
    assert result.session_id == "sess1"
    assert result.target_msg_uuid == "m3"
    assert result.session_display_name == "Test Session"
    assert result.cwd == "/home/user"
    assert result.backend == "claude"

    uuids = [m.msg_uuid for m in result.messages]
    # Should include m1..m5 (rowid 1-5, target rowid=3, window 3-2=1..3+2=5)
    assert "m3" in uuids
    assert "m1" in uuids
    assert "m5" in uuids
    # m6, m7 are outside the window
    assert "m6" not in uuids
    assert "m7" not in uuids

    # Exactly one message is the target
    targets = [m for m in result.messages if m.is_target]
    assert len(targets) == 1
    assert targets[0].msg_uuid == "m3"

    # Non-target messages have is_target=False
    non_targets = [m for m in result.messages if not m.is_target]
    assert all(not m.is_target for m in non_targets)

    # Messages ordered by rowid ascending (ts ascending)
    tss = [m.ts for m in result.messages]
    assert tss == sorted(tss)

    assert result.elapsed_ms >= 0.0


def test_get_search_context_does_not_cross_sessions(db_path: Path) -> None:
    """
    Messages from a different session must never appear in the context window,
    even if their rowids fall within the target's ± around range.
    """
    conn = open_connection(db_path)
    _insert_session(conn, "sessA")
    _insert_session(conn, "sessB")

    # sessB messages are inserted first so they get lower rowids
    for i in range(1, 6):
        _insert_message(
            conn, "sessB", f"b{i}", f"sessB msg {i}",
            ts=f"2026-01-01T09:0{i}:00Z",
        )

    # sessA target inserted after — its rowid lands in the middle of sessB's range
    _insert_message(conn, "sessA", "a1", "sessA target", ts="2026-01-01T10:00:00Z")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(get_search_context(pool, "sessA", "a1", around=10))

    session_ids = {m.msg_uuid for m in result.messages}
    # Only sessA messages should appear — no sessB msg_uuids
    assert "a1" in session_ids
    for m in result.messages:
        assert not m.msg_uuid.startswith("b"), (
            f"Cross-session message {m.msg_uuid!r} leaked into context"
        )


def test_get_search_context_handles_missing_msg_uuid(db_path: Path) -> None:
    """
    If msg_uuid does not exist in messages, return an empty messages list
    and the session metadata if session_id is valid, without raising.
    """
    conn = open_connection(db_path)
    _insert_session(conn, "sess1")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(get_search_context(pool, "sess1", "nonexistent-uuid", around=5))

    assert isinstance(result, SearchContextResponse)
    assert result.messages == []
    assert result.target_msg_uuid == "nonexistent-uuid"
    assert result.elapsed_ms >= 0.0


def test_handle_request_search_context_via_ws(db_path: Path) -> None:
    """
    The WS handler for 'request_search_context' must call send() with
    type='search_context' and a messages array.
    """
    conn = open_connection(db_path)
    _insert_session(conn, "sA")
    _insert_message(conn, "sA", "mX", "context test content", ts="2026-01-01T12:00:00Z")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)

    sent: list[dict] = []

    class FakeWs:
        async def send_json(self, payload: dict) -> None:
            sent.append(payload)

    ws = FakeWs()
    msg = {
        "type": "request_search_context",
        "session_id": "sA",
        "msg_uuid": "mX",
        "around": 3,
    }

    asyncio.run(handle_search_message(ws, msg, pool=pool))

    assert len(sent) == 1, f"Expected 1 response, got {len(sent)}"
    resp = sent[0]
    assert resp["type"] == "search_context", f"Got type={resp['type']!r}"
    assert resp["session_id"] == "sA"
    assert resp["target_msg_uuid"] == "mX"
    assert isinstance(resp["messages"], list)
    assert len(resp["messages"]) >= 1
    # Target message present and marked
    target_msgs = [m for m in resp["messages"] if m["is_target"]]
    assert len(target_msgs) == 1
    assert target_msgs[0]["msg_uuid"] == "mX"
    assert "elapsed_ms" in resp
