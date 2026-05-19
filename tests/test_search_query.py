"""
Unit tests for bridge.search.query — search, health, list_sessions, connection pool,
and the WebSocket handler.

All tests use tmp_path (pytest fixture) for an isolated SQLite DB.
"""
from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure bridge package is importable from repo root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from bridge.search.db import open_connection, init_schema
from bridge.search.query import (
    ConnectionPool,
    search,
    health,
    list_sessions,
    SearchFilters,
)
from bridge.search.query.search import _build_fts_match, _collect_warnings


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "test_search.db"
    conn = open_connection(path)
    init_schema(conn)
    conn.commit()
    conn.close()
    return path


@pytest.fixture()
def pool(db_path: Path) -> ConnectionPool:
    return ConnectionPool(db_path, max_size=4)


def _insert_session(
    conn,
    session_id: str = "sess1",
    project_dir: str = "/proj/a",
    source: str = "claude",
    display_name: str | None = None,
    backend: str | None = None,
    last_ts: str = "2026-01-01T12:00:00Z",
    first_ts: str = "2026-01-01T10:00:00Z",
    msg_count: int = 0,
    is_pinned: int = 0,
    is_hidden: int = 0,
    cwd: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO sessions
          (session_id, source, source_path, project_dir, display_name, backend,
           first_ts, last_ts, msg_count, is_pinned, is_hidden, cwd)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (session_id, source, f"/src/{session_id}", project_dir,
         display_name, backend, first_ts, last_ts, msg_count,
         is_pinned, is_hidden, cwd),
    )
    conn.commit()


def _insert_message(
    conn,
    session_id: str = "sess1",
    msg_uuid: str = "msg1",
    content: str = "hello world",
    role: str = "user",
    ts: str = "2026-01-01T12:00:00Z",
    is_subagent: int = 0,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO messages
          (session_id, msg_uuid, role, ts, is_subagent, content)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (session_id, msg_uuid, role, ts, is_subagent, content),
    )
    conn.commit()


def _setup_basic(conn) -> None:
    """Insert one session and one message for basic tests."""
    _insert_session(conn)
    _insert_message(conn, content="hello world trigram search test")


# ---------------------------------------------------------------------------
# search() tests
# ---------------------------------------------------------------------------

def test_search_returns_empty_on_blank_query(pool: ConnectionPool) -> None:
    result = asyncio.run(search(pool, "   "))
    assert result.hits == []
    assert result.total == 0
    assert any("empty" in w.lower() for w in result.warnings)


def test_search_rejects_oversize_query(pool: ConnectionPool) -> None:
    long_q = "x" * 201
    result = asyncio.run(search(pool, long_q))
    assert result.hits == []
    assert result.total == 0
    assert any("200" in w for w in result.warnings)


def test_search_warns_on_short_ascii_token(pool: ConnectionPool) -> None:
    result = asyncio.run(search(pool, "os fs"))
    assert any("3 chars" in w or "< 3" in w for w in result.warnings)


def test_search_basic_match_returns_hit_with_snippet(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="s1", project_dir="/proj/a")
    _insert_message(conn, session_id="s1", msg_uuid="m1",
                    content="unique_keyword_alpha_bravo_charlie")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(search(pool, "unique_keyword_alpha_bravo_charlie"))
    assert result.total == 1
    hit = result.hits[0]
    assert hit.session_id == "s1"
    assert hit.msg_uuid == "m1"
    assert "unique_keyword" in hit.snippet or "<<" in hit.snippet or hit.snippet != ""
    assert isinstance(hit.rank, float)


def test_search_with_project_filter_excludes_other_projects(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="s_a", project_dir="/proj/a")
    _insert_session(conn, session_id="s_b", project_dir="/proj/b")
    content = "shared_content_zeta_omega"
    _insert_message(conn, session_id="s_a", msg_uuid="m_a", content=content)
    _insert_message(conn, session_id="s_b", msg_uuid="m_b", content=content)
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    filters = SearchFilters(project_dir="/proj/a")
    result = asyncio.run(search(pool, "shared_content_zeta_omega", filters=filters))
    assert all(h.project_dir == "/proj/a" for h in result.hits)
    assert not any(h.session_id == "s_b" for h in result.hits)


def test_search_with_role_filter(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="sx")
    _insert_message(conn, session_id="sx", msg_uuid="mu", content="rolecontent_test_xyz",
                    role="user")
    _insert_message(conn, session_id="sx", msg_uuid="ma", content="rolecontent_test_xyz",
                    role="assistant")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    filters = SearchFilters(role="user")
    result = asyncio.run(search(pool, "rolecontent_test_xyz", filters=filters))
    assert all(h.role == "user" for h in result.hits)


def test_search_with_exclude_subagents_filter(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="sa")
    _insert_message(conn, session_id="sa", msg_uuid="m_main",
                    content="subagent_test_content_beta", is_subagent=0)
    _insert_message(conn, session_id="sa", msg_uuid="m_sub",
                    content="subagent_test_content_beta", is_subagent=1)
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    filters = SearchFilters(exclude_subagents=True)
    result = asyncio.run(search(pool, "subagent_test_content_beta", filters=filters))
    assert all(h.msg_uuid != "m_sub" for h in result.hits)


def test_search_handles_cjk_query_via_trigram(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="scjk")
    _insert_message(conn, session_id="scjk", msg_uuid="mcjk",
                    content="這是一個測試用的中文訊息，包含測試關鍵字")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    # Trigram can match 3+ char CJK sequences
    result = asyncio.run(search(pool, "測試關鍵字"))
    assert result.total >= 1
    assert result.hits[0].session_id == "scjk"


def test_search_phrase_query_works(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="sph")
    _insert_message(conn, session_id="sph", msg_uuid="mph",
                    content="the quick brown fox jumps over the lazy dog")
    _insert_message(conn, session_id="sph", msg_uuid="mph2",
                    content="brown fox is not quick here")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    # Phrase query: "quick brown fox" should only match the first message
    result = asyncio.run(search(pool, '"quick brown fox"'))
    assert result.total >= 1
    assert all(h.msg_uuid == "mph" for h in result.hits)


def test_search_pagination_offset(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="spag")
    for i in range(5):
        _insert_message(conn, session_id="spag", msg_uuid=f"pm{i}",
                        content=f"pagination_test_content_{i}_foxtrot")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    # Use max_per_session=5 so all 5 messages from the single session are eligible;
    # this test is about pagination mechanics, not session diversity.
    filters = SearchFilters(max_per_session=5)
    page1 = asyncio.run(search(pool, "pagination_test_content", filters=filters,
                               limit=3, offset=0))
    page2 = asyncio.run(search(pool, "pagination_test_content", filters=filters,
                               limit=3, offset=3))

    assert len(page1.hits) == 3
    # page2 should have the remaining 2
    assert len(page2.hits) == 2
    # No overlap
    uuids1 = {h.msg_uuid for h in page1.hits}
    uuids2 = {h.msg_uuid for h in page2.hits}
    assert uuids1.isdisjoint(uuids2)


# ---------------------------------------------------------------------------
# health() tests
# ---------------------------------------------------------------------------

def test_health_reports_zero_when_db_empty(db_path: Path) -> None:
    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(health(pool))
    assert result.indexed_sessions == 0
    assert result.indexed_messages == 0
    assert result.errors_last_24h == 0
    assert result.ingest_lag_seconds is None


def test_health_reports_correct_counts(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="h1")
    _insert_session(conn, session_id="h2")
    _insert_message(conn, session_id="h1", msg_uuid="hm1", content="health check content one")
    _insert_message(conn, session_id="h1", msg_uuid="hm2", content="health check content two")
    _insert_message(conn, session_id="h2", msg_uuid="hm3", content="health check content three")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(health(pool))
    assert result.indexed_sessions == 2
    assert result.indexed_messages == 3
    assert isinstance(result.db_size_mb, float)
    assert result.db_size_mb >= 0.0
    # ingest_progress must be a dict (fallback when worker not available)
    assert isinstance(result.ingest_progress, dict)


# ---------------------------------------------------------------------------
# list_sessions() tests
# ---------------------------------------------------------------------------

def test_list_sessions_cursor_pagination(db_path: Path) -> None:
    conn = open_connection(db_path)
    # Insert 5 sessions with different timestamps
    for i in range(5):
        ts = f"2026-01-{i+1:02d}T10:00:00Z"
        _insert_session(conn, session_id=f"lsp{i}", last_ts=ts, first_ts=ts)
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    page1 = asyncio.run(list_sessions(pool, limit=3))
    assert len(page1.items) == 3
    assert page1.next_cursor is not None

    page2 = asyncio.run(list_sessions(pool, cursor=page1.next_cursor, limit=3))
    assert len(page2.items) == 2
    assert page2.next_cursor is None

    ids1 = {i.session_id for i in page1.items}
    ids2 = {i.session_id for i in page2.items}
    assert ids1.isdisjoint(ids2)


def test_list_sessions_pinned_first(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="unpinned", last_ts="2026-01-05T10:00:00Z",
                    first_ts="2026-01-05T10:00:00Z", is_pinned=0)
    _insert_session(conn, session_id="pinned", last_ts="2026-01-01T10:00:00Z",
                    first_ts="2026-01-01T10:00:00Z", is_pinned=1)
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    page = asyncio.run(list_sessions(pool))
    assert page.items[0].session_id == "pinned"
    assert page.items[0].is_pinned is True


def test_list_sessions_project_filter(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="lspa", project_dir="/proj/a")
    _insert_session(conn, session_id="lspb", project_dir="/proj/b")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    page = asyncio.run(list_sessions(pool, project_dir="/proj/a"))
    assert all(i.project_dir == "/proj/a" for i in page.items)
    assert not any(i.session_id == "lspb" for i in page.items)


def test_list_sessions_hidden_excluded(db_path: Path) -> None:
    conn = open_connection(db_path)
    _insert_session(conn, session_id="visible", is_hidden=0)
    _insert_session(conn, session_id="hidden", is_hidden=1)
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    page = asyncio.run(list_sessions(pool, include_hidden=False))
    assert not any(i.session_id == "hidden" for i in page.items)

    page_all = asyncio.run(list_sessions(pool, include_hidden=True))
    assert any(i.session_id == "hidden" for i in page_all.items)


# ---------------------------------------------------------------------------
# WebSocket handler tests
# ---------------------------------------------------------------------------

class _FakeWs:
    """Minimal fake WebSocket that records sent payloads."""

    def __init__(self):
        self.sent: list[dict] = []

    async def send_json(self, payload: dict) -> None:
        self.sent.append(payload)


def test_handle_search_message_dispatches_correctly(db_path: Path) -> None:
    from bridge.handlers.search_ws import handle_search_message

    conn = open_connection(db_path)
    _insert_session(conn, session_id="wsh1")
    _insert_message(conn, session_id="wsh1", msg_uuid="wsm1",
                    content="websocket_handler_dispatch_test_content")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    ws = _FakeWs()

    # request_search
    asyncio.run(handle_search_message(
        ws,
        {"type": "request_search", "query": "websocket_handler_dispatch_test_content"},
        pool=pool,
    ))
    assert ws.sent[-1]["type"] == "search_result"
    assert "hits" in ws.sent[-1]

    # request_search_health
    asyncio.run(handle_search_message(
        ws,
        {"type": "request_search_health"},
        pool=pool,
    ))
    assert ws.sent[-1]["type"] == "search_health"
    assert "indexed_sessions" in ws.sent[-1]

    # request_session_list
    asyncio.run(handle_search_message(
        ws,
        {"type": "request_session_list"},
        pool=pool,
    ))
    assert ws.sent[-1]["type"] == "session_list"
    assert "items" in ws.sent[-1]


def test_handle_search_message_returns_error_on_invalid_type(db_path: Path) -> None:
    from bridge.handlers.search_ws import handle_search_message

    pool = ConnectionPool(db_path, max_size=2)
    ws = _FakeWs()

    asyncio.run(handle_search_message(
        ws,
        {"type": "request_nonexistent"},
        pool=pool,
    ))
    sent = ws.sent[-1]
    assert "error" in sent["type"]
    assert "message" in sent


# ---------------------------------------------------------------------------
# ConnectionPool tests
# ---------------------------------------------------------------------------

def test_connection_pool_borrow_release(db_path: Path) -> None:
    pool = ConnectionPool(db_path, max_size=2)

    async def _run():
        async with pool.borrow() as conn:
            # Should be able to execute a query
            result = conn.execute("SELECT 1").fetchone()
            assert result == (1,)
        await pool.close_all()

    asyncio.run(_run())


def test_connection_pool_concurrent_borrow_no_deadlock(db_path: Path) -> None:
    """Multiple coroutines borrowing concurrently should not deadlock."""
    pool = ConnectionPool(db_path, max_size=3)

    async def _worker(n: int) -> int:
        async with pool.borrow() as conn:
            await asyncio.sleep(0)  # yield to scheduler
            row = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()
            return row[0]

    async def _run():
        results = await asyncio.gather(*[_worker(i) for i in range(6)])
        assert all(isinstance(r, int) for r in results)
        await pool.close_all()

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# F4: Bare NOT / OR NOT query must not raise OperationalError
# ---------------------------------------------------------------------------

def test_search_handles_leading_not_gracefully(pool: ConnectionPool) -> None:
    """Query 'NOT hello' must not raise; returns empty results with optional warning."""
    result = asyncio.run(search(pool, "NOT hello"))
    assert isinstance(result.hits, list)
    assert isinstance(result.warnings, list)


def test_search_handles_or_not_pattern(pool: ConnectionPool) -> None:
    """Query 'foo OR NOT bar' must not raise OperationalError."""
    result = asyncio.run(search(pool, "foo OR NOT bar"))
    assert isinstance(result.hits, list)
    assert isinstance(result.warnings, list)


def test_search_catches_fts5_operationalerror(db_path: Path) -> None:
    """Any FTS5 OperationalError must be caught and returned as a warning, not raised."""
    import sqlite3 as _sqlite3
    from unittest.mock import patch

    pool = ConnectionPool(db_path, max_size=2)

    def _raising_run_search(*args, **kwargs):
        raise _sqlite3.OperationalError("fts5: syntax error near \"NOT\"")

    with patch("bridge.search.query.search._run_search", side_effect=_raising_run_search):
        result = asyncio.run(search(pool, "somequery"))

    assert isinstance(result.hits, list)
    assert result.hits == []
    assert any("syntax" in w.lower() or "invalid" in w.lower() for w in result.warnings), (
        f"Expected a syntax/invalid warning, got: {result.warnings}"
    )


def test_build_fts_match_strips_leading_not() -> None:
    """_build_fts_match('NOT hello') must not start with NOT."""
    from bridge.search.query.search import _build_fts_match
    result = _build_fts_match("NOT hello")
    assert not result.startswith("NOT"), f"Unexpected result: {result!r}"


def test_build_fts_match_strips_or_not_pattern() -> None:
    """_build_fts_match('foo OR NOT bar') must not produce a trailing OR."""
    from bridge.search.query.search import _build_fts_match
    result = _build_fts_match("foo OR NOT bar")
    assert not result.endswith("NOT"), f"Trailing NOT found: {result!r}"
    assert not result.endswith("OR"), f"Trailing OR found: {result!r}"


# ---------------------------------------------------------------------------
# F8: SearchResponse.returned_count + deprecated total alias
# ---------------------------------------------------------------------------

def test_search_response_returned_count_equals_hits_length(db_path: Path) -> None:
    """returned_count must equal len(hits) for a real search result."""
    conn = open_connection(db_path)
    _insert_session(conn, session_id="rc1")
    _insert_message(conn, session_id="rc1", msg_uuid="rcm1",
                    content="returnedcount_test_unique_xyzzy")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(search(pool, "returnedcount_test_unique_xyzzy"))
    assert result.returned_count == len(result.hits)
    assert result.returned_count >= 1


def test_search_response_has_deprecated_total_alias(db_path: Path) -> None:
    """total field must exist and equal returned_count (deprecated alias)."""
    conn = open_connection(db_path)
    _insert_session(conn, session_id="ta1")
    _insert_message(conn, session_id="ta1", msg_uuid="tam1",
                    content="totalalias_test_unique_content_zeta")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(search(pool, "totalalias_test_unique_content_zeta"))
    # Both fields must exist and be equal
    assert hasattr(result, "total"), "deprecated 'total' field must still be present"
    assert hasattr(result, "returned_count"), "'returned_count' field must exist"
    assert result.total == result.returned_count


# ---------------------------------------------------------------------------
# F9: Pagination with filter must not skip hits
# ---------------------------------------------------------------------------

def test_search_pagination_with_filter_does_not_skip_hits(db_path: Path) -> None:
    """Filter + offset must not skip valid results due to FTS5 pre-filter OFFSET bug."""
    conn = open_connection(db_path)
    # pf_main is in /proj/a; pf_other is in /proj/b — only pf_main passes the filter
    _insert_session(conn, session_id="pf_main", project_dir="/proj/a")
    _insert_session(conn, session_id="pf_other", project_dir="/proj/b")

    # 5 messages in pf_main (will pass filter), 3 in pf_other (won't)
    for i in range(5):
        _insert_message(conn, session_id="pf_main", msg_uuid=f"pf_m{i}",
                        content=f"filterpage_unique_token_alpha_{i}")
    for i in range(3):
        _insert_message(conn, session_id="pf_other", msg_uuid=f"pf_o{i}",
                        content=f"filterpage_unique_token_alpha_{i}")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    filters = SearchFilters(project_dir="/proj/a")  # only pf_main's 5 messages match

    page1 = asyncio.run(search(pool, "filterpage_unique_token_alpha",
                               filters=filters, limit=2, offset=0))
    page2 = asyncio.run(search(pool, "filterpage_unique_token_alpha",
                               filters=filters, limit=2, offset=2))

    assert len(page1.hits) == 2, f"page1 should have 2 hits, got {len(page1.hits)}"
    # page2 must have at least 1 hit (the 5th pf_main message at offset=4 is the 5th)
    assert len(page2.hits) >= 1, (
        f"page2 must not be empty — FTS5 OFFSET pagination bug may have skipped valid hits. "
        f"page1 UUIDs: {[h.msg_uuid for h in page1.hits]}"
    )
    # No overlap
    uuids1 = {h.msg_uuid for h in page1.hits}
    uuids2 = {h.msg_uuid for h in page2.hits}
    assert uuids1.isdisjoint(uuids2), "pages must not overlap"


def test_search_emits_pagination_warning_when_offset_and_filter_combined(db_path: Path) -> None:
    """When offset>0 and a post-MATCH filter is set, a pagination warning must be emitted."""
    conn = open_connection(db_path)
    _insert_session(conn, session_id="pw1")
    _insert_message(conn, session_id="pw1", msg_uuid="pwm1",
                    content="paginationwarn_test_content_omega")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    filters = SearchFilters(role="user")
    result = asyncio.run(search(pool, "paginationwarn_test_content_omega",
                                filters=filters, offset=10))
    assert any("pagination" in w.lower() for w in result.warnings), (
        f"Expected a pagination warning, got: {result.warnings}"
    )


# ---------------------------------------------------------------------------
# F10: ConnectionPool must not exceed max_size under concurrency
# ---------------------------------------------------------------------------

def test_pool_does_not_exceed_max_size_under_concurrency(db_path: Path) -> None:
    """Concurrent acquire()+release() cycles must not create more than max_size connections."""
    pool = ConnectionPool(db_path, max_size=4)

    async def _run():
        # Each worker acquires a connection, yields, then releases it.
        # With max_size=4, at most 4 connections should ever be created even with
        # 8 concurrent workers, because connections are reused via the idle queue.
        async def _worker():
            conn = await pool.acquire()
            await asyncio.sleep(0)  # yield so other workers can start
            await pool.release(conn)

        await asyncio.gather(*[_worker() for _ in range(8)])
        return pool._total_created

    total_created = asyncio.run(_run())
    assert total_created <= 4, (
        f"Pool created {total_created} connections, expected <= 4 (max_size)"
    )


def test_pool_borrow_release_cycle_stays_bounded(db_path: Path) -> None:
    """Sequential borrow/release cycles must not accumulate connections."""
    pool = ConnectionPool(db_path, max_size=2)

    async def _run():
        for _ in range(10):
            async with pool.borrow() as conn:
                conn.execute("SELECT 1")
        return pool._total_created

    total = asyncio.run(_run())
    assert total <= 2, f"Pool created {total} connections, expected <= 2"


# ---------------------------------------------------------------------------
# F11: health.ready must reflect worker.is_ready()
# ---------------------------------------------------------------------------

def test_health_ready_false_when_worker_not_started(db_path: Path) -> None:
    """health.ready must be False when get_worker() returns None."""
    pool = ConnectionPool(db_path, max_size=2)

    # get_worker is imported lazily inside health(), so patch the source module.
    with patch("search.ingest.get_worker", return_value=None):
        result = asyncio.run(health(pool))

    assert result.ready is False
    assert result.ingest_progress.get("status") == "unavailable"


def test_health_ready_false_during_bulk(db_path: Path) -> None:
    """health.ready must be False when worker.is_ready() returns False (bulk running)."""
    pool = ConnectionPool(db_path, max_size=2)

    mock_worker = MagicMock()
    mock_worker.is_ready.return_value = False
    mock_worker.get_progress.return_value = {"status": "bulk_running", "ready": False}

    with patch("search.ingest.get_worker", return_value=mock_worker):
        result = asyncio.run(health(pool))

    assert result.ready is False


def test_health_ready_true_when_worker_ready(db_path: Path) -> None:
    """health.ready must be True when worker.is_ready() returns True."""
    pool = ConnectionPool(db_path, max_size=2)

    mock_worker = MagicMock()
    mock_worker.is_ready.return_value = True
    mock_worker.get_progress.return_value = {"status": "ready", "ready": True}

    with patch("search.ingest.get_worker", return_value=mock_worker):
        result = asyncio.run(health(pool))

    assert result.ready is True


# ---------------------------------------------------------------------------
# CJK LIKE fallback tests
# ---------------------------------------------------------------------------

def test_search_handles_2_char_cjk_via_like_fallback(db_path: Path) -> None:
    """query='手感' (2-char CJK) should match content via LIKE fallback."""
    conn = open_connection(db_path)
    _insert_session(conn, session_id="s_cjk", project_dir="/proj/cjk")
    _insert_message(
        conn, session_id="s_cjk", msg_uuid="m_cjk",
        content="聊聊手感升級這件事吧",
        ts="2026-01-01T12:00:00Z",
    )
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(search(pool, "手感"))

    assert result.total >= 1, "Expected at least one hit for 2-char CJK query"
    hit = result.hits[0]
    assert hit.session_id == "s_cjk"
    assert hit.msg_uuid == "m_cjk"
    # Snippet should contain the highlighted token
    assert "手感" in hit.snippet or "<<手感>>" in hit.snippet


def test_like_fallback_warning_emitted(db_path: Path) -> None:
    """search() must emit a warning mentioning LIKE fallback for short CJK tokens."""
    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(search(pool, "感"))
    assert any("LIKE fallback" in w for w in result.warnings), (
        f"Expected LIKE fallback warning, got: {result.warnings}"
    )


def test_like_fallback_respects_filters(db_path: Path) -> None:
    """LIKE fallback must honour SearchFilters.project_dir."""
    conn = open_connection(db_path)
    _insert_session(conn, session_id="sX", project_dir="/proj/X")
    _insert_session(conn, session_id="sY", project_dir="/proj/Y")
    _insert_message(conn, session_id="sX", msg_uuid="mX", content="手感真好", ts="2026-01-01T12:00:00Z")
    _insert_message(conn, session_id="sY", msg_uuid="mY", content="手感不錯", ts="2026-01-01T12:01:00Z")
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    filters = SearchFilters(project_dir="/proj/X")
    result = asyncio.run(search(pool, "手感", filters=filters))

    assert all(h.project_dir == "/proj/X" for h in result.hits), (
        f"Filter not respected: {[h.project_dir for h in result.hits]}"
    )
    uuids = {h.msg_uuid for h in result.hits}
    assert "mX" in uuids
    assert "mY" not in uuids


def test_like_fallback_multi_token_and(db_path: Path) -> None:
    """Multiple short CJK tokens must be AND-ed (all must match)."""
    conn = open_connection(db_path)
    _insert_session(conn, session_id="sM", project_dir="/proj/multi")
    # Only this message contains both tokens
    _insert_message(
        conn, session_id="sM", msg_uuid="m_both",
        content="手感升級很好用",
        ts="2026-01-01T12:00:00Z",
    )
    # This message only has one of the tokens
    _insert_message(
        conn, session_id="sM", msg_uuid="m_one",
        content="感覺不錯",
        ts="2026-01-01T12:01:00Z",
    )
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    # Both "手" and "感" must be present (1-char tokens)
    result = asyncio.run(search(pool, "手 感"))

    # Both tokens are 1 char — each qualifies as short CJK; LIKE fallback ANDs them
    # m_both contains both; m_one only has "感"
    hit_uuids = {h.msg_uuid for h in result.hits}
    assert "m_both" in hit_uuids
    assert "m_one" not in hit_uuids


# ---------------------------------------------------------------------------
# Per-session cap (diversity) tests
# ---------------------------------------------------------------------------

def test_search_caps_per_session_hits_at_3(db_path: Path) -> None:
    """A session with 10 matching messages must contribute at most 3 hits (default cap)."""
    conn = open_connection(db_path)
    _insert_session(conn, session_id="dense_sess", project_dir="/proj/cap")
    for i in range(10):
        _insert_message(
            conn,
            session_id="dense_sess",
            msg_uuid=f"dense_msg_{i}",
            content=f"salon_cap_test_keyword unique content number {i}",
            ts=f"2026-01-01T12:{i:02d}:00Z",
        )
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(search(pool, "salon_cap_test_keyword", limit=50))

    hits_from_dense = [h for h in result.hits if h.session_id == "dense_sess"]
    assert len(hits_from_dense) <= 3, (
        f"Expected at most 3 hits from dense_sess, got {len(hits_from_dense)}"
    )


def test_search_diverse_sessions_after_cap(db_path: Path) -> None:
    """5 sessions × 5 hits each with cap=3 should yield hits from all 5 sessions."""
    conn = open_connection(db_path)
    session_ids = [f"div_sess_{i}" for i in range(5)]
    for sid in session_ids:
        _insert_session(conn, session_id=sid, project_dir="/proj/div")
        for j in range(5):
            _insert_message(
                conn,
                session_id=sid,
                msg_uuid=f"{sid}_msg_{j}",
                content=f"diversity_keyword_test_zeta content {sid} item {j}",
                ts=f"2026-01-0{j+1}T12:00:00Z",
            )
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    result = asyncio.run(search(pool, "diversity_keyword_test_zeta", limit=50))

    unique_sessions = {h.session_id for h in result.hits}
    assert unique_sessions == set(session_ids), (
        f"Expected hits from all 5 sessions, got: {unique_sessions}"
    )
    # Each session must contribute at most 3 hits
    for sid in session_ids:
        count = sum(1 for h in result.hits if h.session_id == sid)
        assert count <= 3, f"Session {sid} contributed {count} hits, expected <= 3"
    # Total hits = 5 sessions × 3 cap = 15
    assert len(result.hits) == 15, f"Expected 15 hits total, got {len(result.hits)}"


def test_search_max_per_session_param_override(db_path: Path) -> None:
    """Passing max_per_session=1 via SearchFilters must cap each session to 1 hit."""
    conn = open_connection(db_path)
    for i in range(3):
        sid = f"override_sess_{i}"
        _insert_session(conn, session_id=sid, project_dir="/proj/override")
        for j in range(4):
            _insert_message(
                conn,
                session_id=sid,
                msg_uuid=f"{sid}_msg_{j}",
                content=f"override_cap_keyword_alpha session {i} msg {j}",
                ts=f"2026-01-0{j+1}T10:00:00Z",
            )
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    filters = SearchFilters(max_per_session=1)
    result = asyncio.run(search(pool, "override_cap_keyword_alpha", filters=filters, limit=50))

    unique_sessions = {h.session_id for h in result.hits}
    assert len(unique_sessions) == 3, (
        f"Expected 3 unique sessions, got {len(unique_sessions)}: {unique_sessions}"
    )
    for sid in unique_sessions:
        count = sum(1 for h in result.hits if h.session_id == sid)
        assert count == 1, f"Session {sid} has {count} hits, expected exactly 1"


def test_like_fallback_also_caps_per_session(db_path: Path) -> None:
    """LIKE fallback (short CJK) must also cap hits per session at max_per_session."""
    conn = open_connection(db_path)
    # Insert 2 sessions: sess_heavy has 6 messages, sess_light has 2
    _insert_session(conn, session_id="like_heavy", project_dir="/proj/like")
    _insert_session(conn, session_id="like_light", project_dir="/proj/like")
    for j in range(6):
        _insert_message(
            conn,
            session_id="like_heavy",
            msg_uuid=f"heavy_msg_{j}",
            content=f"手感升級測試內容 版本{j} 詳細說明",
            ts=f"2026-01-0{j+1}T09:00:00Z",
        )
    for j in range(2):
        _insert_message(
            conn,
            session_id="like_light",
            msg_uuid=f"light_msg_{j}",
            content=f"手感升級測試內容 簡短版本{j}",
            ts=f"2026-01-0{j+1}T10:00:00Z",
        )
    conn.close()

    pool = ConnectionPool(db_path, max_size=2)
    # "手感" is 2-char CJK → triggers LIKE fallback
    result = asyncio.run(search(pool, "手感", limit=50))

    heavy_hits = [h for h in result.hits if h.session_id == "like_heavy"]
    light_hits = [h for h in result.hits if h.session_id == "like_light"]

    assert len(heavy_hits) <= 3, (
        f"LIKE fallback: like_heavy should be capped at 3, got {len(heavy_hits)}"
    )
    # like_light has 2 messages — both should appear
    assert len(light_hits) == 2, (
        f"LIKE fallback: like_light should have 2 hits, got {len(light_hits)}"
    )
