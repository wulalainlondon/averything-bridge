"""
Unit tests for bridge.search.ingest.

All tests use tmp_path and fake JSONL corpora — no access to real ~/.claude/projects.
Run: pytest bridge/tests/test_search_ingest.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import types
from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

# Ensure bridge package importable from repo root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn(tmp_path):
    """Open in-memory (or file-based for WAL) DB with full schema."""
    from bridge.search.db import open_connection, init_schema, migrate
    conn = open_connection(tmp_path / "test.db")
    init_schema(conn)
    migrate(conn)
    return conn


def _write_jsonl(path: Path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + '\n')


def _append_jsonl(path: Path, record: dict) -> None:
    with open(path, 'a', encoding='utf-8') as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + '\n')


def _user_record(text='hello world', uuid='u1', ts='2026-01-01T00:00:00.000Z', cwd='/tmp'):
    return {
        'type': 'user',
        'uuid': uuid,
        'parentUuid': None,
        'isSidechain': False,
        'timestamp': ts,
        'cwd': cwd,
        'message': {'role': 'user', 'content': text},
    }


def _assistant_record(text='reply', uuid='a1', ts='2026-01-01T00:01:00.000Z'):
    return {
        'type': 'assistant',
        'uuid': uuid,
        'parentUuid': 'u1',
        'isSidechain': False,
        'timestamp': ts,
        'message': {'role': 'assistant', 'content': [{'type': 'text', 'text': text}]},
    }


def _count_messages(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]


def _count_sessions(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]


def _get_ingest_state(conn, path: Path) -> dict | None:
    # ingest_file stores path.resolve() — always resolve before querying
    row = conn.execute(
        "SELECT last_offset, head_sha256, file_size, msg_extracted, errors "
        "FROM ingest_state WHERE source_path = ?",
        (str(path.resolve()),),
    ).fetchone()
    if row is None:
        return None
    return {
        'last_offset': row[0],
        'head_sha256': row[1],
        'file_size': row[2],
        'msg_extracted': row[3],
        'errors': row[4],
    }


# ---------------------------------------------------------------------------
# Fixture: a ClaudeJsonlSource patched to read from tmp_path
# ---------------------------------------------------------------------------

@pytest.fixture
def claude_source(tmp_path):
    """Return a ClaudeJsonlSource whose root is redirected to tmp_path."""
    from bridge.search.sources.claude import ClaudeJsonlSource
    src = ClaudeJsonlSource()

    # Patch the module-level _CLAUDE_ROOT so discover() finds our tmp files
    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path
    yield src, tmp_path
    claude_mod._CLAUDE_ROOT = original_root


# ---------------------------------------------------------------------------
# test_ingest_file_processes_new_file_from_offset_0
# ---------------------------------------------------------------------------

def test_ingest_file_processes_new_file_from_offset_0(tmp_path):
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file

    # Build a minimal JSONL at a path the source can handle
    session_dir = tmp_path / 'projects' / 'myproject' / 'abc123'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'abc123.jsonl'
    _write_jsonl(jfile, [
        _user_record('first user message', uuid='u1'),
        _assistant_record('first reply', uuid='a1'),
    ])

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    conn = _make_conn(tmp_path)
    src = ClaudeJsonlSource()
    result = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    assert result.messages_added == 2
    assert result.errors == 0
    assert result.rotated is False
    assert _count_messages(conn) == 2

    state = _get_ingest_state(conn, jfile)
    assert state is not None
    assert state['last_offset'] > 0
    assert state['msg_extracted'] == 2


# ---------------------------------------------------------------------------
# test_ingest_file_resumes_from_last_offset_on_second_call
# ---------------------------------------------------------------------------

def test_ingest_file_resumes_from_last_offset_on_second_call(tmp_path):
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file

    session_dir = tmp_path / 'projects' / 'proj' / 'sess1'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess1.jsonl'
    _write_jsonl(jfile, [
        _user_record('msg one', uuid='u1'),
    ])

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    conn = _make_conn(tmp_path)
    src = ClaudeJsonlSource()

    # First ingest
    r1 = asyncio.run(ingest_file(conn, src, jfile))
    assert r1.messages_added == 1

    # Append a new message
    _append_jsonl(jfile, _assistant_record('response one', uuid='a1'))

    # Second ingest — should only pick up the new line
    r2 = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    assert r2.messages_added == 1
    assert _count_messages(conn) == 2
    state = _get_ingest_state(conn, jfile)
    assert state['msg_extracted'] == 2  # additive


# ---------------------------------------------------------------------------
# test_ingest_file_detects_rotation_and_re_ingests
# ---------------------------------------------------------------------------

def test_ingest_file_detects_rotation_and_re_ingests(tmp_path):
    """
    Rotation: file is replaced with different content.
    We write > 4096 bytes so the head_sha256 is stable for normal appends
    but changes on a full overwrite.
    """
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file

    session_dir = tmp_path / 'projects' / 'proj' / 'sess2'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess2.jsonl'

    # Write enough records to exceed 4096 bytes (rotation detection threshold)
    initial_records = []
    for i in range(30):
        initial_records.append(
            _user_record('original content ' + 'x' * 80, uuid=f'orig{i}')
        )
    _write_jsonl(jfile, initial_records)
    assert jfile.stat().st_size >= 4096, "Need >= 4096 bytes for sha-based rotation detection"

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    conn = _make_conn(tmp_path)
    src = ClaudeJsonlSource()

    # First ingest
    r1 = asyncio.run(ingest_file(conn, src, jfile))
    assert r1.messages_added == 30
    assert _count_messages(conn) == 30

    # Overwrite file with completely different content (rotation)
    _write_jsonl(jfile, [
        _user_record('totally new content after rotation', uuid='new1'),
        _assistant_record('new reply', uuid='newA1'),
    ])

    # Second ingest — must detect rotation via sha mismatch
    r2 = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    assert r2.rotated is True
    assert r2.messages_added == 2
    # Old messages should be gone, new ones present
    assert _count_messages(conn) == 2
    uuids = {r[0] for r in conn.execute("SELECT msg_uuid FROM messages").fetchall()}
    assert 'orig0' not in uuids
    assert 'new1' in uuids


# ---------------------------------------------------------------------------
# test_ingest_file_detects_truncation_and_re_ingests
# ---------------------------------------------------------------------------

def test_ingest_file_detects_truncation_and_re_ingests(tmp_path):
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file

    session_dir = tmp_path / 'projects' / 'proj' / 'sess3'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess3.jsonl'

    # Write 3 messages
    records = [
        _user_record('msg a', uuid='ua'),
        _assistant_record('rep a', uuid='aa'),
        _user_record('msg b', uuid='ub'),
    ]
    _write_jsonl(jfile, records)

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    conn = _make_conn(tmp_path)
    src = ClaudeJsonlSource()

    r1 = asyncio.run(ingest_file(conn, src, jfile))
    assert r1.messages_added == 3
    old_offset = _get_ingest_state(conn, jfile)['last_offset']

    # Truncate file to fewer bytes than last_offset
    _write_jsonl(jfile, [_user_record('fresh start', uuid='uf1')])
    new_size = jfile.stat().st_size
    assert new_size < old_offset

    r2 = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    assert r2.rotated is True
    assert _count_messages(conn) == 1


# ---------------------------------------------------------------------------
# test_ingest_file_skips_partial_last_line
# ---------------------------------------------------------------------------

def test_ingest_file_skips_partial_last_line(tmp_path):
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file

    session_dir = tmp_path / 'projects' / 'proj' / 'sess4'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess4.jsonl'

    # Write one complete line then a partial line (no trailing newline)
    complete = json.dumps(_user_record('complete', uuid='c1'), ensure_ascii=False) + '\n'
    partial = '{"type":"user","uuid":"p1","message":{"role":"user","content":"trunc'
    with open(jfile, 'w', encoding='utf-8') as fh:
        fh.write(complete)
        fh.write(partial)

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    conn = _make_conn(tmp_path)
    src = ClaudeJsonlSource()
    result = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    # Only the complete line should be ingested
    assert result.messages_added == 1
    assert _count_messages(conn) == 1

    # Offset should be after the complete line, not at end of file
    state = _get_ingest_state(conn, jfile)
    assert state['last_offset'] == len(complete.encode('utf-8'))


# ---------------------------------------------------------------------------
# test_ingest_file_advances_offset_past_bad_line
# ---------------------------------------------------------------------------

def test_ingest_file_advances_offset_past_bad_line(tmp_path):
    """Bad JSON lines must advance the offset (not get stuck in a retry loop)."""
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file

    session_dir = tmp_path / 'projects' / 'proj' / 'sess5'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess5.jsonl'

    good1 = json.dumps(_user_record('before bad', uuid='g1'), ensure_ascii=False) + '\n'
    bad   = b'NOT VALID JSON }{{\n'
    good2 = json.dumps(_user_record('after bad', uuid='g2'), ensure_ascii=False) + '\n'

    with open(jfile, 'wb') as fh:
        fh.write(good1.encode('utf-8'))
        fh.write(bad)
        fh.write(good2.encode('utf-8'))

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    conn = _make_conn(tmp_path)
    src = ClaudeJsonlSource()
    result = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    # 2 good messages ingested, bad line skipped
    assert result.messages_added == 2
    # Offset must be at end of file (past the bad line), not stuck before it
    state = _get_ingest_state(conn, jfile)
    file_size = jfile.stat().st_size
    assert state['last_offset'] == file_size, (
        f"Offset {state['last_offset']} != file size {file_size} — bad line may have caused a stuck offset"
    )


# ---------------------------------------------------------------------------
# test_ingest_file_handles_disk_write_error_gracefully
# ---------------------------------------------------------------------------

class _BrokenConn:
    """Wraps a real sqlite3.Connection but raises on INSERT INTO messages executemany."""

    def __init__(self, real_conn, error_cls):
        self._conn = real_conn
        self._error_cls = error_cls

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    def executemany(self, sql, params):
        if 'INSERT INTO messages' in sql:
            raise self._error_cls("disk full simulation")
        return self._conn.executemany(sql, params)

    def executescript(self, sql):
        return self._conn.executescript(sql)

    def commit(self):
        return self._conn.commit()

    def close(self):
        return self._conn.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_ingest_file_handles_disk_write_error_gracefully(tmp_path):
    """DB write errors should not crash ingest_file; errors counter must increment."""
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file
    from bridge.search.db.sqlite_adapter import sqlite3

    session_dir = tmp_path / 'projects' / 'proj' / 'sess6'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess6.jsonl'
    _write_jsonl(jfile, [_user_record('hello', uuid='u1')])

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    real_conn = _make_conn(tmp_path)
    conn = _BrokenConn(real_conn, sqlite3.OperationalError)
    src = ClaudeJsonlSource()

    # Should not raise
    result = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    assert result.errors >= 1  # error was counted


# ---------------------------------------------------------------------------
# test_ingest_file_updates_sessions_metadata_correctly
# ---------------------------------------------------------------------------

def test_ingest_file_updates_sessions_metadata_correctly(tmp_path):
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file

    session_dir = tmp_path / 'projects' / 'proj' / 'sess7'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess7.jsonl'
    _write_jsonl(jfile, [
        _user_record('What is the meaning of life?', uuid='u1', ts='2026-01-01T00:00:00Z', cwd='/workspace'),
        _assistant_record('42', uuid='a1', ts='2026-01-01T00:01:00Z'),
    ])

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    conn = _make_conn(tmp_path)
    src = ClaudeJsonlSource()
    result = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    assert result.messages_added == 2

    row = conn.execute(
        "SELECT display_name, cwd, msg_count, backend, source "
        "FROM sessions WHERE source_path = ?",
        (str(jfile.resolve()),),
    ).fetchone()

    assert row is not None
    display_name, cwd, msg_count, backend, source = row
    # display_name should be first 80 chars of first user message
    assert 'meaning of life' in display_name
    assert cwd == '/workspace'
    assert msg_count == 2
    assert backend == 'claude'
    assert source == 'claude'


# ---------------------------------------------------------------------------
# test_bulk_ingest_processes_all_files_from_all_enabled_sources
# ---------------------------------------------------------------------------

def test_bulk_ingest_processes_all_files_from_all_enabled_sources(tmp_path):
    from bridge.search.ingest.bulk import bulk_ingest
    from bridge.search.sources.claude import ClaudeJsonlSource

    # Create 3 fake session files — structure: <root>/<project>/<session>.jsonl
    projects_root = tmp_path / 'projects'
    for i in range(3):
        proj_dir = projects_root / f'proj{i}'
        proj_dir.mkdir(parents=True)
        jfile = proj_dir / f'sess{i}.jsonl'
        _write_jsonl(jfile, [
            _user_record(f'question {i}', uuid=f'u{i}'),
            _assistant_record(f'answer {i}', uuid=f'a{i}'),
        ])

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = projects_root

    conn = _make_conn(tmp_path)
    sources = [ClaudeJsonlSource()]
    result = asyncio.run(bulk_ingest(conn, sources))

    claude_mod._CLAUDE_ROOT = original_root

    assert result.total_files == 3
    assert result.total_messages == 6
    assert result.total_errors == 0
    assert _count_messages(conn) == 6


# ---------------------------------------------------------------------------
# test_bulk_ingest_progress_callback_invoked
# ---------------------------------------------------------------------------

def test_bulk_ingest_progress_callback_invoked(tmp_path):
    from bridge.search.ingest.bulk import bulk_ingest
    from bridge.search.sources.claude import ClaudeJsonlSource

    projects_root = tmp_path / 'projects'
    for i in range(4):
        proj_dir = projects_root / f'proj{i}'
        proj_dir.mkdir(parents=True)
        jfile = proj_dir / f'sess{i}.jsonl'
        _write_jsonl(jfile, [_user_record(f'q{i}', uuid=f'u{i}')])

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = projects_root

    conn = _make_conn(tmp_path)
    sources = [ClaudeJsonlSource()]
    calls = []

    def cb(path_str, done, total):
        calls.append((done, total))

    asyncio.run(bulk_ingest(conn, sources, progress_callback=cb))
    claude_mod._CLAUDE_ROOT = original_root

    assert len(calls) == 4
    # done should increment 1..4
    assert [c[0] for c in calls] == [1, 2, 3, 4]
    assert all(c[1] == 4 for c in calls)


def test_bulk_ingest_applies_background_pause(tmp_path):
    """Bulk ingest can be throttled in small batches without changing results."""
    from bridge.search.ingest.bulk import bulk_ingest
    from bridge.search.sources.claude import ClaudeJsonlSource

    projects_root = tmp_path / 'projects'
    for i in range(4):
        proj_dir = projects_root / f'proj{i}'
        proj_dir.mkdir(parents=True)
        jfile = proj_dir / f'sess{i}.jsonl'
        _write_jsonl(jfile, [_user_record(f'q{i}', uuid=f'u{i}')])

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = projects_root

    conn = _make_conn(tmp_path)
    sources = [ClaudeJsonlSource()]

    t0 = time.monotonic()
    result = asyncio.run(
        bulk_ingest(conn, sources, pause_every_files=2, pause_sec=0.02)
    )
    elapsed = time.monotonic() - t0
    claude_mod._CLAUDE_ROOT = original_root

    assert result.total_files == 4
    assert result.total_messages == 4
    assert result.total_errors == 0
    assert elapsed >= 0.035


def test_bulk_ingest_uses_activity_pause_when_probe_is_active(tmp_path):
    """Recent foreground activity should stretch batch pauses during bulk ingest."""
    from bridge.search.ingest.bulk import bulk_ingest
    from bridge.search.sources.claude import ClaudeJsonlSource

    projects_root = tmp_path / 'projects'
    for i in range(2):
        proj_dir = projects_root / f'proj{i}'
        proj_dir.mkdir(parents=True)
        jfile = proj_dir / f'sess{i}.jsonl'
        _write_jsonl(jfile, [_user_record(f'q{i}', uuid=f'u{i}')])

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = projects_root

    conn = _make_conn(tmp_path)
    sources = [ClaudeJsonlSource()]

    t0 = time.monotonic()
    result = asyncio.run(
        bulk_ingest(
            conn,
            sources,
            pause_every_files=1,
            pause_sec=0.0,
            activity_probe=lambda: True,
            activity_pause_sec=0.02,
        )
    )
    elapsed = time.monotonic() - t0
    claude_mod._CLAUDE_ROOT = original_root

    assert result.total_files == 2
    assert result.total_messages == 2
    assert elapsed >= 0.035


# ---------------------------------------------------------------------------
# test_bulk_ingest_under_30s_for_simulated_corpus
# ---------------------------------------------------------------------------

def test_bulk_ingest_under_30s_for_simulated_corpus(tmp_path):
    """100 fake JSONL files × 10 lines each must complete in < 5 seconds."""
    from bridge.search.ingest.bulk import bulk_ingest
    from bridge.search.sources.claude import ClaudeJsonlSource

    # Structure: <root>/<project>/<session>.jsonl
    projects_root = tmp_path / 'projects'
    for i in range(100):
        proj_dir = projects_root / f'proj{i:03d}'
        proj_dir.mkdir(parents=True)
        jfile = proj_dir / f'sess{i:03d}.jsonl'
        records = []
        for j in range(10):
            if j % 2 == 0:
                records.append(_user_record(f'question {i}-{j}', uuid=f'u{i}-{j}'))
            else:
                records.append(_assistant_record(f'answer {i}-{j}', uuid=f'a{i}-{j}'))
        _write_jsonl(jfile, records)

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = projects_root

    conn = _make_conn(tmp_path)
    sources = [ClaudeJsonlSource()]

    t0 = time.monotonic()
    result = asyncio.run(bulk_ingest(conn, sources))
    elapsed = time.monotonic() - t0

    claude_mod._CLAUDE_ROOT = original_root

    assert result.total_files == 100
    assert result.total_messages > 0
    assert elapsed < 5.0, f"bulk_ingest took {elapsed:.2f}s, expected < 5s"


# ---------------------------------------------------------------------------
# test_watcher_coalesces_burst_events
# ---------------------------------------------------------------------------

def test_watcher_coalesces_burst_events(tmp_path):
    """Multiple rapid events for the same path should result in a single queue entry."""
    from bridge.search.ingest.watcher import _JsonlEventHandler

    loop = asyncio.new_event_loop()
    queue = asyncio.Queue()
    handler = _JsonlEventHandler(queue, loop)

    path_str = str(tmp_path / 'test.jsonl')

    # Simulate 5 rapid events for the same path
    for _ in range(5):
        handler._schedule(path_str)

    # Run the loop briefly to let coalesce fire
    async def _drain():
        await asyncio.sleep(0.7)  # > 500ms coalesce window

    loop.run_until_complete(_drain())
    loop.close()

    # Should have exactly 1 item in queue despite 5 rapid events
    assert queue.qsize() == 1
    item = queue.get_nowait()
    assert item == Path(path_str)


# ---------------------------------------------------------------------------
# test_watcher_falls_back_to_polling_if_native_observer_unavailable
# ---------------------------------------------------------------------------

def test_watcher_falls_back_to_polling_if_native_observer_unavailable(tmp_path):
    """When native observer raises on import, _make_observer must return a PollingObserver."""
    # Build a fake PollingObserver the watcher can import
    fake_polling_mod = types.ModuleType('watchdog.observers.polling')
    fake_observer_instance = MagicMock()
    fake_observer_instance.start = MagicMock()
    fake_observer_instance.stop = MagicMock()
    fake_observer_instance.schedule = MagicMock()

    class _FakePollingObserver:
        def __new__(cls, *args, **kwargs):
            return fake_observer_instance

    fake_polling_mod.PollingObserver = _FakePollingObserver

    # Provide a top-level watchdog package stub so the import path resolves
    fake_watchdog = types.ModuleType('watchdog')
    fake_watchdog_observers = types.ModuleType('watchdog.observers')

    with patch.dict(sys.modules, {
        'watchdog': fake_watchdog,
        'watchdog.observers': fake_watchdog_observers,
        'watchdog.observers.polling': fake_polling_mod,
        # Remove native observer modules so their imports fail
        'watchdog.observers.kqueue': None,
        'watchdog.observers.fsevents': None,
        'watchdog.observers.inotify': None,
        'watchdog.observers.winapi': None,
    }):
        # Force reimport of watcher module to pick up patched sys.modules
        import importlib
        import bridge.search.ingest.watcher as watcher_mod
        importlib.reload(watcher_mod)

        with patch.object(watcher_mod, 'platform') as mock_platform:
            mock_platform.system.return_value = 'UnknownOS'
            observer = watcher_mod._make_observer(poll_interval=1)

        # Restore original module
        importlib.reload(watcher_mod)

    assert observer is fake_observer_instance
    assert hasattr(observer, 'start')
    assert hasattr(observer, 'stop')
    assert hasattr(observer, 'schedule')


# ---------------------------------------------------------------------------
# test_worker_progress_reports_correct_counts
# ---------------------------------------------------------------------------

def test_worker_progress_reports_correct_counts(tmp_path):
    """IngestWorker.get_progress() must reflect bulk ingest completion."""
    from bridge.search.ingest.worker import IngestWorker
    from bridge.search.ingest.bulk import bulk_ingest
    from bridge.config.schema import BridgeConfig, SearchConfig, SourcesConfig

    # Create 2 fake session files: <root>/<project>/<session>.jsonl
    projects_root = tmp_path / 'projects'
    for i in range(2):
        proj_dir = projects_root / f'proj{i}'
        proj_dir.mkdir(parents=True)
        jfile = proj_dir / f'sess{i}.jsonl'
        _write_jsonl(jfile, [_user_record(f'q{i}', uuid=f'u{i}')])

    import bridge.search.sources.claude as claude_mod
    import bridge.search.sources.codex as codex_mod
    import bridge.search.sources.ollama as ollama_mod

    original_claude_root = claude_mod._CLAUDE_ROOT
    original_codex_root = codex_mod._CODEX_ROOT

    # Patch both Claude and Codex roots to isolate from real data
    claude_mod._CLAUDE_ROOT = projects_root
    nonexistent = tmp_path / 'no_codex'
    codex_mod._CODEX_ROOT = nonexistent

    config = BridgeConfig(
        search=SearchConfig(
            index_path=tmp_path / 'search.db',
            ingest_on_startup=True,
            watch_enabled=False,
        ),
        sources=SourcesConfig(claude_enabled='auto', codex_enabled='no', ollama_enabled='no'),
    )

    async def _run():
        worker = IngestWorker(config=config, activity_probe=lambda: True)
        await worker.start()

        # Wait for bulk to finish (up to 30s)
        for _ in range(300):
            if worker.is_ready():
                break
            await asyncio.sleep(0.1)

        progress = worker.get_progress()
        await worker.stop()
        return progress

    progress = asyncio.run(_run())

    claude_mod._CLAUDE_ROOT = original_claude_root
    codex_mod._CODEX_ROOT = original_codex_root

    assert progress['ready'] is True
    assert progress['done'] == 2
    assert progress['total'] == 2
    assert progress['errors'] == 0


def test_worker_honors_startup_ingest_delay(tmp_path):
    """Startup bulk ingest can be delayed so initial bridge traffic is not competing with bulk I/O."""
    from bridge.search.ingest.worker import IngestWorker
    from bridge.config.schema import BridgeConfig, SearchConfig, SourcesConfig

    config = BridgeConfig(
        search=SearchConfig(
            index_path=tmp_path / 'delay.db',
            ingest_on_startup=True,
            ingest_startup_delay_sec=0.05,
            watch_enabled=False,
        ),
        sources=SourcesConfig(claude_enabled='no', codex_enabled='no', ollama_enabled='no'),
    )

    async def _run():
        worker = IngestWorker(config=config, activity_probe=lambda: True)
        calls = 0

        async def _fake_run_bulk():
            nonlocal calls
            calls += 1

        worker._run_bulk = _fake_run_bulk
        t0 = time.monotonic()
        await worker._run_bulk_after_startup_delay()
        return time.monotonic() - t0, calls

    elapsed, calls = asyncio.run(_run())

    assert calls == 1
    assert elapsed >= 0.045


def test_worker_run_bulk_does_not_pre_discover_before_bulk_ingest(tmp_path):
    """_run_bulk must delegate discovery to bulk_ingest once, not pre-scan files itself."""
    from bridge.search.ingest.worker import IngestWorker
    from bridge.config.schema import BridgeConfig, SearchConfig, SourcesConfig
    import bridge.search.ingest.worker as worker_mod

    class _Source:
        name = 'claude'

        def is_enabled(self):
            return True

        def discover(self):
            raise AssertionError("_run_bulk should not pre-discover files")

    config = BridgeConfig(
        search=SearchConfig(
            index_path=tmp_path / 'single_discover.db',
            ingest_on_startup=True,
            watch_enabled=False,
        ),
        sources=SourcesConfig(claude_enabled='no', codex_enabled='no', ollama_enabled='no'),
    )

    async def _fake_bulk(conn, sources, progress_callback=None, **kwargs):
        assert len(sources) == 1
        if progress_callback:
            progress_callback("fake.jsonl", 1, 1)
        return worker_mod.BulkResult(
            total_files=1,
            total_messages=0,
            total_errors=0,
            total_bytes=0,
            elapsed_sec=0.0,
        )

    async def _run():
        probe = lambda: True
        worker = IngestWorker(config=config, activity_probe=probe)
        worker._conn = object()
        worker._sources = [_Source()]
        original_bulk = worker_mod.bulk_ingest
        worker_mod.bulk_ingest = _fake_bulk
        try:
            await worker._run_bulk()
            return worker.get_progress()
        finally:
            worker_mod.bulk_ingest = original_bulk

    progress = asyncio.run(_run())

    assert progress['ready'] is True
    assert progress['done'] == 1
    assert progress['total'] == 1
    assert progress['errors'] == 0


def test_worker_passes_bulk_throttle_config_to_bulk_ingest(tmp_path):
    """Worker-level config must flow into the bulk ingest throttle options."""
    from bridge.search.ingest.worker import IngestWorker
    from bridge.config.schema import BridgeConfig, SearchConfig, SourcesConfig
    import bridge.search.ingest.worker as worker_mod

    config = BridgeConfig(
        search=SearchConfig(
            index_path=tmp_path / 'throttle.db',
            ingest_on_startup=True,
            ingest_bulk_pause_every_files=7,
            ingest_bulk_pause_sec=0.03,
            ingest_idle_pause_sec=0.09,
            watch_enabled=False,
        ),
        sources=SourcesConfig(claude_enabled='no', codex_enabled='no', ollama_enabled='no'),
    )

    seen = {}

    async def _fake_bulk(
        conn,
        sources,
        progress_callback=None,
        pause_every_files=0,
        pause_sec=0.0,
        activity_probe=None,
        activity_pause_sec=0.0,
    ):
        seen["pause_every_files"] = pause_every_files
        seen["pause_sec"] = pause_sec
        seen["activity_probe"] = activity_probe
        seen["activity_pause_sec"] = activity_pause_sec
        return worker_mod.BulkResult(
            total_files=0,
            total_messages=0,
            total_errors=0,
            total_bytes=0,
            elapsed_sec=0.0,
        )

    async def _run():
        worker = IngestWorker(config=config, activity_probe=lambda: True)
        worker._conn = object()
        original_bulk = worker_mod.bulk_ingest
        worker_mod.bulk_ingest = _fake_bulk
        try:
            await worker._run_bulk()
            return dict(seen)
        finally:
            worker_mod.bulk_ingest = original_bulk

    captured = asyncio.run(_run())

    assert captured["pause_every_files"] == 7
    assert captured["pause_sec"] == 0.03
    assert captured["activity_probe"] is not None
    assert captured["activity_pause_sec"] == 0.09


# ---------------------------------------------------------------------------
# test_singleton_api_idempotent
# ---------------------------------------------------------------------------

def test_singleton_api_idempotent(tmp_path):
    """start_worker() called twice should return same instance."""
    import bridge.search.ingest as ingest_mod
    from bridge.config.schema import BridgeConfig, SearchConfig, SourcesConfig

    # Reset singleton
    ingest_mod._worker_singleton = None

    import bridge.search.sources.claude as claude_mod
    import bridge.search.sources.codex as codex_mod

    original_claude_root = claude_mod._CLAUDE_ROOT
    original_codex_root = codex_mod._CODEX_ROOT

    (tmp_path / 'projects').mkdir(parents=True, exist_ok=True)
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'
    codex_mod._CODEX_ROOT = tmp_path / 'no_codex'

    config = BridgeConfig(
        search=SearchConfig(
            index_path=tmp_path / 'sing.db',
            ingest_on_startup=False,
            watch_enabled=False,
        ),
        sources=SourcesConfig(claude_enabled='auto', codex_enabled='no', ollama_enabled='no'),
    )

    async def _run():
        w1 = await ingest_mod.start_worker(config=config)
        w2 = await ingest_mod.start_worker(config=config)
        same = w1 is w2
        await ingest_mod.stop_worker()
        return same

    result = asyncio.run(_run())

    claude_mod._CLAUDE_ROOT = original_claude_root
    codex_mod._CODEX_ROOT = original_codex_root

    assert result is True
    assert ingest_mod._worker_singleton is None


# ---------------------------------------------------------------------------
# F2: test_worker_bulk_failure_does_not_mark_ready
# ---------------------------------------------------------------------------

def test_worker_bulk_failure_does_not_mark_ready(tmp_path):
    """If bulk_ingest raises, is_ready() must return False and get_progress() must have 'error'."""
    from bridge.search.ingest.worker import IngestWorker
    from bridge.config.schema import BridgeConfig, SearchConfig, SourcesConfig

    import bridge.search.sources.claude as claude_mod
    import bridge.search.sources.codex as codex_mod

    original_claude_root = claude_mod._CLAUDE_ROOT
    original_codex_root = codex_mod._CODEX_ROOT
    (tmp_path / 'projects').mkdir(parents=True, exist_ok=True)
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'
    codex_mod._CODEX_ROOT = tmp_path / 'no_codex'

    config = BridgeConfig(
            search=SearchConfig(
                index_path=tmp_path / 'fail_bulk.db',
                ingest_on_startup=True,
                ingest_startup_delay_sec=0,
                watch_enabled=False,
            ),
        sources=SourcesConfig(claude_enabled='auto', codex_enabled='no', ollama_enabled='no'),
    )

    async def _run():
        worker = IngestWorker(config=config)
        # Patch bulk_ingest to raise
        import bridge.search.ingest.worker as worker_mod
        original_bulk = worker_mod.bulk_ingest

        async def _failing_bulk(*args, **kwargs):
            raise RuntimeError("simulated bulk failure")

        worker_mod.bulk_ingest = _failing_bulk
        try:
            await worker.start()
            # Wait briefly for bulk task to complete (it should fail quickly)
            for _ in range(50):
                if worker._bulk_task is not None and worker._bulk_task.done():
                    break
                await asyncio.sleep(0.05)
            ready = worker.is_ready()
            progress = worker.get_progress()
            await worker.stop()
            return ready, progress
        finally:
            worker_mod.bulk_ingest = original_bulk

    ready, progress = asyncio.run(_run())

    claude_mod._CLAUDE_ROOT = original_claude_root
    codex_mod._CODEX_ROOT = original_codex_root

    assert ready is False, "is_ready() must be False when bulk_ingest fails"
    assert progress['ready'] is False
    assert 'error' in progress, "get_progress() must contain 'error' key on bulk failure"


# ---------------------------------------------------------------------------
# F3: test_concurrent_start_worker_returns_same_instance
# ---------------------------------------------------------------------------

def test_concurrent_start_worker_returns_same_instance(tmp_path):
    """Concurrent asyncio.gather(start_worker(), start_worker(), start_worker())
    must all return the exact same IngestWorker instance (no resource leak)."""
    import bridge.search.ingest as ingest_mod
    from bridge.config.schema import BridgeConfig, SearchConfig, SourcesConfig

    # Reset singleton and lock
    ingest_mod._worker_singleton = None
    ingest_mod._singleton_lock = None

    import bridge.search.sources.claude as claude_mod
    import bridge.search.sources.codex as codex_mod

    original_claude_root = claude_mod._CLAUDE_ROOT
    original_codex_root = codex_mod._CODEX_ROOT

    (tmp_path / 'projects').mkdir(parents=True, exist_ok=True)
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'
    codex_mod._CODEX_ROOT = tmp_path / 'no_codex'

    config = BridgeConfig(
        search=SearchConfig(
            index_path=tmp_path / 'conc.db',
            ingest_on_startup=False,
            watch_enabled=False,
        ),
        sources=SourcesConfig(claude_enabled='auto', codex_enabled='no', ollama_enabled='no'),
    )

    async def _run():
        w1, w2, w3 = await asyncio.gather(
            ingest_mod.start_worker(config=config),
            ingest_mod.start_worker(config=config),
            ingest_mod.start_worker(config=config),
        )
        same = (w1 is w2) and (w2 is w3)
        await ingest_mod.stop_worker()
        return same, id(w1), id(w2), id(w3)

    same, id1, id2, id3 = asyncio.run(_run())

    claude_mod._CLAUDE_ROOT = original_claude_root
    codex_mod._CODEX_ROOT = original_codex_root

    assert same, (
        f"Concurrent start_worker() calls returned different instances: "
        f"id1={id1}, id2={id2}, id3={id3}"
    )
    assert ingest_mod._worker_singleton is None


# ---------------------------------------------------------------------------
# F5: test_rotation_preserves_count_invariants
# ---------------------------------------------------------------------------

def test_rotation_preserves_count_invariants(tmp_path):
    """After rotation, sessions.msg_count must equal COUNT(*) FROM messages for that session."""
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file

    session_dir = tmp_path / 'projects' / 'proj' / 'rot_sess'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'rot_sess.jsonl'

    # Write enough records to exceed 4096 bytes (rotation detection threshold)
    initial_records = []
    for i in range(30):
        initial_records.append(_user_record('initial content ' + 'y' * 80, uuid=f'init{i}'))
    _write_jsonl(jfile, initial_records)
    assert jfile.stat().st_size >= 4096

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    conn = _make_conn(tmp_path)
    src = ClaudeJsonlSource()

    # First ingest: 30 messages
    r1 = asyncio.run(ingest_file(conn, src, jfile))
    assert r1.messages_added == 30
    session_id = src.session_id_for(jfile)

    def _session_msg_count(c, sid):
        return c.execute("SELECT msg_count FROM sessions WHERE session_id = ?", (sid,)).fetchone()[0]

    def _actual_msg_count(c, sid):
        return c.execute("SELECT COUNT(*) FROM messages WHERE session_id = ?", (sid,)).fetchone()[0]

    def _ingest_state_extracted(c, path):
        row = c.execute("SELECT msg_extracted FROM ingest_state WHERE source_path = ?",
                        (str(path.resolve()),)).fetchone()
        return row[0] if row else None

    # Invariant after first ingest
    assert _session_msg_count(conn, session_id) == _actual_msg_count(conn, session_id) == 30
    assert _ingest_state_extracted(conn, jfile) == 30

    # Rotation: overwrite with 2 new messages
    _write_jsonl(jfile, [
        _user_record('new content after rotation', uuid='new1'),
        _assistant_record('new reply', uuid='newA1'),
    ])

    r2 = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    assert r2.rotated is True
    assert r2.messages_added == 2

    # Core invariant: sessions.msg_count == COUNT(*) FROM messages
    actual = _actual_msg_count(conn, session_id)
    stored = _session_msg_count(conn, session_id)
    extracted = _ingest_state_extracted(conn, jfile)
    assert actual == 2, f"Expected 2 messages after rotation, got {actual}"
    assert stored == actual, f"sessions.msg_count={stored} != COUNT(messages)={actual}"
    assert extracted == actual, f"ingest_state.msg_extracted={extracted} != COUNT(messages)={actual}"


# ---------------------------------------------------------------------------
# F6: test_watcher_uses_configured_paths_not_hardcoded
# ---------------------------------------------------------------------------

def test_watcher_uses_configured_paths_not_hardcoded(tmp_path):
    """WatchdogWatcher._collect_watch_dirs must use source.watch_root, not hardcoded paths."""
    from bridge.search.ingest.watcher import WatchdogWatcher
    from bridge.search.sources.claude import ClaudeJsonlSource
    import bridge.search.sources.claude as claude_mod

    custom_dir = tmp_path / 'custom_claude_projects'
    custom_dir.mkdir(parents=True)

    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = custom_dir

    src = ClaudeJsonlSource()
    assert src.is_enabled(), "Source must be enabled (custom dir exists)"

    queue = asyncio.Queue()
    watcher = WatchdogWatcher(sources=[src], queue=queue, poll_interval=2)
    watch_dirs = watcher._collect_watch_dirs()

    claude_mod._CLAUDE_ROOT = original_root

    assert custom_dir in watch_dirs, (
        f"Watcher must watch {custom_dir}, but got: {watch_dirs}"
    )
    # Must NOT watch the default hardcoded path when it's overridden
    default_path = Path.home() / '.claude' / 'projects'
    if default_path != custom_dir:
        assert default_path not in watch_dirs, (
            f"Watcher must not watch hardcoded {default_path} when config overrides it"
        )


# ---------------------------------------------------------------------------
# F12: IngestWorker.stop() must cancel tasks before closing DB
# ---------------------------------------------------------------------------

def test_worker_stop_propagates_cancel_to_running_task(tmp_path):
    """stop() must cancel consumer/bulk tasks; they must be done after stop() returns."""
    from bridge.search.ingest.worker import IngestWorker
    from bridge.config.schema import BridgeConfig, SearchConfig, SourcesConfig

    import bridge.search.sources.claude as claude_mod
    import bridge.search.sources.codex as codex_mod

    original_claude_root = claude_mod._CLAUDE_ROOT
    original_codex_root = codex_mod._CODEX_ROOT

    (tmp_path / 'projects').mkdir(parents=True, exist_ok=True)
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'
    codex_mod._CODEX_ROOT = tmp_path / 'no_codex'

    config = BridgeConfig(
        search=SearchConfig(
            index_path=tmp_path / 'stop_test.db',
            ingest_on_startup=False,
            watch_enabled=False,
        ),
        sources=SourcesConfig(claude_enabled='auto', codex_enabled='no', ollama_enabled='no'),
    )

    async def _run():
        worker = IngestWorker(config=config)
        await worker.start()
        consumer_task = worker._consumer_task
        await worker.stop()
        # After stop(), the consumer task must be done (cancelled or finished)
        return consumer_task is None or consumer_task.done()

    result = asyncio.run(_run())

    claude_mod._CLAUDE_ROOT = original_claude_root
    codex_mod._CODEX_ROOT = original_codex_root

    assert result, "Consumer task must be done after stop()"


def test_worker_stop_closes_db_after_tasks_finish(tmp_path):
    """stop() must close _conn only after tasks are done — not while they're still running."""
    from bridge.search.ingest.worker import IngestWorker
    from bridge.config.schema import BridgeConfig, SearchConfig, SourcesConfig

    import bridge.search.sources.claude as claude_mod
    import bridge.search.sources.codex as codex_mod

    original_claude_root = claude_mod._CLAUDE_ROOT
    original_codex_root = codex_mod._CODEX_ROOT

    (tmp_path / 'projects').mkdir(parents=True, exist_ok=True)
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'
    codex_mod._CODEX_ROOT = tmp_path / 'no_codex'

    config = BridgeConfig(
        search=SearchConfig(
            index_path=tmp_path / 'stop_order.db',
            ingest_on_startup=False,
            watch_enabled=False,
        ),
        sources=SourcesConfig(claude_enabled='auto', codex_enabled='no', ollama_enabled='no'),
    )

    async def _run():
        worker = IngestWorker(config=config)
        await worker.start()
        await worker.stop()
        # conn must be None after stop
        return worker._conn is None

    result = asyncio.run(_run())

    claude_mod._CLAUDE_ROOT = original_claude_root
    codex_mod._CODEX_ROOT = original_codex_root

    assert result, "_conn must be None after stop()"


# ---------------------------------------------------------------------------
# F13: _flush_batch failure must not corrupt last_offset
# ---------------------------------------------------------------------------

def test_flush_batch_failure_does_not_corrupt_offset(tmp_path):
    """DB error on flush must leave last_offset at the committed batch boundary, not corrupt it."""
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file
    from bridge.search.db.sqlite_adapter import sqlite3 as _sqlite3

    session_dir = tmp_path / 'projects' / 'proj' / 'sess_f13'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess_f13.jsonl'

    # Write exactly 1 user message (small, fits in one batch that will fail)
    _write_jsonl(jfile, [_user_record('flush fail test', uuid='ff1')])

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    real_conn = _make_conn(tmp_path)
    broken = _BrokenConn(real_conn, _sqlite3.OperationalError)
    src = ClaudeJsonlSource()

    result = asyncio.run(ingest_file(broken, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    # The flush failed; last_offset must not be negative or arbitrary
    state = _get_ingest_state(real_conn, jfile)
    assert result.errors >= 1, "errors counter must increment on DB write failure"
    # last_offset must be >= 0 (no negative corruption)
    if state is not None:
        assert state['last_offset'] >= 0, (
            f"last_offset={state['last_offset']} is negative — offset was corrupted by bad subtraction"
        )


def test_subsequent_ingest_after_flush_failure_is_idempotent(tmp_path):
    """After a flush failure, re-ingesting with a working connection produces no duplicates."""
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file
    from bridge.search.db.sqlite_adapter import sqlite3 as _sqlite3

    session_dir = tmp_path / 'projects' / 'proj' / 'sess_f13b'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess_f13b.jsonl'
    _write_jsonl(jfile, [_user_record('idempotent test', uuid='id1')])

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    src = ClaudeJsonlSource()

    # First ingest with broken conn → fails
    real_conn = _make_conn(tmp_path)
    broken = _BrokenConn(real_conn, _sqlite3.OperationalError)
    asyncio.run(ingest_file(broken, src, jfile))

    # Second ingest with real conn → must succeed without duplicates
    r2 = asyncio.run(ingest_file(real_conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    assert r2.errors == 0
    # Only 1 message should exist (no duplicates)
    count = _count_messages(real_conn)
    assert count <= 1, f"Expected at most 1 message after idempotent re-ingest, got {count}"


# ---------------------------------------------------------------------------
# F14: Small file rotation detection
# ---------------------------------------------------------------------------

def test_small_file_rotation_detected_when_content_replaced(tmp_path):
    """A small file (<64KB) overwritten with different content must trigger rotation."""
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file

    session_dir = tmp_path / 'projects' / 'proj' / 'sess_f14a'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess_f14a.jsonl'

    # Write a small file (~150 bytes, well under 4KB and 64KB)
    _write_jsonl(jfile, [_user_record('original small content', uuid='sm1')])
    assert jfile.stat().st_size < 4096, "Test requires file < 4096 bytes"

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    conn = _make_conn(tmp_path)
    src = ClaudeJsonlSource()

    r1 = asyncio.run(ingest_file(conn, src, jfile))
    assert r1.messages_added == 1
    assert _count_messages(conn) == 1

    # Overwrite with completely different content (same or larger size)
    _write_jsonl(jfile, [_user_record('COMPLETELY DIFFERENT replacement content', uuid='sm2')])

    r2 = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    # Must detect rotation — old message gone, new one present
    assert r2.rotated is True, "Small file overwrite must be detected as rotation"
    assert _count_messages(conn) == 1
    uuids = {r[0] for r in conn.execute("SELECT msg_uuid FROM messages").fetchall()}
    assert 'sm1' not in uuids, "Old message must be deleted after rotation"
    assert 'sm2' in uuids, "New message must be present after rotation"


def test_large_file_append_does_not_trigger_rotation(tmp_path):
    """Appending to a large file (>= 64KB) must not trigger rotation."""
    from bridge.search.sources.claude import ClaudeJsonlSource
    from bridge.search.ingest.single_file import ingest_file
    from bridge.search.ingest.single_file import _FULL_FILE_SHA_THRESHOLD

    session_dir = tmp_path / 'projects' / 'proj' / 'sess_f14b'
    session_dir.mkdir(parents=True)
    jfile = session_dir / 'sess_f14b.jsonl'

    # Write enough records to exceed _FULL_FILE_SHA_THRESHOLD (64KB)
    large_records = []
    for i in range(400):
        large_records.append(_user_record('large file content ' + 'x' * 100, uuid=f'lg{i}'))
    _write_jsonl(jfile, large_records)
    assert jfile.stat().st_size >= _FULL_FILE_SHA_THRESHOLD, (
        f"Need >= {_FULL_FILE_SHA_THRESHOLD} bytes for large-file test"
    )

    import bridge.search.sources.claude as claude_mod
    original_root = claude_mod._CLAUDE_ROOT
    claude_mod._CLAUDE_ROOT = tmp_path / 'projects'

    conn = _make_conn(tmp_path)
    src = ClaudeJsonlSource()

    r1 = asyncio.run(ingest_file(conn, src, jfile))
    assert r1.messages_added == 400

    # Append more records
    with open(jfile, 'a', encoding='utf-8') as fh:
        for i in range(10):
            fh.write(json.dumps(_user_record('appended content', uuid=f'ap{i}')) + '\n')

    r2 = asyncio.run(ingest_file(conn, src, jfile))

    claude_mod._CLAUDE_ROOT = original_root

    assert r2.rotated is False, "Append to large file must NOT trigger rotation"
    assert r2.messages_added == 10, f"Expected 10 new messages, got {r2.messages_added}"
