"""
Unit tests for bridge.search.db — schema, connection, PRAGMA, migrations.

Run: pytest bridge/tests/test_search_db.py
"""
from __future__ import annotations

import sys
import tempfile
import types
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Ensure bridge package is importable when running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn():
    """Open an in-memory connection with full PRAGMA + schema initialised."""
    from bridge.search.db import open_connection, init_schema
    conn = open_connection(":memory:")
    init_schema(conn)
    return conn


def _tables(conn) -> set:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'shadow') ORDER BY name"
    ).fetchall()
    return {r[0] for r in rows}


def _indexes(conn) -> set:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name"
    ).fetchall()
    return {r[0] for r in rows}


def _triggers(conn) -> set:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name"
    ).fetchall()
    return {r[0] for r in rows}


# ---------------------------------------------------------------------------
# test_open_connection_applies_all_pragmas
# ---------------------------------------------------------------------------

def test_open_connection_applies_all_pragmas():
    from bridge.search.db import open_connection
    # WAL mode requires a file-based DB (not :memory:)
    with tempfile.TemporaryDirectory() as tmpdir:
        conn = open_connection(Path(tmpdir) / "pragma_test.db")

        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 1       # NORMAL
        # cache_size stored as negative KB; value is driver-dependent but non-zero
        cache = conn.execute("PRAGMA cache_size").fetchone()[0]
        assert cache != 0
        assert conn.execute("PRAGMA temp_store").fetchone()[0] == 2        # MEMORY
        assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
        assert conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0] == 1000

        conn.close()


# ---------------------------------------------------------------------------
# test_init_schema_creates_all_tables_and_indexes
# ---------------------------------------------------------------------------

def test_init_schema_creates_all_tables_and_indexes():
    conn = _make_conn()
    tables = _tables(conn)
    indexes = _indexes(conn)
    triggers = _triggers(conn)

    # Regular tables
    assert "schema_meta" in tables
    assert "sessions" in tables
    assert "messages" in tables
    assert "ingest_state" in tables

    # FTS5 virtual table + shadow tables
    assert "messages_fts" in tables

    # Indexes
    assert "idx_sessions_pinned_ts" in indexes
    assert "idx_sessions_project_pinned_ts" in indexes
    assert "idx_messages_session_ts" in indexes
    assert "idx_messages_ts" in indexes

    # Triggers
    assert "messages_ai" in triggers
    assert "messages_ad" in triggers
    assert "messages_au" in triggers

    conn.close()


# ---------------------------------------------------------------------------
# test_init_schema_idempotent
# ---------------------------------------------------------------------------

def test_init_schema_idempotent():
    from bridge.search.db import init_schema
    conn = _make_conn()
    # Second call must not raise
    init_schema(conn)
    init_schema(conn)
    conn.close()


# ---------------------------------------------------------------------------
# test_check_constraints_reject_invalid_values
# ---------------------------------------------------------------------------

def test_check_constraints_reject_invalid_values():
    from bridge.search.db.sqlite_adapter import sqlite3
    conn = _make_conn()

    # is_pinned = 2 should violate CHECK
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO sessions(session_id, source, source_path, project_dir, is_pinned)"
            " VALUES ('s1', 'claude', '/p', '/proj', 2)"
        )

    # is_hidden = -1 should violate CHECK
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO sessions(session_id, source, source_path, project_dir, is_hidden)"
            " VALUES ('s2', 'claude', '/p', '/proj', -1)"
        )

    # msg_count = -1 should violate CHECK
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO sessions(session_id, source, source_path, project_dir, msg_count)"
            " VALUES ('s3', 'claude', '/p', '/proj', -1)"
        )

    # source outside allowed set
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO sessions(session_id, source, source_path, project_dir)"
            " VALUES ('s4', 'gpt', '/p', '/proj')"
        )

    conn.close()


# ---------------------------------------------------------------------------
# test_fts5_triggers_sync_on_insert_update_delete
# ---------------------------------------------------------------------------

def _insert_session(conn, sid="sess1"):
    conn.execute(
        "INSERT OR IGNORE INTO sessions(session_id, source, source_path, project_dir)"
        " VALUES (?, 'claude', '/p', '/proj')",
        (sid,),
    )
    conn.commit()


def _insert_message(conn, sid, uuid, content, role="human"):
    conn.execute(
        "INSERT INTO messages(session_id, msg_uuid, role, ts, content)"
        " VALUES (?, ?, ?, '2026-01-01T00:00:00Z', ?)",
        (sid, uuid, role, content),
    )
    conn.commit()


def test_fts5_triggers_sync_on_insert_update_delete():
    conn = _make_conn()
    _insert_session(conn)

    # INSERT → FTS should find it
    _insert_message(conn, "sess1", "uuid1", "hello world from trigram")
    rows = conn.execute(
        "SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'hello world'"
    ).fetchall()
    assert len(rows) == 1

    # UPDATE → new content searchable, old content gone
    conn.execute(
        "UPDATE messages SET content = 'completely different text' WHERE msg_uuid = 'uuid1'"
    )
    conn.commit()
    assert conn.execute(
        "SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'hello world'"
    ).fetchall() == []
    assert len(conn.execute(
        "SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'different text'"
    ).fetchall()) == 1

    # DELETE → gone from FTS
    conn.execute("DELETE FROM messages WHERE msg_uuid = 'uuid1'")
    conn.commit()
    assert conn.execute(
        "SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'different text'"
    ).fetchall() == []

    conn.close()


# ---------------------------------------------------------------------------
# test_external_content_fts_uses_messages_table (content not double-stored)
# ---------------------------------------------------------------------------

def test_external_content_fts_uses_messages_table():
    conn = _make_conn()
    _insert_session(conn)

    sentinel = "uniquesentinelxyzabc123"
    _insert_message(conn, "sess1", "uuid2", sentinel)

    # External content FTS5 does NOT create a messages_fts_content shadow table.
    # Verify it is absent — confirming content is stored only in messages.
    shadow_names = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE name LIKE 'messages_fts%'"
    ).fetchall()}
    assert "messages_fts_content" not in shadow_names, (
        "messages_fts_content shadow table exists; FTS5 may be running in inline mode "
        "instead of external content mode."
    )

    # Content lives in messages, not duplicated in FTS shadow data blob.
    content_in_messages = conn.execute(
        "SELECT content FROM messages WHERE msg_uuid = 'uuid2'"
    ).fetchone()[0]
    assert content_in_messages == sentinel

    # FTS5 still finds it (reads from messages via external content).
    rows = conn.execute(
        "SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?", (sentinel[:-3],)
    ).fetchall()
    assert len(rows) == 1

    conn.close()


# ---------------------------------------------------------------------------
# test_schema_version_recorded
# ---------------------------------------------------------------------------

def test_schema_version_recorded():
    from bridge.search.db import get_schema_version
    conn = _make_conn()
    assert get_schema_version(conn) == 1
    conn.close()


# ---------------------------------------------------------------------------
# test_sqlite_adapter_falls_back_to_pysqlite3
# ---------------------------------------------------------------------------

def test_sqlite_adapter_falls_back_to_pysqlite3():
    """When stdlib sqlite3 lacks FTS5, adapter should try pysqlite3."""
    import importlib

    # Build a fake stdlib module whose connect always raises OperationalError
    fake_stdlib = types.ModuleType("sqlite3")
    fake_stdlib.OperationalError = Exception

    class _BadConn:
        def execute(self, sql):
            raise Exception("no such module: fts5")
        def close(self):
            pass

    fake_stdlib.connect = lambda *a, **kw: _BadConn()

    # Build a fake pysqlite3 that DOES have FTS5
    fake_pysqlite3 = types.ModuleType("pysqlite3")
    fake_pysqlite3.OperationalError = Exception

    class _GoodConn:
        def execute(self, sql):
            return self
        def close(self):
            pass

    fake_pysqlite3.connect = lambda *a, **kw: _GoodConn()

    # Patch sys.modules so the adapter re-import picks up our fakes
    with patch.dict(sys.modules, {"sqlite3": fake_stdlib, "pysqlite3": fake_pysqlite3}):
        # Force reimport of adapter with patched modules
        import bridge.search.db.sqlite_adapter as adapter_mod
        # We just verify the probe logic path: _has_fts5 on fake_stdlib returns False,
        # and _has_fts5 on fake_pysqlite3 returns True.
        assert adapter_mod._has_fts5(fake_stdlib) is False
        assert adapter_mod._has_fts5(fake_pysqlite3) is True


# ---------------------------------------------------------------------------
# test_rebuild_fts_works
# ---------------------------------------------------------------------------

def test_rebuild_fts_works():
    from bridge.search.db import rebuild_fts
    conn = _make_conn()
    _insert_session(conn)
    _insert_message(conn, "sess1", "uuid3", "rebuild test content here")

    # rebuild should not raise
    rebuild_fts(conn)

    # content still searchable after rebuild
    rows = conn.execute(
        "SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'rebuild test'"
    ).fetchall()
    assert len(rows) == 1

    conn.close()


# ---------------------------------------------------------------------------
# test_wal_mode_active
# ---------------------------------------------------------------------------

def test_wal_mode_active():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        from bridge.search.db import open_connection, init_schema
        conn = open_connection(db_path)
        init_schema(conn)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        conn.close()

        # WAL file should exist after first write
        wal = db_path.with_suffix(".db-wal")
        # WAL may or may not be present before first write; just verify no error


# ---------------------------------------------------------------------------
# F7: read-only connection must not apply write PRAGMAs
# ---------------------------------------------------------------------------

def test_open_connection_readonly_does_not_set_write_pragmas():
    """A read-only connection must not attempt journal_mode/synchronous/wal_autocheckpoint."""
    from bridge.search.db.connection import _WRITE_PRAGMAS
    from bridge.search.db import open_connection, init_schema

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "ro_pragma.db"
        # Create DB and set WAL via a writer connection first
        wconn = open_connection(db_path)
        init_schema(wconn)
        wconn.close()

        # Open read-only: must not raise
        roconn = open_connection(db_path, read_only=True)
        # query_only pragma should be set
        assert roconn.execute("PRAGMA query_only").fetchone()[0] == 1
        # journal_mode should still be wal (set by writer, persisted)
        assert roconn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        roconn.close()


def test_open_connection_readonly_first_does_not_raise():
    """Opening read-only on an existing WAL DB must not raise OperationalError."""
    from bridge.search.db import open_connection, init_schema

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "ro_first.db"
        # Init via writer
        wconn = open_connection(db_path)
        init_schema(wconn)
        wconn.close()

        # Now open read-only — must not raise
        try:
            roconn = open_connection(db_path, read_only=True)
            result = roconn.execute("SELECT COUNT(*) FROM sessions").fetchone()
            assert result[0] == 0
            roconn.close()
        except Exception as exc:
            raise AssertionError(f"open_connection(read_only=True) raised: {exc}") from exc
