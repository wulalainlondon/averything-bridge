"""
DDL constants and init_schema().

DDL is 1:1 aligned with SCHEMA_REVIEW.md:
  §1.1  external content FTS5 (messages + messages_fts)
  §1.3  FTS5 options: columnsize=0, detail=full, trigram remove_diacritics 0
  §2.1  composite indexes on sessions
  §2.3  CHECK constraints on sessions
  §4.3  CHECK constraints on ingest_state
  §6.3  schema_meta version table with ON CONFLICT DO UPDATE
"""

SCHEMA_VERSION = 1

DDL = """
CREATE TABLE IF NOT EXISTS schema_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
  session_id      TEXT PRIMARY KEY,
  source          TEXT NOT NULL CHECK (source IN ('claude', 'codex', 'ollama')),
  source_path     TEXT NOT NULL,
  project_dir     TEXT NOT NULL,
  cwd             TEXT,
  display_name    TEXT,
  first_ts        TEXT,
  last_ts         TEXT,
  msg_count       INTEGER NOT NULL DEFAULT 0 CHECK (msg_count >= 0),
  backend         TEXT,
  is_pinned       INTEGER NOT NULL DEFAULT 0 CHECK (is_pinned IN (0, 1)),
  is_hidden       INTEGER NOT NULL DEFAULT 0 CHECK (is_hidden IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_sessions_pinned_ts
  ON sessions(is_hidden, is_pinned DESC, last_ts DESC);

CREATE INDEX IF NOT EXISTS idx_sessions_project_pinned_ts
  ON sessions(is_hidden, project_dir, is_pinned DESC, last_ts DESC);

CREATE TABLE IF NOT EXISTS messages (
  rowid           INTEGER PRIMARY KEY,
  session_id      TEXT NOT NULL,
  msg_uuid        TEXT NOT NULL,
  parent_uuid     TEXT,
  role            TEXT NOT NULL,
  ts              TEXT NOT NULL,
  is_subagent     INTEGER NOT NULL DEFAULT 0 CHECK (is_subagent IN (0, 1)),
  content         TEXT NOT NULL,
  UNIQUE(session_id, msg_uuid)
);

CREATE INDEX IF NOT EXISTS idx_messages_session_ts
  ON messages(session_id, ts);
CREATE INDEX IF NOT EXISTS idx_messages_ts
  ON messages(ts);

CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
  content,
  content='messages',
  content_rowid='rowid',
  tokenize='trigram remove_diacritics 0',
  columnsize=0,
  detail=full
);

CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
  INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
END;
CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
  INSERT INTO messages_fts(messages_fts, rowid, content) VALUES('delete', old.rowid, old.content);
END;
CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
  INSERT INTO messages_fts(messages_fts, rowid, content) VALUES('delete', old.rowid, old.content);
  INSERT INTO messages_fts(rowid, content) VALUES (new.rowid, new.content);
END;

CREATE TABLE IF NOT EXISTS ingest_state (
  source_path     TEXT PRIMARY KEY,
  file_size       INTEGER NOT NULL,
  last_mtime      REAL NOT NULL,
  last_offset     INTEGER NOT NULL,
  head_sha256     TEXT,
  last_ingest_at  REAL NOT NULL,
  msg_extracted   INTEGER NOT NULL DEFAULT 0 CHECK (msg_extracted >= 0),
  errors          INTEGER NOT NULL DEFAULT 0 CHECK (errors >= 0)
);

INSERT INTO schema_meta(key, value) VALUES ('schema_version', '1')
ON CONFLICT(key) DO UPDATE SET value = excluded.value;
"""


def init_schema(conn) -> None:
    """Execute full DDL against conn. Safe to call multiple times (idempotent)."""
    conn.executescript(DDL)


def rebuild_fts(conn) -> None:
    """Rebuild external content FTS5 index from messages table.

    Use after bulk inserts that bypass triggers, or after FTS5 desync.
    Per SCHEMA_REVIEW §6.2: ~5–15 s for 26 MB corpus.
    """
    conn.execute("INSERT INTO messages_fts(messages_fts) VALUES('rebuild')")
    conn.commit()
