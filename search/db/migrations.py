"""
Migration framework for bridge search DB.

v1 is the bootstrap schema (handled by init_schema / DDL directly).
Future column additions go here as new Migration entries.

Usage:
    conn = open_connection(db_path)
    init_schema(conn)        # bootstrap only — creates tables if absent
    migrate(conn)            # apply any pending migrations above current version
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class Migration:
    version: int
    description: str
    sql: str


# v1 is the initial schema; init_schema() covers it.
# Add future migrations here in ascending version order:
#   Migration(version=2, description="add foo column", sql="ALTER TABLE messages ADD COLUMN foo TEXT")
MIGRATIONS: List[Migration] = []


def get_schema_version(conn) -> int:
    """Read schema_version from schema_meta. Returns 0 if table/row absent."""
    try:
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def _set_schema_version(conn, version: int) -> None:
    conn.execute(
        "INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?)"
        " ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (str(version),),
    )


def migrate(conn) -> int:
    """Apply all pending migrations above current schema version.

    Returns the new (post-migration) schema version.
    Each migration runs in its own transaction; on failure the exception
    propagates and subsequent migrations are skipped.
    """
    current = get_schema_version(conn)

    pending = [m for m in MIGRATIONS if m.version > current]
    if not pending:
        return current

    for migration in pending:
        conn.execute("BEGIN")
        try:
            conn.executescript(migration.sql)
            _set_schema_version(conn, migration.version)
            conn.execute("COMMIT")
            current = migration.version
        except Exception:
            conn.execute("ROLLBACK")
            raise

    return current
