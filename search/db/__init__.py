"""
bridge.search.db — public API for the search SQLite database layer.

Typical usage:
    from search.db import open_connection, init_schema, get_schema_version, migrate

    conn = open_connection("/path/to/search.db")
    init_schema(conn)
    migrate(conn)
"""
from .connection import open_connection
from .schema import init_schema, rebuild_fts, SCHEMA_VERSION
from .migrations import migrate, get_schema_version

__all__ = [
    "open_connection",
    "init_schema",
    "rebuild_fts",
    "migrate",
    "get_schema_version",
    "SCHEMA_VERSION",
]
