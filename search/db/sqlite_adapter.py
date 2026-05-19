"""
Try stdlib sqlite3 first; fall back to pysqlite3 if FTS5 missing.
Re-exports the chosen module as `sqlite3`.

Per TECH_RESEARCH Q1/Q2:
- macOS Apple-shipped Python lacks FTS5.
- pysqlite3-binary provides a statically linked SQLite with FTS5.
- Linux aarch64 has no pysqlite3-binary wheel; must build from source.
"""
import sqlite3 as _stdlib


def _has_fts5(mod) -> bool:
    try:
        conn = mod.connect(":memory:")
        conn.execute("CREATE VIRTUAL TABLE _fts5_probe USING fts5(x)")
        conn.close()
        return True
    except mod.OperationalError:
        return False


if _has_fts5(_stdlib):
    sqlite3 = _stdlib
else:
    try:
        import pysqlite3 as _pysqlite3  # type: ignore[import-untyped]

        if _has_fts5(_pysqlite3):
            sqlite3 = _pysqlite3
        else:
            raise RuntimeError(
                "Both stdlib sqlite3 and pysqlite3 lack FTS5 support. "
                "On Linux aarch64, build from source: pip install pysqlite3 --no-binary :all:"
            )
    except ImportError:
        raise RuntimeError(
            "SQLite FTS5 unavailable and pysqlite3-binary is not installed. "
            "Fix: pip install pysqlite3-binary\n"
            "Linux aarch64: pip install pysqlite3 --no-binary :all:"
        )
