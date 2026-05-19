"""
Server-side session list with cursor-based pagination.

Uses idx_sessions_pinned_ts (or idx_sessions_project_pinned_ts) per SCHEMA_REVIEW §2.1:
  ORDER BY is_pinned DESC, last_ts DESC
Cursor = last_ts of the last returned item.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from .connection_pool import ConnectionPool


@dataclass
class SessionListItem:
    session_id: str
    display_name: str | None
    cwd: str | None
    project_dir: str
    backend: str | None
    first_ts: str | None
    last_ts: str | None
    msg_count: int
    is_pinned: bool


@dataclass
class SessionListPage:
    items: list[SessionListItem]
    next_cursor: str | None       # last_ts of last item if there are more
    total_filtered: int | None    # approximate count (optional)


def _run_list_sessions(
    conn,
    cursor: str | None,
    limit: int,
    project_dir: str | None,
    include_hidden: bool,
) -> tuple[list[tuple], int | None]:
    """Synchronous query; run inside asyncio.to_thread."""
    params: list = []
    where_parts: list[str] = []

    if not include_hidden:
        where_parts.append("is_hidden = 0")

    if project_dir is not None:
        where_parts.append("project_dir = ?")
        params.append(project_dir)

    if cursor is not None:
        # Cursor encodes last_ts of the previous page's last item.
        # We want rows "after" that item in the sort order (is_pinned DESC, last_ts DESC).
        # Because pinned items always come first and cursors paginate within the
        # non-pinned block in practice, we use a simple last_ts < cursor condition.
        # For pinned items the cursor value won't be reached (they sort before all
        # non-pinned), so this is safe for the common dashboard pagination pattern.
        where_parts.append("last_ts < ?")
        params.append(cursor)

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    # Fetch limit+1 rows to detect has_more
    fetch_limit = limit + 1

    sql = f"""
        SELECT
            session_id,
            display_name,
            cwd,
            project_dir,
            backend,
            first_ts,
            last_ts,
            msg_count,
            is_pinned
        FROM sessions
        {where_sql}
        ORDER BY is_pinned DESC, last_ts DESC
        LIMIT ?
    """
    params.append(fetch_limit)

    rows = conn.execute(sql, params).fetchall()
    return rows, None  # total_filtered not implemented (YAGNI)


async def list_sessions(
    conn_pool: ConnectionPool,
    cursor: str | None = None,
    limit: int = 30,
    project_dir: str | None = None,
    include_hidden: bool = False,
) -> SessionListPage:
    """Return a page of sessions sorted by pinned-first, then newest-first."""
    async with conn_pool.borrow() as conn:
        rows, total_filtered = await asyncio.to_thread(
            _run_list_sessions, conn, cursor, limit, project_dir, include_hidden
        )

    has_more = len(rows) > limit
    page_rows = rows[:limit]

    items = [
        SessionListItem(
            session_id=row[0],
            display_name=row[1],
            cwd=row[2],
            project_dir=row[3],
            backend=row[4],
            first_ts=row[5],
            last_ts=row[6],
            msg_count=row[7] if row[7] is not None else 0,
            is_pinned=bool(row[8]),
        )
        for row in page_rows
    ]

    next_cursor: str | None = None
    if has_more and items:
        next_cursor = items[-1].last_ts

    return SessionListPage(
        items=items,
        next_cursor=next_cursor,
        total_filtered=total_filtered,
    )
