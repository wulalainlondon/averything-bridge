"""
Full-text search query over the messages_fts FTS5 table.

Per SCHEMA_REVIEW §3:
- snippet() column index = 0 (external content has one indexable column)
- Post-MATCH filters apply after LIMIT; caller receives up to `limit` pre-filter hits
- JOIN on PK is fine; planner picks top-N from FTS5 first

Per TECH_RESEARCH Q4:
- trigram: queries < 3 chars do not match; warn on short pure-ASCII tokens
"""
from __future__ import annotations

import asyncio
import re
import sqlite3
import time
from dataclasses import dataclass, field

from .connection_pool import ConnectionPool


@dataclass
class SearchHit:
    session_id: str
    session_display_name: str | None
    cwd: str | None
    project_dir: str
    backend: str | None
    session_last_ts: str | None
    session_msg_count: int
    msg_uuid: str
    role: str
    msg_ts: str
    snippet: str
    rank: float


@dataclass
class SearchFilters:
    project_dir: str | None = None
    since: str | None = None          # ISO8601 string
    role: str | None = None           # 'user' | 'assistant'
    exclude_subagents: bool = False
    source: str | None = None         # 'claude' | 'codex' | 'ollama'
    max_per_session: int = 3          # cap hits returned per session (diversity)


@dataclass
class SearchResponse:
    """Result of a search() call.

    Fields:
        returned_count: Number of hits returned in this page.  This is NOT the
            absolute count of all matching messages — it equals len(hits) and is
            bounded by the requested limit.  No full COUNT(*) is performed.
        total: Deprecated alias for returned_count kept for backwards
            compatibility with downstream clients.  Will be removed in a future
            version.  Both fields always carry the same value.
    """

    query: str
    hits: list[SearchHit]
    returned_count: int
    total: int          # deprecated alias — equals returned_count
    warnings: list[str]
    elapsed_ms: float


# Maximum allowed query length
_MAX_QUERY_LEN = 200

# FTS5 special characters that need to be escaped / stripped from bare tokens
_FTS5_SPECIAL = re.compile(r'[\'";]')

# Detect pure-ASCII tokens shorter than 3 chars (trigram won't match them)
_SHORT_ASCII_TOKEN = re.compile(r'\b([A-Za-z]{1,2})\b')

# FTS5 logical keywords — preserved as-is in the match expression
_FTS5_KEYWORDS = frozenset({"AND", "OR", "NOT"})


def _build_fts_match(user_query: str) -> str:
    """
    Convert a user query into an FTS5 MATCH expression.

    Rules:
    - Tokens separated by whitespace get implicit AND (each wrapped in double-quotes).
    - OR / NOT (uppercase) keywords are preserved verbatim.
    - Double-quoted phrases from the user are kept as-is.
    - Dangerous characters (', ;) inside bare tokens are stripped.
    """
    # Split while preserving double-quoted phrases
    # Pattern: quoted string OR non-whitespace run
    token_re = re.compile(r'"[^"]*"|\S+')
    tokens = token_re.findall(user_query.strip())

    parts: list[str] = []
    for tok in tokens:
        if tok in _FTS5_KEYWORDS:
            # NOT requires a left operand in FTS5; skip leading NOT/OR/AND tokens
            # that would produce a syntax error (e.g. bare "NOT hello" or "OR NOT x").
            if parts:  # only keep operator if there is already a left operand
                parts.append(tok)
            # else: leading operator — silently drop it
        elif tok.startswith('"') and tok.endswith('"') and len(tok) >= 2:
            # User-supplied phrase — keep as-is (content inside is just text)
            parts.append(tok)
        else:
            # Bare token — strip dangerous chars, wrap in double-quotes
            clean = _FTS5_SPECIAL.sub("", tok)
            if clean:
                parts.append(f'"{clean}"')

    # Strip any trailing operator token that has no right operand
    while parts and parts[-1] in _FTS5_KEYWORDS:
        parts.pop()

    return " ".join(parts)


def _collect_warnings(query: str) -> list[str]:
    """Return warning strings for query patterns that trigram cannot handle."""
    warnings: list[str] = []
    # Look for bare (non-quoted) pure-ASCII tokens of length 1 or 2
    # Remove quoted sections first
    bare = re.sub(r'"[^"]*"', " ", query)
    short_tokens = _SHORT_ASCII_TOKEN.findall(bare)
    # Exclude FTS5 keywords
    short_tokens = [t for t in short_tokens if t.upper() not in _FTS5_KEYWORDS]
    if short_tokens:
        joined = ", ".join(repr(t) for t in short_tokens)
        warnings.append(
            f"Short ASCII token(s) {joined} are < 3 chars and cannot be matched "
            "by the trigram index — they will produce no results."
        )
    return warnings


# CJK Unified Ideographs + CJK Extension A/B + Kana (hiragana/katakana) + Hangul
_CJK_RE = re.compile(
    r'[㐀-䶿'        # CJK Extension A
    r'一-鿿'         # CJK Unified Ideographs
    r'豈-﫿'         # CJK Compatibility Ideographs
    r'぀-ヿ'         # Hiragana + Katakana
    r'가-힯]+'       # Hangul syllables
)


def _short_cjk_tokens(query: str) -> list[str]:
    """Return 1-2 char CJK tokens that the trigram index cannot match."""
    bare = re.sub(r'"[^"]*"', " ", query)
    return [t for t in _CJK_RE.findall(bare) if 1 <= len(t) <= 2]


def _inject_highlights(text: str, tokens: list[str]) -> str:
    """Wrap each token occurrence in <<token>> highlight markers."""
    for tok in tokens:
        text = text.replace(tok, f'<<{tok}>>')
    return text


def _run_like_fallback(
    conn,
    tokens: list[str],
    filters: "SearchFilters",
    limit: int,
    offset: int,
) -> list[tuple]:
    """
    LIKE-based full scan for short CJK tokens that FTS5 trigram cannot handle.

    Returns rows in the same 12-column shape as _run_search so the caller
    can build SearchHit objects identically.
    Columns: session_id, display_name, cwd, project_dir, backend,
             last_ts, msg_count, msg_uuid, role, ts, snippet, rank

    Per-session cap: at most filters.max_per_session rows are returned per
    session_id (applied in Python after SQL fetch), promoting result diversity.
    """
    max_per_session: int = max(1, filters.max_per_session)

    # Build WHERE clause: one LIKE per token (AND-ed together)
    like_clauses = " AND ".join("m.content LIKE ?" for _ in tokens)
    like_params: list = [f"%{tok}%" for tok in tokens]

    extra_where = ""
    extra_params: list = []
    if filters.since is not None:
        extra_where += " AND m.ts >= ?"
        extra_params.append(filters.since)

    internal_limit = min((offset + limit) * max_per_session * 10, 2000)

    sql = f"""
        SELECT
            s.session_id,
            s.display_name,
            s.cwd,
            s.project_dir,
            s.backend,
            s.last_ts,
            s.msg_count,
            m.msg_uuid,
            m.role,
            m.ts,
            m.content,
            0 AS rank,
            m.is_subagent,
            s.source
        FROM messages m
        JOIN sessions s ON s.session_id = m.session_id
        WHERE {like_clauses}{extra_where}
        ORDER BY m.ts DESC
        LIMIT ?
    """

    rows = conn.execute(sql, like_params + extra_params + [internal_limit]).fetchall()

    # Python-side post-filters (mirrors _run_search)
    if filters.project_dir is not None:
        rows = [r for r in rows if r[3] == filters.project_dir]
    if filters.role is not None:
        rows = [r for r in rows if r[8] == filters.role]
    if filters.exclude_subagents:
        rows = [r for r in rows if r[12] == 0]
    if filters.source is not None:
        rows = [r for r in rows if r[13] == filters.source]

    # Per-session cap: keep at most max_per_session rows per session_id.
    # Rows arrive ordered by ts DESC so the most-recent messages are preferred.
    per_session_count: dict[str, int] = {}
    capped: list = []
    for r in rows:
        sid = r[0]
        count = per_session_count.get(sid, 0)
        if count < max_per_session:
            capped.append(r)
            per_session_count[sid] = count + 1

    sliced = capped[offset:offset + limit]

    # Build snippet: extract ~80 chars around the first token hit, add highlights
    result_rows = []
    for r in sliced:
        content: str = r[10] or ""
        first_tok = tokens[0]
        pos = content.find(first_tok)
        if pos >= 0:
            start = max(0, pos - 24)
            raw_snippet = content[start:start + 80]
        else:
            raw_snippet = content[:80]
        snippet = _inject_highlights(raw_snippet, tokens)
        # Return 12-column shape (replace content col with snippet, drop extra cols)
        result_rows.append((r[0], r[1], r[2], r[3], r[4], r[5], r[6],
                            r[7], r[8], r[9], snippet, r[11]))

    return result_rows


def _run_search(
    conn,
    fts_query: str,
    filters: SearchFilters,
    limit: int,
    offset: int,
) -> list[tuple]:
    """Synchronous SQLite query; intended to run inside asyncio.to_thread.

    FTS5 + post-MATCH filter interaction (SCHEMA_REVIEW §3.2):
    SQL OFFSET skips pre-filter rows, so pages can drop valid post-filter hits.
    Fix: fetch min((offset+limit)*N, 2000) rows from FTS5 internally, apply all
    post-MATCH filters (project_dir, role, is_subagent, source) in Python,
    then slice [offset:offset+limit].

    Per-session cap (diversity):
    SQLite 3.25+ window functions are used to rank rows within each session by
    bm25 score.  Only the top max_per_session rows per session_id survive into
    the final result set.  This prevents a single high-density session from
    monopolising all top-N positions.

    The SELECT always includes m.is_subagent (col 12) and s.source (col 13) for
    Python-side filtering; caller receives only the first 12 columns.
    """
    max_per_session: int = max(1, filters.max_per_session)

    params: list = [fts_query]
    where_clauses = ["messages_fts MATCH ?"]

    # since-filter applies uniformly across all rows so it is safe in SQL.
    if filters.since is not None:
        where_clauses.append("m.ts >= ?")
        params.append(filters.since)

    where_sql = " AND ".join(where_clauses)

    # Fetch enough pre-filter rows so that post-filter slicing yields a full page
    # even after the per-session cap.  Generous factor ensures diverse sessions
    # are included in the inner scan.
    internal_limit = min((offset + limit) * max_per_session * 10, 2000)

    # ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY bm25) assigns a rank
    # within each session (lower bm25 = better match in FTS5 convention).
    # Filtering per_sess_rank <= max_per_session caps hits per session.
    sql = f"""
        WITH _inner AS (
            SELECT
                s.session_id,
                s.display_name,
                s.cwd,
                s.project_dir,
                s.backend,
                s.last_ts,
                s.msg_count,
                m.msg_uuid,
                m.role,
                m.ts,
                snippet(messages_fts, 0, '<<', '>>', '…', 24) AS snippet,
                bm25(messages_fts) AS rank,
                m.is_subagent,
                s.source
            FROM messages_fts
            JOIN messages m ON m.rowid = messages_fts.rowid
            JOIN sessions s ON s.session_id = m.session_id
            WHERE {where_sql}
            ORDER BY rank
            LIMIT ?
        ),
        _ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY rank) AS per_sess_rank
            FROM _inner
        )
        SELECT
            session_id, display_name, cwd, project_dir, backend,
            last_ts, msg_count, msg_uuid, role, ts, snippet, rank,
            is_subagent, source
        FROM _ranked
        WHERE per_sess_rank <= ?
        ORDER BY rank
    """
    params.append(internal_limit)
    params.append(max_per_session)

    rows = conn.execute(sql, params).fetchall()

    # Apply post-MATCH filters in Python to avoid the OFFSET skipping bug.
    if filters.project_dir is not None:
        rows = [r for r in rows if r[3] == filters.project_dir]
    if filters.role is not None:
        rows = [r for r in rows if r[8] == filters.role]
    if filters.exclude_subagents:
        rows = [r for r in rows if r[12] == 0]
    if filters.source is not None:
        rows = [r for r in rows if r[13] == filters.source]

    # Python-side pagination slice; trim extended columns back to 12-column shape.
    return [r[:12] for r in rows[offset:offset + limit]]


async def search(
    conn_pool: ConnectionPool,
    query: str,
    filters: SearchFilters | None = None,
    limit: int = 50,
    offset: int = 0,
) -> SearchResponse:
    """
    Execute a full-text search.

    Returns empty SearchResponse (with warnings) on blank or oversize query.
    """
    t0 = time.monotonic()

    if filters is None:
        filters = SearchFilters()

    warnings: list[str] = []

    # Sanitize: blank query
    stripped = query.strip()
    if not stripped:
        return SearchResponse(
            query=query,
            hits=[],
            returned_count=0,
            total=0,
            warnings=["Query is empty."],
            elapsed_ms=0.0,
        )

    # Sanitize: query too long
    if len(stripped) > _MAX_QUERY_LEN:
        return SearchResponse(
            query=query,
            hits=[],
            returned_count=0,
            total=0,
            warnings=[f"Query exceeds maximum length of {_MAX_QUERY_LEN} characters."],
            elapsed_ms=0.0,
        )

    # Detect short CJK tokens that trigram index cannot match — use LIKE fallback
    short_cjk = _short_cjk_tokens(stripped)
    if short_cjk:
        joined = ", ".join(repr(t) for t in short_cjk)
        warnings.append(
            f"Short CJK token(s) {joined} use LIKE fallback (slower, no ranking)."
        )
        try:
            async with conn_pool.borrow() as conn:
                rows = await asyncio.to_thread(
                    _run_like_fallback, conn, short_cjk, filters, limit, offset
                )
        except sqlite3.OperationalError as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000
            return SearchResponse(
                query=query,
                hits=[],
                returned_count=0,
                total=0,
                warnings=warnings + [f"Invalid query syntax: {exc}"],
                elapsed_ms=elapsed_ms,
            )

        hits = [
            SearchHit(
                session_id=row[0],
                session_display_name=row[1],
                cwd=row[2],
                project_dir=row[3],
                backend=row[4],
                session_last_ts=row[5],
                session_msg_count=row[6] if row[6] is not None else 0,
                msg_uuid=row[7],
                role=row[8],
                msg_ts=row[9],
                snippet=row[10] or "",
                rank=row[11] if row[11] is not None else 0.0,
            )
            for row in rows
        ]
        elapsed_ms = (time.monotonic() - t0) * 1000
        n = len(hits)
        return SearchResponse(
            query=query,
            hits=hits,
            returned_count=n,
            total=n,
            warnings=warnings,
            elapsed_ms=elapsed_ms,
        )

    # Collect trigram warnings before building the FTS expression
    warnings.extend(_collect_warnings(stripped))

    fts_query = _build_fts_match(stripped)

    try:
        async with conn_pool.borrow() as conn:
            rows = await asyncio.to_thread(
                _run_search, conn, fts_query, filters, limit, offset
            )
    except sqlite3.OperationalError as exc:
        elapsed_ms = (time.monotonic() - t0) * 1000
        return SearchResponse(
            query=query,
            hits=[],
            returned_count=0,
            total=0,
            warnings=warnings + [f"Invalid query syntax: {exc}"],
            elapsed_ms=elapsed_ms,
        )

    hits = [
        SearchHit(
            session_id=row[0],
            session_display_name=row[1],
            cwd=row[2],
            project_dir=row[3],
            backend=row[4],
            session_last_ts=row[5],
            session_msg_count=row[6] if row[6] is not None else 0,
            msg_uuid=row[7],
            role=row[8],
            msg_ts=row[9],
            snippet=row[10] or "",
            rank=row[11] if row[11] is not None else 0.0,
        )
        for row in rows
    ]

    elapsed_ms = (time.monotonic() - t0) * 1000

    # F9 warning: when the caller uses offset>0 AND any post-MATCH filter is active,
    # pagination may still miss some results if the internal_limit cap (200) was hit.
    has_post_filter = any([
        filters.project_dir is not None,
        filters.role is not None,
        filters.exclude_subagents,
        filters.source is not None,
    ])
    if offset > 0 and has_post_filter:
        warnings.append(
            "pagination may skip results due to FTS5 + filter interaction"
        )

    n = len(hits)
    return SearchResponse(
        query=query,
        hits=hits,
        returned_count=n,
        total=n,        # deprecated alias — equals returned_count
        warnings=warnings,
        elapsed_ms=elapsed_ms,
    )
