"""
WebSocket message handler for search-related message types.

Dispatches:
  'request_search'         → search()  → {'type': 'search_result', ...}
  'request_search_health'  → health()  → {'type': 'search_health', ...}
  'request_session_list'   → list_sessions() → {'type': 'session_list', ...}

On any exception: {'type': '<original_type>_error', 'message': str(e)}

Does NOT modify bridge_v2.py. Expose handle_search_message() for future wiring.
"""
from __future__ import annotations

import dataclasses
import logging

from search.query import (
    ConnectionPool,
    search,
    health,
    list_sessions,
    SearchFilters,
    get_search_context,
)

log = logging.getLogger(__name__)

_HANDLED_TYPES = frozenset({
    "request_search",
    "request_search_health",
    "request_session_list",
    "request_search_context",
})


def _to_dict(obj) -> dict:
    """Recursively convert a dataclass (possibly nested) to a plain dict."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _to_dict(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, list):
        return [_to_dict(i) for i in obj]
    return obj


async def handle_search_message(
    ws,
    msg: dict,
    *,
    pool: ConnectionPool,
) -> None:
    """
    Inspect msg['type'] and dispatch to the appropriate query function.

    ws must support an async send_json(dict) method (websockets library) or
    equivalent. Falls back to ws.send(json.dumps(dict)) if send_json absent.
    """
    msg_type: str = msg.get("type", "")

    async def send(payload: dict) -> None:
        if hasattr(ws, "send_json"):
            await ws.send_json(payload)
        else:
            import json
            await ws.send(json.dumps(payload))

    if msg_type not in _HANDLED_TYPES:
        await send({
            "type": "unknown_type_error",
            "message": f"Unrecognised message type: {msg_type!r}",
        })
        return

    try:
        if msg_type == "request_search":
            query_str: str = msg.get("query", "")
            limit: int = int(msg.get("limit", 50))
            offset: int = int(msg.get("offset", 0))

            raw_filters = msg.get("filters") or {}
            filters = SearchFilters(
                project_dir=raw_filters.get("project_dir"),
                since=raw_filters.get("since"),
                role=raw_filters.get("role"),
                exclude_subagents=bool(raw_filters.get("exclude_subagents", False)),
                source=raw_filters.get("source"),
                max_per_session=int(raw_filters.get("max_per_session", 3)),
            )

            result = await search(pool, query_str, filters=filters, limit=limit, offset=offset)
            await send({"type": "search_result", **_to_dict(result)})

        elif msg_type == "request_search_health":
            result = await health(pool)
            await send({"type": "search_health", **_to_dict(result)})

        elif msg_type == "request_session_list":
            cursor: str | None = msg.get("cursor")
            limit = int(msg.get("limit", 30))
            project_dir: str | None = msg.get("project_dir")
            include_hidden: bool = bool(msg.get("include_hidden", False))

            result = await list_sessions(
                pool,
                cursor=cursor,
                limit=limit,
                project_dir=project_dir,
                include_hidden=include_hidden,
            )
            await send({"type": "session_list", **_to_dict(result)})

        elif msg_type == "request_search_context":
            ctx_session_id: str = msg.get("session_id", "")
            ctx_msg_uuid: str = msg.get("msg_uuid", "")
            ctx_around: int = max(1, min(30, int(msg.get("around", 10))))

            result = await get_search_context(pool, ctx_session_id, ctx_msg_uuid, ctx_around)
            await send({"type": "search_context", **_to_dict(result)})

    except Exception as exc:
        log.exception("handle_search_message: error handling %r", msg_type)
        await send({
            "type": f"{msg_type}_error",
            "message": str(exc),
        })
