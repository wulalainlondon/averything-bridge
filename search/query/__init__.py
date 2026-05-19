"""
bridge.search.query — public API for the query layer.

Usage:
    from search.query import search, health, list_sessions, ConnectionPool
"""
from __future__ import annotations

from .connection_pool import ConnectionPool
from .search import search, SearchHit, SearchFilters, SearchResponse
from .health import health, SearchHealth
from .sessions import list_sessions, SessionListItem, SessionListPage
from .context import get_search_context, ContextMessage, SearchContextResponse

__all__ = [
    "ConnectionPool",
    "search",
    "SearchHit",
    "SearchFilters",
    "SearchResponse",
    "health",
    "SearchHealth",
    "list_sessions",
    "SessionListItem",
    "SessionListPage",
    "get_search_context",
    "ContextMessage",
    "SearchContextResponse",
]
