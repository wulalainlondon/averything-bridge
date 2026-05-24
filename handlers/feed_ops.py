"""Feed persistence — push HTML articles from Mac pipeline to mobile app."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

log = logging.getLogger(__name__)

BroadcastJson = Callable[[dict], Awaitable[int]]
SpawnTask = Callable[[str, Awaitable[Any]], Any]

# ---------------------------------------------------------------------------
# Module-level config (injected by configure() at bridge startup)
# ---------------------------------------------------------------------------
_DATA_DIR: str = ""
_FCM_TOKEN_FILE: str = ""
_broadcast_json: BroadcastJson | None = None
_spawn_task: SpawnTask | None = None

# ---------------------------------------------------------------------------
# In-memory registry (metadata only, no html)
# ---------------------------------------------------------------------------
_FEED_REGISTRY: dict[str, dict] = {}  # feed_id -> FeedMeta (no html)


def configure(
    *,
    data_dir: str,
    fcm_token_file: str,
    broadcast_json: BroadcastJson,
    spawn_task: SpawnTask,
) -> None:
    global _DATA_DIR, _FCM_TOKEN_FILE, _broadcast_json, _spawn_task
    _DATA_DIR = data_dir
    _FCM_TOKEN_FILE = fcm_token_file
    _broadcast_json = broadcast_json
    _spawn_task = spawn_task


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _feed_index_path() -> str:
    return os.path.join(_DATA_DIR, "feed", "index.json")


_VALID_CONTENT_TYPES = frozenset({"html", "markdown"})


def _feed_article_path(feed_id: str) -> str:
    return os.path.join(_DATA_DIR, "feed", "articles", f"{feed_id}.html")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _save_feed_index() -> None:
    path = _feed_index_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_FEED_REGISTRY, f, ensure_ascii=False)


def load_feed_index() -> None:
    global _FEED_REGISTRY
    path = _feed_index_path()
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            _FEED_REGISTRY = json.load(f)
    except Exception:
        _FEED_REGISTRY = {}


def feed_gc_deleted() -> None:
    """Hard-delete feed entries whose soft-deleted_at is older than 7 days."""
    cutoff = time.time() - 7 * 24 * 3600
    to_remove = [
        fid for fid, m in _FEED_REGISTRY.items()
        if m.get("deleted_at") and m["deleted_at"] < cutoff
    ]
    for fid in to_remove:
        article_path = _feed_article_path(fid)
        if os.path.exists(article_path):
            try:
                os.remove(article_path)
            except OSError as exc:
                log.warning("feed_gc: could not remove %s: %s", article_path, exc)
        del _FEED_REGISTRY[fid]
    if to_remove:
        _save_feed_index()
        log.info("feed_gc: removed %d expired entries", len(to_remove))


# ---------------------------------------------------------------------------
# FCM notification (stub — can be fleshed out later without changing callers)
# ---------------------------------------------------------------------------

async def notify_fcm_feed_new(feed_id: str, title: str) -> None:
    """Send an FCM push for a new feed article."""
    try:
        import firebase_admin
        from firebase_admin import messaging as fb_messaging
    except ImportError:
        return

    if not _FCM_TOKEN_FILE:
        return
    try:
        with open(_FCM_TOKEN_FILE) as f:
            token = f.read().strip()
    except FileNotFoundError:
        return
    if not token:
        return

    message = fb_messaging.Message(
        notification=fb_messaging.Notification(
            title="新文章",
            body=title,
        ),
        data={
            "type": "feed_new",
            "feed_id": feed_id,
            "title": title,
            "deep_link": "bridge://feed",
        },
        token=token,
    )
    loop = asyncio.get_event_loop()
    try:
        await loop.run_in_executor(None, fb_messaging.send, message)
        log.info("FCM feed_new notification sent: feed_id=%s", feed_id)
    except Exception as exc:
        log.warning("FCM feed_new notify failed: %s", exc)


# ---------------------------------------------------------------------------
# Inbound handlers
# ---------------------------------------------------------------------------

async def handle_feed_push(
    ws: Any,
    title: str,
    html: str,
    source: str = "pipeline",
    url: str = "",
    client_dedup_key: str = "",
    content_type: str = "html",
) -> None:
    if content_type not in _VALID_CONTENT_TYPES:
        content_type = "html"
    if len(html.encode("utf-8")) > 5 * 1024 * 1024:
        await ws.send(json.dumps({"type": "error", "message": "Feed article exceeds 5 MB limit"}))
        return

    if client_dedup_key:
        for m in _FEED_REGISTRY.values():
            if m.get("client_dedup_key") == client_dedup_key:
                await ws.send(json.dumps({"type": "feed_ack", "feed_id": m["feed_id"]}))
                return

    feed_id = f"feed_{uuid.uuid4().hex[:12]}"
    meta: dict = {
        "feed_id": feed_id,
        "title": title,
        "source": source,
        "url": url,
        "content_type": content_type,
        "client_dedup_key": client_dedup_key,
        "created_at": time.time(),
        "read": False,
        "deleted": False,
        "deleted_at": None,
    }
    _FEED_REGISTRY[feed_id] = meta

    article_path = _feed_article_path(feed_id)
    os.makedirs(os.path.dirname(article_path), exist_ok=True)
    with open(article_path, "w", encoding="utf-8") as f:
        f.write(html)
    _save_feed_index()

    pub_meta = {k: v for k, v in meta.items() if k != "client_dedup_key"}
    await ws.send(json.dumps({"type": "feed_ack", "feed_id": feed_id}))

    if _broadcast_json is not None and _spawn_task is not None:
        _spawn_task(
            f"broadcast-feed-new:{feed_id}",
            _broadcast_json({"type": "feed_new", "item": pub_meta}),
        )
        _spawn_task(
            f"notify-fcm-feed:{feed_id}",
            notify_fcm_feed_new(feed_id, title),
        )


async def handle_feed_list_request(ws: Any) -> None:
    feed_gc_deleted()
    items = [
        {k: v for k, v in m.items() if k != "client_dedup_key"}
        for m in sorted(
            _FEED_REGISTRY.values(),
            key=lambda x: x.get("created_at", 0),
            reverse=True,
        )
        if not m.get("deleted")
    ]
    await ws.send(json.dumps({"type": "feed_list", "items": items}))


async def handle_feed_fetch(ws: Any, feed_id: str) -> None:
    meta = _FEED_REGISTRY.get(feed_id)
    if not meta or meta.get("deleted"):
        await ws.send(json.dumps({"type": "error", "message": f"Feed item not found: {feed_id}"}))
        return
    article_path = _feed_article_path(feed_id)
    if not os.path.exists(article_path):
        await ws.send(json.dumps({"type": "error", "message": f"Feed article file missing: {feed_id}"}))
        return
    with open(article_path, "r", encoding="utf-8") as f:
        html = f.read()
    content_type = meta.get("content_type", "html")
    await ws.send(json.dumps({"type": "feed_detail", "feed_id": feed_id, "html": html, "content_type": content_type}))


async def handle_feed_mark_read(ws: Any, feed_id: str) -> None:
    meta = _FEED_REGISTRY.get(feed_id)
    if not meta:
        return
    meta["read"] = True
    _save_feed_index()
    if _broadcast_json is not None and _spawn_task is not None:
        _spawn_task(
            f"broadcast-feed-updated:{feed_id}",
            _broadcast_json({
                "type": "feed_updated",
                "feed_id": feed_id,
                "read": True,
                "deleted": False,
            }),
        )


async def handle_feed_delete(ws: Any, feed_id: str) -> None:
    meta = _FEED_REGISTRY.get(feed_id)
    if not meta:
        return
    meta["deleted"] = True
    meta["deleted_at"] = time.time()
    _save_feed_index()
    if _broadcast_json is not None and _spawn_task is not None:
        _spawn_task(
            f"broadcast-feed-updated:{feed_id}",
            _broadcast_json({
                "type": "feed_updated",
                "feed_id": feed_id,
                "read": meta.get("read", False),
                "deleted": True,
            }),
        )
