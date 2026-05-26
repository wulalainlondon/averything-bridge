from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time

from utils.path_jail import resolve_jailed, JailEscape


def _dir_hash(entries: list[dict]) -> str:
    fp = [(e["name"], e["is_dir"], e["modified"]) for e in entries]
    return hashlib.sha1(json.dumps(fp, separators=(",", ":")).encode()).hexdigest()[:16]

log = logging.getLogger(__name__)

_SESSIONS_CACHE: dict[str, tuple[float, list[dict]]] = {}
_SESSIONS_TTL_SEC = 3.0

_ENTRIES_CACHE: dict[str, tuple[float, list[dict]]] = {}
_ENTRIES_TTL_SEC = 2.0

# ---------------------------------------------------------------------------
# Global preload cache — populated at bridge startup, invalidated on session
# create/close, refreshed every _ALL_TTL seconds as a safety net.
# ---------------------------------------------------------------------------
_ALL_SESSIONS: list[dict] = []
_ALL_SESSIONS_TIME: float = 0.0
_ALL_SESSIONS_TTL = 300.0  # 5 minutes


async def preload_sessions_cache(backends: dict) -> None:
    global _ALL_SESSIONS, _ALL_SESSIONS_TIME
    rows: list[dict] = []
    for bname, backend in backends.items():
        if not backend.supports_resume():
            continue
        try:
            items = await backend.get_resumable_sessions(limit=500)
            for item in items:
                item.setdefault("backend", bname)
            rows.extend(items)
        except Exception as exc:
            log.warning("preload_sessions_cache: backend %r scan failed: %s", bname, exc)
    _ALL_SESSIONS = rows
    _ALL_SESSIONS_TIME = time.time()


def invalidate_sessions_cache() -> None:
    global _ALL_SESSIONS_TIME
    _ALL_SESSIONS_TIME = 0.0


# Directories that are never useful to browse in a file picker
_SKIP_DIRS = frozenset({
    "node_modules", ".git", ".hg", ".svn",
    "__pycache__", ".pytest_cache", ".mypy_cache", ".tox", ".ruff_cache",
    ".next", ".nuxt", ".svelte-kit", ".turbo",
    "dist", "build", "out", "target", ".gradle",
    ".venv", "venv", "env", ".env",
    ".idea", ".vscode",
    "coverage", ".nyc_output",
})

# Files/dirs whose names start with these prefixes are hidden by default
_SKIP_PREFIXES = (".", "~")

_MAX_ENTRIES = 500


def _list_entries_cached(path: str) -> list[dict]:
    now = time.time()
    cached = _ENTRIES_CACHE.get(path)
    if cached and cached[0] > now:
        return cached[1]
    entries = _list_entries(path)
    _ENTRIES_CACHE[path] = (now + _ENTRIES_TTL_SEC, entries)
    return entries


def _list_entries(path: str) -> list[dict]:
    entries: list[dict] = []
    if os.path.isdir(path):
        try:
            for entry in os.scandir(path):
                name = entry.name
                # Skip known noisy dirs
                if entry.is_dir(follow_symlinks=False) and name in _SKIP_DIRS:
                    continue
                # Skip hidden files/dirs (dotfiles, temp)
                if name.startswith(_SKIP_PREFIXES):
                    continue
                try:
                    stat = entry.stat(follow_symlinks=False)
                    entries.append({
                        "name": name,
                        "is_dir": entry.is_dir(follow_symlinks=True),
                        "size": stat.st_size,
                        "modified": int(stat.st_mtime),
                    })
                except Exception as exc:
                    log.debug("_list_entries: stat failed for %r: %s", entry.path, exc)
                if len(entries) >= _MAX_ENTRIES:
                    break
        except PermissionError as exc:
            log.warning("_list_entries: permission denied scanning %r: %s", path, exc)
    entries.sort(key=lambda e: (not e["is_dir"], e["name"].lower()))
    return entries


def _active_sessions_for_path(path: str, sessions: dict) -> list[dict]:
    items: list[dict] = []
    for sid, s in list(sessions.items()):
        try:
            if os.path.realpath(s.cwd) == path:
                items.append({
                    "id": sid,
                    "name": s.name,
                    "claude_uuid": s.resume_id or "",
                    "last_used": int(s.last_activity or s.created_at),
                    "backend": s.backend_name,
                    "is_active": True,
                })
        except Exception as exc:
            log.debug("_active_sessions_for_path: session %r skipped: %s", sid, exc)
    return items


async def _resumable_for_path(path: str, backends: dict, active_uuids: set[str]) -> list[dict]:
    now = time.time()

    # Fast path: use global preload cache when warm.
    if _ALL_SESSIONS_TIME > 0 and now - _ALL_SESSIONS_TIME < _ALL_SESSIONS_TTL:
        return [
            {
                "id": r["id"],
                "name": r["name"],
                "claude_uuid": r["claude_uuid"],
                "last_used": r["last_used"],
                "backend": r.get("backend", ""),
                "is_active": False,
            }
            for r in _ALL_SESSIONS
            if os.path.realpath(r.get("cwd", "")) == path
            and r.get("claude_uuid") not in active_uuids
        ]

    # Slow path: per-path cache then direct scan (used before preload finishes).
    cached = _SESSIONS_CACHE.get(path)
    if cached and cached[0] > now:
        rows = cached[1]
        return [r for r in rows if r.get("claude_uuid") not in active_uuids]

    rows: list[dict] = []
    for bname, backend in backends.items():
        if not backend.supports_resume():
            continue
        try:
            resumable = await backend.get_resumable_sessions()
        except Exception as exc:
            log.warning("_resumable_for_path: backend %r load failed: %s", bname, exc)
            continue
        for r in resumable:
            try:
                if os.path.realpath(r["cwd"]) == path and r["claude_uuid"] not in active_uuids:
                    rows.append({
                        "id": r["id"],
                        "name": r["name"],
                        "claude_uuid": r["claude_uuid"],
                        "last_used": r["last_used"],
                        "backend": bname,
                        "is_active": False,
                    })
            except Exception as exc:
                log.debug("_resumable_for_path: malformed session record skipped: %s", exc)

    _SESSIONS_CACHE[path] = (now + _SESSIONS_TTL_SEC, rows)
    return rows


async def handle_file_msg(mtype: str, msg: dict, ws, ctx: dict) -> bool:
    if mtype == "browse_dir":
        req_path = msg.get("path") or "~"
        root_dir = ctx.get("root_dir", "")
        try:
            path = resolve_jailed(req_path, root_dir)
        except JailEscape as e:
            try:
                await ws.send(json.dumps({"type": "error", "text": f"Path outside instance root: {req_path}"}))
            except Exception:
                pass
            log.warning("[jail] browse_dir escape: req=%r resolved=%r root=%r", e.req_path, e.resolved, e.root_dir)
            return True
        entries = _list_entries_cached(path)
        current_hash = _dir_hash(entries)
        client_hash = msg.get("client_hash", "")
        unchanged = bool(client_hash) and client_hash == current_hash

        active_items = _active_sessions_for_path(path, ctx["sessions"])

        def _build(send_entries: list[dict], sessions: list[dict]) -> str:
            payload = ctx["msg_dir_listing"](path, send_entries, sessions)
            payload["hash"] = current_hash
            payload["unchanged"] = unchanged
            return json.dumps(payload)

        # Stage 1: return filesystem + active sessions quickly.
        try:
            await ws.send(_build([] if unchanged else entries, active_items))
        except Exception as exc:
            log.warning("browse_dir: WS send (stage 1) failed: %s", exc)
            return True

        # Stage 2: enrich with resumable sessions (cached).
        active_uuids = {s.resume_id for s in ctx["sessions"].values() if s.resume_id}
        resumable = await _resumable_for_path(path, ctx["backends"], active_uuids)
        merged = active_items + resumable
        try:
            await ws.send(_build([] if unchanged else entries, merged))
        except Exception as exc:
            log.warning("browse_dir: WS send (stage 2) failed: %s", exc)
        return True

    if mtype == "fcm_token":
        token = msg.get("token", "").strip()
        if token:
            try:
                with open(ctx["fcm_token_file"], "w") as f:
                    f.write(token)
                ctx["log"].info("FCM token registered: %s…", token[:20])
            except Exception as exc:
                ctx["log"].warning("Failed to save FCM token: %s", exc)
            tunnel_url = ctx.get("get_tunnel_url", lambda: None)()
            if tunnel_url and not ctx.get("is_tunnel_delivered", lambda: True)():
                notify_fn = ctx.get("notify_tunnel_fcm_once")
                if notify_fn:
                    asyncio.ensure_future(notify_fn(tunnel_url))
                    ctx["log"].info("FCM token arrived with pending tunnel URL — resending immediately")
        return True

    return False
