from __future__ import annotations

import json
import os
import time

_SESSIONS_CACHE: dict[str, tuple[float, list[dict]]] = {}
_SESSIONS_TTL_SEC = 3.0

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
        except Exception:
            pass
    _ALL_SESSIONS = rows
    _ALL_SESSIONS_TIME = time.time()


def invalidate_sessions_cache() -> None:
    global _ALL_SESSIONS_TIME
    _ALL_SESSIONS_TIME = 0.0


def _list_entries(path: str) -> list[dict]:
    entries: list[dict] = []
    if os.path.isdir(path):
        try:
            for entry in os.scandir(path):
                try:
                    stat = entry.stat(follow_symlinks=False)
                    entries.append({
                        "name": entry.name,
                        "is_dir": entry.is_dir(follow_symlinks=True),
                        "size": stat.st_size,
                        "modified": int(stat.st_mtime),
                    })
                except Exception:
                    pass
        except PermissionError:
            pass
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
        except Exception:
            pass
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
        except Exception:
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
            except Exception:
                pass

    _SESSIONS_CACHE[path] = (now + _SESSIONS_TTL_SEC, rows)
    return rows


async def handle_file_msg(mtype: str, msg: dict, ws, ctx: dict) -> bool:
    if mtype == "browse_dir":
        req_path = msg.get("path") or "~"
        path = os.path.realpath(os.path.expanduser(req_path))
        entries = _list_entries(path)

        active_items = _active_sessions_for_path(path, ctx["sessions"])

        # Stage 1: return filesystem + active sessions quickly.
        try:
            await ws.send(json.dumps(ctx["msg_dir_listing"](path, entries, active_items)))
        except Exception:
            return True

        # Stage 2: enrich with resumable sessions (cached).
        active_uuids = {s.resume_id for s in ctx["sessions"].values() if s.resume_id}
        resumable = await _resumable_for_path(path, ctx["backends"], active_uuids)
        merged = active_items + resumable
        try:
            await ws.send(json.dumps(ctx["msg_dir_listing"](path, entries, merged)))
        except Exception:
            pass
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
        return True

    return False
