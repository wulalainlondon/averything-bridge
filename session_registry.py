"""Session data model and list/summary helpers."""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Optional

from utils.uuid_helper import is_valid_uuid


DEFAULT_CWD = os.path.expanduser(os.environ.get("BRIDGE_DEFAULT_CWD", "") or "~")


@dataclass
class QueuedCommand:
    request_id: str
    device_id: str
    client_id: str
    content: str
    images: list | None
    files: list | None
    enqueued_at: float


@dataclass
class Session:
    session_id: str
    name: str
    created_at: float
    cwd: str = field(default_factory=lambda: DEFAULT_CWD)
    is_streaming: bool = False
    is_stopping: bool = False
    resume_id: Optional[str] = None
    effort: str = ""
    last_activity: float = 0.0
    accumulated_text: str = ""
    model: str = ""
    sandbox: str = "danger-full-access"
    image_dir: str = ""
    context_used: int = 0
    context_max: int = 0
    backend_name: str = "claude"
    pinned: bool = False
    hidden: bool = False
    queue: Deque[QueuedCommand] = field(default_factory=deque)
    processing: bool = False
    current_request_id: str = ""
    message_seq: int = 0
    pending_clients: set[str] = field(default_factory=set)
    ws_ref: Optional[Any] = field(default=None, repr=False)
    offline_buffer: list = field(default_factory=list)
    recent_request_ids: set[str] = field(default_factory=set)
    # BUG-07: set to True after first user message is indexed into FTS5 search.db
    _fts_first_msg_indexed: bool = False


SESSIONS: dict[str, Session] = {}
SESSIONS_LOCK = asyncio.Lock()
READ_CURSORS: dict[str, dict[str, int]] = {}


def session_has_valid_resume(session: Session) -> bool:
    """Return False only when the session has a resume_id that is provably invalid."""
    if not session.resume_id:
        return True
    backend = getattr(session, "backend_name", "claude")
    if backend in ("claude", "codex"):
        return is_valid_uuid(session.resume_id)
    return True


def session_to_summary(
    session: Session,
    *,
    recent_messages: Callable[[Session, int], list],
) -> dict:
    return {
        "id": session.session_id,
        "name": session.name,
        "is_streaming": session.is_streaming,
        "created_at": session.created_at,
        "last_activity": session.last_activity or session.created_at,
        "cwd": session.cwd,
        "model": session.model,
        "context_used": session.context_used,
        "context_max": session.context_max,
        "backend": session.backend_name,
        "sandbox": session.sandbox,
        "image_dir": session.image_dir,
        "pinned": session.pinned,
        "hidden": session.hidden,
        "queue_length": len(session.queue),
        "recent_messages": recent_messages(session, 2),
    }


def sorted_visible_sessions(sessions: Mapping[str, Session], *, limit: int | None = None) -> list[Session]:
    result = sorted(
        (session for session in sessions.values() if session_has_valid_resume(session)),
        key=lambda session: session.last_activity or session.created_at,
        reverse=True,
    )
    return result[:limit] if limit is not None else result


def build_sessions_list(
    sessions: Mapping[str, Session],
    *,
    recent_messages: Callable[[Session, int], list],
    limit: int = 50,
) -> dict:
    return {
        "type": "sessions_list",
        "sessions": [
            session_to_summary(session, recent_messages=recent_messages)
            for session in sorted_visible_sessions(sessions, limit=limit)
        ],
    }


async def send_all_sessions(
    ws: Any,
    sessions: Mapping[str, Session],
    *,
    recent_messages: Callable[[Session, int], list],
    batch_size: int = 50,
) -> None:
    all_sessions = sorted_visible_sessions(sessions)
    total = len(all_sessions)
    for offset in range(0, total, batch_size):
        batch = all_sessions[offset: offset + batch_size]
        done = (offset + batch_size) >= total
        payload = {
            "type": "sessions_list_append",
            "sessions": [
                session_to_summary(session, recent_messages=recent_messages)
                for session in batch
            ],
            "offset": offset,
            "total": total,
            "done": done,
        }
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            return
        if not done:
            await asyncio.sleep(0.1)


def load_saved_sessions(saved_sessions_file: str) -> dict:
    try:
        with open(saved_sessions_file) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def migrate_codex_saved_sessions(
    *,
    saved_sessions_file: str,
    codex_saved_sessions_file: str,
    default_cwd: str = DEFAULT_CWD,
    log_warning: Callable[..., None] | None = None,
) -> None:
    """Merge legacy Codex metadata into saved_sessions.json without copying history."""
    try:
        with open(codex_saved_sessions_file, encoding="utf-8") as f:
            legacy = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return
    if not isinstance(legacy, dict):
        return
    saved = load_saved_sessions(saved_sessions_file)
    changed = False
    for sid, entry in legacy.items():
        if not isinstance(entry, dict):
            continue
        current = saved.get(sid, {}) if isinstance(saved.get(sid), dict) else {}
        saved[sid] = {
            **current,
            "name": current.get("name") or entry.get("name") or str(sid)[:8],
            "claude_uuid": current.get("claude_uuid") or entry.get("resume_id") or entry.get("claude_uuid") or "",
            "last_used": int(current.get("last_used") or entry.get("last_used") or time.time()),
            "cwd": current.get("cwd") or entry.get("cwd") or default_cwd,
            "backend": "codex",
            "model": current.get("model") or entry.get("model") or "",
            "sandbox": current.get("sandbox") or entry.get("sandbox") or "danger-full-access",
            "image_dir": current.get("image_dir") or entry.get("image_dir") or "",
        }
        changed = True
    if changed:
        try:
            with open(saved_sessions_file, "w", encoding="utf-8") as f:
                json.dump(saved, f, indent=2, ensure_ascii=False)
        except Exception as exc:
            if log_warning:
                log_warning("Failed to migrate Codex saved session metadata: %s", exc)


def persist_session(
    session: Session,
    *,
    saved_sessions_file: str,
    log_warning: Callable[..., None] | None = None,
) -> None:
    saved = load_saved_sessions(saved_sessions_file)
    saved[session.session_id] = {
        "name": session.name,
        # Both keys written for human-readability and forward/backward compat.
        # Authoritative field is resume_id; claude_uuid is legacy alias.
        "resume_id": session.resume_id,
        "claude_uuid": session.resume_id,
        "last_used": int(time.time()),
        "cwd": session.cwd,
        "backend": session.backend_name,
        "model": session.model,
        "sandbox": session.sandbox,
        "image_dir": session.image_dir,
    }
    cutoff = int(time.time()) - 90 * 24 * 3600
    saved = {
        key: value for key, value in saved.items()
        if value.get("last_used", 0) > cutoff
    }
    if len(saved) > 200:
        saved = dict(sorted(saved.items(), key=lambda item: item[1].get("last_used", 0), reverse=True)[:200])
    try:
        with open(saved_sessions_file, "w") as f:
            json.dump(saved, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        if log_warning:
            log_warning("Failed to persist session: %s", exc)


def remove_saved_session(
    session_id: str,
    *,
    saved_sessions_file: str,
    log_warning: Callable[..., None] | None = None,
) -> None:
    saved = load_saved_sessions(saved_sessions_file)
    if session_id not in saved:
        return
    del saved[session_id]
    try:
        with open(saved_sessions_file, "w") as f:
            json.dump(saved, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        if log_warning:
            log_warning("Failed to remove session from disk: %s", exc)


def restore_sessions_from_disk(
    sessions: dict[str, Session],
    *,
    saved_sessions_file: str,
    default_cwd: str = DEFAULT_CWD,
    normalize_backend: Callable[[str | None], str],
    log_info: Callable[..., None] | None = None,
    log_warning: Callable[..., None] | None = None,
) -> int:
    """Load saved_sessions.json into sessions so sessions survive bridge restarts."""
    from auto_register import prune_old_saved_sessions

    prune_old_saved_sessions(Path(saved_sessions_file), days=30)
    saved = load_saved_sessions(saved_sessions_file)

    dropped: list[str] = []
    for sid, data in list(saved.items()):
        backend = data.get("backend", "claude")
        claude_uuid = data.get("resume_id") or data.get("claude_uuid", "") or ""
        if backend in ("claude", "codex") and claude_uuid and not is_valid_uuid(claude_uuid):
            dropped.append(sid)
            del saved[sid]
            if log_info:
                log_info("dropping saved session %s: invalid UUID %r", sid, claude_uuid)
    if dropped:
        try:
            with open(saved_sessions_file, "w", encoding="utf-8") as f:
                json.dump(saved, f, indent=2, ensure_ascii=False)
            if log_info:
                log_info("saved_sessions.json: dropped %d invalid session(s): %s", len(dropped), dropped)
        except Exception as exc:
            if log_warning:
                log_warning("Failed to rewrite saved_sessions.json after dropping invalid UUIDs: %s", exc)

    count = 0
    for sid, data in saved.items():
        if sid in sessions:
            continue
        try:
            saved_last_used = float(data.get("last_used") or time.time())
            session = Session(
                session_id=sid,
                name=data.get("name", sid[:8]),
                created_at=saved_last_used,
                cwd=os.path.expanduser(data.get("cwd") or default_cwd),
                backend_name=normalize_backend(data.get("backend")),
                model=str(data.get("model") or ""),
                sandbox=str(data.get("sandbox") or "danger-full-access"),
                image_dir=str(data.get("image_dir") or ""),
            )
            session.resume_id = data.get("resume_id") or data.get("claude_uuid") or None
            session.last_activity = saved_last_used
            sessions[sid] = session
            count += 1
        except Exception as exc:
            if log_warning:
                log_warning("Failed to restore session %s: %s", sid, exc)
    if count and log_info:
        log_info("Restored %d session(s) from disk", count)
    return count


def load_session_meta(session_meta_file: str) -> dict[str, dict]:
    try:
        with open(session_meta_file) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def persist_session_meta(
    sessions: Mapping[str, Session],
    *,
    session_meta_file: str,
    log_warning: Callable[..., None] | None = None,
) -> None:
    payload = {
        sid: {"pinned": bool(session.pinned), "hidden": bool(session.hidden)}
        for sid, session in sessions.items()
        if session.pinned or session.hidden
    }
    try:
        with open(session_meta_file, "w") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        if log_warning:
            log_warning("Failed to persist session metadata: %s", exc)


def apply_session_meta(
    sessions: Mapping[str, Session],
    *,
    session_meta_file: str,
    log_info: Callable[..., None] | None = None,
) -> int:
    meta = load_session_meta(session_meta_file)
    applied = 0
    for sid, val in meta.items():
        session = sessions.get(sid)
        if not session or not isinstance(val, dict):
            continue
        session.pinned = bool(val.get("pinned", False))
        session.hidden = bool(val.get("hidden", False))
        applied += 1
    if applied and log_info:
        log_info("Applied metadata for %d session(s)", applied)
    return applied


def load_read_cursors(read_cursor_file: str) -> dict[str, dict[str, int]]:
    try:
        with open(read_cursor_file) as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {}
        out: dict[str, dict[str, int]] = {}
        for sid, cursors in data.items():
            if not isinstance(cursors, dict):
                continue
            out[sid] = {}
            for dev, seq in cursors.items():
                try:
                    out[sid][str(dev)] = int(seq)
                except Exception:
                    pass
        return out
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def persist_read_cursors(
    read_cursors: Mapping[str, Mapping[str, int]],
    *,
    read_cursor_file: str,
    log_warning: Callable[..., None] | None = None,
) -> None:
    try:
        with open(read_cursor_file, "w") as f:
            json.dump(read_cursors, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        if log_warning:
            log_warning("Failed to persist read cursors: %s", exc)


def mark_read(read_cursors: dict[str, dict[str, int]], session_id: str, device_id: str, seq: int) -> None:
    if not device_id:
        return
    dev_map = read_cursors.setdefault(session_id, {})
    dev_map[device_id] = max(int(seq), int(dev_map.get(device_id, 0)))


def unread_for(read_cursors: Mapping[str, Mapping[str, int]], session: Session, device_id: str) -> int:
    if not device_id:
        return 0
    read = read_cursors.get(session.session_id, {}).get(device_id, 0)
    return max(0, int(session.message_seq) - int(read))
