"""Session data model and list/summary helpers."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import tempfile
import threading
import time
from collections import deque
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Deque, Optional

try:
    import fcntl as _fcntl
    _FCNTL_AVAILABLE = True
except ImportError:
    _fcntl = None  # type: ignore[assignment]
    _FCNTL_AVAILABLE = False

from utils.uuid_helper import is_valid_uuid

log = logging.getLogger(__name__)

# Shared cross-thread lock for saved_sessions.json.  Both the asyncio side
# (persist_session / remove_saved_session) and the search-ingest worker thread
# (auto_register.auto_register_session) must hold this lock before touching
# saved_sessions.json.  The lock is imported by auto_register at runtime to
# avoid a circular-import; see auto_register._SAVED_LOCK.
_SAVED_SESSIONS_LOCK = threading.Lock()


@contextmanager
def _saved_sessions_file_lock(saved_sessions_file: str):
    """Acquire both the in-process threading.Lock and a cross-process fcntl.flock.

    Yields inside the combined lock so callers can safely read-modify-write
    saved_sessions.json without racing against other bridge instances.
    """
    lock_path = saved_sessions_file + ".lock"
    with _SAVED_SESSIONS_LOCK:
        if _FCNTL_AVAILABLE:
            lock_fh = open(lock_path, "w")
            try:
                _fcntl.flock(lock_fh, _fcntl.LOCK_EX)
                yield
            finally:
                try:
                    _fcntl.flock(lock_fh, _fcntl.LOCK_UN)
                except Exception:
                    pass
                try:
                    lock_fh.close()
                except Exception:
                    pass
        else:
            yield


def _atomic_write_saved_sessions(saved_sessions_file: str, saved: dict) -> None:
    """Atomically write the saved_sessions mapping (tempfile in same dir + os.replace).

    Callers MUST already hold _saved_sessions_file_lock to make the surrounding
    read-modify-write race-free across instances; this helper only guarantees the
    on-disk file is never left truncated/partial.
    """
    path = Path(saved_sessions_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=".json")
    tmp = Path(tmp_str)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(saved, f, indent=2, ensure_ascii=False)
        tmp.replace(path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise


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
    # W5: insertion-ordered list that mirrors recent_request_ids for deterministic
    # trimming and disk persistence. The set is used for O(1) dedup lookup;
    # this list tracks insertion order so we evict the oldest entries first.
    recent_request_ids_seq: list = field(default_factory=list, repr=False)
    # BUG-07: set to True after first user message is indexed into FTS5 search.db
    _fts_first_msg_indexed: bool = False
    parent_session_id: str | None = None
    forked_at: float | None = None
    # Per-session asyncio lock that serialises all ws.send() calls for this session.
    # Prevents live events from interleaving with offline-buffer replay frames.
    _ws_send_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    # All prior resume_ids this session has held (UUIDs rotate each Claude turn).
    # Used by the JSONL watcher to avoid registering ghost sessions for old .jsonl files.
    historical_resume_ids: set = field(default_factory=set, repr=False)
    # source_message_id of the last message in this session's JSONL history.
    # Used by request_history fast-path and sessions_list cursor broadcast.
    latest_source_line: str = ""


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
        "latest_source_line": session.latest_source_line,
    }


def sorted_visible_sessions(sessions: Mapping[str, Session], *, limit: int | None = None) -> list[Session]:
    sorted_all = sorted(
        (session for session in sessions.values() if session_has_valid_resume(session)),
        key=lambda session: session.last_activity or session.created_at,
        reverse=True,
    )
    # Deduplicate by resume_id: when multiple sessions share the same resume_id
    # (e.g. a saved s_* session and a ghost jl_* session from an old .jsonl file),
    # keep only one.  Prefer s_* sessions because they carry user-set name/sandbox
    # metadata.  Among jl_* duplicates keep the most-recently-active one (already
    # first in the sorted order).  Sessions with resume_id=None are never deduped.
    seen_resume_ids: set[str] = set()
    result: list[Session] = []
    for session in sorted_all:
        rid = session.resume_id
        if rid is None:
            result.append(session)
            continue
        if rid in seen_resume_ids:
            # Already have a winner for this resume_id — check if this entry is a
            # non-jl_ session that should displace the previously chosen jl_ one.
            if not session.session_id.startswith("jl_"):
                # Replace the jl_ winner that snuck in earlier.
                result = [
                    s for s in result
                    if s.resume_id != rid or not s.session_id.startswith("jl_")
                ]
                result.append(session)
            # else: skip this duplicate
        else:
            seen_resume_ids.add(rid)
            result.append(session)
    # Re-sort after potential displacement (non-jl_ sessions appended at end).
    result.sort(key=lambda s: s.last_activity or s.created_at, reverse=True)
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
    # Hold the lock across load→modify→write so the read-modify-write is atomic
    # against concurrent persist_session from another instance, and write via
    # tempfile+replace so a crash mid-write can't truncate saved_sessions.json.
    with _saved_sessions_file_lock(saved_sessions_file):
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
                _atomic_write_saved_sessions(saved_sessions_file, saved)
            except Exception as exc:
                if log_warning:
                    log_warning("Failed to migrate Codex saved session metadata: %s", exc)


def persist_session(
    session: Session,
    *,
    saved_sessions_file: str,
    log_warning: Callable[..., None] | None = None,
) -> None:
    import tempfile
    with _saved_sessions_file_lock(saved_sessions_file):
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
            "parent_session_id": session.parent_session_id,
            "forked_at": session.forked_at,
            # JSON does not support sets; serialise as a sorted list for stability.
            "historical_resume_ids": sorted(session.historical_resume_ids),
            "latest_source_line": session.latest_source_line,
            # W5: persist last 250 request IDs in insertion order for cross-restart dedup.
            "recent_request_ids": session.recent_request_ids_seq[-250:],
        }
        cutoff = int(time.time()) - 30 * 24 * 3600
        saved = {
            key: value for key, value in saved.items()
            if value.get("last_used", 0) > cutoff
        }
        if len(saved) > 500:
            # Prefer evicting sessions that have a resume_id (recoverable via JSONL).
            # Sessions with resume_id=None are unique and cannot be recovered.
            with_resume = sorted(
                [(k, v) for k, v in saved.items() if v.get("resume_id")],
                key=lambda item: item[1].get("last_used", 0),
            )
            evict_count = len(saved) - 500
            to_evict = {k for k, _v in with_resume[:evict_count]}
            saved = {k: v for k, v in saved.items() if k not in to_evict}
        path = Path(saved_sessions_file)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_str = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=".json")
            tmp = Path(tmp_str)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(saved, f, indent=2, ensure_ascii=False)
                tmp.replace(path)
            except Exception:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
                raise
        except Exception as exc:
            if log_warning:
                log_warning("Failed to persist session: %s", exc)


def remove_saved_session(
    session_id: str,
    *,
    saved_sessions_file: str,
    log_warning: Callable[..., None] | None = None,
) -> None:
    import tempfile
    with _saved_sessions_file_lock(saved_sessions_file):
        saved = load_saved_sessions(saved_sessions_file)
        if session_id not in saved:
            return
        del saved[session_id]
        path = Path(saved_sessions_file)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp_str = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=".json")
            tmp = Path(tmp_str)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(saved, f, indent=2, ensure_ascii=False)
                tmp.replace(path)
            except Exception:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
                raise
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
    root_dir: str = "",
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
            with _saved_sessions_file_lock(saved_sessions_file):
                _atomic_write_saved_sessions(saved_sessions_file, saved)
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
            cwd = os.path.expanduser(data.get("cwd") or default_cwd)
            if root_dir:
                from utils.path_jail import is_inside_jail
                real_cwd = os.path.realpath(cwd)
                real_root = os.path.realpath(root_dir)
                if not is_inside_jail(real_cwd, real_root):
                    log.warning(
                        "[jail] Dropping restored session %s: cwd=%r outside root=%r",
                        sid, cwd, root_dir,
                    )
                    continue
            session = Session(
                session_id=sid,
                name=data.get("name", sid[:8]),
                created_at=saved_last_used,
                cwd=cwd,
                backend_name=normalize_backend(data.get("backend")),
                model=str(data.get("model") or ""),
                sandbox=str(data.get("sandbox") or "danger-full-access"),
                image_dir=str(data.get("image_dir") or ""),
            )
            session.resume_id = data.get("resume_id") or data.get("claude_uuid") or None
            session.last_activity = saved_last_used
            session.parent_session_id = data.get("parent_session_id") or None
            session.forked_at = float(data["forked_at"]) if data.get("forked_at") is not None else None
            session.latest_source_line = str(data.get("latest_source_line") or "")
            raw_hist = data.get("historical_resume_ids")
            if isinstance(raw_hist, list):
                session.historical_resume_ids = set(str(x) for x in raw_hist if x)
            raw_recent = data.get("recent_request_ids")
            if isinstance(raw_recent, list):
                seq = [str(x) for x in raw_recent if x]
                session.recent_request_ids = set(seq)
                session.recent_request_ids_seq = seq
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
    import tempfile
    payload = {
        sid: {"hidden": bool(session.hidden)}
        for sid, session in sessions.items()
        if session.hidden
    }
    path = Path(session_meta_file)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_str = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=".json")
        tmp = Path(tmp_str)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False)
            tmp.replace(path)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            raise
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
    import tempfile
    path = Path(read_cursor_file)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_str = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=".json")
        tmp = Path(tmp_str)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(read_cursors, f, indent=2, ensure_ascii=False)
            tmp.replace(path)
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            raise
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
