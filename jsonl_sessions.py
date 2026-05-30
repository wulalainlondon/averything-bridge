"""Native Claude/Codex JSONL session discovery and live watcher."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import client_manager
import session_registry
from session_registry import Session
from utils.uuid_helper import is_valid_uuid


_TURN_ABORTED_RE = re.compile(r"<turn_aborted>.*?</turn_aborted>", re.IGNORECASE | re.DOTALL)
CODEX_SESSIONS_DIR = str(Path.home() / ".codex" / "sessions")
_JSONL_TURN_END_STOP_REASONS: frozenset[str] = frozenset(
    {"end_turn", "max_tokens", "stop_sequence"}
)

_sessions: dict[str, Session] = {}
_default_cwd = session_registry.DEFAULT_CWD
_claude_projects_dir = str(Path.home() / ".claude" / "projects")
_session_backend: Callable[[Session], Any] | None = None
_broadcast_json: Callable[[dict], Awaitable[int]] | None = None
_build_sessions_list: Callable[[], dict] | None = None
_dispatch_event: Callable[[dict, Session], Awaitable[bool]] | None = None
_evt_done: Callable[[], dict] | None = None
_log: Any = None
_recent_msgs_cache: dict[str, tuple[float, list]] = {}


def configure(
    *,
    sessions: dict[str, Session],
    default_cwd: str,
    claude_projects_dir: str,
    session_backend: Callable[[Session], Any],
    broadcast_json: Callable[[dict], Awaitable[int]],
    build_sessions_list: Callable[[], dict],
    dispatch_event: Callable[[dict, Session], Awaitable[bool]],
    evt_done: Callable[[], dict],
    log: Any,
) -> None:
    global _sessions, _default_cwd, _claude_projects_dir, _session_backend
    global _broadcast_json, _build_sessions_list, _dispatch_event, _evt_done, _log
    _sessions = sessions
    _default_cwd = default_cwd
    _claude_projects_dir = claude_projects_dir
    _session_backend = session_backend
    _broadcast_json = broadcast_json
    _build_sessions_list = build_sessions_list
    _dispatch_event = dispatch_event
    _evt_done = evt_done
    _log = log


def _warning(message: str, *args: Any) -> None:
    if _log is not None:
        _log.warning(message, *args)


def _info(message: str, *args: Any) -> None:
    if _log is not None:
        _log.info(message, *args)


def ensure_local_session_dirs() -> None:
    for p in (_claude_projects_dir, CODEX_SESSIONS_DIR):
        try:
            Path(p).mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            _warning("Failed to create local session dir %s: %s", p, exc)


def codex_session_id_from_stem(stem: str) -> str:
    candidate = stem[-36:]
    return candidate if is_valid_uuid(candidate) else stem


def extract_codex_text(content: object) -> str:
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            parts.append(text.strip())
    return "\n".join(parts).strip()


def _is_session_title_noise(text: str) -> bool:
    stripped = text.strip()
    return (
        not stripped
        or stripped.startswith("<turn_aborted>")
        or stripped.startswith("# AGENTS.md instructions")
        or stripped.startswith("<permissions instructions>")
        or stripped.startswith("<environment_context>")
    )


def strip_turn_aborted_notice(text: str) -> str:
    cleaned = _TURN_ABORTED_RE.sub("", text or "")
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _read_recent_msgs(path: str, fmt: str, n: int = 2) -> list:
    try:
        mtime = os.path.getmtime(path)
        cache_key = f"{path}:{fmt}:{n}"
        if cache_key in _recent_msgs_cache:
            cached_mtime, cached_msgs = _recent_msgs_cache[cache_key]
            if cached_mtime == mtime:
                return cached_msgs
    except Exception:
        pass
    messages: list = []
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            f.seek(max(0, size - 32768))
            chunk = f.read()
        nl = chunk.find(b"\n")
        lines = chunk[nl + 1:].decode("utf-8", errors="ignore").splitlines()
        for raw in lines:
            try:
                d = json.loads(raw)
                role = text = None
                if fmt == "claude":
                    if d.get("isSidechain") or d.get("type") not in ("user", "assistant"):
                        continue
                    role = d["type"]
                    content = d.get("message", {}).get("content", "")
                    if isinstance(content, str):
                        text = content
                    elif isinstance(content, list):
                        parts = [
                            blk.get("text", "") for blk in content
                            if isinstance(blk, dict) and blk.get("type") == "text"
                        ]
                        text = "\n".join(p for p in parts if p)
                elif fmt == "codex":
                    if d.get("type") != "response_item":
                        continue
                    payload = d.get("payload", {})
                    if not isinstance(payload, dict) or payload.get("type") != "message":
                        continue
                    role = payload.get("role")
                    if role not in ("user", "assistant"):
                        continue
                    text = extract_codex_text(payload.get("content", ""))
                if role and text and not text.startswith("<") and not text.startswith("[Request interrupted"):
                    messages.append({"role": role, "text": text[:80]})
            except Exception:
                pass
    except Exception:
        pass
    result = messages[-n:] if len(messages) > n else messages
    try:
        mtime = os.path.getmtime(path)
        cache_key = f"{path}:{fmt}:{n}"
        _recent_msgs_cache[cache_key] = (mtime, result)
    except Exception:
        pass
    return result


def get_recent_messages_sync(session: Session, n: int = 2) -> list:
    try:
        if not session.resume_id or _session_backend is None:
            return []
        backend = _session_backend(session)
        path = backend.find_session_file(session.resume_id)
        if not path:
            return []
        fmt = "codex" if session.backend_name == "codex" else "claude"
        return _read_recent_msgs(path, fmt, n)
    except Exception:
        return []


def _register_jsonl_session(path: str) -> bool:
    try:
        fn = os.path.basename(path)
        if not fn.endswith(".jsonl"):
            return False
        stem = fn[:-6]

        if CODEX_SESSIONS_DIR and path.startswith(CODEX_SESSIONS_DIR):
            backend_name = "codex"
            resume_id = codex_session_id_from_stem(stem)
            if not is_valid_uuid(resume_id):
                return False
            sid = f"jl_x_{resume_id[:12]}"
        else:
            backend_name = "claude"
            resume_id = stem
            if len(resume_id) < 8:
                return False
            sid = f"jl_c_{resume_id[:12]}"

        existing_uuids: set[str] = set()
        for s in _sessions.values():
            if s.resume_id:
                existing_uuids.add(s.resume_id)
            existing_uuids.update(getattr(s, "historical_resume_ids", set()))
        if resume_id in existing_uuids or sid in _sessions:
            return False

        name = ""
        cwd = _default_cwd
        fmt = "codex" if backend_name == "codex" else "claude"
        try:
            with open(path, encoding="utf-8", errors="ignore") as f:
                for line_no, raw in enumerate(f):
                    if line_no >= 200:
                        break
                    try:
                        d = json.loads(raw)
                        if fmt == "claude":
                            if not cwd or cwd == _default_cwd:
                                raw_cwd = d.get("cwd")
                                if isinstance(raw_cwd, str) and raw_cwd.strip():
                                    cwd = raw_cwd.strip()
                            if not name and d.get("type") == "user":
                                content = d.get("message", {}).get("content", "")
                                t = content if isinstance(content, str) else next(
                                    (blk.get("text", "") for blk in content
                                     if isinstance(blk, dict) and blk.get("type") == "text"),
                                    "",
                                )
                                if t and not t.startswith("<"):
                                    name = t[:50].strip()
                        elif fmt == "codex":
                            if d.get("type") == "session_meta":
                                pl = d.get("payload", {})
                                if isinstance(pl, dict) and (not cwd or cwd == _default_cwd):
                                    c = pl.get("cwd") or pl.get("workingDirectory", "")
                                    if c:
                                        cwd = c
                            if not name and d.get("type") == "response_item":
                                payload = d.get("payload", {})
                                if (
                                    isinstance(payload, dict)
                                    and payload.get("type") == "message"
                                    and payload.get("role") == "user"
                                ):
                                    t = extract_codex_text(payload.get("content"))
                                    if t and not _is_session_title_noise(t):
                                        name = t[:50].strip()
                        if name and cwd != _default_cwd:
                            break
                    except Exception:
                        pass
        except Exception:
            pass

        if not name:
            name = resume_id[:8]
        try:
            mtime = os.stat(path).st_mtime
        except OSError:
            mtime = 0.0

        _sessions[sid] = Session(
            session_id=sid,
            name=name,
            created_at=mtime or time.time(),
            last_activity=mtime or time.time(),
            cwd=os.path.expanduser(cwd),
            backend_name=backend_name,
            resume_id=resume_id,
        )
        return True
    except Exception as exc:
        _warning("_register_jsonl_session(%s) failed: %s", path, exc)
        return False


def _session_for_jsonl_path(path: str) -> Session | None:
    fn = os.path.basename(path)
    if not fn.endswith(".jsonl"):
        return None
    stem = fn[:-6]
    if CODEX_SESSIONS_DIR and path.startswith(CODEX_SESSIONS_DIR):
        resume_id = codex_session_id_from_stem(stem)
    else:
        resume_id = stem
    for session in _sessions.values():
        if session.resume_id == resume_id:
            return session
    return None


def merge_jsonl_sessions_into_state() -> bool:
    added = False
    existing_uuids: set[str] = set()
    for s in _sessions.values():
        if s.resume_id:
            existing_uuids.add(s.resume_id)
        existing_uuids.update(getattr(s, "historical_resume_ids", set()))

    for base, backend_name in ((_claude_projects_dir, "claude"), (CODEX_SESSIONS_DIR, "codex")):
        if not os.path.isdir(base):
            _info("JSONL initial scan (%s): source dir missing, skipped: %s", backend_name, base)
            continue
        try:
            for root, _dirs, files in os.walk(base):
                for fn in files:
                    if not fn.endswith(".jsonl"):
                        continue
                    native_id = codex_session_id_from_stem(fn[:-6]) if backend_name == "codex" else fn[:-6]
                    if len(native_id) < 8 or native_id in existing_uuids:
                        continue
                    if _register_jsonl_session(os.path.join(root, fn)):
                        existing_uuids.add(native_id)
                        added = True
        except FileNotFoundError:
            _info("JSONL initial scan (%s): source dir disappeared during scan, skipped", backend_name)
        except Exception as exc:
            _warning("JSONL initial scan (%s) error: %s", backend_name, exc)

    return added


def _read_new_jsonl_lines(path: str, from_offset: int) -> tuple[list[dict], int]:
    try:
        size = os.path.getsize(path)
        if size <= from_offset:
            return [], from_offset
        with open(path, "rb") as fh:
            fh.seek(from_offset)
            raw = fh.read(size - from_offset)
        lines = raw.decode("utf-8", errors="ignore").splitlines()
        parsed: list[dict] = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                parsed.append(json.loads(line))
            except Exception:
                pass
        return parsed, size
    except OSError:
        return [], from_offset


def _jsonl_lines_contain_turn_end(lines: list[dict], fmt: str) -> bool:
    if fmt == "claude":
        for d in lines:
            if (
                d.get("type") == "assistant"
                and not d.get("isSidechain")
                and d.get("message", {}).get("stop_reason") in _JSONL_TURN_END_STOP_REASONS
            ):
                return True
    elif fmt == "codex":
        for d in lines:
            if d.get("type") != "response_item":
                continue
            payload = d.get("payload", {})
            if not isinstance(payload, dict):
                continue
            if payload.get("role") == "assistant" and payload.get("stop_reason") in _JSONL_TURN_END_STOP_REASONS:
                return True
    return False


async def jsonl_watcher_task() -> None:
    loop = asyncio.get_event_loop()
    merge_jsonl_sessions_into_state()

    pending: dict[str, float] = {}
    flush_handle: list = [None]
    jsonl_known_size: dict[str, int] = {}

    def _seed_known_size(path: str) -> None:
        if path not in jsonl_known_size:
            try:
                jsonl_known_size[path] = os.path.getsize(path)
            except OSError:
                jsonl_known_size[path] = 0

    async def _flush_changes() -> None:
        paths = list(pending.keys())
        pending.clear()
        changed_sessions: dict[str, Session] = {}
        for p in paths:
            _seed_known_size(p)
            _register_jsonl_session(p)
            session = _session_for_jsonl_path(p)
            if session is None:
                continue
            try:
                mtime = os.stat(p).st_mtime
                session.last_activity = max(session.last_activity, mtime)
            except OSError:
                pass
            changed_sessions[session.session_id] = session
            if not session.is_streaming:
                try:
                    jsonl_known_size[p] = os.path.getsize(p)
                except OSError:
                    pass
                continue

            fmt = "codex" if session.backend_name == "codex" else "claude"
            prior_offset = jsonl_known_size.get(p, 0)
            new_lines, new_size = _read_new_jsonl_lines(p, prior_offset)
            jsonl_known_size[p] = new_size
            if not new_lines:
                continue
            if (
                _jsonl_lines_contain_turn_end(new_lines, fmt)
                and _dispatch_event is not None
                and _evt_done is not None
                and session.session_id.startswith(("jl_c_", "jl_x_"))
            ):
                _info(
                    "emit external done for session %s (jsonl=%s, new_lines=%d)",
                    session.session_id, os.path.basename(p), len(new_lines),
                )
                session.is_streaming = False
                await _dispatch_event(
                    {**_evt_done(), "session_id": session.session_id, "request_id": session.current_request_id or "external"},
                    session,
                )

        if client_manager.has_clients() and _broadcast_json is not None and _build_sessions_list is not None:
            await _broadcast_json(_build_sessions_list())
            for session in changed_sessions.values():
                await _broadcast_json({
                    "type": "history_sync_hint",
                    "session_id": session.session_id,
                    "reason": "file_changed",
                })

    def _on_file_event(path: str) -> None:
        if not path.endswith(".jsonl"):
            return
        pending[path] = time.time()
        if flush_handle[0]:
            flush_handle[0].cancel()
        flush_handle[0] = loop.call_later(0.8, lambda: asyncio.ensure_future(_flush_changes()))

    try:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer

        class _Handler(FileSystemEventHandler):
            def on_modified(self, event):
                if not event.is_directory:
                    loop.call_soon_threadsafe(_on_file_event, event.src_path)

            def on_created(self, event):
                if not event.is_directory:
                    loop.call_soon_threadsafe(_on_file_event, event.src_path)

        observer = Observer()
        handler = _Handler()
        for d in (_claude_projects_dir, CODEX_SESSIONS_DIR):
            if os.path.isdir(d):
                observer.schedule(handler, d, recursive=True)
        observer.start()
        _info("JSONL watcher: FSEvents observer started")
        try:
            await asyncio.Future()
        finally:
            observer.stop()
            observer.join()

    except ImportError:
        _warning("watchdog not installed — falling back to 5s polling")

        _mtime_cache: dict[str, float] = {}

        def _dir_fingerprint() -> str:
            changed_parts: list[str] = []
            seen_paths: set[str] = set()
            for base in (_claude_projects_dir, CODEX_SESSIONS_DIR):
                if not os.path.isdir(base):
                    continue
                for root, _dirs, files in os.walk(base):
                    for fn in sorted(files):
                        if not fn.endswith(".jsonl"):
                            continue
                        fp_path = os.path.join(root, fn)
                        seen_paths.add(fp_path)
                        try:
                            mtime = os.stat(fp_path).st_mtime
                        except OSError:
                            continue
                        if _mtime_cache.get(fp_path) != mtime:
                            _mtime_cache[fp_path] = mtime
                            changed_parts.append(f"{fn}:{mtime:.0f}")
            # treat deleted files as a change too
            deleted = set(_mtime_cache) - seen_paths
            for p in deleted:
                del _mtime_cache[p]
                changed_parts.append(f"DEL:{os.path.basename(p)}")
            if not changed_parts:
                return ""
            return hashlib.md5("|".join(changed_parts).encode()).hexdigest()

        # prime the cache without treating everything as changed
        _dir_fingerprint()
        while True:
            await asyncio.sleep(30)
            try:
                fp = _dir_fingerprint()
                if not fp:
                    continue
                merge_jsonl_sessions_into_state()
                if client_manager.has_clients() and _broadcast_json is not None and _build_sessions_list is not None:
                    await _broadcast_json(_build_sessions_list())
            except Exception as exc:
                _warning("JSONL polling error: %s", exc)
