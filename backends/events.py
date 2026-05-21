"""
Shared event/message builder functions and session I/O helpers.
No imports from bridge_v2 — every backend can import from here and
remain independently testable without spinning up the full bridge.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from typing import TYPE_CHECKING, Awaitable, Callable

if TYPE_CHECKING:
    from bridge_v2 import Session

OFFLINE_BUFFER_MAX = 10000
_MEDIA_BASE_URL: str = ""
HTTP_PORT = 9090
_http_server_proc: "asyncio.subprocess.Process | None" = None


async def ensure_http_server() -> None:
    global _http_server_proc
    if _http_server_proc is not None and _http_server_proc.returncode is None:
        return
    _http_server_proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "http.server", str(HTTP_PORT), "--directory", "/",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )


def set_media_base_url(url: str) -> None:
    global _MEDIA_BASE_URL
    _MEDIA_BASE_URL = url.rstrip("/")
_EVENT_DISPATCHER: Callable[[dict, "Session"], Awaitable[bool]] | None = None

MEDIA_RE = re.compile(
    r'(/(?:[^\s\'"<>]+\.(?:jpg|jpeg|png|gif|webp|mp4|mov|m4v|avi)))',
    re.IGNORECASE,
)
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".avi"}
DOC_EXTS   = {".html", ".htm", ".pdf"}


# ---------------------------------------------------------------------------
# Session-scoped event builders — session_id injected by send_event()
# ---------------------------------------------------------------------------

def _evt_text_chunk(content: str) -> dict:
    return {"type": "text_chunk", "content": content}

def _evt_tool_start(tool_use_id: str, name: str, command: str) -> dict:
    return {"type": "tool_start", "tool_use_id": tool_use_id, "name": name, "command": command}

def _evt_tool_result(tool_use_id: str, output: str) -> dict:
    return {"type": "tool_result", "tool_use_id": tool_use_id, "output": output}

def _evt_tool_end(tool_use_id: str) -> dict:
    return {"type": "tool_end", "tool_use_id": tool_use_id}

def _evt_media(media_type: str, path: str, url: str) -> dict:
    return {"type": "media", "media_type": media_type, "path": path, "url": url}

def _evt_document(path: str, url: str, title: str, doc_type: str) -> dict:
    return {"type": "document", "path": path, "url": url, "title": title, "doc_type": doc_type}

def _evt_thinking_chunk(content: str) -> dict:
    return {"type": "thinking_chunk", "content": content}

def _evt_done() -> dict:
    return {"type": "done"}

def _evt_stopped() -> dict:
    return {"type": "stopped"}

def _evt_error(message: str, code: str | None = None) -> dict:
    d: dict = {"type": "error", "message": message}
    if code is not None:
        d["code"] = code
    return d

def _evt_session_warning(message: str) -> dict:
    return {"type": "session_warning", "message": message}

def _evt_session_died(message: str) -> dict:
    return {"type": "session_died", "message": message}

def _evt_session_closed() -> dict:
    return {"type": "session_closed"}

def _evt_resume_progress(stage: str, progress: int | None = None, message: str | None = None) -> dict:
    payload: dict = {"type": "resume_progress", "stage": stage}
    if progress is not None:
        payload["progress"] = progress
    if message:
        payload["message"] = message
    return payload


# ---------------------------------------------------------------------------
# WebSocket-level message builders — include every field, sent directly via ws
# ---------------------------------------------------------------------------

def _msg_pong() -> dict:
    return {"type": "pong"}

def _msg_error(message: str, session_id: str = "") -> dict:
    d: dict = {"type": "error", "message": message}
    if session_id:
        d["session_id"] = session_id
    return d

def _msg_sessions_list(sessions: list[dict]) -> dict:
    return {"type": "sessions_list", "sessions": sessions}

def _msg_session_created(
    session_id: str,
    name: str,
    created_at: float,
    cwd: str,
    backend: str = "claude",
    model: str = "",
    sandbox: str = "danger-full-access",
    image_dir: str = "",
) -> dict:
    payload = {
        "type": "session_created",
        "session_id": session_id,
        "name": name,
        "created_at": created_at,
        "cwd": cwd,
        "backend": backend,
        "sandbox": sandbox,
    }
    if model:
        payload["model"] = model
    if image_dir:
        payload["image_dir"] = image_dir
    return payload

def _msg_session_renamed(session_id: str, name: str) -> dict:
    return {"type": "session_renamed", "session_id": session_id, "name": name}

def _msg_session_history(
    session_id: str,
    messages: list[dict],
    *,
    source_count: int | None = None,
    has_more_before: bool | None = None,
    runtime: dict | None = None,
) -> dict:
    payload: dict = {"type": "session_history", "session_id": session_id, "messages": messages}
    if source_count is not None:
        payload["source_count"] = source_count
    if has_more_before is not None:
        payload["has_more_before"] = has_more_before
    if runtime:
        payload["runtime"] = runtime
    return payload

def _msg_history_snapshot(
    session_id: str,
    messages: list[dict],
    *,
    source_count: int,
    has_more_before: bool,
    known_id_found: bool = True,
    snapshot_reason: str = "",
    runtime: dict | None = None,
) -> dict:
    payload: dict = {
        "type": "history_snapshot",
        "session_id": session_id,
        "messages": messages,
        "source_count": source_count,
        "has_more_before": has_more_before,
        "known_id_found": known_id_found,
    }
    if snapshot_reason:
        payload["snapshot_reason"] = snapshot_reason
    if runtime:
        payload["runtime"] = runtime
    return payload

def _msg_history_delta(
    session_id: str,
    messages: list[dict],
    *,
    after_source_message_id: str,
    source_count: int,
    runtime: dict | None = None,
) -> dict:
    payload: dict = {
        "type": "history_delta",
        "session_id": session_id,
        "after_source_message_id": after_source_message_id,
        "messages": messages,
        "source_count": source_count,
    }
    if runtime:
        payload["runtime"] = runtime
    return payload

def _msg_resumable_sessions(sessions: list[dict]) -> dict:
    return {"type": "resumable_sessions", "sessions": sessions}

def _msg_session_uuid(session_id: str, resume_id: str) -> dict:
    return {"type": "session_uuid", "session_id": session_id, "claude_uuid": resume_id}

def _msg_shell_created(shell_id: str) -> dict:
    return {"type": "shell_created", "shell_id": shell_id}

def _msg_shell_output(shell_id: str, data: str) -> dict:
    return {"type": "shell_output", "shell_id": shell_id, "data": data}

def _msg_shell_closed(shell_id: str) -> dict:
    return {"type": "shell_closed", "shell_id": shell_id}

def _msg_tasks_list(tasks: list[dict]) -> dict:
    return {"type": "tasks_list", "tasks": tasks}

def _msg_task_killed(task_id: str, success: bool) -> dict:
    return {"type": "task_killed", "id": task_id, "success": success}

def _msg_processes_list(processes: list[dict]) -> dict:
    return {"type": "processes_list", "processes": processes}

def _msg_process_killed(pid: int, success: bool, message: str = "") -> dict:
    payload: dict = {"type": "process_killed", "pid": pid, "success": success}
    if message:
        payload["message"] = message
    return payload

def _msg_dir_listing(path: str, entries: list[dict], sessions: list[dict]) -> dict:
    return {"type": "dir_listing", "path": path, "entries": entries, "sessions": sessions}

def _msg_usage_report(
    five_hour: dict | None,
    seven_day: dict | None,
    seven_day_sonnet: dict | None,
) -> dict:
    return {
        "type": "usage_report",
        "five_hour": five_hour,
        "seven_day": seven_day,
        "seven_day_sonnet": seven_day_sonnet,
    }


# ---------------------------------------------------------------------------
# send_event — route session-scoped events; buffer when client is offline
# ---------------------------------------------------------------------------

async def send_event(session: "Session", event: dict) -> None:
    payload = {**event, "session_id": session.session_id}
    if session.current_request_id and "request_id" not in payload and event.get("type") in {
        "text_chunk", "tool_start", "tool_result", "tool_end", "media", "done", "stopped", "error",
    }:
        payload["request_id"] = session.current_request_id
    if _EVENT_DISPATCHER is not None:
        try:
            delivered = await _EVENT_DISPATCHER(payload, session)
            if delivered:
                return
        except Exception:
            pass
    if session.ws_ref is not None:
        try:
            await session.ws_ref.send(json.dumps(payload))
            return
        except Exception:
            session.ws_ref = None
    if len(session.offline_buffer) >= OFFLINE_BUFFER_MAX:
        if payload.get("type") == "text_chunk":
            # Merge into the most recent text_chunk rather than dropping either event
            for i in range(len(session.offline_buffer) - 1, -1, -1):
                last = session.offline_buffer[i]
                if last.get("type") == "text_chunk":
                    session.offline_buffer[i] = {**last, "content": last["content"] + payload["content"]}
                    return
        # No mergeable event; drop the oldest to stay under the cap
        session.offline_buffer.pop(0)
    session.offline_buffer.append(payload)


def set_event_dispatcher(dispatcher: Callable[[dict, "Session"], Awaitable[bool]] | None) -> None:
    global _EVENT_DISPATCHER
    _EVENT_DISPATCHER = dispatcher


# ---------------------------------------------------------------------------
# stream_text — send the full text block as one chunk; no artificial delay
# ---------------------------------------------------------------------------

async def stream_text(text: str, session: "Session") -> None:
    await send_event(session, _evt_text_chunk(text))


# ---------------------------------------------------------------------------
# scan_for_media — detect image/video paths in assistant text, emit media events
# ---------------------------------------------------------------------------

def _extract_html_title(path: str) -> str:
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            content = f.read(4096)
        import re as _re
        m = _re.search(r'<title[^>]*>([^<]+)</title>', content, _re.IGNORECASE)
        if m:
            return m.group(1).strip()
    except Exception:
        pass
    return ""

async def scan_for_media(text: str, session: "Session") -> None:
    from urllib.parse import quote as urlquote
    import logging
    log = logging.getLogger("bridge_v2")
    matches = MEDIA_RE.findall(text)
    for path in matches:
        if not os.path.exists(path):
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext in IMAGE_EXTS:
            media_type = "image"
        elif ext in VIDEO_EXTS:
            media_type = "video"
        elif ext in DOC_EXTS:
            encoded = urlquote(path)
            if _MEDIA_BASE_URL:
                url = f"{_MEDIA_BASE_URL}/media{encoded}"
            else:
                await ensure_http_server()
                url = f"http://127.0.0.1:{HTTP_PORT}{encoded}"
            title = _extract_html_title(path) if ext in {".html", ".htm"} else os.path.basename(path)
            if not title:
                title = os.path.basename(path)
            doc_type = "pdf" if ext == ".pdf" else "html"
            payload = _evt_document(path, url, title, doc_type)
            log.info("Document detected: %s", payload)
            await send_event(session, payload)
            continue
        else:
            continue
        encoded = urlquote(path)
        if _MEDIA_BASE_URL:
            url = f"{_MEDIA_BASE_URL}/media{encoded}"
        else:
            await ensure_http_server()
            url = f"http://127.0.0.1:{HTTP_PORT}{encoded}"
        payload = _evt_media(media_type, path, url)
        log.info("Media detected: %s", payload)
        await send_event(session, payload)
