"""
Shared event/message builder functions and session I/O helpers.
No imports from bridge_v2 — every backend can import from here and
remain independently testable without spinning up the full bridge.
"""
from __future__ import annotations

import asyncio
import os
import re
import secrets
import sys
import contextlib
from typing import TYPE_CHECKING, Awaitable, Callable

if TYPE_CHECKING:
    from bridge_v2 import Session

OFFLINE_BUFFER_MAX = 10000
EVENT_QUEUE_MAX = 10000

# Per-BOOT generation id. Stamped on every session event (and hello_ack) so the
# client can tell a post-restart seq=1 apart from a stale/duplicate pre-restart
# event (the per-session _event_seq counter resets to 0 on every bridge start).
# Generated once at import time = process start; changes every restart.
_GENERATION: str = secrets.token_hex(8)


def get_generation() -> str:
    return _GENERATION
_MEDIA_BASE_URL: str = ""
HTTP_PORT = 9090
_http_server_proc: "asyncio.subprocess.Process | None" = None
_http_serve_dir: str = ""


def set_http_serve_dir(directory: str) -> None:
    global _http_serve_dir
    _http_serve_dir = os.path.realpath(os.path.expanduser(directory)) if directory else ""


async def ensure_http_server() -> None:
    global _http_server_proc
    if _http_server_proc is not None and _http_server_proc.returncode is None:
        return
    serve_dir = _http_serve_dir or os.path.expanduser("~")
    _http_server_proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "http.server", str(HTTP_PORT), "--directory", serve_dir,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )


def set_media_base_url(url: str) -> None:
    global _MEDIA_BASE_URL
    _MEDIA_BASE_URL = url.rstrip("/")

def get_media_base_url() -> str:
    return _MEDIA_BASE_URL
_EVENT_DISPATCHER: Callable[[dict, "Session"], Awaitable[bool]] | None = None

MEDIA_RE = re.compile(
    r'(/(?:[^\s\'"<>]+\.(?:jpg|jpeg|png|gif|webp|mp4|mov|m4v|avi|html|htm|pdf)))',
    re.IGNORECASE,
)
# Relative paths: ./img.png  ../dir/img.png
MEDIA_RE_REL = re.compile(
    r'(?<![/\w])(\.\.?/[^\s\'"<>]+\.(?:jpg|jpeg|png|gif|webp|mp4|mov|m4v|avi|html|htm|pdf))',
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

# Backend-agnostic task/plan list. Folds Claude TodoWrite + TaskCreate/Update/Delete,
# Codex update_plan, and Gemini write_todos into one normalized snapshot. `todos` is
# the full current list (full replace each emit). Item: {id, content, status, activeForm}.
def _evt_todo_update(todos: list) -> dict:
    return {"type": "todo_update", "todos": todos}

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

def _msg_artifacts_list(artifacts: list[dict]) -> dict:
    return {"type": "artifacts_list", "artifacts": artifacts}

def _msg_artifact_created(artifact: dict) -> dict:
    return {"type": "artifact_created", "artifact": artifact}

def _msg_artifact_updated(artifact: dict) -> dict:
    return {"type": "artifact_updated", "artifact": artifact}

def _msg_youtube_task_started(task_id: str, artifact: dict) -> dict:
    return {"type": "youtube_task_started", "task_id": task_id, "artifact": artifact}

def _msg_youtube_task_done(task_id: str, artifacts: list[dict]) -> dict:
    return {"type": "youtube_task_done", "task_id": task_id, "artifacts": artifacts}

def _msg_youtube_task_failed(task_id: str, artifact: dict, message: str) -> dict:
    return {"type": "youtube_task_failed", "task_id": task_id, "artifact": artifact, "message": message}

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

def _msg_agent_tree(session_id: str, tree_data: dict) -> dict:
    return {
        "type": "agent_tree",
        "session_id": session_id,
        **tree_data,  # resume_id, total_agents, tree
    }


# ---------------------------------------------------------------------------
# send_event — route session-scoped events; buffer when client is offline
# ---------------------------------------------------------------------------

# Per-turn streaming content that history will faithfully reproduce. Once a turn
# ends, replaying these on reconnect is redundant (the client fetches a history
# snapshot on open) and only makes a finished turn briefly animate as if it were
# still streaming — so a terminal event collapses them away. Kept in sync with
# the Go bridge's streamingContentEvent (internal/governance/offline.go).
_OFFLINE_STREAMING_CONTENT_TYPES = frozenset({
    "text_chunk", "thinking_chunk", "tool_start", "tool_result",
    "tool_end", "media", "document", "todo_update",
})
_OFFLINE_TERMINAL_TYPES = frozenset({"done", "stopped", "error"})


def _collapse_completed_turn(session: "Session", session_id, request_id) -> None:
    """Drop buffered streaming content for a just-terminated turn, keyed by exact
    (session, request) so an in-flight turn and other sessions are untouched."""
    session.offline_buffer[:] = [
        evt for evt in session.offline_buffer
        if not (
            evt.get("type") in _OFFLINE_STREAMING_CONTENT_TYPES
            and evt.get("session_id") == session_id
            and evt.get("request_id") == request_id
        )
    ]


def _append_offline(session: "Session", payload: dict) -> None:
    # A turn is identified by its request id; without one we cannot tell turns
    # apart, so leave such (rare/legacy) events untouched rather than risk
    # collapsing a different turn's content.
    if (
        payload.get("type") in _OFFLINE_TERMINAL_TYPES
        and payload.get("session_id")
        and payload.get("request_id")
    ):
        _collapse_completed_turn(session, payload["session_id"], payload["request_id"])
    if len(session.offline_buffer) >= OFFLINE_BUFFER_MAX:
        if payload.get("type") == "text_chunk":
            # Merge into the most recent text_chunk rather than dropping either event.
            for i in range(len(session.offline_buffer) - 1, -1, -1):
                last = session.offline_buffer[i]
                if last.get("type") == "text_chunk":
                    session.offline_buffer[i] = {
                        **last,
                        "content": last["content"] + payload["content"],
                    }
                    return
        # No mergeable event; drop the oldest to stay under the cap.
        session.offline_buffer.pop(0)
    session.offline_buffer.append(payload)


def _event_queue(session: "Session") -> asyncio.Queue:
    queue = getattr(session, "_event_queue", None)
    if not isinstance(queue, asyncio.Queue):
        queue = asyncio.Queue(maxsize=EVENT_QUEUE_MAX)
        setattr(session, "_event_queue", queue)
    return queue


def _enqueue_payload(session: "Session", payload: dict) -> None:
    queue = _event_queue(session)
    try:
        queue.put_nowait(payload)
        return
    except asyncio.QueueFull:
        pass

    if payload.get("type") == "text_chunk":
        # asyncio.Queue intentionally hides its deque, but inspecting it here
        # keeps send_event non-blocking while preserving bursty text chunks.
        pending = getattr(queue, "_queue", None)
        if pending is not None:
            for i in range(len(pending) - 1, -1, -1):
                item = pending[i]
                if item.get("type") == "text_chunk":
                    pending[i] = {**item, "content": item["content"] + payload["content"]}
                    return

    try:
        queue.get_nowait()
        queue.task_done()
    except asyncio.QueueEmpty:
        pass
    try:
        queue.put_nowait(payload)
    except asyncio.QueueFull:
        _append_offline(session, payload)


async def _drain_session_events(session: "Session") -> None:
    queue = _event_queue(session)
    try:
        while True:
            payload = await queue.get()
            try:
                delivered = False
                event_sink = getattr(session, "event_sink", None)
                if event_sink is not None:
                    try:
                        delivered = await event_sink.emit(payload, session)
                    except Exception:
                        delivered = False
                if not delivered and _EVENT_DISPATCHER is not None:
                    try:
                        delivered = await _EVENT_DISPATCHER(payload, session)
                    except Exception:
                        delivered = False
                if not delivered:
                    _append_offline(session, payload)
            finally:
                queue.task_done()
    except asyncio.CancelledError:
        pending = getattr(queue, "_queue", None)
        if pending is not None:
            while pending:
                _append_offline(session, pending.popleft())
        raise


def _ensure_session_drain(session: "Session") -> None:
    task = getattr(session, "_event_drain_task", None)
    if isinstance(task, asyncio.Task) and not task.done():
        return
    try:
        task = asyncio.create_task(_drain_session_events(session))
    except RuntimeError:
        # No running loop. This should only happen in unusual tests/import paths;
        # callers will enqueue again once the bridge is running.
        return
    setattr(session, "_event_drain_task", task)


async def stop_session_drain(session: "Session") -> None:
    task = getattr(session, "_event_drain_task", None)
    if not isinstance(task, asyncio.Task):
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    setattr(session, "_event_drain_task", None)


async def flush_session_events(session: "Session") -> None:
    queue = getattr(session, "_event_queue", None)
    if not isinstance(queue, asyncio.Queue):
        return
    await queue.join()


async def send_event(session: "Session", event: dict) -> None:
    payload = {**event, "session_id": session.session_id}
    if session.current_request_id and "request_id" not in payload and event.get("type") in {
        "text_chunk", "thinking_chunk", "tool_start", "tool_result", "tool_end", "media", "done", "stopped", "error",
    }:
        payload["request_id"] = session.current_request_id
    # Stamp a per-session monotonic seq + per-boot gen so the client can detect
    # dropped/missed events (gap in seq) and trigger a reconcile. Atomic: there
    # is no `await` between this read-modify-write and payload construction, so
    # concurrent send_event calls for one session cannot interleave the bump.
    # Stamped here (before dispatch) so every client sees the SAME seq, and the
    # offline_buffer stores the already-stamped payload. NOTE: distinct from
    # session.message_seq (that one is for unread cursors, terminal events only).
    session._event_seq += 1
    payload["seq"] = session._event_seq
    payload["gen"] = _GENERATION
    if getattr(session, "event_sink", None) is None and _EVENT_DISPATCHER is None and session.ws_ref is None:
        _append_offline(session, payload)
        return
    _enqueue_payload(session, payload)
    _ensure_session_drain(session)


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

async def emit_done(session: "Session") -> None:
    """Scan accumulated text for media/document paths, then send done event.

    Call this instead of send_event(session, _evt_done()) so all backends
    get media detection without each needing to import scan_for_media.
    """
    if session.accumulated_text:
        await scan_for_media(session.accumulated_text, session)
        session.accumulated_text = ""
    await send_event(session, _evt_done())


def _local_media_url(abs_path: str) -> str:
    """Return http://127.0.0.1:HTTP_PORT/<rel> where <rel> is path relative to _http_serve_dir."""
    from urllib.parse import quote as urlquote
    serve_dir = _http_serve_dir or os.path.expanduser("~")
    try:
        rel = os.path.relpath(abs_path, serve_dir)
    except ValueError:
        rel = abs_path.lstrip("/")
    # relpath with ".." means file is outside serve_dir — serve anyway but flag
    return f"http://127.0.0.1:{HTTP_PORT}/{urlquote(rel)}"


async def scan_for_media(text: str, session: "Session") -> None:
    from urllib.parse import quote as urlquote
    import logging
    log = logging.getLogger("bridge_v2")

    # Collect absolute + relative paths, dedup while preserving order
    abs_matches = MEDIA_RE.findall(text)
    rel_matches = [
        os.path.normpath(os.path.join(session.cwd or ".", p))
        for p in MEDIA_RE_REL.findall(text)
    ] if session.cwd else []
    seen: set[str] = set()
    all_paths: list[str] = []
    for p in abs_matches + rel_matches:
        if p not in seen:
            seen.add(p)
            all_paths.append(p)

    for path in all_paths:
        if not os.path.exists(path):
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext in IMAGE_EXTS:
            media_type = "image"
        elif ext in VIDEO_EXTS:
            media_type = "video"
        elif ext in DOC_EXTS:
            if _MEDIA_BASE_URL:
                url = f"{_MEDIA_BASE_URL}/media{urlquote(path)}"
            else:
                await ensure_http_server()
                url = _local_media_url(path)
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
        if _MEDIA_BASE_URL:
            url = f"{_MEDIA_BASE_URL}/media{urlquote(path)}"
        else:
            await ensure_http_server()
            url = _local_media_url(path)
        payload = _evt_media(media_type, path, url)
        log.info("Media detected: %s", payload)
        await send_event(session, payload)
