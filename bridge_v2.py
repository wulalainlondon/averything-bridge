#!/usr/bin/env python3
"""
Bridge v2 — Multi-session WebSocket server that proxies Claude Code CLI.
Supports multiple independent concurrent Claude sessions, each backed by a
persistent claude CLI subprocess.

Uses websockets.asyncio.server API (websockets >= 14).
Default port: 8766 (v1 keeps 8765).
"""

import argparse
import asyncio
from collections import deque
import http
import json
import logging
import mimetypes
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import time
import sys
import uuid

# Raise file descriptor limit before any subsystem opens files (search ingest +
# watchdog kqueue observer can consume thousands of fds).
# macOS launchd default is 256; plist HardResourceLimits may cap at 8192.
# Strategy: always attempt to raise to _WANT_FDS; try relaxing hard limit too.
_WANT_FDS = 65536
_TURN_ABORTED_RE = re.compile(r"<turn_aborted>.*?</turn_aborted>", re.IGNORECASE | re.DOTALL)
try:
    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # Determine the new hard: prefer _WANT_FDS; if the kernel hard is lower and
    # > 0 (i.e. not RLIM_INFINITY) we must stay at or below it — but on macOS
    # non-root processes can raise the hard up to the system kern.maxfilesperproc
    # limit even if launchd set a lower hard, so try _WANT_FDS unconditionally.
    _new_hard = _WANT_FDS if (_hard <= 0 or _hard < _WANT_FDS) else _hard
    _new_soft = _WANT_FDS
    resource.setrlimit(resource.RLIMIT_NOFILE, (_new_soft, _new_hard))
except (ValueError, OSError):
    # Couldn't raise hard limit; try raising soft only up to existing hard.
    try:
        _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        _cap = _hard if _hard > 0 else _WANT_FDS
        if _soft < _cap:
            resource.setrlimit(resource.RLIMIT_NOFILE, (_cap, _hard))
    except (ValueError, OSError):
        pass
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Literal, NotRequired, Optional, TYPE_CHECKING, TypedDict

from websockets.asyncio.server import serve, ServerConnection
from websockets.http11 import Response as WsResponse
from websockets.datastructures import Headers as WsHeaders
from handlers.file_ops import handle_file_msg, preload_sessions_cache, invalidate_sessions_cache
from handlers.perf import PerfTracker
from handlers.runtime_ops import handle_runtime_msg
from handlers.system_ops import handle_system_msg
from handlers.webrtc_signaling import (
    WEBRTC_MESSAGE_TYPES,
    cleanup_for_ws as _webrtc_cleanup_for_ws,
    handle_webrtc_message,
)
from utils.uuid_helper import is_valid_uuid

try:
    import socket
    from zeroconf import ServiceInfo, Zeroconf
    _ZEROCONF_AVAILABLE = True
except ImportError:
    _ZEROCONF_AVAILABLE = False

try:
    from config import get_config
    from search.ingest import start_worker, stop_worker, get_worker
    from search.query import ConnectionPool
    from handlers.search_ws import handle_search_message
    _SEARCH_AVAILABLE = True
    _SEARCH_IMPORT_ERR: "str | None" = None
except ImportError as _ie:
    _SEARCH_AVAILABLE = False
    _SEARCH_IMPORT_ERR = str(_ie)
from backends.events import (
    send_event, stream_text, scan_for_media, set_media_base_url,
    _evt_error, _evt_done, _evt_stopped, _evt_session_warning, _evt_session_died, _evt_session_closed,
    _evt_resume_progress,
    _evt_text_chunk, _evt_tool_start, _evt_tool_result, _evt_tool_end, _evt_media,
    _msg_pong, _msg_error, _msg_sessions_list, _msg_session_created, _msg_session_renamed,
    _msg_session_history, _msg_history_snapshot, _msg_history_delta, _msg_resumable_sessions, _msg_session_uuid,
    _msg_shell_created, _msg_shell_output, _msg_shell_closed,
    _msg_tasks_list, _msg_task_killed, _msg_processes_list, _msg_process_killed, _msg_dir_listing, _msg_usage_report,
    set_event_dispatcher,
)
from backends.history import DEFAULT_HISTORY_LIMIT, clamp_history_limit
from backends.history_sqlite import init_cache_db

try:
    import firebase_admin
    from firebase_admin import credentials, messaging as fb_messaging, storage as fb_storage
    _FIREBASE_AVAILABLE = True
except ImportError:
    _FIREBASE_AVAILABLE = False

BRIDGE_DIR           = os.path.dirname(os.path.abspath(__file__))
LOG_FILE             = os.path.join(BRIDGE_DIR, "bridge_v2.log")
CLAUDE_BIN           = ""
CODEX_BIN            = ""
BUN_BIN              = ""
DEFAULT_CWD          = os.path.expanduser(os.environ.get("BRIDGE_DEFAULT_CWD", "") or "~")
MAX_SESSIONS         = 0
MAX_SHELLS           = 5
FCM_TOKEN_FILE        = os.path.join(BRIDGE_DIR, "fcm_token.txt")
SERVICE_ACCOUNT_FILE  = os.path.join(BRIDGE_DIR, "serviceAccountKey.json")
SAVED_SESSIONS_FILE   = os.path.join(BRIDGE_DIR, "saved_sessions.json")
CODEX_SAVED_SESSIONS_FILE = os.path.join(BRIDGE_DIR, "saved_sessions_codex.json")
SESSION_META_FILE     = os.path.join(BRIDGE_DIR, "session_meta.json")
READ_CURSOR_FILE      = os.path.join(BRIDGE_DIR, "read_cursors.json")
CLAUDE_PROJECTS_DIR   = os.path.expanduser("~/.claude/projects")

def _find_claude_bin() -> str:
    env = os.environ.get("CLAUDE_PATH")
    if env and os.path.isfile(env):
        return env
    found = shutil.which("claude")
    if found:
        return found
    candidates = [
        "~/.npm-global/bin/claude",
        "~/.local/bin/claude",
        "~/.bun/bin/claude",
        "/usr/local/bin/claude",
        "/opt/homebrew/bin/claude",
    ]
    for c in candidates:
        p = os.path.expanduser(c)
        if os.path.isfile(p):
            return p
    print("ERROR: claude binary not found. Set CLAUDE_PATH env var or ensure claude is on PATH.")
    sys.exit(1)


def _find_bun_bin() -> str:
    env = os.environ.get("BUN_PATH")
    if env and os.path.isfile(env):
        return env
    found = shutil.which("bun")
    if found:
        return found
    candidates = [
        "/opt/homebrew/bin/bun",
        "~/.bun/bin/bun",
        "/usr/local/bin/bun",
    ]
    for c in candidates:
        p = os.path.expanduser(c)
        if os.path.isfile(p):
            return p
    return "bun"


def _find_codex_bin() -> str:
    env = os.environ.get("CODEX_PATH")
    if env and os.path.isfile(env):
        return env
    found = shutil.which("codex")
    if found:
        return found
    candidates = [
        "~/.npm-global/bin/codex",
        "~/.local/bin/codex",
        "/usr/local/bin/codex",
        "/opt/homebrew/bin/codex",
    ]
    for c in candidates:
        p = os.path.expanduser(c)
        if os.path.isfile(p):
            return p
    print("ERROR: codex binary not found. Set CODEX_PATH env var or ensure codex is on PATH.")
    sys.exit(1)


def _detect_tailscale_ip() -> "str | None":
    ts = shutil.which("tailscale")
    if not ts:
        return None
    try:
        result = subprocess.run([ts, "ip", "-4"], capture_output=True, text=True, timeout=3)
        ip = result.stdout.strip().split("\n")[0]
        return ip if ip else None
    except Exception:
        return None


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("bridge_v2")

if not _SEARCH_AVAILABLE:
    log.warning("[search] module unavailable: %s; search disabled", _SEARCH_IMPORT_ERR)

# ---------------------------------------------------------------------------
# Search subsystem state
# ---------------------------------------------------------------------------
_search_pool: Optional[ConnectionPool] = None
_search_enabled: bool = False

SEARCH_MESSAGE_TYPES: frozenset[str] = frozenset({
    "request_search",
    "request_search_health",
    "request_session_list",
    "request_search_context",
})


async def _init_search() -> None:
    global _search_pool, _search_enabled
    if not _SEARCH_AVAILABLE:
        _search_enabled = False
        return
    cfg = get_config()
    if not cfg.search.enabled:
        log.info("[search] disabled via config")
        _search_enabled = False
        return
    try:
        await start_worker(cfg)
        _search_pool = ConnectionPool(cfg.search.index_path, max_size=4)
        _search_enabled = True
        log.info("[search] worker started; index at %s", cfg.search.index_path)
    except Exception as e:
        log.error("[search] failed to start (%r); bridge will run without search", e)
        _search_enabled = False


async def _shutdown_search() -> None:
    global _search_pool, _search_enabled
    _search_enabled = False
    if _search_pool is not None:
        await _search_pool.close_all()
        _search_pool = None
    if _SEARCH_AVAILABLE:
        await stop_worker()


async def _dispatch_ws_message(ws, msg: dict) -> bool:
    """Returns True if handled by search layer."""
    if not _search_enabled or _search_pool is None:
        return False
    t = msg.get("type")
    if t not in SEARCH_MESSAGE_TYPES:
        return False
    try:
        await handle_search_message(ws, msg, pool=_search_pool)
    except Exception as e:
        log.exception("[search] handler raised on %s", t)
        try:
            await ws.send(json.dumps({"type": f"{t}_error", "message": str(e)}))
        except Exception:
            pass
    return True


# ---------------------------------------------------------------------------
# Inbound message schema (TypedDict = IDE hints; validate_client_msg = runtime)
# ---------------------------------------------------------------------------

class PingMsg(TypedDict):
    type: Literal["ping"]

class MessageMsg(TypedDict):
    type: Literal["message"]
    session_id: str
    content: NotRequired[str]
    images: NotRequired[list]
    files: NotRequired[list]

class NewSessionMsg(TypedDict):
    type: Literal["new_session"]
    session_id: str
    name: str
    cwd: NotRequired[str]
    resume_claude_id: NotRequired[str]
    backend: NotRequired[str]
    model: NotRequired[str]
    sandbox: NotRequired[str]
    image_dir: NotRequired[str]

class StopMsg(TypedDict):
    type: Literal["stop"]
    session_id: str

class CloseSessionMsg(TypedDict):
    type: Literal["close_session"]
    session_id: str

class RenameSessionMsg(TypedDict):
    type: Literal["rename_session"]
    session_id: str
    name: str

class ClearSessionMsg(TypedDict):
    type: Literal["clear_session"]
    session_id: str

class GetUsageMsg(TypedDict):
    type: Literal["get_usage"]

class GetResumableSessionsMsg(TypedDict):
    type: Literal["get_resumable_sessions"]

class ShellCreateMsg(TypedDict):
    type: Literal["shell_create"]
    cwd: NotRequired[str]

class ShellInputMsg(TypedDict):
    type: Literal["shell_input"]
    shell_id: str
    data: str

class ShellCloseMsg(TypedDict):
    type: Literal["shell_close"]
    shell_id: str

class GetTasksMsg(TypedDict):
    type: Literal["get_tasks"]

class KillTaskMsg(TypedDict):
    type: Literal["kill_task"]
    id: str

class GetProcessesMsg(TypedDict):
    type: Literal["get_processes"]

class KillProcessMsg(TypedDict):
    type: Literal["kill_process"]
    pid: int
    force: NotRequired[bool]

class FcmTokenMsg(TypedDict):
    type: Literal["fcm_token"]
    token: str

class RequestSessionsListMsg(TypedDict):
    type: Literal["request_sessions_list"]

class BrowseDirMsg(TypedDict):
    type: Literal["browse_dir"]
    path: NotRequired[str]

class HelloMsg(TypedDict):
    type: Literal["hello"]
    device_id: NotRequired[str]
    device_name: NotRequired[str]

class SetSessionMetaMsg(TypedDict):
    type: Literal["set_session_meta"]
    session_id: str
    pinned: NotRequired[bool]
    hidden: NotRequired[bool]

class SwitchSessionConfigMsg(TypedDict):
    type: Literal["switch_session_config"]
    session_id: str
    backend: NotRequired[str]
    model: NotRequired[str]
    effort: NotRequired[str]
    sandbox: NotRequired[str]
    image_dir: NotRequired[str]


# Required fields (name → [(field, type), ...]) — checked at runtime
_INBOUND_REQUIRED: dict[str, list[tuple[str, type]]] = {
    "new_session":     [("session_id", str), ("name", str)],
    "message":         [("session_id", str)],
    "stop":            [("session_id", str)],
    "close_session":   [("session_id", str)],
    "rename_session":  [("session_id", str), ("name", str)],
    "clear_session":   [("session_id", str)],
    "shell_input":     [("shell_id", str), ("data", str)],
    "shell_close":     [("shell_id", str)],
    "kill_task":       [("id", str)],
    "kill_process":    [("pid", int)],
    "browse_dir":      [],
    "request_history": [("session_id", str)],
    "set_effort":      [("session_id", str), ("effort", str)],
    "set_session_meta":[("session_id", str)],
    "switch_session_config":[("session_id", str)],
}

_KNOWN_MSG_TYPES: frozenset[str] = frozenset({
    "ping", "message", "new_session", "stop", "close_session",
    "rename_session", "clear_session", "get_usage", "get_resumable_sessions",
    "shell_create", "shell_input", "shell_close", "get_tasks", "kill_task",
    "get_processes", "kill_process",
    "fcm_token", "request_sessions_list", "browse_dir", "request_history",
    "set_effort", "hello", "set_session_meta", "switch_session_config",
    "push_file", "file_push_ack",
    "get_all_sessions",
    # search subsystem
    "request_search", "request_search_health", "request_session_list",
    "request_search_context",
    # WebRTC P2P signaling (handled by handlers.webrtc_signaling)
    "webrtc_offer", "webrtc_answer", "webrtc_ice",
})

def validate_client_msg(msg: object) -> str | None:
    """Return an error description, or None if the message is valid."""
    if not isinstance(msg, dict):
        return "message must be a JSON object"
    mtype = msg.get("type")
    if not isinstance(mtype, str):
        return "missing or non-string 'type' field"
    if mtype not in _KNOWN_MSG_TYPES:
        return f"unknown message type '{mtype}'"
    for field_name, expected_type in _INBOUND_REQUIRED.get(mtype, []):
        val = msg.get(field_name)
        if val is None:
            return f"'{mtype}' missing required field '{field_name}'"
        if not isinstance(val, expected_type):
            return f"'{mtype}.{field_name}' must be {expected_type.__name__}, got {type(val).__name__}"
    return None


if TYPE_CHECKING:
    from backends.base import Backend

# ---------------------------------------------------------------------------
# Backend registry/config
# ---------------------------------------------------------------------------
_BACKENDS: dict[str, "Backend"] = {}
_DEFAULT_BACKEND_NAME = "claude"
_DEFAULT_OLLAMA_MODEL = "llama3.2"
_OLLAMA_HOST = "http://localhost:11434"

# ---------------------------------------------------------------------------
# Global session registry
# ---------------------------------------------------------------------------
_SESSIONS: Dict[str, "Session"] = {}
_SESSIONS_LOCK = asyncio.Lock()

# ---------------------------------------------------------------------------
# Shell sessions
# ---------------------------------------------------------------------------
@dataclass
class ShellSession:
    shell_id: str
    proc: asyncio.subprocess.Process
    ws_ref: Any
    cwd: str = ""
    read_task: Optional[asyncio.Task] = field(default=None, repr=False)

_SHELL_SESSIONS: Dict[str, "ShellSession"] = {}
_READ_CURSORS: dict[str, dict[str, int]] = {}


@dataclass
class ClientConn:
    client_id: str
    device_id: str
    device_name: str
    ws: Any
    connected_at: float
    last_seen: float


@dataclass
class QueuedCommand:
    request_id: str
    device_id: str
    client_id: str
    content: str
    images: list | None
    files: list | None
    enqueued_at: float


def _normalize_backend_name(raw: str | None) -> str:
    name = (raw or "").strip().lower()
    return name if name in {"claude", "codex", "ollama"} else _DEFAULT_BACKEND_NAME


def _get_or_create_backend(name: str) -> "Backend":
    global CLAUDE_BIN, CODEX_BIN, BUN_BIN
    backend_name = _normalize_backend_name(name)
    existing = _BACKENDS.get(backend_name)
    if existing is not None:
        return existing

    if backend_name == "codex":
        if not CODEX_BIN:
            CODEX_BIN = _find_codex_bin()
        from backends.codex_appserver import CodexAppServerBackend
        backend = CodexAppServerBackend(codex_bin=CODEX_BIN, broadcast_fn=_broadcast_json)
    elif backend_name == "ollama":
        from backends.ollama import OllamaBackend
        backend = OllamaBackend(model=_DEFAULT_OLLAMA_MODEL, host=_OLLAMA_HOST)
    else:
        if not CLAUDE_BIN:
            CLAUDE_BIN = _find_claude_bin()
        if not BUN_BIN:
            BUN_BIN = _find_bun_bin()
        from backends.claude_cli import ClaudeCliBackend
        backend = ClaudeCliBackend(
            claude_bin=CLAUDE_BIN,
            bun_bin=BUN_BIN,
            notify_fcm_fn=notify_fcm,
            persist_session_fn=_persist_session,
            claude_projects_dir=CLAUDE_PROJECTS_DIR,
            load_saved_sessions_fn=_load_saved_sessions,
            broadcast_fn=_broadcast_json,
        )

    _BACKENDS[backend_name] = backend
    return backend


def _session_backend(session: "Session") -> "Backend":
    return _get_or_create_backend(session.backend_name)


async def _emit_resume_progress(
    session: "Session",
    stage: str,
    progress: int | None = None,
    message: str | None = None,
) -> None:
    await send_event(session, _evt_resume_progress(stage, progress, message))


async def _shell_reader(shell: "ShellSession") -> None:
    try:
        while True:
            line = await shell.proc.stdout.readline()
            if not line:
                break
            if shell.ws_ref:
                try:
                    await shell.ws_ref.send(json.dumps(
                        _msg_shell_output(shell.shell_id, line.decode("utf-8", errors="replace"))
                    ))
                except Exception:
                    break
    except Exception:
        pass
    _SHELL_SESSIONS.pop(shell.shell_id, None)
    if shell.ws_ref:
        try:
            await shell.ws_ref.send(json.dumps(_msg_shell_closed(shell.shell_id)))
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Session dataclass
# ---------------------------------------------------------------------------
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


# Legacy alias — keeps any remaining references working during transition
ClaudeSession = Session


async def _load_session_history_for_transfer(session: "Session", limit: int = 80) -> list[dict]:
    try:
        if not session.resume_id:
            return []
        backend = _session_backend(session)
        if not backend.supports_resume():
            return []
        history = await backend.load_session_history(session.resume_id, limit=limit, mode="snapshot")
        if isinstance(history, dict):
            return history.get("messages", []) if isinstance(history.get("messages"), list) else []
        return history if isinstance(history, list) else []
    except Exception:
        return []


def _history_runtime_payload(session: "Session") -> dict | None:
    if not session.is_streaming and not session.processing:
        return None
    return {
        "streaming": bool(session.is_streaming or session.processing),
        "current_request_id": session.current_request_id or "",
        "accumulated_text": session.accumulated_text or "",
    }


async def _send_session_history_response(
    ws: Any,
    session: "Session",
    *,
    limit: object = None,
    known_last_source_message_id: str = "",
    mode: str = "auto",
    before_source_message_id: str = "",
) -> None:
    if not session.resume_id:
        runtime = _history_runtime_payload(session)
        await ws.send(json.dumps(_msg_session_history(session.session_id, [], source_count=0, has_more_before=False, runtime=runtime)))
        return
    backend = _session_backend(session)
    if not backend.supports_resume():
        runtime = _history_runtime_payload(session)
        await ws.send(json.dumps(_msg_session_history(session.session_id, [], source_count=0, has_more_before=False, runtime=runtime)))
        return
    n = clamp_history_limit(limit or DEFAULT_HISTORY_LIMIT)
    history = await backend.load_session_history(
        session.resume_id,
        limit=n,
        known_last_source_message_id=known_last_source_message_id,
        mode=mode,
        before_source_message_id=before_source_message_id,
    )
    runtime = _history_runtime_payload(session)
    if isinstance(history, dict):
        kind = history.get("kind")
        messages = history.get("messages", [])
        if not isinstance(messages, list):
            messages = []
        if kind == "delta":
            payload = _msg_history_delta(
                session.session_id,
                messages,
                after_source_message_id=known_last_source_message_id,
                source_count=int(history.get("source_count") or len(messages)),
                runtime=runtime,
            )
        else:
            payload = _msg_history_snapshot(
                session.session_id,
                messages,
                source_count=int(history.get("source_count") or len(messages)),
                has_more_before=bool(history.get("has_more_before")),
                known_id_found=bool(history.get("known_id_found", True)),
                snapshot_reason=str(history.get("snapshot_reason") or ""),
                runtime=runtime,
            )
        await ws.send(json.dumps(payload))
        return
    messages = history if isinstance(history, list) else []
    await ws.send(json.dumps(_msg_session_history(session.session_id, messages, runtime=runtime)))


def _build_handoff_prompt(history: list[dict], user_request: str = "") -> str:
    lines: list[str] = [
        "Context handoff from previous session. Continue seamlessly.",
        "Use the transcript below as prior context.",
        "",
    ]
    for item in history[-80:]:
        role = str(item.get("role", "user")).upper()
        content = str(item.get("content", ""))
        lines.append(f"{role}:")
        lines.append(content)
        lines.append("")
    if user_request.strip():
        lines.append("LATEST USER REQUEST:")
        lines.append(user_request.strip())
    else:
        lines.append("LATEST USER REQUEST:")
        lines.append("Please continue from the latest point with the same task.")
    return "\n".join(lines)



# ---------------------------------------------------------------------------
# Firebase Admin init
# ---------------------------------------------------------------------------
_firebase_app = None
_firebase_storage_app = None  # separate app for Storage (may use different project)

_PUSH_FILE_REGISTRY: dict[str, dict] = {}  # file_id → {blob_path, filename, url, size, mime_type, target_device_ids, acked_device_ids}

_STORAGE_KEY_FILE = os.environ.get(
    "BRIDGE_STORAGE_KEY",
    os.path.expanduser("~/.claude/line/ulala-helper-firebase-adminsdk-fbsvc-d4353102d1.json"),
)

def init_firebase() -> None:
    global _firebase_app, _firebase_storage_app
    if not _FIREBASE_AVAILABLE:
        log.warning("firebase-admin not installed — FCM disabled. Run: pip install firebase-admin")
        return

    # FCM app (averthing project)
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        try:
            cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
            _firebase_app = firebase_admin.initialize_app(cred)
            log.info("Firebase FCM initialized")
        except Exception as exc:
            log.warning("Firebase FCM init failed: %s", exc)
    else:
        log.warning("serviceAccountKey.json not found at %s — FCM disabled", SERVICE_ACCOUNT_FILE)

    # Storage app — use ulala-helper key if available, otherwise fall back to FCM key
    storage_key = _STORAGE_KEY_FILE if os.path.exists(_STORAGE_KEY_FILE) else SERVICE_ACCOUNT_FILE
    if os.path.exists(storage_key):
        try:
            with open(storage_key) as f:
                sk = json.load(f)
            bucket_name = f"{sk['project_id']}.firebasestorage.app"
            storage_cred = credentials.Certificate(storage_key)
            _firebase_storage_app = firebase_admin.initialize_app(storage_cred, {"storageBucket": bucket_name}, name="storage")
            log.info("Firebase Storage initialized (bucket: %s)", bucket_name)
        except Exception as exc:
            log.warning("Firebase Storage init failed: %s", exc)


async def _handle_push_file(ws: Any, path: str, sender_device_id: str = "") -> None:
    if _firebase_storage_app is None:
        try:
            await ws.send(json.dumps({"type": "error", "message": "Firebase Storage not available — check storage key"}))
        except Exception:
            pass
        return

    expanded = os.path.expanduser(path)
    if not os.path.isfile(expanded):
        try:
            await ws.send(json.dumps({"type": "error", "message": f"File not found: {path}"}))
        except Exception:
            pass
        return

    filename = os.path.basename(expanded)
    size = os.path.getsize(expanded)
    file_id = f"push_{uuid.uuid4().hex[:12]}"
    blob_path = f"bridge_pushes/{file_id}/{filename}"

    import mimetypes
    mime_type, _ = mimetypes.guess_type(filename)
    if not mime_type:
        mime_type = "application/octet-stream"

    try:
        loop = asyncio.get_event_loop()
        bucket = fb_storage.bucket(app=_firebase_storage_app)

        def _upload() -> str:
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(expanded, content_type=mime_type)
            import datetime
            url = blob.generate_signed_url(
                expiration=datetime.timedelta(hours=1),
                method="GET",
                version="v4",
            )
            return url

        url = await loop.run_in_executor(None, _upload)
        target_device_ids = sorted({
            c.device_id
            for c in _CLIENTS.values()
            if getattr(c, "device_id", "")
        } | ({sender_device_id} if sender_device_id else set()))
        _PUSH_FILE_REGISTRY[file_id] = {
            "blob_path": blob_path,
            "filename": filename,
            "url": url,
            "size": size,
            "mime_type": mime_type,
            "target_device_ids": target_device_ids,
            "acked_device_ids": [],
        }
        log.info("push_file uploaded: %s → %s", filename, blob_path)

        await _broadcast_json({
            "type": "file_push",
            "file_id": file_id,
            "filename": filename,
            "url": url,
            "size": size,
            "mime_type": mime_type,
        })
        asyncio.create_task(notify_fcm("Bridge", f"📎 {filename}", ""))
    except Exception as exc:
        log.warning("push_file upload failed: %s", exc)
        try:
            await ws.send(json.dumps({"type": "error", "message": f"Upload failed: {exc}"}))
        except Exception:
            pass


async def _handle_file_push_ack(file_id: str, device_id: str = "") -> None:
    entry = _PUSH_FILE_REGISTRY.get(file_id)
    if not entry:
        log.debug("file_push_ack: unknown file_id %s", file_id)
        return
    target = set(entry.get("target_device_ids") or [])
    acked = set(entry.get("acked_device_ids") or [])
    if device_id:
        acked.add(device_id)
        entry["acked_device_ids"] = sorted(acked)
    should_delete = (not target) or target.issubset(acked)
    if not should_delete:
        return
    _PUSH_FILE_REGISTRY.pop(file_id, None)
    if _firebase_storage_app is None:
        return
    blob_path = entry["blob_path"]
    try:
        loop = asyncio.get_event_loop()
        bucket = fb_storage.bucket(app=_firebase_storage_app)

        def _delete() -> None:
            blob = bucket.blob(blob_path)
            blob.delete()

        await loop.run_in_executor(None, _delete)
        log.info("push_file deleted from storage: %s", blob_path)
    except Exception as exc:
        log.warning("push_file delete failed: %s", exc)


# ---------------------------------------------------------------------------
# FCM notification
# ---------------------------------------------------------------------------
async def _notify_fcm_tunnel(ws_url: str) -> None:
    if _firebase_app is None:
        return
    try:
        with open(FCM_TOKEN_FILE) as f:
            token = f.read().strip()
    except FileNotFoundError:
        log.warning("FCM tunnel notify: no token on file")
        return
    try:
        message = fb_messaging.Message(
            data={"type": "tunnel_url", "url": ws_url},
            token=token,
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, fb_messaging.send, message)
        log.info("FCM tunnel URL pushed to device: %s", ws_url)
    except Exception as exc:
        log.warning("FCM tunnel notify failed: %s", exc)


async def notify_fcm(session_name: str, last_text: str, session_id: str = "") -> None:
    if _firebase_app is None:
        log.warning("FCM not ready, skipping notification")
        return
    try:
        with open(FCM_TOKEN_FILE) as f:
            token = f.read().strip()
    except FileNotFoundError:
        log.warning("No FCM token on file — skipping notification")
        return

    # Extract first meaningful line, strip markdown markers
    import re as _re
    clean = _re.sub(r'[*`#_~>]+', '', last_text).strip()
    summary = ""
    for line in clean.splitlines():
        line = line.strip()
        if line:
            summary = line[:160]
            break
    if not summary:
        summary = clean[:160]

    message = fb_messaging.Message(
        notification=fb_messaging.Notification(
            title=f"✓ {session_name}",
            body=summary,
        ),
        data={"session_id": session_id},
        token=token,
    )
    loop = asyncio.get_event_loop()
    for attempt in range(3):
        try:
            await loop.run_in_executor(None, fb_messaging.send, message)
            log.info("FCM notification sent")
            return
        except Exception as exc:
            if attempt < 2:
                wait = 2 ** attempt
                log.warning("FCM send failed (attempt %d/3): %s — retrying in %ds", attempt + 1, exc, wait)
                await asyncio.sleep(wait)
            else:
                log.error("FCM send failed after 3 attempts: %s", exc)


# ---------------------------------------------------------------------------
# Saved sessions (persistence helpers — used by _persist_session, _restore_sessions_from_disk)
# ---------------------------------------------------------------------------
def _load_saved_sessions() -> dict:
    try:
        with open(SAVED_SESSIONS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _migrate_codex_saved_sessions() -> None:
    """Merge legacy Codex metadata into saved_sessions.json without copying history."""
    try:
        with open(CODEX_SAVED_SESSIONS_FILE, encoding="utf-8") as f:
            legacy = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return
    if not isinstance(legacy, dict):
        return
    saved = _load_saved_sessions()
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
            "cwd": current.get("cwd") or entry.get("cwd") or DEFAULT_CWD,
            "backend": "codex",
            "model": current.get("model") or entry.get("model") or "",
            "sandbox": current.get("sandbox") or entry.get("sandbox") or "danger-full-access",
            "image_dir": current.get("image_dir") or entry.get("image_dir") or "",
        }
        changed = True
    if changed:
        try:
            with open(SAVED_SESSIONS_FILE, "w", encoding="utf-8") as f:
                json.dump(saved, f, indent=2, ensure_ascii=False)
        except Exception as exc:
            log.warning("Failed to migrate Codex saved session metadata: %s", exc)

def _persist_session(session: "Session") -> None:
    saved = _load_saved_sessions()
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
    # Prune entries older than 90 days (keep at most 200)
    cutoff = int(time.time()) - 90 * 24 * 3600
    saved = {
        k: v for k, v in saved.items()
        if v.get("last_used", 0) > cutoff
    }
    if len(saved) > 200:
        saved = dict(sorted(saved.items(), key=lambda x: x[1].get("last_used", 0), reverse=True)[:200])
    try:
        with open(SAVED_SESSIONS_FILE, "w") as f:
            json.dump(saved, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        log.warning("Failed to persist session: %s", exc)


def _restore_sessions_from_disk() -> None:
    """Load saved_sessions.json into _SESSIONS so sessions survive bridge restarts."""
    # Prune stale entries before loading so _SESSIONS never contains sessions
    # older than 30 days.  This eliminates the bulk-resume thundering-herd that
    # occurs when auto_register accumulates thousands of old entries.
    from auto_register import prune_old_saved_sessions
    prune_old_saved_sessions(Path(SAVED_SESSIONS_FILE), days=30)

    saved = _load_saved_sessions()

    # Drop entries whose resume ID is not a valid UUID so bridge never tries
    # to spawn claude --resume with a bad ID (which causes 3 auto-retry cycles
    # and a supervisor kill loop).  Accept both "resume_id" (new) and the
    # legacy "claude_uuid" key for backward compat.
    dropped: list[str] = []
    for sid, data in list(saved.items()):
        backend = data.get("backend", "claude")
        claude_uuid = data.get("resume_id") or data.get("claude_uuid", "") or ""
        if backend in ("claude", "codex") and claude_uuid and not is_valid_uuid(claude_uuid):
            dropped.append(sid)
            del saved[sid]
            log.info("dropping saved session %s: invalid UUID %r", sid, claude_uuid)
    if dropped:
        try:
            with open(SAVED_SESSIONS_FILE, "w", encoding="utf-8") as f:
                json.dump(saved, f, indent=2, ensure_ascii=False)
            log.info("saved_sessions.json: dropped %d invalid session(s): %s", len(dropped), dropped)
        except Exception as exc:
            log.warning("Failed to rewrite saved_sessions.json after dropping invalid UUIDs: %s", exc)

    count = 0
    for sid, data in saved.items():
        if sid in _SESSIONS:
            continue
        try:
            saved_last_used = float(data.get("last_used") or time.time())
            session = Session(
                session_id=sid,
                name=data.get("name", sid[:8]),
                created_at=saved_last_used,
                cwd=os.path.expanduser(data.get("cwd") or DEFAULT_CWD),
                backend_name=_normalize_backend_name(data.get("backend")),
                model=str(data.get("model") or ""),
                sandbox=str(data.get("sandbox") or "danger-full-access"),
                image_dir=str(data.get("image_dir") or ""),
            )
            # Accept both "resume_id" (new canonical) and "claude_uuid" (legacy) keys.
            session.resume_id = data.get("resume_id") or data.get("claude_uuid") or None
            # last_activity is set from saved last_used which auto_register writes
            # as jsonl mtime — so sort order reflects real conversation activity.
            session.last_activity = saved_last_used
            _SESSIONS[sid] = session
            count += 1
        except Exception as exc:
            log.warning("Failed to restore session %s: %s", sid, exc)
    if count:
        log.info("Restored %d session(s) from disk", count)


def _load_session_meta() -> dict[str, dict]:
    try:
        with open(SESSION_META_FILE) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _persist_session_meta() -> None:
    payload = {
        sid: {"pinned": bool(s.pinned), "hidden": bool(s.hidden)}
        for sid, s in _SESSIONS.items()
        if s.pinned or s.hidden
    }
    try:
        with open(SESSION_META_FILE, "w") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        log.warning("Failed to persist session metadata: %s", exc)


def _apply_session_meta() -> None:
    meta = _load_session_meta()
    applied = 0
    for sid, val in meta.items():
        s = _SESSIONS.get(sid)
        if not s or not isinstance(val, dict):
            continue
        s.pinned = bool(val.get("pinned", False))
        s.hidden = bool(val.get("hidden", False))
        applied += 1
    if applied:
        log.info("Applied metadata for %d session(s)", applied)


def _load_read_cursors() -> dict[str, dict[str, int]]:
    try:
        with open(READ_CURSOR_FILE) as f:
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


def _persist_read_cursors() -> None:
    try:
        with open(READ_CURSOR_FILE, "w") as f:
            json.dump(_READ_CURSORS, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        log.warning("Failed to persist read cursors: %s", exc)


def _mark_read(session_id: str, device_id: str, seq: int) -> None:
    if not device_id:
        return
    dev_map = _READ_CURSORS.setdefault(session_id, {})
    dev_map[device_id] = max(int(seq), int(dev_map.get(device_id, 0)))


def _unread_for(session: "Session", device_id: str) -> int:
    if not device_id:
        return 0
    read = _READ_CURSORS.get(session.session_id, {}).get(device_id, 0)
    return max(0, int(session.message_seq) - int(read))


async def _broadcast_json(payload: dict) -> None:
    dead: list[Any] = []
    raw = json.dumps(payload)
    for ws, client in list(_CLIENTS.items()):
        try:
            await ws.send(raw)
            client.last_seen = time.time()
        except Exception:
            dead.append(ws)
    for ws in dead:
        _CLIENTS.pop(ws, None)


async def _send_unread_for_session(session: "Session") -> None:
    dead: list[Any] = []
    for ws, client in list(_CLIENTS.items()):
        unread = _unread_for(session, client.device_id)
        payload = {"type": "session_unread", "session_id": session.session_id, "unread": unread}
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            dead.append(ws)
    for ws in dead:
        _CLIENTS.pop(ws, None)


async def _send_unread_snapshot(ws: Any, client: ClientConn) -> None:
    # Push unread for EVERY session, including count=0. Client persists unread
    # to AsyncStorage; if we skip the zeros, stale unread badges from previous
    # sessions stay forever and break the dashboard sort (unread > 0 sessions
    # get sticky-pinned at the top by sortSessions). Must explicitly send 0
    # so client setUnread resets the badge.
    for s in list(_SESSIONS.values()):
        count = _unread_for(s, client.device_id)
        payload = {"type": "session_unread", "session_id": s.session_id, "unread": count}
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            return


async def _send_unread_for_client_session(ws: Any, client: ClientConn, session: "Session") -> None:
    payload = {"type": "session_unread", "session_id": session.session_id, "unread": _unread_for(session, client.device_id)}
    try:
        await ws.send(json.dumps(payload))
    except Exception:
        pass


async def _dispatch_event(payload: dict, session: "Session") -> bool:
    et = payload.get("type")
    if et in {"done", "stopped", "error"}:
        session.message_seq += 1
    if not _CLIENTS:
        # No live clients — signal undelivered so send_event falls through to offline_buffer
        return False
    await _broadcast_json(payload)
    if et in {"done", "stopped", "error"}:
        await _send_unread_for_session(session)
        # Broadcast hub: signal clients to pull a delta so they can persist the
        # completed turn into local SQLite.  Cheap: no message body transmitted,
        # just the cursor hint.  App ignores this if it already has the messages.
        if session.resume_id:
            await _broadcast_json({
                "type": "history_sync_hint",
                "session_id": session.session_id,
                "reason": et,   # "done" | "stopped" | "error"
            })
    return True






# ---------------------------------------------------------------------------
# JSONL recent-message helpers + live directory watcher
# ---------------------------------------------------------------------------
CODEX_SESSIONS_DIR = os.path.expanduser("~/.codex/sessions")
_WATCHER_LAST_MAX_MTIME: float = 0.0


_recent_msgs_cache: dict[str, tuple[float, list]] = {}

def _codex_session_id_from_stem(stem: str) -> str:
    """Codex JSONL files are named rollout-<timestamp>-<uuid>.jsonl.

    The native Codex history loader indexes files by the trailing UUID, not by
    the full rollout filename stem.
    """
    candidate = stem[-36:]
    return candidate if is_valid_uuid(candidate) else stem


def _extract_codex_text(content: object) -> str:
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


def _strip_turn_aborted_notice(text: str) -> str:
    cleaned = _TURN_ABORTED_RE.sub("", text or "")
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


def _read_recent_msgs(path: str, fmt: str, n: int = 2) -> list:
    """Parse last n user+assistant messages — reads only the final 32KB of the file."""
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
                        parts = [blk.get("text", "") for blk in content
                                 if isinstance(blk, dict) and blk.get("type") == "text"]
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
                    content = payload.get("content", "")
                    text = _extract_codex_text(content)
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


def _get_recent_messages_sync(session: "Session", n: int = 2) -> list:
    try:
        if not session.resume_id:
            return []
        backend = _session_backend(session)
        if session.backend_name == "codex":
            if not hasattr(backend, "_find_native_session_file"):
                return []
            path = backend._find_native_session_file(session.resume_id)
            return _read_recent_msgs(path, "codex", n) if path else []
        else:
            if not hasattr(backend, "_find_session_file_sync"):
                return []
            path = backend._find_session_file_sync(session.resume_id)
            return _read_recent_msgs(path, "claude", n) if path else []
    except Exception:
        return []


def _register_jsonl_session(path: str) -> bool:
    """Register a single JSONL file as a session in _SESSIONS if not already present.
    Returns True if a new session was added."""
    try:
        fn = os.path.basename(path)
        if not fn.endswith(".jsonl"):
            return False
        stem = fn[:-6]

        # Determine backend from path
        if CODEX_SESSIONS_DIR and path.startswith(CODEX_SESSIONS_DIR):
            backend_name = "codex"
            resume_id = _codex_session_id_from_stem(stem)
            if not is_valid_uuid(resume_id):
                return False
            sid = f"jl_x_{resume_id[:12]}"
        else:
            backend_name = "claude"
            resume_id = stem
            if len(resume_id) < 8:
                return False
            sid = f"jl_c_{resume_id[:12]}"

        existing_uuids = {s.resume_id for s in _SESSIONS.values() if s.resume_id}
        if resume_id in existing_uuids:
            return False

        if sid in _SESSIONS:
            return False

        # Read name + cwd from the JSONL itself
        name = ""
        cwd = DEFAULT_CWD
        fmt = "codex" if backend_name == "codex" else "claude"
        try:
            with open(path, encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    try:
                        d = json.loads(raw)
                        if fmt == "claude":
                            if not cwd or cwd == DEFAULT_CWD:
                                raw_cwd = d.get("cwd")
                                if isinstance(raw_cwd, str) and raw_cwd.strip():
                                    cwd = raw_cwd.strip()
                            if not name and d.get("type") == "user":
                                content = d.get("message", {}).get("content", "")
                                t = content if isinstance(content, str) else next(
                                    (blk.get("text","") for blk in content
                                     if isinstance(blk, dict) and blk.get("type") == "text"), "")
                                if t and not t.startswith("<"):
                                    name = t[:50].strip()
                        elif fmt == "codex":
                            if d.get("type") == "session_meta":
                                pl = d.get("payload", {})
                                if isinstance(pl, dict) and (not cwd or cwd == DEFAULT_CWD):
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
                                    t = _extract_codex_text(payload.get("content"))
                                    if t and not _is_session_title_noise(t):
                                        name = t[:50].strip()
                        if name and cwd != DEFAULT_CWD:
                            break
                    except Exception:
                        pass
        except Exception:
            pass

        if not name:
            name = resume_id[:8]

        mtime = 0.0
        try:
            mtime = os.stat(path).st_mtime
        except OSError:
            pass

        _SESSIONS[sid] = Session(
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
        log.warning("_register_jsonl_session(%s) failed: %s", path, exc)
        return False


def _session_for_jsonl_path(path: str) -> "Session | None":
    fn = os.path.basename(path)
    if not fn.endswith(".jsonl"):
        return None
    stem = fn[:-6]
    if CODEX_SESSIONS_DIR and path.startswith(CODEX_SESSIONS_DIR):
        resume_id = _codex_session_id_from_stem(stem)
    else:
        resume_id = stem
    for session in _SESSIONS.values():
        if session.resume_id == resume_id:
            return session
    return None


def _merge_jsonl_sessions_into_state() -> bool:
    """Initial scan: register all JSONL sessions not yet in _SESSIONS."""
    added = False
    existing_uuids = {s.resume_id for s in _SESSIONS.values() if s.resume_id}

    for base, backend_name in ((CLAUDE_PROJECTS_DIR, "claude"), (CODEX_SESSIONS_DIR, "codex")):
        if not os.path.isdir(base):
            continue
        try:
            for root, _dirs, files in os.walk(base):
                for fn in files:
                    if not fn.endswith(".jsonl"):
                        continue
                    uuid = fn[:-6]
                    if len(uuid) < 8 or uuid in existing_uuids:
                        continue
                    if _register_jsonl_session(os.path.join(root, fn)):
                        existing_uuids.add(uuid)
                        added = True
        except Exception as exc:
            log.warning("JSONL initial scan (%s) error: %s", backend_name, exc)

    return added


_JSONL_TURN_END_STOP_REASONS: frozenset[str] = frozenset(
    {"end_turn", "max_tokens", "stop_sequence"}
)


def _read_new_jsonl_lines(path: str, from_offset: int) -> tuple[list[dict], int]:
    """Read lines appended to a JSONL file since `from_offset` bytes.

    Returns (parsed_lines, new_offset).  Skips malformed JSON silently.
    """
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
    """Return True if any line signals that an assistant turn has completed."""
    if fmt == "claude":
        for d in lines:
            if (
                d.get("type") == "assistant"
                and not d.get("isSidechain")
                and d.get("message", {}).get("stop_reason") in _JSONL_TURN_END_STOP_REASONS
            ):
                return True
    # Codex format: response_item with role=assistant and stop_reason
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


async def _jsonl_watcher_task() -> None:
    """Use watchdog FSEvents to watch Claude + Codex JSONL dirs.
    Falls back to 5-second polling if watchdog is unavailable."""
    loop = asyncio.get_event_loop()
    _merge_jsonl_sessions_into_state()

    # Debounce: accumulate changed paths, flush after 0.8s of quiet
    _pending: dict[str, float] = {}
    _flush_handle: list = [None]

    # Track last-known file size per JSONL path so we only inspect new bytes.
    # Seeded at current size on first encounter so historical lines are skipped.
    _jsonl_known_size: dict[str, int] = {}

    def _seed_known_size(path: str) -> None:
        """Record current file size so the next flush only sees new appends."""
        if path not in _jsonl_known_size:
            try:
                _jsonl_known_size[path] = os.path.getsize(path)
            except OSError:
                _jsonl_known_size[path] = 0

    async def _flush_changes() -> None:
        paths = list(_pending.keys())
        _pending.clear()
        changed_sessions: dict[str, Session] = {}
        for p in paths:
            _seed_known_size(p)  # no-op if already seeded; seeds before register
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

            # --- External-session done detection ---
            # Only check when the session is marked as streaming (meaning the app
            # is waiting for a done signal) but has no live bridge-spawned process
            # managing it.  Bridge-spawned sessions emit done themselves via
            # claude_cli.py; we must not double-emit.
            if not session.is_streaming:
                # Advance the known-size cursor even when not streaming, so that
                # if the session later becomes streaming we don't re-scan old lines.
                try:
                    _jsonl_known_size[p] = os.path.getsize(p)
                except OSError:
                    pass
                continue

            fmt = "codex" if session.backend_name == "codex" else "claude"
            prior_offset = _jsonl_known_size.get(p, 0)
            new_lines, new_size = _read_new_jsonl_lines(p, prior_offset)
            _jsonl_known_size[p] = new_size

            if not new_lines:
                continue

            if _jsonl_lines_contain_turn_end(new_lines, fmt):
                log.info(
                    "emit external done for session %s (jsonl=%s, new_lines=%d)",
                    session.session_id, os.path.basename(p), len(new_lines),
                )
                session.is_streaming = False
                # Reuse _dispatch_event so unread + history_sync_hint are handled
                # identically to bridge-spawned sessions.
                await _dispatch_event(
                    {**_evt_done(), "session_id": session.session_id, "request_id": session.current_request_id or "external"},
                    session,
                )

        if _CLIENTS:
            await _broadcast_json(build_sessions_list())
            for session in changed_sessions.values():
                await _broadcast_json({
                    "type": "history_sync_hint",
                    "session_id": session.session_id,
                    "reason": "file_changed",
                })

    def _on_file_event(path: str) -> None:
        if not path.endswith(".jsonl"):
            return
        _pending[path] = time.time()
        if _flush_handle[0]:
            _flush_handle[0].cancel()
        _flush_handle[0] = loop.call_later(0.8, lambda: asyncio.ensure_future(_flush_changes()))

    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler

        class _Handler(FileSystemEventHandler):
            def on_modified(self, event):
                if not event.is_directory:
                    loop.call_soon_threadsafe(_on_file_event, event.src_path)
            def on_created(self, event):
                if not event.is_directory:
                    loop.call_soon_threadsafe(_on_file_event, event.src_path)

        observer = Observer()
        handler = _Handler()
        for d in (CLAUDE_PROJECTS_DIR, CODEX_SESSIONS_DIR):
            if os.path.isdir(d):
                observer.schedule(handler, d, recursive=True)
        observer.start()
        log.info("JSONL watcher: FSEvents observer started")
        try:
            await asyncio.Future()  # run forever alongside the observer thread
        finally:
            observer.stop()
            observer.join()

    except ImportError:
        log.warning("watchdog not installed — falling back to 5s polling")
        import hashlib

        def _dir_fingerprint() -> str:
            parts = []
            for base in (CLAUDE_PROJECTS_DIR, CODEX_SESSIONS_DIR):
                if not os.path.isdir(base):
                    continue
                for root, _dirs, files in os.walk(base):
                    for fn in sorted(files):
                        if fn.endswith(".jsonl"):
                            try:
                                parts.append(f"{fn}:{os.stat(os.path.join(root,fn)).st_mtime:.0f}")
                            except OSError:
                                pass
            return hashlib.md5("|".join(parts).encode()).hexdigest()

        last_fp = _dir_fingerprint()
        while True:
            await asyncio.sleep(5)
            try:
                fp = _dir_fingerprint()
                if fp == last_fp:
                    continue
                last_fp = fp
                _merge_jsonl_sessions_into_state()
                if _CLIENTS:
                    await _broadcast_json(build_sessions_list())
            except Exception as exc:
                log.warning("JSONL polling error: %s", exc)


def _session_has_valid_resume(s: "Session") -> bool:
    """Return False only when the session has a resume_id that is provably invalid.

    Sessions with no resume_id (new sessions) are always included.
    """
    if not s.resume_id:
        return True
    backend = getattr(s, "backend_name", "claude")
    if backend in ("claude", "codex"):
        return is_valid_uuid(s.resume_id)
    return True


def build_sessions_list() -> dict:
    sessions = sorted(
        (s for s in _SESSIONS.values() if _session_has_valid_resume(s)),
        key=lambda s: s.last_activity or s.created_at,
        reverse=True,
    )[:50]
    return _msg_sessions_list([_session_to_summary(s) for s in sessions])

def _session_to_summary(s: "Session") -> dict:
    return {
        "id": s.session_id,
        "name": s.name,
        "is_streaming": s.is_streaming,
        "created_at": s.created_at,
        "cwd": s.cwd,
        "model": s.model,
        "context_used": s.context_used,
        "context_max": s.context_max,
        "backend": s.backend_name,
        "sandbox": s.sandbox,
        "image_dir": s.image_dir,
        "pinned": s.pinned,
        "hidden": s.hidden,
        "queue_length": len(s.queue),
        "recent_messages": _get_recent_messages_sync(s, n=2),
    }


async def _send_all_sessions(ws: Any, batch_size: int = 50) -> None:
    """Stream all sessions to a single client in batches, yielding between each."""
    all_sessions = sorted(
        (s for s in _SESSIONS.values() if _session_has_valid_resume(s)),
        key=lambda s: s.last_activity or s.created_at,
        reverse=True,
    )
    total = len(all_sessions)
    for offset in range(0, total, batch_size):
        batch = all_sessions[offset: offset + batch_size]
        done = (offset + batch_size) >= total
        payload = {
            "type": "sessions_list_append",
            "sessions": [_session_to_summary(s) for s in batch],
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


# ---------------------------------------------------------------------------
# Active WebSocket clients — multi-client design
# ---------------------------------------------------------------------------
_CLIENTS: Dict[Any, ClientConn] = {}
_AUTO_TUNNEL_TASK: "asyncio.Task | None" = None
_CLOUDFLARED_PROC: "asyncio.subprocess.Process | None" = None
_BRIDGE_PORT = 8766
_PERF = PerfTracker(slow_threshold_ms=250.0, report_interval_s=60.0)


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------
async def _auto_tunnel_after_delay(delay: int = 30) -> None:
    await asyncio.sleep(delay)
    if _CLIENTS:
        return
    if _is_cloudflared_running():
        return
    log.info("No client for %ds — auto-starting Cloudflare tunnel", delay)
    print(f"\n[auto-tunnel] No client for {delay}s, starting tunnel...")
    await _start_cloudflared_tunnel(_BRIDGE_PORT)


async def _run_session_queue(session: Session) -> None:
    if session.processing:
        return
    session.processing = True
    try:
        while session.queue:
            cmd = session.queue[0]
            session.current_request_id = cmd.request_id
            await _broadcast_json({
                "type": "session_command_started",
                "session_id": session.session_id,
                "request_id": cmd.request_id,
                "device_id": cmd.device_id,
                "queue_length": len(session.queue),
            })

            try:
                await _session_backend(session).send(session, cmd.content, cmd.images, cmd.files)
                # backend.send() spawns a subprocess and returns immediately;
                # wait here so session.processing stays True until streaming ends,
                # preventing the next queued command from seeing is_streaming=True.
                while session.is_streaming:
                    if session.is_stopping:
                        return  # stop() is responsible for sending the stopped event
                    await asyncio.sleep(0.15)
                session.recent_request_ids.add(cmd.request_id)
                if len(session.recent_request_ids) > 500:
                    session.recent_request_ids = set(list(session.recent_request_ids)[-250:])
                await _broadcast_json({
                    "type": "session_command_done",
                    "session_id": session.session_id,
                    "request_id": cmd.request_id,
                    "queue_length": max(0, len(session.queue) - 1),
                })
            except Exception as exc:
                log.error("[%s] backend exception in queue: %s", session.session_id, exc, exc_info=True)
                session.is_streaming = False
                # Notify the frontend so it stops the spinner
                await send_event(session, _evt_error(str(exc)))
                await _broadcast_json({
                    "type": "session_command_failed",
                    "session_id": session.session_id,
                    "request_id": cmd.request_id,
                    "message": str(exc),
                    "queue_length": max(0, len(session.queue) - 1),
                })
            finally:
                if session.queue and session.queue[0].request_id == cmd.request_id:
                    session.queue.popleft()
                session.current_request_id = ""
    finally:
        session.processing = False



async def _session_cache_refresher() -> None:
    while True:
        await asyncio.sleep(300)
        await preload_sessions_cache(_BACKENDS)


async def handler(ws: ServerConnection) -> None:
    global _AUTO_TUNNEL_TASK

    # Liveness probe short-circuit.  The supervisor's bridge_healthcheck.py
    # opens a WS every 3s, sends a control PING, then closes.  Without this
    # gate the handler would (a) register the probe in _CLIENTS, (b) reassign
    # session.ws_ref on every existing session to this dying socket — causing
    # the next broadcast event to be sent to a closed socket and dropped from
    # the real app — and (c) serialize+send a 29KB sessions_list and replay
    # offline buffers into a socket that closes 12ms later.  Probes only need
    # the TCP/WS handshake to succeed and their control PING to be ponged
    # (websockets library handles control PING automatically); we just need to
    # keep the connection open until the probe closes it.
    try:
        ua = ws.request.headers.get("User-Agent", "") if ws.request else ""
    except Exception:
        ua = ""
    if ua.startswith("bridge-healthcheck/"):
        try:
            async for _ in ws:
                pass  # discard any frames; probe normally sends none
        except Exception:
            pass
        return

    # Cancel pending auto-tunnel — client is back
    if _AUTO_TUNNEL_TASK and not _AUTO_TUNNEL_TASK.done():
        _AUTO_TUNNEL_TASK.cancel()
        _AUTO_TUNNEL_TASK = None

    remote = ws.remote_address
    client = ClientConn(
        client_id=f"c_{uuid.uuid4().hex[:8]}",
        device_id=f"device_{uuid.uuid4().hex[:8]}",
        device_name="Unknown device",
        ws=ws,
        connected_at=time.time(),
        last_seen=time.time(),
    )
    _CLIENTS[ws] = client
    log.info("Client connected: %s (%s)", remote, client.client_id)

    # Inject this ws into all existing sessions (reconnect scenario).
    # ws_ref must be set before hello_ack/sessions_list so live events dispatched
    # during the handshake go to this client instead of the offline buffer.
    for session in list(_SESSIONS.values()):
        session.ws_ref = ws

    try:
        await ws.send(json.dumps({
            "type": "hello_ack",
            "client_id": client.client_id,
            "device_id": client.device_id,
            "device_name": client.device_name,
        }))
        await ws.send(json.dumps(build_sessions_list()))

        # Replay offline buffers AFTER sessions_list so the frontend has already
        # run reconcileFromServer (and hydrated its session state) before it
        # processes buffered events.  Sending before sessions_list caused a cold-
        # start race where the Zustand store wasn't hydrated yet, so done/stopped
        # events were silently dropped and isStreaming stayed stuck.
        for session in list(_SESSIONS.values()):
            if session.offline_buffer:
                buf = session.offline_buffer[:]
                session.offline_buffer.clear()
                for idx, evt in enumerate(buf):
                    try:
                        await ws.send(json.dumps(evt))
                    except Exception:
                        session.offline_buffer = buf[idx:] + session.offline_buffer
                        break
        await _send_unread_snapshot(ws, client)
        # Re-deliver any file pushes that were broadcast before this client connected
        for fid, entry in list(_PUSH_FILE_REGISTRY.items()):
            await ws.send(json.dumps({
                "type": "file_push",
                "file_id": fid,
                "filename": entry["filename"],
                "url": entry["url"],
                "size": entry["size"],
                "mime_type": entry["mime_type"],
            }))
    except Exception:
        pass

    try:
        system_ctx = {
            "asyncio": asyncio,
            "sessions": _SESSIONS,
            "backends": _BACKENDS,
            "session_backend": _session_backend,
            "msg_resumable_sessions": _msg_resumable_sessions,
        }
        runtime_ctx = {
            "sessions": _SESSIONS,
            "shell_sessions": _SHELL_SESSIONS,
            "max_shells": MAX_SHELLS,
            "session_backend": _session_backend,
            "shell_cls": ShellSession,
            "shell_reader": _shell_reader,
            "msg_error": _msg_error,
            "msg_shell_created": _msg_shell_created,
            "msg_tasks_list": _msg_tasks_list,
            "msg_task_killed": _msg_task_killed,
            "msg_processes_list": _msg_processes_list,
            "msg_process_killed": _msg_process_killed,
        }
        file_ctx = {
            "sessions": _SESSIONS,
            "backends": _BACKENDS,
            "msg_dir_listing": _msg_dir_listing,
            "fcm_token_file": FCM_TOKEN_FILE,
            "log": log,
        }
        async for raw in ws:
            log.debug("Received: %s", str(raw)[:300])

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("Non-JSON from client: %s", str(raw)[:200])
                continue

            # --- Inbound schema validation ---
            validation_err = validate_client_msg(msg)
            if validation_err:
                log.warning("Invalid client msg: %s | %s", validation_err, str(raw)[:200])
                try:
                    await ws.send(json.dumps(_msg_error(f"Protocol error: {validation_err}")))
                except Exception:
                    pass
                continue

            mtype = msg["type"]  # safe after validation
            op_started = time.perf_counter()

            if await _dispatch_ws_message(ws, msg):
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue

            if await handle_system_msg(mtype, msg, ws, system_ctx):
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue
            if await handle_runtime_msg(mtype, msg, ws, runtime_ctx):
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue
            if await handle_file_msg(mtype, msg, ws, file_ctx):
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue

            # WebRTC signaling — once the DataChannel opens we re-enter
            # handler() on the WebRTCChannel adapter so the entire dispatch
            # stack (including this very loop) runs unmodified over P2P.
            if mtype in WEBRTC_MESSAGE_TYPES:
                async def _on_channel_ready(adapter):
                    try:
                        await handler(adapter)
                    except Exception:
                        log.exception("[webrtc] handler raised on adapter")
                if await handle_webrtc_message(mtype, msg, ws, _on_channel_ready):
                    _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                    continue

            # ------------------------------------------------------------------
            if mtype == "ping":
                client.last_seen = time.time()
                try:
                    await ws.send(json.dumps(_msg_pong()))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "hello":
                device_id = str(msg.get("device_id", "")).strip()
                if device_id:
                    client.device_id = device_id[:128]
                    # Enforce single active websocket per device_id.
                    # Reconnect races can briefly leave two sockets alive, which causes
                    # duplicate broadcasted chat events on the same device.
                    stale: list[Any] = []
                    for other_ws, other_client in list(_CLIENTS.items()):
                        if other_ws is ws:
                            continue
                        if other_client.device_id != client.device_id:
                            continue
                        stale.append(other_ws)
                    for old_ws in stale:
                        _CLIENTS.pop(old_ws, None)
                        try:
                            await old_ws.close()
                        except Exception:
                            pass
                device_name = str(msg.get("device_name", "")).strip()
                if device_name:
                    client.device_name = device_name[:128]
                client.last_seen = time.time()
                try:
                    await ws.send(json.dumps({
                        "type": "hello_ack",
                        "client_id": client.client_id,
                        "device_id": client.device_id,
                        "device_name": client.device_name,
                    }))
                    await _send_unread_snapshot(ws, client)
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "request_sessions_list":
                try:
                    await ws.send(json.dumps(build_sessions_list()))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "request_history":
                sid     = msg["session_id"]
                session = _SESSIONS.get(sid)
                if not session:
                    try:
                        await ws.send(json.dumps(_msg_session_history(sid, [], source_count=0, has_more_before=False, runtime=None)))
                    except Exception:
                        pass
                    continue
                _mark_read(sid, client.device_id, session.message_seq)
                _persist_read_cursors()
                await _send_unread_for_client_session(ws, client, session)
                if session.resume_id:
                    backend = _session_backend(session)
                    await _emit_resume_progress(session, "resume_started", 5, "Resume started")
                    await _emit_resume_progress(session, "resume_loading_history", 35, "Loading history")
                    try:
                        await _send_session_history_response(
                            ws,
                            session,
                            limit=msg.get("limit"),
                            known_last_source_message_id=str(msg.get("known_last_source_message_id") or ""),
                            mode=str(msg.get("mode") or "auto"),
                            before_source_message_id=str(msg.get("before_source_message_id") or ""),
                        )
                    except Exception as exc:
                        log.warning("history response error sid=%s: %s", sid, exc)
                        try:
                            runtime = _history_runtime_payload(session)
                            await ws.send(json.dumps(_msg_session_history(sid, [], source_count=0, has_more_before=False, runtime=runtime)))
                        except Exception:
                            pass
                    # Lazy spawn: do NOT pre-warm here.  Spawning claude for every
                    # request_history caused a thundering-herd when 2,000+ saved
                    # sessions were loaded and the dashboard pre-fetched history for
                    # all visible sessions on connect.  Claude will be spawned on
                    # demand when the user actually sends the first message.
                    await _emit_resume_progress(session, "resume_ready", 100, "Resume ready")
                elif session:
                    # Keep client state deterministic even when this session has no resumable backend id.
                    runtime = _history_runtime_payload(session)
                    try:
                        await ws.send(json.dumps(_msg_session_history(
                            session.session_id,
                            [],
                            source_count=0,
                            has_more_before=False,
                            runtime=runtime,
                        )))
                    except Exception:
                        pass

            # ------------------------------------------------------------------
            elif mtype == "new_session":
                sid              = msg["session_id"]
                name             = msg["name"]
                cwd              = os.path.expanduser(msg.get("cwd") or DEFAULT_CWD)
                resume_claude_id = msg.get("resume_claude_id", "")
                backend_name     = _normalize_backend_name(msg.get("backend"))
                effort           = msg.get("effort", "")
                sandbox          = str(msg.get("sandbox") or "danger-full-access")
                image_dir        = str(msg.get("image_dir") or "")

                async with _SESSIONS_LOCK:
                    if sid in _SESSIONS:
                        _SESSIONS[sid].ws_ref = ws
                        try:
                            await ws.send(json.dumps(_msg_session_created(
                                sid, _SESSIONS[sid].name,
                                _SESSIONS[sid].created_at, _SESSIONS[sid].cwd,
                                _SESSIONS[sid].backend_name,
                                _SESSIONS[sid].model,
                                _SESSIONS[sid].sandbox,
                                _SESSIONS[sid].image_dir,
                            )))
                        except Exception:
                            pass
                        continue

                    if MAX_SESSIONS > 0 and len(_SESSIONS) >= MAX_SESSIONS:
                        try:
                            await ws.send(json.dumps(_msg_error(
                                f"Maximum sessions ({MAX_SESSIONS}) reached."
                            )))
                        except Exception:
                            pass
                        continue

                    import time as _time
                    session = Session(
                        session_id=sid,
                        name=name,
                        created_at=_time.time(),
                        cwd=cwd,
                        ws_ref=ws,
                        resume_id=resume_claude_id or None,
                        effort=effort,
                        backend_name=backend_name,
                        sandbox=sandbox,
                        image_dir=image_dir,
                    )
                    _SESSIONS[sid] = session
                    invalidate_sessions_cache()
                    asyncio.create_task(preload_sessions_cache(_BACKENDS))

                # BUG-07: immediately upsert session into FTS5 index so search
                # finds it without waiting for the file-watcher to trigger ingest.
                if _search_enabled:
                    try:
                        _sw = get_worker()
                        if _sw is not None:
                            _sw.upsert_session_metadata(
                                session_id=sid,
                                source=backend_name if backend_name in ("claude", "codex", "ollama") else "claude",
                                cwd=cwd,
                                display_name=name,
                            )
                    except Exception as _e:
                        log.debug("FTS5 early upsert failed (non-fatal): %s", _e)

                backend = _session_backend(session)
                if resume_claude_id:
                    # Resume path: spawn must complete before loading history.
                    await _emit_resume_progress(session, "resume_started", 5, "Resume started")
                    await _emit_resume_progress(session, "resume_spawning_backend", 20, "Spawning backend")
                    await backend.spawn(session)
                else:
                    # New session: notify frontend immediately, spawn in background.
                    asyncio.create_task(backend.spawn(session))

                try:
                    await ws.send(json.dumps(_msg_session_created(
                        sid, name, session.created_at, cwd, session.backend_name, session.model, session.sandbox, session.image_dir
                    )))
                except Exception:
                    pass

                if resume_claude_id and backend.supports_resume():
                    try:
                        await _emit_resume_progress(session, "resume_loading_history", 65, "Loading history")
                        await _send_session_history_response(ws, session, limit=DEFAULT_HISTORY_LIMIT, mode="snapshot")
                        await _emit_resume_progress(session, "resume_ready", 100, "Resume ready")
                    except Exception as exc:
                        await _emit_resume_progress(session, "resume_failed", 100, f"Resume failed: {exc}")

                log.info("Session created: %s (%s)", sid, name)
                _persist_session_meta()
                await _broadcast_json(build_sessions_list())

            # ------------------------------------------------------------------
            elif mtype == "message":
                sid     = msg["session_id"]
                content = _strip_turn_aborted_notice(msg.get("content", ""))
                session = _SESSIONS.get(sid)

                if not session:
                    try:
                        await ws.send(json.dumps(_msg_error(f"Unknown session: {sid}", sid)))
                    except Exception:
                        pass
                    continue

                session.ws_ref = ws

                images = msg.get("images")
                files = msg.get("files")
                request_id = str(msg.get("request_id") or f"r_{uuid.uuid4().hex[:10]}")
                if not content and not images and not files:
                    err = {**_evt_error("Empty content"), "session_id": session.session_id, "request_id": request_id}
                    try:
                        await ws.send(json.dumps(err))
                    except Exception:
                        pass
                    continue
                if (
                    any(cmd.request_id == request_id for cmd in session.queue)
                    or session.current_request_id == request_id
                    or request_id in session.recent_request_ids
                ):
                    try:
                        await ws.send(json.dumps({
                            "type": "message_ack",
                            "session_id": sid,
                            "request_id": request_id,
                            "status": "duplicate",
                        }))
                    except Exception:
                        pass
                    continue
                try:
                    await ws.send(json.dumps({
                        "type": "message_ack",
                        "session_id": sid,
                        "request_id": request_id,
                        "status": "queued",
                    }))
                except Exception:
                    pass
                session.queue.append(QueuedCommand(
                    request_id=request_id,
                    device_id=client.device_id,
                    client_id=client.client_id,
                    content=content,
                    images=images,
                    files=files,
                    enqueued_at=time.time(),
                ))
                await _broadcast_json({
                    "type": "session_command_queued",
                    "session_id": sid,
                    "request_id": request_id,
                    "device_id": client.device_id,
                    "queue_position": len(session.queue),
                    "queue_length": len(session.queue),
                })
                asyncio.create_task(_run_session_queue(session))

                # BUG-07: index first user message into FTS5 immediately so it is
                # searchable without waiting for the file-watcher ingest cycle.
                if _search_enabled and not session._fts_first_msg_indexed and content:
                    session._fts_first_msg_indexed = True
                    _fts_msg_uuid = f"live_{uuid.uuid4().hex}"
                    _fts_ts = __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                    _fts_content_snap = content
                    _fts_sid = sid
                    async def _index_first_msg(
                        _sid=_fts_sid,
                        _muuid=_fts_msg_uuid,
                        _ts=_fts_ts,
                        _text=_fts_content_snap,
                    ) -> None:
                        try:
                            _sw = get_worker()
                            if _sw is not None:
                                _sw.upsert_first_user_message(
                                    session_id=_sid,
                                    content=_text,
                                    msg_uuid=_muuid,
                                    ts=_ts,
                                )
                        except Exception as _e:
                            log.debug("FTS5 first-msg index failed (non-fatal): %s", _e)
                    asyncio.create_task(_index_first_msg())

            # ------------------------------------------------------------------
            elif mtype == "stop":
                sid     = msg["session_id"]
                session = _SESSIONS.get(sid)
                if not session:
                    try:
                        await ws.send(json.dumps(_msg_error(f"Unknown session: {sid}", sid)))
                    except Exception:
                        pass
                    continue
                session.ws_ref = ws
                async def _do_stop(s: Session) -> None:
                    queued_before = list(s.queue)
                    s.queue.clear()  # clear before stop() to prevent _run_session_queue from picking up stale items
                    pending = queued_before[1:] if s.processing else queued_before
                    remain = len(pending)
                    for cmd in pending:
                        remain = max(0, remain - 1)
                        await _broadcast_json({
                            "type": "session_command_failed",
                            "session_id": s.session_id,
                            "request_id": cmd.request_id,
                            "message": "Cancelled by stop",
                            "queue_length": remain,
                        })
                    await _session_backend(s).stop(s)
                asyncio.create_task(_do_stop(session))

            # ------------------------------------------------------------------
            elif mtype == "close_session":
                sid     = msg["session_id"]
                session = _SESSIONS.get(sid)
                if not session:
                    try:
                        await ws.send(json.dumps(_msg_error(f"Unknown session: {sid}", sid)))
                    except Exception:
                        pass
                    continue
                session.ws_ref = ws
                async def _do_close(s: Session) -> None:
                    await _session_backend(s).close(s)
                    async with _SESSIONS_LOCK:
                        _SESSIONS.pop(s.session_id, None)
                    _READ_CURSORS.pop(s.session_id, None)
                    _persist_read_cursors()
                    _persist_session_meta()
                    saved = _load_saved_sessions()
                    if s.session_id in saved:
                        del saved[s.session_id]
                        try:
                            with open(SAVED_SESSIONS_FILE, "w") as f:
                                json.dump(saved, f, indent=2, ensure_ascii=False)
                        except Exception as exc:
                            log.warning("Failed to remove session from disk: %s", exc)
                    invalidate_sessions_cache()
                    asyncio.create_task(preload_sessions_cache(_BACKENDS))
                asyncio.create_task(_do_close(session))

            # ------------------------------------------------------------------
            elif mtype == "rename_session":
                sid      = msg["session_id"]
                new_name = msg["name"]
                session  = _SESSIONS.get(sid)
                if not session:
                    try:
                        await ws.send(json.dumps(_msg_error(f"Unknown session: {sid}", sid)))
                    except Exception:
                        pass
                    continue
                session.name   = new_name
                session.ws_ref = ws
                _persist_session(session)
                await _broadcast_json(_msg_session_renamed(sid, new_name))

            # ------------------------------------------------------------------
            elif mtype == "clear_session":
                sid     = msg["session_id"]
                session = _SESSIONS.get(sid)
                if not session:
                    try:
                        await ws.send(json.dumps(_msg_error(f"Unknown session: {sid}", sid)))
                    except Exception:
                        pass
                    continue
                session.ws_ref = ws
                asyncio.create_task(_session_backend(session).clear(session))

            # ------------------------------------------------------------------
            elif mtype == "set_effort":
                sid    = msg.get("session_id", "")
                effort = msg.get("effort", "")
                session = _SESSIONS.get(sid)
                if session:
                    session.effort = effort
                    session.ws_ref = ws
                    label = effort or "auto"
                    await send_event(session, _evt_session_warning(f"Effort set to {label}, restarting…"))
                    async def _restart_effort(s: Session) -> None:
                        backend = _session_backend(s)
                        await backend.stop(s)
                        await backend.spawn(s)
                    asyncio.create_task(_restart_effort(session))

            # ------------------------------------------------------------------
            elif mtype == "switch_session_config":
                sid = msg.get("session_id", "")
                source = _SESSIONS.get(sid)
                if not source:
                    try:
                        await ws.send(json.dumps(_msg_error(f"Unknown session: {sid}", sid)))
                    except Exception:
                        pass
                    continue
                if source.is_streaming or source.processing:
                    await send_event(source, _evt_error("Session is currently processing a request.", "session_busy"))
                    continue

                target_backend = _normalize_backend_name(msg.get("backend") or source.backend_name)
                target_model = str(msg.get("model") or source.model or "")
                target_effort = str(msg.get("effort") if "effort" in msg else source.effort or "")
                requested_sandbox = str(msg.get("sandbox") or "")
                target_sandbox = requested_sandbox or source.sandbox or "danger-full-access"
                target_image_dir = str(msg.get("image_dir") or source.image_dir or "")
                if requested_sandbox:
                    await send_event(source, _evt_session_warning(
                        f"Sandbox change requested ({requested_sandbox}) — will apply by creating a new session."
                    ))

                transfer_history = await _load_session_history_for_transfer(source, 80)
                new_sid = f"s_{uuid.uuid4().hex[:8]}"
                now = time.time()
                carry_resume = (target_backend == source.backend_name)
                if target_backend == "codex" and (
                    target_model != (source.model or "")
                    or target_effort != (source.effort or "")
                    or target_sandbox != (source.sandbox or "danger-full-access")
                    or target_image_dir != (source.image_dir or "")
                ):
                    # Codex thread config changes are applied by creating a fresh bridge session and handoff context.
                    carry_resume = False
                new_session = Session(
                    session_id=new_sid,
                    name=f"{source.name} (switch)",
                    created_at=now,
                    cwd=source.cwd,
                    ws_ref=ws,
                    resume_id=(source.resume_id if carry_resume else None),
                    effort=target_effort,
                    backend_name=target_backend,
                    model=target_model,
                    sandbox=target_sandbox,
                    image_dir=target_image_dir,
                )

                async with _SESSIONS_LOCK:
                    _SESSIONS[new_sid] = new_session

                await _emit_resume_progress(new_session, "resume_spawning_backend", 20, "Spawning backend")
                await _session_backend(new_session).spawn(new_session)

                try:
                    await ws.send(json.dumps(_msg_session_created(
                        new_sid,
                        new_session.name,
                        new_session.created_at,
                        new_session.cwd,
                        new_session.backend_name,
                        new_session.model,
                        new_session.sandbox,
                        new_session.image_dir,
                    )))
                except Exception:
                    pass
                await _broadcast_json(build_sessions_list())

                if transfer_history:
                    transfer_request_id = f"r_handoff_{uuid.uuid4().hex[:8]}"
                    new_session.queue.append(QueuedCommand(
                        request_id=transfer_request_id,
                        device_id=client.device_id,
                        client_id=client.client_id,
                        content=_build_handoff_prompt(transfer_history),
                        images=None,
                        files=None,
                        enqueued_at=time.time(),
                    ))
                    await _broadcast_json({
                        "type": "session_command_queued",
                        "session_id": new_sid,
                        "request_id": transfer_request_id,
                        "device_id": client.device_id,
                        "queue_position": 1,
                        "queue_length": 1,
                    })
                    asyncio.create_task(_run_session_queue(new_session))

                try:
                    await ws.send(json.dumps({
                        "type": "session_switched",
                        "from_session_id": sid,
                        "to_session_id": new_sid,
                    }))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "set_session_meta":
                sid = msg["session_id"]
                session = _SESSIONS.get(sid)
                if not session:
                    continue
                if "pinned" in msg:
                    session.pinned = bool(msg["pinned"])
                if "hidden" in msg:
                    session.hidden = bool(msg["hidden"])
                _persist_session_meta()
                await _broadcast_json({
                    "type": "session_meta_updated",
                    "session_id": sid,
                    "pinned": session.pinned,
                    "hidden": session.hidden,
                })
                await _broadcast_json(build_sessions_list())

            # ------------------------------------------------------------------
            elif mtype == "push_file":
                path = msg.get("path", "")
                asyncio.create_task(_handle_push_file(ws, path, client.device_id))

            # ------------------------------------------------------------------
            elif mtype == "file_push_ack":
                file_id = msg.get("file_id", "")
                asyncio.create_task(_handle_file_push_ack(file_id, client.device_id))

            elif mtype == "get_all_sessions":
                asyncio.create_task(_send_all_sessions(ws))

            else:
                log.debug("No direct handler matched for type=%s", mtype)

            _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)

    except Exception as exc:
        name = type(exc).__name__
        if "ConnectionClosed" in name:
            log.info("Client disconnected: %s (%s)", remote, exc)
        else:
            log.exception("Unhandled error in handler: %s", exc)
    finally:
        _CLIENTS.pop(ws, None)
        for session in list(_SESSIONS.values()):
            if session.ws_ref is ws:
                session.ws_ref = None
        for shell in list(_SHELL_SESSIONS.values()):
            if shell.ws_ref is ws:
                shell.ws_ref = None
        # Tear down any pending WebRTC PC anchored on this signaling socket
        # (the DC adapter, if promoted, has its own lifecycle).
        _webrtc_cleanup_for_ws(ws)
        log.info("Client gone: %s (%s)", remote, client.client_id)

        if (
            os.environ.get("BRIDGE_AUTO_TUNNEL") == "1"
            and not _CLIENTS
            and not _is_cloudflared_running()
        ):
            _AUTO_TUNNEL_TASK = asyncio.create_task(_auto_tunnel_after_delay(120))


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# HTTP media handler — serves local media files via /media/<abs-path>
# ---------------------------------------------------------------------------

_ALLOWED_MEDIA_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov", ".m4v", ".avi"}


async def _media_request_handler(connection, request):
    if not request.path.startswith("/media/"):
        return None  # proceed with WebSocket upgrade
    from urllib.parse import unquote
    file_path = unquote(request.path[6:])  # strip "/media", keep leading "/"
    ext = os.path.splitext(file_path)[1].lower()
    if ext not in _ALLOWED_MEDIA_EXTS:
        return connection.respond(http.HTTPStatus.FORBIDDEN, "Forbidden\n")
    real_path = os.path.realpath(file_path)
    if not os.path.isfile(real_path):
        return connection.respond(http.HTTPStatus.NOT_FOUND, "Not found\n")
    try:
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(None, lambda: open(real_path, "rb").read())
    except OSError:
        return connection.respond(http.HTTPStatus.INTERNAL_SERVER_ERROR, "Read error\n")
    mime_type, _ = mimetypes.guess_type(real_path)
    return WsResponse(
        status_code=200,
        reason_phrase="OK",
        headers=WsHeaders([
            ("Content-Type", mime_type or "application/octet-stream"),
            ("Content-Length", str(len(data))),
            ("Cache-Control", "no-cache"),
        ]),
        body=data,
    )


# ---------------------------------------------------------------------------
# mDNS / Bonjour discovery
# ---------------------------------------------------------------------------

def _start_mdns(port: int) -> "Zeroconf | None":
    if os.environ.get("BRIDGE_DISABLE_MDNS", "1") == "1":
        log.info("mDNS disabled by BRIDGE_DISABLE_MDNS=1")
        return None
    if not _ZEROCONF_AVAILABLE:
        log.warning("zeroconf not installed — mDNS disabled. Run: pip install zeroconf")
        return None
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
        info = ServiceInfo(
            "_bridge._tcp.local.",
            "bridge._bridge._tcp.local.",
            addresses=[socket.inet_aton(local_ip)],
            port=port,
            properties={"version": "2"},
        )
        zc = Zeroconf()
        zc.register_service(info)
        log.info("mDNS: bridge.local advertised at %s:%d", local_ip, port)
        return zc
    except Exception as exc:
        log.warning("mDNS registration failed: %s", exc)
        return None


# cloudflared tunnel helpers
# ---------------------------------------------------------------------------
async def _drain_proc_stderr(proc) -> None:
    try:
        async for _ in proc.stderr:
            pass
    except Exception:
        pass


def _is_cloudflared_running() -> bool:
    return _CLOUDFLARED_PROC is not None and _CLOUDFLARED_PROC.returncode is None


async def _start_cloudflared_tunnel(port: int) -> None:
    global _CLOUDFLARED_PROC
    if _is_cloudflared_running():
        log.info("cloudflared already running, skipping")
        return
    cfd = shutil.which("cloudflared")
    if not cfd:
        print("WARNING: cloudflared not installed, skipping tunnel")
        print("   Install: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/")
        return
    proc = await asyncio.create_subprocess_exec(
        cfd, "tunnel", "--url", f"http://localhost:{port}",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _CLOUDFLARED_PROC = proc
    print("Waiting for cloudflared tunnel...")
    async for line_bytes in proc.stderr:
        line = line_bytes.decode(errors="replace")
        m = re.search(r'https://[\w.-]+\.trycloudflare\.com', line)
        if m:
            url = m.group(0)
            ws_url = url.replace("https://", "wss://")
            print(f"\n{'='*56}")
            print(f"Tunnel URL (fill in app Settings):")
            print(f"   {ws_url}")
            print(f"{'='*56}\n")
            log.info("Cloudflared tunnel: %s", ws_url)
            set_media_base_url(url)
            asyncio.create_task(_drain_proc_stderr(proc))
            asyncio.create_task(_notify_fcm_tunnel(ws_url))
            return
    log.warning("cloudflared tunnel URL not detected")
    _CLOUDFLARED_PROC = None


async def _warmup_history_cache_background() -> None:
    """啟動後延遲 8 秒，趁空閒預建所有 backend 的 session history index。"""
    await asyncio.sleep(8)
    for name, backend in _BACKENDS.items():
        if not hasattr(backend, "warmup_history_cache"):
            continue
        try:
            await backend.warmup_history_cache()
        except Exception as exc:
            log.warning("warmup_history_cache_background [%s] failed: %s", name, exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main(port: int, tunnel: bool = False,
               backend_name: str = "claude", model: str = "",
               ollama_host: str = "http://localhost:11434") -> None:
    global CLAUDE_BIN, CODEX_BIN, BUN_BIN, _DEFAULT_BACKEND_NAME, _DEFAULT_OLLAMA_MODEL, _OLLAMA_HOST, _READ_CURSORS

    _DEFAULT_BACKEND_NAME = _normalize_backend_name(backend_name)
    _DEFAULT_OLLAMA_MODEL = model or "llama3.2"
    _OLLAMA_HOST = ollama_host

    _get_or_create_backend(_DEFAULT_BACKEND_NAME)
    # Pre-create both scan-capable backends so _merge_jsonl_sessions_into_state works at startup.
    try:
        _get_or_create_backend("claude")
    except Exception:
        pass
    try:
        _get_or_create_backend("codex")
    except Exception:
        pass
    init_firebase()
    _migrate_codex_saved_sessions()
    _restore_sessions_from_disk()
    _apply_session_meta()
    _READ_CURSORS = _load_read_cursors()
    set_event_dispatcher(_dispatch_event)

    ts_ip = _detect_tailscale_ip()
    print(f"\n{'='*56}")
    print(f"  Bridge v2  |  port {port}  |  default backend: {_DEFAULT_BACKEND_NAME}")
    print(f"{'='*56}")
    if _DEFAULT_BACKEND_NAME == "claude":
        print(f"  Claude : {CLAUDE_BIN}")
    elif _DEFAULT_BACKEND_NAME == "codex":
        print(f"  Codex  : {CODEX_BIN}")
    else:
        print(f"  Ollama : {_OLLAMA_HOST}  model={_DEFAULT_OLLAMA_MODEL}")
    if ts_ip:
        print(f"  Tailscale: ws://{ts_ip}:{port}")
        set_media_base_url(f"http://{ts_ip}:{port}")
    else:
        print(f"  Local   : ws://127.0.0.1:{port}")
        print(f"  (No Tailscale — use --tunnel for a public URL)")
        set_media_base_url(f"http://127.0.0.1:{port}")
    print(f"{'='*56}\n")

    global _BRIDGE_PORT
    _BRIDGE_PORT = port
    log.info("Bridge v2 starting on port %d (default_backend=%s)", port, _DEFAULT_BACKEND_NAME)
    await _init_search()
    zc = _start_mdns(port)
    async with serve(
        handler,
        ["0.0.0.0", "::"],
        port,
        ping_interval=30,
        ping_timeout=60,
        process_request=_media_request_handler,
        compression="deflate",
        reuse_port=True,
    ):
        log.info("Bridge v2 listening on port %d (IPv4 + IPv6)", port)
        if tunnel:
            asyncio.create_task(_start_cloudflared_tunnel(port))
        init_cache_db()
        asyncio.create_task(preload_sessions_cache(_BACKENDS))
        asyncio.create_task(_session_cache_refresher())
        asyncio.create_task(_warmup_history_cache_background())
        asyncio.create_task(_jsonl_watcher_task())
        try:
            await asyncio.Future()  # run forever
        finally:
            await _shutdown_search()
            if zc:
                zc.unregister_all_services()
                zc.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Claude WebSocket Bridge v2")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--tunnel", action="store_true", help="Start a cloudflared tunnel for public access")
    parser.add_argument("--backend", default="claude", choices=["claude", "ollama", "codex"],
                        help="AI backend (default: claude)")
    parser.add_argument("--model", default="", help="Model name (for ollama backend)")
    parser.add_argument("--ollama-host", default="http://localhost:11434",
                        help="Ollama server URL")
    args = parser.parse_args()
    asyncio.run(main(
        args.port,
        tunnel=args.tunnel,
        backend_name=args.backend,
        model=args.model,
        ollama_host=args.ollama_host,
    ))
