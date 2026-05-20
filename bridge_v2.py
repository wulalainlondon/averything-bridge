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
import http
import json
import logging
import mimetypes
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import sys
import uuid
try:
    import resource
except ImportError:
    resource = None

# Raise file descriptor limit before any subsystem opens files (search ingest +
# watchdog kqueue observer can consume thousands of fds).
# macOS launchd default is 256; plist HardResourceLimits may cap at 8192.
# Strategy: always attempt to raise to _WANT_FDS; try relaxing hard limit too.
_WANT_FDS = 65536
_TURN_ABORTED_RE = re.compile(r"<turn_aborted>.*?</turn_aborted>", re.IGNORECASE | re.DOTALL)
if resource is not None:
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
from typing import Any, Dict, Literal, NotRequired, Optional, TYPE_CHECKING, TypedDict

import client_manager
import session_registry
from client_manager import ClientConn
from session_registry import QueuedCommand, Session
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
from message_router import RouterContext, handle_low_coupling_message
from offline_replay import replay_offline_buffers
from queue_runner import log_prompt_lifecycle as _log_prompt_lifecycle
from queue_runner import run_session_queue as _run_session_queue_impl
from task_manager import cancel_all as _cancel_tasks
from task_manager import spawn as _spawn_task
from utils.uuid_helper import is_valid_uuid
from permission_manager import PermissionManager
from discovery_broadcaster import DiscoveryBroadcaster

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
    _msg_pong, _msg_error, _msg_session_created, _msg_session_renamed,
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
DEFAULT_CWD          = session_registry.DEFAULT_CWD
MAX_SESSIONS         = 0
MAX_SHELLS           = 5
FCM_TOKEN_FILE        = os.path.join(BRIDGE_DIR, "fcm_token.txt")
SERVICE_ACCOUNT_FILE  = os.path.join(BRIDGE_DIR, "serviceAccountKey.json")
SAVED_SESSIONS_FILE   = os.path.join(BRIDGE_DIR, "saved_sessions.json")
CODEX_SAVED_SESSIONS_FILE = os.path.join(BRIDGE_DIR, "saved_sessions_codex.json")
SESSION_META_FILE     = os.path.join(BRIDGE_DIR, "session_meta.json")
READ_CURSOR_FILE      = os.path.join(BRIDGE_DIR, "read_cursors.json")
CLAUDE_PROJECTS_DIR   = str(Path.home() / ".claude" / "projects")
PAIRING_FILE          = os.path.join(BRIDGE_DIR, "pairing.json")


def _load_pairing() -> dict:
    try:
        with open(PAIRING_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_pairing(data: dict) -> None:
    with open(PAIRING_FILE, "w") as f:
        json.dump(data, f)


def _clear_pairing() -> None:
    try:
        os.remove(PAIRING_FILE)
    except FileNotFoundError:
        pass


_PAIRING: dict = _load_pairing()


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
        r"%APPDATA%\npm\claude.cmd",
        r"%APPDATA%\npm\claude.exe",
        r"%USERPROFILE%\AppData\Roaming\npm\claude.cmd",
        r"%USERPROFILE%\AppData\Roaming\npm\claude.exe",
        "/usr/local/bin/claude",
        "/opt/homebrew/bin/claude",
    ]
    for c in candidates:
        p = os.path.expanduser(os.path.expandvars(c))
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
        r"%USERPROFILE%\.bun\bin\bun.exe",
        "/usr/local/bin/bun",
    ]
    for c in candidates:
        p = os.path.expanduser(os.path.expandvars(c))
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
        r"%APPDATA%\npm\codex.cmd",
        r"%APPDATA%\npm\codex.exe",
        r"%USERPROFILE%\AppData\Roaming\npm\codex.cmd",
        r"%USERPROFILE%\AppData\Roaming\npm\codex.exe",
        "/usr/local/bin/codex",
        "/opt/homebrew/bin/codex",
    ]
    for c in candidates:
        p = os.path.expanduser(os.path.expandvars(c))
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
logging.getLogger("watchdog").setLevel(logging.INFO)
logging.getLogger("fsevents").setLevel(logging.INFO)
logging.getLogger("websockets").setLevel(logging.INFO)

if not _SEARCH_AVAILABLE:
    log.warning("[search] module unavailable: %s; search disabled", _SEARCH_IMPORT_ERR)

# ---------------------------------------------------------------------------
# Search subsystem state
# ---------------------------------------------------------------------------
_search_pool: Optional[ConnectionPool] = None
_search_enabled: bool = False
_last_client_activity_monotonic: float = 0.0

SEARCH_MESSAGE_TYPES: frozenset[str] = frozenset({
    "request_search",
    "request_search_health",
    "request_session_list",
    "request_search_context",
})


def _mark_client_activity() -> None:
    global _last_client_activity_monotonic
    _last_client_activity_monotonic = time.monotonic()


def _search_ingest_should_yield_to_activity() -> bool:
    if not _SEARCH_AVAILABLE:
        return False
    try:
        window = max(0.0, float(get_config().search.ingest_idle_recent_window_sec))
    except Exception:
        window = 3.0
    return window > 0 and (time.monotonic() - _last_client_activity_monotonic) < window


def _short_id(value: Any) -> str:
    text = str(value) if value is not None else ""
    if len(text) <= 18:
        return text
    return f"{text[:12]}...{text[-4:]}"


def _summarize_client_msg(msg: dict, raw_len: int) -> str:
    parts = [f"type={msg.get('type', '<missing>')}", f"bytes={raw_len}"]
    for key in ("session_id", "request_id", "client_id", "device_id"):
        if msg.get(key):
            parts.append(f"{key}={_short_id(msg.get(key))}")
    for key in ("content", "query", "message"):
        val = msg.get(key)
        if isinstance(val, str):
            parts.append(f"{key}_len={len(val)}")
    for key in ("files", "images", "items"):
        val = msg.get(key)
        if isinstance(val, list):
            parts.append(f"{key}_count={len(val)}")
    return " ".join(parts)


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
        await start_worker(cfg, activity_probe=_search_ingest_should_yield_to_activity)
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
    auth_token: NotRequired[str]

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

class PermissionResponseMsg(TypedDict):
    type: Literal["permission_response"]
    request_id: str
    decision: str

class RequestStatusMsg(TypedDict):
    type: Literal["request_status"]
    session_id: NotRequired[str]


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
    "permission_response":[("request_id", str), ("decision", str)],
    "request_status": [],
    "claim_bridge":   [],   # auth_token validated at handler level
    "unclaim_bridge": [],   # auth_token validated at handler level
}

_KNOWN_MSG_TYPES: frozenset[str] = frozenset({
    "ping", "message", "new_session", "stop", "close_session",
    "rename_session", "clear_session", "get_usage", "get_resumable_sessions",
    "shell_create", "shell_input", "shell_close", "get_tasks", "kill_task",
    "get_processes", "kill_process",
    "fcm_token", "request_sessions_list", "browse_dir", "request_history",
    "set_effort", "hello", "set_session_meta", "switch_session_config",
    "permission_response",
    "request_status",
    "claim_bridge", "unclaim_bridge",
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


def _is_auth_token_valid(msg: dict) -> bool:
    # Environment variable takes priority (advanced users, manual override).
    expected = os.environ.get("BRIDGE_AUTH_TOKEN", "").strip()
    if expected:
        provided = str(msg.get("auth_token") or "").strip()
        return bool(provided) and provided == expected

    # App-side pairing lock: if a device has claimed this bridge, only that
    # device's token is accepted.
    paired_token = _PAIRING.get("paired_token", "").strip()
    if paired_token:
        provided = str(msg.get("auth_token") or "").strip()
        return bool(provided) and provided == paired_token

    # Unlocked: allow all connections.
    return True


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
_SESSIONS = session_registry.SESSIONS
_SESSIONS_LOCK = session_registry.SESSIONS_LOCK

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
_READ_CURSORS = session_registry.READ_CURSORS

def _normalize_backend_name(raw: str | None) -> str:
    name = (raw or "").strip().lower()
    return name if name in {"claude", "codex", "ollama", "gemini"} else _DEFAULT_BACKEND_NAME


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
    elif backend_name == "gemini":
        from backends.gemini_cli import GeminiCliBackend
        backend = GeminiCliBackend()
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
    os.path.expanduser("~/.config/claude-bridge/storage-serviceAccountKey.json"),
)

def init_firebase() -> None:
    global _firebase_app, _firebase_storage_app
    if not _FIREBASE_AVAILABLE:
        log.warning("firebase-admin not installed — FCM disabled. Run: pip install firebase-admin")
        return

    # FCM app
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        try:
            cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
            _firebase_app = firebase_admin.initialize_app(cred)
            log.info("Firebase FCM initialized")
        except Exception as exc:
            log.warning("Firebase FCM init failed: %s", exc)
    else:
        log.warning("serviceAccountKey.json not found at %s — FCM disabled", SERVICE_ACCOUNT_FILE)

    # Storage app — use separate storage key if available, otherwise fall back to FCM key
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


_PUSH_INLINE_MAX_BYTES = 50 * 1024 * 1024  # 50 MB


async def _handle_push_file(ws: Any, path: str, sender_device_id: str = "") -> None:
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

    import mimetypes
    mime_type, _ = mimetypes.guess_type(filename)
    if not mime_type:
        mime_type = "application/octet-stream"

    target_device_ids = client_manager.connected_device_ids(sender_device_id)

    # Inline path: base64-encode and send directly over WebSocket (no Firebase)
    if size <= _PUSH_INLINE_MAX_BYTES:
        try:
            import base64 as _b64
            with open(expanded, "rb") as fh:
                data_b64 = _b64.b64encode(fh.read()).decode("ascii")
            _PUSH_FILE_REGISTRY[file_id] = {
                "blob_path": None,
                "filename": filename,
                "size": size,
                "mime_type": mime_type,
                "data": data_b64,
                "target_device_ids": target_device_ids,
                "acked_device_ids": [],
            }
            log.info("push_file inline: %s (%d bytes)", filename, size)
            broadcast_payload = {
                "type": "file_push",
                "file_id": file_id,
                "filename": filename,
                "size": size,
                "mime_type": mime_type,
                "data": data_b64,
            }
            # Ack to pusher immediately before the slow broadcast
            try:
                await ws.send(json.dumps({
                    "type": "push_ack",
                    "file_id": file_id,
                    "filename": filename,
                    "size": size,
                }))
            except Exception:
                pass
            _spawn_task(f"broadcast-push:{file_id}", _broadcast_json(broadcast_payload))
            _spawn_task(f"notify-fcm:push-file:{file_id}", notify_fcm("Bridge", f"📎 {filename}", ""))
        except Exception as exc:
            log.warning("push_file inline failed: %s", exc)
            try:
                await ws.send(json.dumps({"type": "error", "message": f"Push failed: {exc}"}))
            except Exception:
                pass
        return

    # Large file fallback: Firebase Storage
    if _firebase_storage_app is None:
        try:
            await ws.send(json.dumps({"type": "error", "message": "File too large for inline transfer and Firebase Storage not available"}))
        except Exception:
            pass
        return

    blob_path = f"bridge_pushes/{file_id}/{filename}"
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
        _spawn_task(f"notify-fcm:push-file:{file_id}", notify_fcm("Bridge", f"📎 {filename}", ""))
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
    blob_path = entry.get("blob_path")
    if not blob_path or _firebase_storage_app is None:
        return
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
    return session_registry.load_saved_sessions(SAVED_SESSIONS_FILE)


def _migrate_codex_saved_sessions() -> None:
    session_registry.migrate_codex_saved_sessions(
        saved_sessions_file=SAVED_SESSIONS_FILE,
        codex_saved_sessions_file=CODEX_SAVED_SESSIONS_FILE,
        default_cwd=DEFAULT_CWD,
        log_warning=log.warning,
    )

def _persist_session(session: "Session") -> None:
    session_registry.persist_session(
        session,
        saved_sessions_file=SAVED_SESSIONS_FILE,
        log_warning=log.warning,
    )


def _restore_sessions_from_disk() -> None:
    session_registry.restore_sessions_from_disk(
        _SESSIONS,
        saved_sessions_file=SAVED_SESSIONS_FILE,
        default_cwd=DEFAULT_CWD,
        normalize_backend=_normalize_backend_name,
        log_info=log.info,
        log_warning=log.warning,
    )


def _load_session_meta() -> dict[str, dict]:
    return session_registry.load_session_meta(SESSION_META_FILE)


def _persist_session_meta() -> None:
    session_registry.persist_session_meta(
        _SESSIONS,
        session_meta_file=SESSION_META_FILE,
        log_warning=log.warning,
    )


def _apply_session_meta() -> None:
    session_registry.apply_session_meta(
        _SESSIONS,
        session_meta_file=SESSION_META_FILE,
        log_info=log.info,
    )


def _load_read_cursors() -> dict[str, dict[str, int]]:
    return session_registry.load_read_cursors(READ_CURSOR_FILE)


def _persist_read_cursors() -> None:
    session_registry.persist_read_cursors(
        _READ_CURSORS,
        read_cursor_file=READ_CURSOR_FILE,
        log_warning=log.warning,
    )


def _mark_read(session_id: str, device_id: str, seq: int) -> None:
    session_registry.mark_read(_READ_CURSORS, session_id, device_id, seq)


def _unread_for(session: "Session", device_id: str) -> int:
    return session_registry.unread_for(_READ_CURSORS, session, device_id)


async def _broadcast_json(payload: dict) -> int:
    return await client_manager.broadcast_json(payload)


async def _send_unread_for_session(session: "Session") -> None:
    await client_manager.send_unread_for_session(session, _unread_for)


async def _send_unread_snapshot(ws: Any, client: ClientConn) -> None:
    # Push unread for EVERY session, including count=0. Client persists unread
    # to AsyncStorage; if we skip the zeros, stale unread badges from previous
    # sessions stay forever and break the dashboard sort (unread > 0 sessions
    # get sticky-pinned at the top by sortSessions). Must explicitly send 0
    # so client setUnread resets the badge.
    await client_manager.send_unread_snapshot(ws, client, _SESSIONS.values(), _unread_for)


async def _send_unread_snapshot_deferred(ws: Any, client: ClientConn, delay: float = 0.5) -> None:
    await asyncio.sleep(delay)
    if client_manager.CLIENTS.get(ws) is not client:
        return
    await _send_unread_snapshot(ws, client)


async def _send_unread_for_client_session(ws: Any, client: ClientConn, session: "Session") -> None:
    await client_manager.send_unread_for_client_session(ws, client, session, _unread_for)


async def _dispatch_event(payload: dict, session: "Session") -> bool:
    et = payload.get("type")
    if et in {"done", "stopped", "error"}:
        session.message_seq += 1
    if not client_manager.has_clients():
        # No live clients — signal undelivered so send_event falls through to offline_buffer
        return False
    delivered = await _broadcast_json(payload)
    if delivered <= 0:
        return False
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
CODEX_SESSIONS_DIR = str(Path.home() / ".codex" / "sessions")
_WATCHER_LAST_MAX_MTIME: float = 0.0


_recent_msgs_cache: dict[str, tuple[float, list]] = {}


def _ensure_local_session_dirs() -> None:
    """Create expected local session source dirs (first-run friction reduction)."""
    for p in (CLAUDE_PROJECTS_DIR, CODEX_SESSIONS_DIR):
        try:
            Path(p).mkdir(parents=True, exist_ok=True)
        except Exception as exc:
            log.warning("Failed to create local session dir %s: %s", p, exc)

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
            log.info("JSONL initial scan (%s): source dir missing, skipped: %s", backend_name, base)
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
        except FileNotFoundError:
            log.info("JSONL initial scan (%s): source dir disappeared during scan, skipped", backend_name)
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

        if client_manager.has_clients():
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
                if client_manager.has_clients():
                    await _broadcast_json(build_sessions_list())
            except Exception as exc:
                log.warning("JSONL polling error: %s", exc)


def _session_has_valid_resume(s: "Session") -> bool:
    return session_registry.session_has_valid_resume(s)


def build_sessions_list() -> dict:
    return session_registry.build_sessions_list(
        _SESSIONS,
        recent_messages=_get_recent_messages_sync,
    )

def _session_to_summary(s: "Session") -> dict:
    return session_registry.session_to_summary(s, recent_messages=_get_recent_messages_sync)


async def _send_all_sessions(ws: Any, batch_size: int = 50) -> None:
    await session_registry.send_all_sessions(
        ws,
        _SESSIONS,
        recent_messages=_get_recent_messages_sync,
        batch_size=batch_size,
    )


# ---------------------------------------------------------------------------
# Active WebSocket clients — multi-client design
# ---------------------------------------------------------------------------
_AUTO_TUNNEL_TASK: "asyncio.Task | None" = None
_CLOUDFLARED_PROC: "asyncio.subprocess.Process | None" = None
_BRIDGE_PORT = 8766
_PERF = PerfTracker(slow_threshold_ms=250.0, report_interval_s=60.0)
_PERMISSION_MANAGER: "PermissionManager | None" = None
_INSTANCE_ID = "b_" + uuid.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------
async def _auto_tunnel_after_delay(delay: int = 30) -> None:
    await asyncio.sleep(delay)
    if client_manager.has_clients():
        return
    if _is_cloudflared_running():
        return
    log.info("No client for %ds — auto-starting Cloudflare tunnel", delay)
    print(f"\n[auto-tunnel] No client for {delay}s, starting tunnel...")
    await _start_cloudflared_tunnel(_BRIDGE_PORT)


async def _run_session_queue(session: Session) -> None:
    await _run_session_queue_impl(
        session,
        get_backend=_session_backend,
        broadcast_json=_broadcast_json,
    )



async def _session_cache_refresher() -> None:
    while True:
        await asyncio.sleep(300)
        await preload_sessions_cache(_BACKENDS)


async def handler(ws: ServerConnection) -> None:
    global _AUTO_TUNNEL_TASK

    # Liveness probe short-circuit.  The supervisor's bridge_healthcheck.py
    # opens a WS every 3s, sends a control PING, then closes.  Without this
    # gate the handler would (a) register the probe as a client, (b) reassign
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
    try:
        raw_first = await asyncio.wait_for(ws.recv(), timeout=20)
        first_msg = json.loads(str(raw_first))
    except asyncio.TimeoutError:
        try:
            await ws.send(json.dumps(_msg_error("Handshake timeout: expected hello")))
        except Exception:
            pass
        return
    except Exception:
        try:
            await ws.send(json.dumps(_msg_error("Handshake failed: invalid JSON")))
        except Exception:
            pass
        return

    first_err = validate_client_msg(first_msg)
    if first_err or first_msg.get("type") != "hello":
        try:
            await ws.send(json.dumps(_msg_error("Protocol error: first message must be hello")))
        except Exception:
            pass
        return
    if not _is_auth_token_valid(first_msg):
        try:
            await ws.send(json.dumps(_msg_error("Unauthorized: invalid auth token")))
        except Exception:
            pass
        return

    if isinstance(first_msg.get("device_id"), str) and first_msg.get("device_id", "").strip():
        client.device_id = first_msg["device_id"].strip()
    if isinstance(first_msg.get("device_name"), str) and first_msg.get("device_name", "").strip():
        client.device_name = first_msg["device_name"].strip()

    client_manager.register(ws, client)
    log.info("Client connected: %s (%s) device=%s", remote, client.client_id, client.device_id)
    _mark_client_activity()

    # Inject this ws into all existing sessions (reconnect scenario).
    # ws_ref must be set before hello_ack/sessions_list so live events dispatched
    # during the handshake go to this client instead of the offline buffer.
    for session in list(_SESSIONS.values()):
        session.ws_ref = ws

    try:
        _paired_token = _PAIRING.get("paired_token", "").strip()
        _provided_token = str(first_msg.get("auth_token") or "").strip()
        await ws.send(json.dumps({
            "type": "hello_ack",
            "client_id": client.client_id,
            "device_id": client.device_id,
            "device_name": client.device_name,
            "is_locked": bool(_paired_token),
            "locked_to_me": bool(_paired_token) and _paired_token == _provided_token,
        }))
        await ws.send(json.dumps(build_sessions_list()))

        # Replay offline buffers AFTER sessions_list so the frontend has already
        # run reconcileFromServer (and hydrated its session state) before it
        # processes buffered events.  Sending before sessions_list caused a cold-
        # start race where the Zustand store wasn't hydrated yet, so done/stopped
        # events were silently dropped and isStreaming stayed stuck.
        await replay_offline_buffers(ws, _SESSIONS.values())
        _spawn_task(f"unread-snapshot:connect:{client.client_id}", _send_unread_snapshot_deferred(ws, client))
        # Re-deliver any file pushes that were broadcast before this client connected
        for fid, entry in list(_PUSH_FILE_REGISTRY.items()):
            payload: dict = {
                "type": "file_push",
                "file_id": fid,
                "filename": entry["filename"],
                "size": entry["size"],
                "mime_type": entry["mime_type"],
            }
            if "data" in entry:
                payload["data"] = entry["data"]
            elif "url" in entry:
                payload["url"] = entry["url"]
            else:
                continue
            await ws.send(json.dumps(payload))
    except Exception:
        pass

    try:
        system_ctx = {
            "asyncio": asyncio,
            "sessions": _SESSIONS,
            "backends": _BACKENDS,
            "session_backend": _session_backend,
            "msg_resumable_sessions": _msg_resumable_sessions,
            "permission_mode": _PERMISSION_MANAGER.mode() if _PERMISSION_MANAGER else "off",
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
            "permission_manager": _PERMISSION_MANAGER,
            "client": client,
        }
        file_ctx = {
            "sessions": _SESSIONS,
            "backends": _BACKENDS,
            "msg_dir_listing": _msg_dir_listing,
            "fcm_token_file": FCM_TOKEN_FILE,
            "log": log,
        }
        router_ctx = RouterContext(
            sessions=_SESSIONS,
            build_sessions_list=build_sessions_list,
            broadcast_json=_broadcast_json,
            persist_session_meta=_persist_session_meta,
            send_all_sessions=_send_all_sessions,
            spawn_task=_spawn_task,
            handle_push_file=_handle_push_file,
            handle_file_push_ack=_handle_file_push_ack,
            msg_pong=_msg_pong,
            msg_session_history=_msg_session_history,
            send_unread_snapshot=_send_unread_snapshot,
            send_unread_for_client_session=_send_unread_for_client_session,
            mark_read=_mark_read,
            persist_read_cursors=_persist_read_cursors,
            send_session_history_response=_send_session_history_response,
            history_runtime_payload=_history_runtime_payload,
            emit_resume_progress=_emit_resume_progress,
            close_duplicate_device_clients=client_manager.close_duplicate_device_clients,
            log_warning=log.warning,
            log_debug=log.debug,
            sessions_lock=_SESSIONS_LOCK,
            max_sessions=MAX_SESSIONS,
            default_cwd=DEFAULT_CWD,
            normalize_backend_name=_normalize_backend_name,
            session_cls=Session,
            queued_command_cls=QueuedCommand,
            msg_session_created=_msg_session_created,
            msg_error=_msg_error,
            msg_session_renamed=_msg_session_renamed,
            session_backend=_session_backend,
            send_event=send_event,
            evt_session_warning=_evt_session_warning,
            evt_error=_evt_error,
            persist_session=_persist_session,
            read_cursors=_READ_CURSORS,
            remove_saved_session=lambda sid: session_registry.remove_saved_session(
                sid,
                saved_sessions_file=SAVED_SESSIONS_FILE,
                log_warning=log.warning,
            ),
            invalidate_sessions_cache=invalidate_sessions_cache,
            preload_sessions_cache=preload_sessions_cache,
            backends=_BACKENDS,
            load_session_history_for_transfer=_load_session_history_for_transfer,
            build_handoff_prompt=_build_handoff_prompt,
            run_session_queue=_run_session_queue,
            search_enabled=_search_enabled,
            get_search_worker=get_worker,
            strip_turn_aborted_notice=_strip_turn_aborted_notice,
            log_prompt_lifecycle=_log_prompt_lifecycle,
        )
        async for raw in ws:
            _mark_client_activity()
            raw_text = str(raw)
            raw_len = len(raw_text.encode("utf-8", errors="ignore"))

            try:
                msg = json.loads(raw_text)
            except json.JSONDecodeError:
                log.warning("Non-JSON from client: bytes=%d", raw_len)
                continue
            log.debug("Received: %s", _summarize_client_msg(msg, raw_len))

            # --- Inbound schema validation ---
            validation_err = validate_client_msg(msg)
            if validation_err:
                log.warning("Invalid client msg: %s | %s", validation_err, _summarize_client_msg(msg, raw_len))
                try:
                    await ws.send(json.dumps(_msg_error(f"Protocol error: {validation_err}")))
                except Exception:
                    pass
                continue

            mtype = msg["type"]  # safe after validation
            runtime_ctx["client"] = client
            if mtype == "hello":
                if isinstance(msg.get("device_id"), str) and msg.get("device_id", "").strip():
                    client.device_id = msg["device_id"].strip()
                if isinstance(msg.get("device_name"), str) and msg.get("device_name", "").strip():
                    client.device_name = msg["device_name"].strip()
                _paired_token = _PAIRING.get("paired_token", "").strip()
                _provided_token = str(msg.get("auth_token") or "").strip()
                await ws.send(json.dumps({
                    "type": "hello_ack",
                    "client_id": client.client_id,
                    "device_id": client.device_id,
                    "device_name": client.device_name,
                    "is_locked": bool(_paired_token),
                    "locked_to_me": bool(_paired_token) and _paired_token == _provided_token,
                }))
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue
            op_started = time.perf_counter()

            if mtype == "claim_bridge":
                global _PAIRING
                token = str(msg.get("auth_token") or "").strip()
                device_id = str(msg.get("device_id") or "").strip()
                if not token:
                    await ws.send(json.dumps(_msg_error("auth_token required for claim_bridge")))
                    _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                    continue
                existing = _PAIRING.get("paired_token", "").strip()
                if existing and existing != token:
                    await ws.send(json.dumps(_msg_error("Bridge already claimed by another device")))
                    _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                    continue
                _PAIRING = {"paired_token": token, "paired_device_id": device_id, "paired_at": int(time.time())}
                _save_pairing(_PAIRING)
                log.info("Bridge claimed by device_id=%s", device_id)
                await ws.send(json.dumps({"type": "claim_ack", "is_locked": True, "locked_to_me": True}))
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue

            if mtype == "unclaim_bridge":
                global _PAIRING
                token = str(msg.get("auth_token") or "").strip()
                paired = _PAIRING.get("paired_token", "").strip()
                if paired and paired != token:
                    await ws.send(json.dumps(_msg_error("Unauthorized: token mismatch")))
                    _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                    continue
                _PAIRING = {}
                _clear_pairing()
                log.info("Bridge unclaimed")
                await ws.send(json.dumps({"type": "unclaim_ack", "is_locked": False}))
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue

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

            if await handle_low_coupling_message(
                mtype=mtype,
                msg=msg,
                ws=ws,
                client=client,
                ctx=router_ctx,
            ):
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue

            log.debug("No direct handler matched for type=%s", mtype)

            _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)

    except Exception as exc:
        name = type(exc).__name__
        if "ConnectionClosed" in name:
            log.info("Client disconnected: %s (%s)", remote, exc)
        else:
            log.exception("Unhandled error in handler: %s", exc)
    finally:
        client_manager.remove(ws)
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
            and not client_manager.has_clients()
            and not _is_cloudflared_running()
        ):
            _AUTO_TUNNEL_TASK = _spawn_task("auto-tunnel-delayed", _auto_tunnel_after_delay(120))


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
    if os.environ.get("BRIDGE_DISABLE_MDNS", "0") == "1":
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
            _spawn_task("cloudflared-drain-stderr", _drain_proc_stderr(proc))
            _spawn_task("notify-fcm:tunnel", _notify_fcm_tunnel(ws_url))
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
               ollama_host: str = "http://localhost:11434",
               discovery_port: int = 8767,
               no_discovery: bool = False) -> None:
    global CLAUDE_BIN, CODEX_BIN, BUN_BIN, _DEFAULT_BACKEND_NAME, _DEFAULT_OLLAMA_MODEL, _OLLAMA_HOST, _PERMISSION_MANAGER

    _DEFAULT_BACKEND_NAME = _normalize_backend_name(backend_name)
    _DEFAULT_OLLAMA_MODEL = model or "llama3.2"
    _OLLAMA_HOST = ollama_host
    _ensure_local_session_dirs()
    _PERMISSION_MANAGER = PermissionManager(
        _broadcast_json,
        ttl_seconds=int(os.environ.get("BRIDGE_PERMISSION_TIMEOUT_SEC", "60")),
        mode=os.environ.get("BRIDGE_PERMISSION_MODE", "enforce"),
    )

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
    _READ_CURSORS.clear()
    _READ_CURSORS.update(_load_read_cursors())
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
    broadcaster: "DiscoveryBroadcaster | None" = None
    if not no_discovery:
        broadcaster = DiscoveryBroadcaster(
            ws_port=port,
            discovery_port=discovery_port,
            instance_id=_INSTANCE_ID,
            version="2.0",
        )
        await broadcaster.start()
    serve_kwargs = {
        "handler": handler,
        "host": ["0.0.0.0", "::"],
        "port": port,
        "ping_interval": 30,
        "ping_timeout": 60,
        "max_size": None,
        "process_request": _media_request_handler,
        "compression": "deflate",
    }
    if os.name != "nt":
        serve_kwargs["reuse_port"] = True

    async with serve(**serve_kwargs):
        log.info("Bridge v2 listening on port %d (IPv4 + IPv6)", port)
        if tunnel:
            _spawn_task("cloudflared-start", _start_cloudflared_tunnel(port))
        init_cache_db()
        _spawn_task("preload-sessions-cache:startup", preload_sessions_cache(_BACKENDS))
        _spawn_task("session-cache-refresher", _session_cache_refresher())
        _spawn_task("history-cache-warmup", _warmup_history_cache_background())
        _spawn_task("jsonl-watcher", _jsonl_watcher_task())
        try:
            await asyncio.Future()  # run forever
        finally:
            await _cancel_tasks()
            await _shutdown_search()
            if broadcaster:
                await broadcaster.stop()
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
    parser.add_argument("--discovery-port", type=int, default=8767,
                        help="UDP port for LAN discovery broadcasts (default: 8767)")
    parser.add_argument("--no-discovery", action="store_true",
                        help="Disable UDP LAN discovery broadcasts")
    args = parser.parse_args()
    asyncio.run(main(
        args.port,
        tunnel=args.tunnel,
        backend_name=args.backend,
        model=args.model,
        ollama_host=args.ollama_host,
        discovery_port=args.discovery_port,
        no_discovery=args.no_discovery,
    ))
