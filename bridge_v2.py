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
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
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
from typing import Any, Dict, Optional, TYPE_CHECKING

import client_manager
import session_registry
from client_manager import ClientConn
from session_registry import QueuedCommand, Session
from websockets.asyncio.server import serve, ServerConnection
from handlers.feed_ops import (
    configure as configure_feed_ops,
    load_feed_index as _load_feed_index,
    feed_gc_deleted as _feed_gc_deleted,
)
from handlers.file_ops import handle_file_msg, preload_sessions_cache, invalidate_sessions_cache
from handlers.perf import PerfTracker
from handlers.runtime_ops import handle_runtime_msg
from handlers.system_ops import handle_system_msg
from handlers.webrtc_signaling import (
    WEBRTC_MESSAGE_TYPES,
    cleanup_for_ws as _webrtc_cleanup_for_ws,
    handle_webrtc_message,
)
from handlers.interactions_ws import handle_interaction_message, send_pending_interactions
from message_router import RouterContext, handle_low_coupling_message
from offline_replay import replay_offline_buffers
from queue_runner import log_prompt_lifecycle as _log_prompt_lifecycle
from queue_runner import run_session_queue as _run_session_queue_impl
from task_manager import cancel_all as _cancel_tasks
from task_manager import spawn as _spawn_task
from utils.uuid_helper import is_valid_uuid
from permission_manager import PermissionManager
from protocol import _KNOWN_MSG_TYPES, validate_client_msg
from push_registry import (
    configure as configure_push_registry,
    handle_file_push_ack as _handle_file_push_ack,
    handle_push_file as _handle_push_file,
    init_firebase,
    load_inbox as _load_inbox,
    notify_fcm,
    notify_fcm_tunnel_with_retry as _notify_fcm_tunnel_with_retry,
    pending_file_push_items,
    send_tunnel_fcm_once as _do_send_tunnel_fcm,
)
from jsonl_sessions import (
    configure as configure_jsonl_sessions,
    ensure_local_session_dirs as _ensure_local_session_dirs,
    get_recent_messages_sync as _get_recent_messages_sync,
    jsonl_watcher_task as _jsonl_watcher_task,
    strip_turn_aborted_notice as _strip_turn_aborted_notice,
)
from network_services import (
    configure as configure_network_services,
    get_current_tunnel_url,
    is_cloudflared_running as _is_cloudflared_running,
    is_tunnel_url_delivered,
    mark_tunnel_url_delivered,
    media_request_handler as _media_request_handler,
    set_current_tunnel_url,
    start_cloudflared_tunnel as _start_cloudflared_tunnel,
    start_mdns as _start_mdns,
    tunnel_url_file_watcher as _tunnel_url_file_watcher,
)
from discovery_broadcaster import DiscoveryBroadcaster

try:
    import socket
except ImportError:
    socket = None  # type: ignore[assignment]

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
    _msg_agent_tree,
    set_event_dispatcher,
)
from backends.history import DEFAULT_HISTORY_LIMIT, clamp_history_limit
from backends.history_sqlite import init_cache_db

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

_DATA_DIR: str = ""  # set by _init_paths() in main()
_ROOT_DIR: str = ""  # set in main(); "" means no jail
_INSTANCE_NAME: str = ""  # set in main(); from --instance-name or BRIDGE_INSTANCE_NAME env
_LAN_IP: str = ""  # set in main(); LAN IP sent in hello_ack so clients can auto-update their config
_INSTANCE_ID: str = ""  # set by _init_paths() from bridge_identity.json (stable across restarts)

# Restart-agent trigger file — bridge writes this to ask launchd's
# restart-agent (a sibling service, NOT a bridge child) to kickstart us.
_RESTART_TRIGGER_PATH: str = os.environ.get(
    "BRIDGE_RESTART_TRIGGER",
    str(Path.home() / ".claude-bridge-runtime" / ".restart-trigger"),
)


def _load_or_create_stable_id(data_dir: str) -> str:
    """Load a stable bridge ID from bridge_identity.json, creating it on first run."""
    identity_file = os.path.join(data_dir, "bridge_identity.json")
    try:
        with open(identity_file) as f:
            data = json.load(f)
        stored = data.get("bridge_id", "")
        if isinstance(stored, str) and stored.startswith("b_") and len(stored) > 4:
            return stored
    except Exception:
        pass
    new_id = "b_" + uuid.uuid4().hex[:12]
    try:
        with open(identity_file, "w") as f:
            json.dump({"bridge_id": new_id, "created_at": int(time.time())}, f)
    except Exception:
        pass
    return new_id


def _init_paths(data_dir: str) -> None:
    global LOG_FILE, FCM_TOKEN_FILE, SERVICE_ACCOUNT_FILE, \
           SAVED_SESSIONS_FILE, CODEX_SAVED_SESSIONS_FILE, \
           SESSION_META_FILE, READ_CURSOR_FILE, PAIRING_FILE, _DATA_DIR, _INSTANCE_ID
    _DATA_DIR = data_dir
    os.makedirs(data_dir, exist_ok=True)
    LOG_FILE                  = os.path.join(data_dir, "bridge_v2.log")
    FCM_TOKEN_FILE            = os.path.join(data_dir, "fcm_token.txt")
    SERVICE_ACCOUNT_FILE      = os.path.join(data_dir, "serviceAccountKey.json")
    SAVED_SESSIONS_FILE       = os.path.join(data_dir, "saved_sessions.json")
    CODEX_SAVED_SESSIONS_FILE = os.path.join(data_dir, "saved_sessions_codex.json")
    SESSION_META_FILE         = os.path.join(data_dir, "session_meta.json")
    READ_CURSOR_FILE          = os.path.join(data_dir, "read_cursors.json")
    PAIRING_FILE              = os.path.join(data_dir, "pairing.json")
    # Point search index into this instance's data dir so instances don't share search.db
    os.environ["BRIDGE_SEARCH__INDEX_PATH"] = os.path.join(data_dir, "search.db")
    _INSTANCE_ID = _load_or_create_stable_id(data_dir)
    # Deferred log — logger may not be configured yet at this point; will be printed after basicConfig
    print(f"[bridge] stable instance_id={_INSTANCE_ID}")


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


_PAIRING: dict = {}


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
        backend = CodexAppServerBackend(
            codex_bin=CODEX_BIN,
            broadcast_fn=_broadcast_json,
            notify_fcm_fn=notify_fcm,
            persist_session_fn=_persist_session,
        )
    elif backend_name == "ollama":
        from backends.ollama import OllamaBackend
        backend = OllamaBackend(model=_DEFAULT_OLLAMA_MODEL, host=_OLLAMA_HOST,
                                notify_fcm_fn=notify_fcm)
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
        root_dir=_ROOT_DIR,
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
# Path to the file written by cloudflared_launcher.sh (external tunnel management).
# Set from BRIDGE_TUNNEL_URL_FILE env var in main().  Empty = self-managed mode.
_TUNNEL_URL_FILE: str = ""
_BRIDGE_PORT = 8766
_PERF = PerfTracker(slow_threshold_ms=250.0, report_interval_s=60.0)
_PERMISSION_MANAGER: "PermissionManager | None" = None


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


async def _cloudflared_watchdog() -> None:
    """Restart cloudflared if it crashes while no client is connected."""
    while True:
        await asyncio.sleep(60)
        if _is_cloudflared_running():
            continue
        if client_manager.has_clients():
            # Client is present — normal client-disconnect path will handle this
            continue
        log.info("cloudflared watchdog: process died with no clients — restarting tunnel")
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
    global _AUTO_TUNNEL_TASK, _PAIRING

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

    # Proactively start tunnel so client gets the URL while still on WiFi,
    # avoiding FCM dependency on the next reconnect.
    if os.environ.get("BRIDGE_AUTO_TUNNEL") == "1" and not _is_cloudflared_running():
        _spawn_task("cloudflared-start:on-connect", _start_cloudflared_tunnel(_BRIDGE_PORT))

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
        tunnel_url = get_current_tunnel_url()
        log.info("hello_ack → client=%s instance_id=%s tunnel=%s",
                 client.client_id, _INSTANCE_ID, bool(tunnel_url))
        await ws.send(json.dumps({
            "type": "hello_ack",
            "instance_id": _INSTANCE_ID,
            "client_id": client.client_id,
            "device_id": client.device_id,
            "device_name": client.device_name,
            "is_locked": bool(_paired_token),
            "locked_to_me": bool(_paired_token) and _paired_token == _provided_token,
            "instance_name": _INSTANCE_NAME,
            "root_dir": _ROOT_DIR,
            "data_dir": _DATA_DIR,
            **({"lan_ip": _LAN_IP} if _LAN_IP else {}),
            **({"tunnel_url": tunnel_url} if tunnel_url else {}),
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
        device_id = client.device_id or ""
        for item in pending_file_push_items(device_id):
            payload = {"type": "file_push", **item}
            try:
                await ws.send(json.dumps(payload))
            except Exception:
                break
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
            "restart_trigger_path": _RESTART_TRIGGER_PATH,
            "msg_agent_tree": _msg_agent_tree,
        }
        runtime_ctx = {
            "sessions": _SESSIONS,
            "shell_sessions": _SHELL_SESSIONS,
            "max_shells": MAX_SHELLS,
            "session_backend": _session_backend,
            "shell_cls": ShellSession,
            "root_dir": _ROOT_DIR,
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
            "root_dir": _ROOT_DIR,
            "get_tunnel_url": get_current_tunnel_url,
            "is_tunnel_delivered": is_tunnel_url_delivered,
            "notify_tunnel_fcm_once": _do_send_tunnel_fcm,
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
            root_dir=_ROOT_DIR,
            data_dir=_DATA_DIR,
            instance_name=_INSTANCE_NAME,
            pairing=_PAIRING,
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
                    "instance_id": _INSTANCE_ID,
                    "client_id": client.client_id,
                    "device_id": client.device_id,
                    "device_name": client.device_name,
                    "is_locked": bool(_paired_token),
                    "locked_to_me": bool(_paired_token) and _paired_token == _provided_token,
                    "instance_name": _INSTANCE_NAME,
                    "root_dir": _ROOT_DIR,
                    "data_dir": _DATA_DIR,
                    **({"lan_ip": _LAN_IP} if _LAN_IP else {}),
                    **({"tunnel_url": get_current_tunnel_url()} if get_current_tunnel_url() else {}),
                }))
                await send_pending_interactions(ws)
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue
            op_started = time.perf_counter()

            if mtype == "claim_bridge":
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
                _PAIRING.clear()
                _PAIRING.update({"paired_token": token, "paired_device_id": device_id, "paired_at": int(time.time())})
                _save_pairing(_PAIRING)
                log.info("Bridge claimed by device_id=%s", device_id)
                await ws.send(json.dumps({"type": "claim_ack", "is_locked": True, "locked_to_me": True}))
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue

            if mtype == "unclaim_bridge":
                token = str(msg.get("auth_token") or "").strip()
                paired = _PAIRING.get("paired_token", "").strip()
                if paired and paired != token:
                    await ws.send(json.dumps(_msg_error("Unauthorized: token mismatch")))
                    _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                    continue
                _PAIRING.clear()
                _clear_pairing()
                log.info("Bridge unclaimed")
                await ws.send(json.dumps({"type": "unclaim_ack", "is_locked": False}))
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue

            if mtype == "get_inbox":
                _inbox_conn = client_manager.CLIENTS.get(ws)
                inbox_device_id = (_inbox_conn.device_id if _inbox_conn else "") or ""
                inbox_items = pending_file_push_items(inbox_device_id, include_pushed_at=True)
                try:
                    await ws.send(json.dumps({"type": "inbox_list", "items": inbox_items}))
                except Exception:
                    pass
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue

            if mtype == "tunnel_url_ack":
                mark_tunnel_url_delivered()
                log.info("tunnel_url_ack received — FCM retry cancelled")
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue

            if await handle_interaction_message(
                mtype=mtype,
                msg=msg,
                ws=ws,
                sessions=_SESSIONS,
                session_backend=_session_backend,
                broadcast_json=_broadcast_json,
                msg_error=_msg_error,
            ):
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
               no_discovery: bool = False,
               data_dir: str = "",
               root_dir: str = "",
               instance_name: str = "") -> None:
    global CLAUDE_BIN, CODEX_BIN, BUN_BIN, _DEFAULT_BACKEND_NAME, _DEFAULT_OLLAMA_MODEL, _OLLAMA_HOST, _PERMISSION_MANAGER

    resolved_data_dir = (
        os.path.realpath(os.path.expanduser(data_dir))
        if data_dir
        else os.environ.get("BRIDGE_DATA_DIR", "") or BRIDGE_DIR
    )
    _init_paths(resolved_data_dir)

    # Load tunnel URL from external file if cloudflared is managed by launchd.
    global _TUNNEL_URL_FILE
    _TUNNEL_URL_FILE = os.environ.get("BRIDGE_TUNNEL_URL_FILE", "")
    if _TUNNEL_URL_FILE and os.path.isfile(_TUNNEL_URL_FILE):
        _stored = open(_TUNNEL_URL_FILE).read().strip()
        if _stored:
            set_current_tunnel_url(_stored)
            log.info("Loaded tunnel URL from file: %s", _stored)

    global _PAIRING
    _PAIRING = _load_pairing()

    # Re-configure root logger to write to the correct data_dir log file
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            RotatingFileHandler(
                LOG_FILE,
                maxBytes=10 * 1024 * 1024,  # 10 MB per file
                backupCount=3,
                encoding="utf-8",
            ),
            logging.StreamHandler(sys.stdout),
        ],
    )

    global _ROOT_DIR
    if root_dir:
        _ROOT_DIR = os.path.realpath(os.path.expanduser(root_dir))
        if not os.path.isdir(_ROOT_DIR):
            print(f"[bridge] ERROR: --root-dir {root_dir!r} does not exist or is not a directory", file=sys.stderr)
            sys.exit(1)
        log.info("[bridge] root_dir=%s", _ROOT_DIR)
    else:
        _ROOT_DIR = os.environ.get("BRIDGE_ROOT_DIR", "")
        if _ROOT_DIR:
            _ROOT_DIR = os.path.realpath(os.path.expanduser(_ROOT_DIR))

    global _INSTANCE_NAME, _LAN_IP
    _INSTANCE_NAME = instance_name or os.environ.get("BRIDGE_INSTANCE_NAME", "")
    try:
        _LAN_IP = socket.gethostbyname(socket.gethostname())
    except Exception:
        _LAN_IP = ""

    _DEFAULT_BACKEND_NAME = _normalize_backend_name(backend_name)
    _DEFAULT_OLLAMA_MODEL = model or "llama3.2"
    _OLLAMA_HOST = ollama_host
    configure_jsonl_sessions(
        sessions=_SESSIONS,
        default_cwd=DEFAULT_CWD,
        claude_projects_dir=CLAUDE_PROJECTS_DIR,
        session_backend=_session_backend,
        broadcast_json=_broadcast_json,
        build_sessions_list=build_sessions_list,
        dispatch_event=_dispatch_event,
        evt_done=_evt_done,
        log=log,
    )
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
    configure_network_services(
        root_dir=_ROOT_DIR,
        instance_id=_INSTANCE_ID,
        log=log,
        broadcast_json=_broadcast_json,
        spawn_task=_spawn_task,
        notify_tunnel_with_retry=_notify_fcm_tunnel_with_retry,
        set_media_base_url=set_media_base_url,
    )
    configure_push_registry(
        data_dir=_DATA_DIR,
        root_dir=_ROOT_DIR,
        fcm_token_file=FCM_TOKEN_FILE,
        service_account_file=SERVICE_ACCOUNT_FILE,
        instance_id=_INSTANCE_ID,
        log=log,
        broadcast_json=_broadcast_json,
        spawn_task=_spawn_task,
        is_tunnel_delivered=is_tunnel_url_delivered,
    )
    init_firebase()
    _load_inbox()
    configure_feed_ops(
        data_dir=_DATA_DIR,
        fcm_token_file=FCM_TOKEN_FILE,
        broadcast_json=_broadcast_json,
        spawn_task=_spawn_task,
    )
    _load_feed_index()
    _feed_gc_deleted()
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
        if _TUNNEL_URL_FILE:
            # External tunnel management: cloudflared_launcher.sh owns the process.
            # Bridge just polls the URL file and broadcasts changes.
            log.info("External tunnel mode: watching %s", _TUNNEL_URL_FILE)
            _spawn_task("tunnel-url-watcher", _tunnel_url_file_watcher(_TUNNEL_URL_FILE))
        else:
            # Self-managed tunnel (legacy / manual --tunnel flag).
            if tunnel:
                _spawn_task("cloudflared-start", _start_cloudflared_tunnel(port))
            if os.environ.get("BRIDGE_AUTO_TUNNEL") == "1":
                _spawn_task("cloudflared-watchdog", _cloudflared_watchdog())
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
    parser.add_argument("--data-dir", default="",
                        help="Per-instance data directory for persistence files (default: bridge source dir)")
    parser.add_argument("--root-dir", default="",
                        help="Filesystem jail root; user-provided paths cannot escape this directory (default: no jail)")
    parser.add_argument("--instance-name", default="",
                        help="Instance name for identification in hello_ack (default: empty)")
    args = parser.parse_args()
    asyncio.run(main(
        args.port,
        tunnel=args.tunnel,
        backend_name=args.backend,
        model=args.model,
        ollama_host=args.ollama_host,
        discovery_port=args.discovery_port,
        no_discovery=args.no_discovery,
        data_dir=args.data_dir,
        root_dir=args.root_dir,
        instance_name=args.instance_name,
    ))
