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
from task_manager import cancel_owner as _cancel_client_tasks
from task_manager import spawn as _spawn_task
from utils.uuid_helper import is_valid_uuid
from permission_manager import PermissionManager
from protocol import _KNOWN_MSG_TYPES, parse_client_command, validate_client_msg
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
    send_event, stream_text, scan_for_media, set_media_base_url, set_http_serve_dir,
    _evt_error, _evt_done, _evt_stopped, _evt_session_warning, _evt_session_died, _evt_session_closed,
    _evt_resume_progress,
    _evt_text_chunk, _evt_tool_start, _evt_tool_result, _evt_tool_end, _evt_media,
    _msg_pong, _msg_error, _msg_session_created, _msg_session_renamed,
    _msg_session_history, _msg_history_snapshot, _msg_history_delta, _msg_resumable_sessions, _msg_session_uuid,
    _msg_shell_created, _msg_shell_output, _msg_shell_closed,
    _msg_tasks_list, _msg_task_killed, _msg_processes_list, _msg_process_killed, _msg_dir_listing, _msg_usage_report,
    _msg_agent_tree,
    set_event_dispatcher,
    stop_session_drain,
    get_generation,
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


# Pairing persistence (_load_pairing/_save_pairing/_clear_pairing) lives in
# pairing.py; the in-memory _PAIRING dict stays here.
from pairing import _load_pairing, _save_pairing, _clear_pairing
# Binary / Tailscale discovery helpers live in bin_discovery.py.
from bin_discovery import (
    _find_claude_bin, _find_bun_bin, _find_codex_bin, _detect_tailscale_ip,
)
# Pure client-message helpers live in client_msg_utils.py.
from client_msg_utils import _short_id, _summarize_client_msg, _build_handoff_prompt

_PAIRING: dict = {}


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


async def _init_search() -> None:
    global _search_pool, _search_enabled
    if not _SEARCH_AVAILABLE:
        _search_enabled = False
        return
    cfg = get_config()
    cfg.root_dir = _ROOT_DIR
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


def _session_in_scope(session: "Session") -> bool:
    """Return True if this session's cwd is inside the instance root_dir jail."""
    if not _ROOT_DIR:
        return True
    if not session.cwd:
        return False
    from utils.path_jail import is_inside_jail
    return is_inside_jail(os.path.realpath(session.cwd), _ROOT_DIR)


def _scoped_sessions() -> dict:
    """Return a filtered snapshot of _SESSIONS restricted to root_dir scope."""
    if not _ROOT_DIR:
        return _SESSIONS
    return {k: v for k, v in _SESSIONS.items() if _session_in_scope(v)}

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
            chunk = await shell.proc.stdout.read(4096)
            if not chunk:
                break
            if shell.ws_ref:
                try:
                    await shell.ws_ref.send(json.dumps(
                        _msg_shell_output(shell.shell_id, chunk.decode("utf-8", errors="replace"))
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
    # Post-load: refresh latest_source_line from the cache that was just populated
    # (or validated) by load_session_history.  After that call returns, the cache
    # for this resume_id is guaranteed to reflect the current file state, so it is
    # safe to read it here — unlike at result-event time where the cache can be stale.
    try:
        from backends.history import _JSONL_HISTORY_CACHE
        _rid = session.resume_id or ""
        _post_idx = _JSONL_HISTORY_CACHE.get(f"claude:{_rid}") or _JSONL_HISTORY_CACHE.get(f"codex:{_rid}")
        if _post_idx and _post_idx.messages:
            _post_lsl = str(_post_idx.messages[-1].get("source_message_id") or "")
            if _post_lsl:
                session.latest_source_line = _post_lsl
    except Exception:
        pass
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
    await client_manager.send_unread_snapshot(ws, client, _scoped_sessions().values(), _unread_for)


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
        _scoped_sessions(),
        recent_messages=_get_recent_messages_sync,
    )

def _session_to_summary(s: "Session") -> dict:
    return session_registry.session_to_summary(s, recent_messages=_get_recent_messages_sync)


async def _send_all_sessions(ws: Any, batch_size: int = 50) -> None:
    await session_registry.send_all_sessions(
        ws,
        _scoped_sessions(),
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


# The WebSocket connection handler lives in handlers/connection.py.
# It is re-exported at the bottom of this module (see end of file) so
# `bridge_v2.handler` keeps working for main(), webrtc re-entry, and tests.



async def _warmup_history_cache_background() -> None:
    """啟動後延遲 8 秒，趁空閒預建所有 session 的 history index。

    依 last_activity 排序（最近用的先跑），用 semaphore 控制 4 路並行，
    讓重連後 request_history 能直接命中記憶體 cache，無須重新解析 JSONL。
    """
    await asyncio.sleep(8)

    # Legacy per-backend warmup (each backend's own get_resumable_sessions call)
    for name, backend in _BACKENDS.items():
        if not hasattr(backend, "warmup_history_cache"):
            continue
        try:
            await backend.warmup_history_cache()
        except Exception as exc:
            log.warning("warmup_history_cache_background [%s] failed: %s", name, exc)

    # Extended warmup: cover ALL sessions in _SESSIONS (not just the 30 from Claude CLI)
    from backends.history import _JSONL_HISTORY_CACHE
    sessions_sorted = sorted(
        _SESSIONS.values(),
        key=lambda s: s.last_activity,
        reverse=True,
    )
    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(4)
    warmed = skipped = 0

    async def _warm_one(session: "Session") -> None:
        nonlocal warmed, skipped
        if not session.resume_id:
            return
        backend = _session_backend(session)
        if not hasattr(backend, "_load_session_history_sync"):
            return
        cache_key = f"claude:{session.resume_id}"
        if cache_key in _JSONL_HISTORY_CACHE:
            # Cache already warm — still update latest_source_line if missing.
            if not session.latest_source_line:
                idx = _JSONL_HISTORY_CACHE.get(cache_key)
                if idx and idx.messages:
                    lsl = str(idx.messages[-1].get("source_message_id") or "")
                    if lsl:
                        session.latest_source_line = lsl
            skipped += 1
            return
        async with sem:
            try:
                await loop.run_in_executor(
                    None,
                    backend._load_session_history_sync,
                    session.resume_id, DEFAULT_HISTORY_LIMIT, "", "snapshot", "",
                )
                warmed += 1
                # Back-fill latest_source_line from freshly built cache entry.
                idx = _JSONL_HISTORY_CACHE.get(cache_key)
                if idx and idx.messages:
                    lsl = str(idx.messages[-1].get("source_message_id") or "")
                    if lsl:
                        session.latest_source_line = lsl
                await asyncio.sleep(0.02)
            except Exception:
                pass

    await asyncio.gather(*[_warm_one(s) for s in sessions_sorted], return_exceptions=True)
    if warmed or skipped:
        log.info("history-cache-warmup: warmed=%d skipped=%d of %d sessions", warmed, skipped, len(sessions_sorted))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def _reconfigure_logging() -> None:
    """Re-configure root logger to write to the correct data_dir log file."""
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)
    logging.basicConfig(
        level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
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


async def _serve_forever(port: int, tunnel: bool, broadcaster, zc) -> None:
    """Open the websockets server and run until cancelled, then tear down
    background tasks, the search worker, the discovery broadcaster and mDNS."""
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
        with open(_TUNNEL_URL_FILE) as _f:
            _stored = _f.read().strip()
        if _stored:
            set_current_tunnel_url(_stored)
            log.info("Loaded tunnel URL from file: %s", _stored)

    global _PAIRING
    _PAIRING = _load_pairing()

    _reconfigure_logging()

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
    set_http_serve_dir(_ROOT_DIR)

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
    zc = await _start_mdns(port)
    broadcaster: "DiscoveryBroadcaster | None" = None
    if not no_discovery:
        broadcaster = DiscoveryBroadcaster(
            ws_port=port,
            discovery_port=discovery_port,
            instance_id=_INSTANCE_ID,
            version="2.0",
        )
        await broadcaster.start()

    await _serve_forever(port, tunnel, broadcaster, zc)


# --- bottom re-export: defined here (after all module state) so that
# handlers/connection.py can `import bridge_v2` and resolve bv.* attributes. ---
from handlers.connection import handler  # noqa: E402

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
