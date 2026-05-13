#!/usr/bin/env python3
"""
Claude Bridge v2 — Multi-session WebSocket server that proxies Claude Code CLI.
Supports multiple independent concurrent Claude sessions, each backed by a
persistent claude CLI subprocess.

Uses websockets.asyncio.server API (websockets >= 14).
Default port: 8766 (v1 keeps 8765).
"""

import argparse
import asyncio
from collections import deque
import json
import logging
import os
import re
import shutil
import subprocess
import time
import sys
import uuid
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Literal, NotRequired, Optional, TYPE_CHECKING, TypedDict

from websockets.asyncio.server import serve, ServerConnection
from handlers.file_ops import handle_file_msg, preload_sessions_cache, invalidate_sessions_cache
from handlers.perf import PerfTracker
from handlers.runtime_ops import handle_runtime_msg
from handlers.system_ops import handle_system_msg

try:
    import socket
    from zeroconf import ServiceInfo, Zeroconf
    _ZEROCONF_AVAILABLE = True
except ImportError:
    _ZEROCONF_AVAILABLE = False
from backends.events import (
    send_event, stream_text, scan_for_media, ensure_http_server,
    _evt_error, _evt_done, _evt_stopped, _evt_session_warning, _evt_session_died, _evt_session_closed,
    _evt_resume_progress,
    _evt_text_chunk, _evt_tool_start, _evt_tool_result, _evt_tool_end, _evt_media,
    _msg_pong, _msg_error, _msg_sessions_list, _msg_session_created, _msg_session_renamed,
    _msg_session_history, _msg_resumable_sessions, _msg_session_uuid,
    _msg_shell_created, _msg_shell_output, _msg_shell_closed,
    _msg_tasks_list, _msg_task_killed, _msg_processes_list, _msg_process_killed, _msg_dir_listing, _msg_usage_report,
    set_event_dispatcher,
)

try:
    import firebase_admin
    from firebase_admin import credentials, messaging as fb_messaging
    _FIREBASE_AVAILABLE = True
except ImportError:
    _FIREBASE_AVAILABLE = False

BRIDGE_DIR           = os.path.dirname(os.path.abspath(__file__))
LOG_FILE             = os.path.join(BRIDGE_DIR, "bridge_v2.log")
CLAUDE_BIN           = ""
CODEX_BIN            = ""
BUN_BIN              = ""
DEFAULT_CWD          = os.path.expanduser("~")
MAX_SESSIONS         = 0
MAX_SHELLS           = 5
FCM_TOKEN_FILE        = os.path.join(BRIDGE_DIR, "fcm_token.txt")
SERVICE_ACCOUNT_FILE  = os.path.join(BRIDGE_DIR, "serviceAccountKey.json")
SAVED_SESSIONS_FILE   = os.path.join(BRIDGE_DIR, "saved_sessions.json")
SESSION_META_FILE     = os.path.join(BRIDGE_DIR, "session_meta.json")
READ_CURSOR_FILE      = os.path.join(BRIDGE_DIR, "read_cursors.json")
CLAUDE_PROJECTS_DIR   = os.path.expanduser("~/.claude/projects")
LOCK_TTL_SECONDS      = 60

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
        from backends.codex_cli import CodexCliBackend
        backend = CodexCliBackend(codex_bin=CODEX_BIN)
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
    sandbox: str = "workspace-write"
    context_used: int = 0
    backend_name: str = "claude"
    pinned: bool = False
    hidden: bool = False
    lock_owner_device: str = ""
    lock_owner_device_name: str = ""
    lock_owner_client: str = ""
    lock_until: float = 0.0
    queue: Deque[QueuedCommand] = field(default_factory=deque)
    processing: bool = False
    current_request_id: str = ""
    message_seq: int = 0
    pending_clients: set[str] = field(default_factory=set)
    ws_ref: Optional[Any] = field(default=None, repr=False)
    offline_buffer: list = field(default_factory=list)


# Legacy alias — keeps any remaining references working during transition
ClaudeSession = Session


async def _load_session_history_for_transfer(session: "Session", limit: int = 80) -> list[dict]:
    try:
        if not session.resume_id:
            return []
        backend = _session_backend(session)
        if not backend.supports_resume():
            return []
        history = await backend.load_session_history(session.resume_id, limit=limit)
        return history if isinstance(history, list) else []
    except Exception:
        return []


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

def init_firebase() -> None:
    global _firebase_app
    if not _FIREBASE_AVAILABLE:
        log.warning("firebase-admin not installed — FCM disabled. Run: pip install firebase-admin")
        return
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        log.warning("serviceAccountKey.json not found at %s — FCM disabled", SERVICE_ACCOUNT_FILE)
        return
    try:
        cred = credentials.Certificate(SERVICE_ACCOUNT_FILE)
        _firebase_app = firebase_admin.initialize_app(cred)
        log.info("Firebase Admin initialized")
    except Exception as exc:
        log.warning("Firebase Admin init failed: %s", exc)


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
    summary = last_text[-80:] if len(last_text) > 80 else last_text
    try:
        message = fb_messaging.Message(
            notification=fb_messaging.Notification(
                title=f"✓ {session_name}",
                body=summary,
            ),
            data={"session_id": session_id},
            token=token,
        )
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, fb_messaging.send, message)
        log.info("FCM notification sent")
    except Exception as exc:
        log.warning("FCM notification failed: %s", exc)


# ---------------------------------------------------------------------------
# Saved sessions (persistence helpers — used by _persist_session, _restore_sessions_from_disk)
# ---------------------------------------------------------------------------
def _load_saved_sessions() -> dict:
    try:
        with open(SAVED_SESSIONS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _persist_session(session: "Session") -> None:
    if not session.resume_id:
        return
    saved = _load_saved_sessions()
    saved[session.session_id] = {
        "name": session.name,
        "claude_uuid": session.resume_id,
        "last_used": int(time.time()),
        "cwd": session.cwd,
        "backend": session.backend_name,
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
    saved = _load_saved_sessions()
    count = 0
    for sid, data in saved.items():
        if sid in _SESSIONS:
            continue
        try:
            session = Session(
                session_id=sid,
                name=data.get("name", sid[:8]),
                created_at=float(data.get("last_used", time.time())),
                cwd=data.get("cwd", DEFAULT_CWD),
                backend_name=_normalize_backend_name(data.get("backend")),
            )
            session.resume_id = data.get("claude_uuid") or None
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
    for s in list(_SESSIONS.values()):
        payload = {"type": "session_unread", "session_id": s.session_id, "unread": _unread_for(s, client.device_id)}
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
    await _broadcast_json(payload)
    if et in {"done", "stopped", "error"}:
        await _send_unread_for_session(session)
    return True


def _session_lock_payload(session: "Session") -> dict:
    return {
        "type": "session_lock",
        "session_id": session.session_id,
        "locked": bool(session.lock_owner_device and session.lock_until > time.time()),
        "owner_device_id": session.lock_owner_device,
        "owner_device_name": session.lock_owner_device_name,
        "owner_client_id": session.lock_owner_client,
        "lock_until": session.lock_until,
        "queue_length": len(session.queue),
        "current_request_id": session.current_request_id,
    }


def _is_session_locked_for(session: "Session", client: ClientConn) -> bool:
    if session.lock_until <= time.time():
        return False
    if not session.lock_owner_client:
        return False
    return session.lock_owner_client != client.client_id


def _acquire_session_lock(session: "Session", client: ClientConn) -> None:
    session.lock_owner_device = client.device_id
    session.lock_owner_device_name = client.device_name
    session.lock_owner_client = client.client_id
    session.lock_until = time.time() + LOCK_TTL_SECONDS


def _refresh_session_lock(session: "Session", client_id: str) -> None:
    if session.lock_owner_client == client_id:
        session.lock_until = time.time() + LOCK_TTL_SECONDS


def _release_session_lock(session: "Session") -> None:
    session.lock_owner_device = ""
    session.lock_owner_device_name = ""
    session.lock_owner_client = ""
    session.lock_until = 0.0
    session.current_request_id = ""


async def _emit_busy_locked(session: "Session", ws: Any) -> None:
    msg = {
        "type": "error",
        "session_id": session.session_id,
        "code": "busy_locked",
        "message": "Session is locked by another device.",
        "owner_device_id": session.lock_owner_device,
        "owner_device_name": session.lock_owner_device_name,
        "retry_after": max(0, int(session.lock_until - time.time())),
    }
    try:
        await ws.send(json.dumps(msg))
    except Exception:
        pass




# ---------------------------------------------------------------------------
# sessions_list payload helper
# ---------------------------------------------------------------------------
def build_sessions_list() -> dict:
    return _msg_sessions_list([
        {
            "id": s.session_id,
            "name": s.name,
            "is_streaming": s.is_streaming,
            "created_at": s.created_at,
            "cwd": s.cwd,
            "model": s.model,
            "context_used": s.context_used,
            "backend": s.backend_name,
            "sandbox": s.sandbox,
            "pinned": s.pinned,
            "hidden": s.hidden,
            "lock_owner_device": s.lock_owner_device,
            "lock_owner_device_name": s.lock_owner_device_name,
            "lock_until": s.lock_until,
            "queue_length": len(s.queue),
        }
        for s in _SESSIONS.values()
    ])


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
            client = next((c for c in _CLIENTS.values() if c.client_id == cmd.client_id), None)
            if client is None:
                session.queue.popleft()
                continue
            _acquire_session_lock(session, client)
            session.current_request_id = cmd.request_id
            await _broadcast_json({
                "type": "session_command_started",
                "session_id": session.session_id,
                "request_id": cmd.request_id,
                "device_id": cmd.device_id,
                "queue_length": len(session.queue),
            })
            await _broadcast_json(_session_lock_payload(session))

            try:
                await _session_backend(session).send(session, cmd.content, cmd.images, cmd.files)
                await _broadcast_json({
                    "type": "session_command_done",
                    "session_id": session.session_id,
                    "request_id": cmd.request_id,
                })
            except Exception as exc:
                await _broadcast_json({
                    "type": "session_command_failed",
                    "session_id": session.session_id,
                    "request_id": cmd.request_id,
                    "message": str(exc),
                })
            finally:
                if session.queue and session.queue[0].request_id == cmd.request_id:
                    session.queue.popleft()
                _release_session_lock(session)
                await _broadcast_json(_session_lock_payload(session))
    finally:
        session.processing = False


async def _lock_sweeper() -> None:
    while True:
        await asyncio.sleep(2)
        now = time.time()
        changed = False
        for s in list(_SESSIONS.values()):
            if s.lock_until and s.lock_until <= now and not s.processing:
                _release_session_lock(s)
                changed = True
        if changed:
            await _broadcast_json(build_sessions_list())


async def _session_cache_refresher() -> None:
    while True:
        await asyncio.sleep(300)
        await preload_sessions_cache(_BACKENDS)


async def handler(ws: ServerConnection) -> None:
    global _AUTO_TUNNEL_TASK

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

    # Inject this ws into all existing sessions (reconnect scenario)
    for session in list(_SESSIONS.values()):
        session.ws_ref = ws
        if session.offline_buffer:
            buf = session.offline_buffer[:]
            session.offline_buffer.clear()
            for evt in buf:
                try:
                    await ws.send(json.dumps(evt))
                except Exception:
                    session.offline_buffer = buf
                    break

    try:
        await ws.send(json.dumps({
            "type": "hello_ack",
            "client_id": client.client_id,
            "device_id": client.device_id,
            "device_name": client.device_name,
        }))
        await ws.send(json.dumps(build_sessions_list()))
        await _send_unread_snapshot(ws, client)
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

            if await handle_system_msg(mtype, msg, ws, system_ctx):
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue
            if await handle_runtime_msg(mtype, msg, ws, runtime_ctx):
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue
            if await handle_file_msg(mtype, msg, ws, file_ctx):
                _PERF.record(mtype, (time.perf_counter() - op_started) * 1000.0, log)
                continue

            # ------------------------------------------------------------------
            if mtype == "ping":
                client.last_seen = time.time()
                sid = msg.get("session_id")
                if isinstance(sid, str):
                    s = _SESSIONS.get(sid)
                    if s:
                        _refresh_session_lock(s, client.client_id)
                try:
                    await ws.send(json.dumps(_msg_pong()))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "hello":
                device_id = str(msg.get("device_id", "")).strip()
                if device_id:
                    client.device_id = device_id[:128]
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
                if session:
                    if _is_session_locked_for(session, client):
                        await _emit_busy_locked(session, ws)
                        continue
                    _acquire_session_lock(session, client)
                    await _broadcast_json(_session_lock_payload(session))
                    _mark_read(sid, client.device_id, session.message_seq)
                    _persist_read_cursors()
                    await _send_unread_for_client_session(ws, client, session)
                if session and session.resume_id:
                    backend = _session_backend(session)
                    if not backend.supports_resume():
                        _release_session_lock(session)
                        await _broadcast_json(_session_lock_payload(session))
                        continue
                    await _emit_resume_progress(session, "resume_started", 5, "Resume started")
                    await _emit_resume_progress(session, "resume_loading_history", 35, "Loading history")
                    history = await backend.load_session_history(session.resume_id)
                    if history:
                        try:
                            await ws.send(json.dumps(_msg_session_history(sid, history)))
                        except Exception:
                            pass
                    # Pre-warm: spawn the claude process in background so first message is faster
                    asyncio.create_task(backend.spawn(session))
                    await _emit_resume_progress(session, "resume_ready", 100, "Resume ready")
                    _release_session_lock(session)
                    await _broadcast_json(_session_lock_payload(session))

            # ------------------------------------------------------------------
            elif mtype == "new_session":
                sid              = msg["session_id"]
                name             = msg["name"]
                cwd              = msg.get("cwd", DEFAULT_CWD)
                resume_claude_id = msg.get("resume_claude_id", "")
                backend_name     = _normalize_backend_name(msg.get("backend"))
                effort           = msg.get("effort", "")
                sandbox          = str(msg.get("sandbox") or "workspace-write")

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
                    )
                    _SESSIONS[sid] = session
                    invalidate_sessions_cache()
                    asyncio.create_task(preload_sessions_cache(_BACKENDS))

                backend = _session_backend(session)
                if resume_claude_id:
                    await _emit_resume_progress(session, "resume_started", 5, "Resume started")
                await _emit_resume_progress(session, "resume_spawning_backend", 20, "Spawning backend")
                await backend.spawn(session)

                try:
                    await ws.send(json.dumps(_msg_session_created(
                        sid, name, session.created_at, cwd, session.backend_name, session.model, session.sandbox
                    )))
                    await ws.send(json.dumps(_session_lock_payload(session)))
                except Exception:
                    pass

                if resume_claude_id and backend.supports_resume():
                    try:
                        await _emit_resume_progress(session, "resume_loading_history", 65, "Loading history")
                        history = await backend.load_session_history(resume_claude_id)
                        if history:
                            try:
                                await ws.send(json.dumps(_msg_session_history(sid, history)))
                            except Exception:
                                pass
                        await _emit_resume_progress(session, "resume_ready", 100, "Resume ready")
                    except Exception as exc:
                        await _emit_resume_progress(session, "resume_failed", 100, f"Resume failed: {exc}")

                log.info("Session created: %s (%s)", sid, name)
                _persist_session_meta()

            # ------------------------------------------------------------------
            elif mtype == "message":
                sid     = msg["session_id"]
                content = msg.get("content", "")
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
                if not content and not images and not files:
                    await send_event(session, _evt_error("Empty content"))
                    continue
                if _is_session_locked_for(session, client):
                    await _emit_busy_locked(session, ws)
                    continue
                request_id = str(msg.get("request_id") or f"r_{uuid.uuid4().hex[:10]}")
                if any(cmd.request_id == request_id for cmd in session.queue) or session.current_request_id == request_id:
                    continue
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
                await _broadcast_json(_session_lock_payload(session))
                asyncio.create_task(_run_session_queue(session))

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
                if _is_session_locked_for(session, client):
                    await _emit_busy_locked(session, ws)
                    continue
                _acquire_session_lock(session, client)
                await _broadcast_json(_session_lock_payload(session))
                async def _do_stop(s: Session, requestor: ClientConn) -> None:
                    try:
                        await _session_backend(s).stop(s)
                        s.queue.clear()
                    finally:
                        _release_session_lock(s)
                        await _broadcast_json(_session_lock_payload(s))
                asyncio.create_task(_do_stop(session, client))

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
                if _is_session_locked_for(source, client):
                    await _emit_busy_locked(source, ws)
                    continue
                if source.is_streaming or source.processing:
                    await send_event(source, _evt_error("Session is currently processing a request.", "session_busy"))
                    continue

                target_backend = _normalize_backend_name(msg.get("backend") or source.backend_name)
                target_model = str(msg.get("model") or source.model or "")
                target_effort = str(msg.get("effort") if "effort" in msg else source.effort or "")
                requested_sandbox = str(msg.get("sandbox") or "")
                target_sandbox = requested_sandbox or source.sandbox or "workspace-write"
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
                    or target_sandbox != (source.sandbox or "workspace-write")
                ):
                    # codex exec resume doesn't accept all runtime flags; open fresh process and handoff context.
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
                    )))
                    await ws.send(json.dumps(_session_lock_payload(new_session)))
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
                    await _broadcast_json(_session_lock_payload(new_session))
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
        log.info("Client gone: %s (%s)", remote, client.client_id)

        if not _CLIENTS and not _is_cloudflared_running():
            _AUTO_TUNNEL_TASK = asyncio.create_task(_auto_tunnel_after_delay(10))


# ---------------------------------------------------------------------------
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
            "_claude-bridge._tcp.local.",
            "claude-bridge._claude-bridge._tcp.local.",
            addresses=[socket.inet_aton(local_ip)],
            port=port,
            properties={"version": "2"},
        )
        zc = Zeroconf()
        zc.register_service(info)
        log.info("mDNS: claude-bridge.local advertised at %s:%d", local_ip, port)
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
            asyncio.create_task(_drain_proc_stderr(proc))
            asyncio.create_task(_notify_fcm_tunnel(ws_url))
            return
    log.warning("cloudflared tunnel URL not detected")
    _CLOUDFLARED_PROC = None


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
    init_firebase()
    _restore_sessions_from_disk()
    _apply_session_meta()
    _READ_CURSORS = _load_read_cursors()
    set_event_dispatcher(_dispatch_event)

    ts_ip = _detect_tailscale_ip()
    print(f"\n{'='*56}")
    print(f"  Claude Bridge v2  |  port {port}  |  default backend: {_DEFAULT_BACKEND_NAME}")
    print(f"{'='*56}")
    if _DEFAULT_BACKEND_NAME == "claude":
        print(f"  Claude : {CLAUDE_BIN}")
    elif _DEFAULT_BACKEND_NAME == "codex":
        print(f"  Codex  : {CODEX_BIN}")
    else:
        print(f"  Ollama : {_OLLAMA_HOST}  model={_DEFAULT_OLLAMA_MODEL}")
    if ts_ip:
        print(f"  Tailscale: ws://{ts_ip}:{port}")
    else:
        print(f"  Local   : ws://127.0.0.1:{port}")
        print(f"  (No Tailscale — use --tunnel for a public URL)")
    print(f"{'='*56}\n")

    global _BRIDGE_PORT
    _BRIDGE_PORT = port
    log.info("Claude Bridge v2 starting on port %d (default_backend=%s)", port, _DEFAULT_BACKEND_NAME)
    zc = _start_mdns(port)
    async with serve(
        handler,
        ["0.0.0.0", "::"],
        port,
        ping_interval=30,
        ping_timeout=30,
    ):
        log.info("Bridge v2 listening on port %d (IPv4 + IPv6)", port)
        if tunnel:
            asyncio.create_task(_start_cloudflared_tunnel(port))
        asyncio.create_task(_lock_sweeper())
        asyncio.create_task(preload_sessions_cache(_BACKENDS))
        asyncio.create_task(_session_cache_refresher())
        try:
            await asyncio.Future()  # run forever
        finally:
            if zc:
                zc.unregister_all_services()
                zc.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Claude WebSocket Bridge v2")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--tunnel", action="store_true", help="Start a cloudflared tunnel for public access")
    parser.add_argument("--backend", default="claude", choices=["claude", "ollama", "codex"],
                        help="AI backend (default: claude)")
    parser.add_argument("--model", default="",
                        help="Model name (for ollama backend)")
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
