#!/usr/bin/env python3
"""
Claude Bridge v2 — Multi-session WebSocket server that proxies Claude Code CLI.
Supports up to 10 independent concurrent Claude sessions, each backed by a
persistent claude CLI subprocess.

Uses websockets.asyncio.server API (websockets >= 14).
Default port: 8766 (v1 keeps 8765).
"""

import argparse
import asyncio
import json
import logging
import os
import re
import time
import signal
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, Literal, NotRequired, Optional, TypedDict
from urllib.parse import quote as urlquote

from websockets.asyncio.server import serve, ServerConnection

try:
    import firebase_admin
    from firebase_admin import credentials, messaging as fb_messaging
    _FIREBASE_AVAILABLE = True
except ImportError:
    _FIREBASE_AVAILABLE = False

BRIDGE_DIR           = os.path.dirname(os.path.abspath(__file__))
LOG_FILE             = os.path.join(BRIDGE_DIR, "bridge_v2.log")
CLAUDE_BIN           = "/Users/wulala/.npm-global/bin/claude"
BUN_BIN              = "/opt/homebrew/bin/bun"
HTTP_PORT            = 9090
MAX_SESSIONS         = 10
OFFLINE_BUFFER_MAX   = 500
MAX_SHELLS           = 5
FCM_TOKEN_FILE        = os.path.join(BRIDGE_DIR, "fcm_token.txt")
SERVICE_ACCOUNT_FILE  = os.path.join(BRIDGE_DIR, "serviceAccountKey.json")
SAVED_SESSIONS_FILE   = os.path.join(BRIDGE_DIR, "saved_sessions.json")
CLAUDE_PROJECTS_DIR   = os.path.expanduser("~/.claude/projects")

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("bridge_v2")

MEDIA_RE = re.compile(
    r'(/(?:[^\s\'"<>]+\.(?:jpg|jpeg|png|gif|webp|mp4|mov|m4v|avi)))',
    re.IGNORECASE,
)
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
VIDEO_EXTS = {".mp4", ".mov", ".m4v", ".avi"}


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

class FcmTokenMsg(TypedDict):
    type: Literal["fcm_token"]
    token: str

class RequestSessionsListMsg(TypedDict):
    type: Literal["request_sessions_list"]

class BrowseDirMsg(TypedDict):
    type: Literal["browse_dir"]
    path: NotRequired[str]


# Required fields (name → [(field, type), ...]) — checked at runtime
_INBOUND_REQUIRED: dict[str, list[tuple[str, type]]] = {
    "new_session":    [("session_id", str), ("name", str)],
    "message":        [("session_id", str)],
    "stop":           [("session_id", str)],
    "close_session":  [("session_id", str)],
    "rename_session": [("session_id", str), ("name", str)],
    "clear_session":  [("session_id", str)],
    "shell_input":    [("shell_id", str), ("data", str)],
    "shell_close":    [("shell_id", str)],
    "kill_task":      [("id", str)],
    "browse_dir":     [],
}

_KNOWN_MSG_TYPES: frozenset[str] = frozenset({
    "ping", "message", "new_session", "stop", "close_session",
    "rename_session", "clear_session", "get_usage", "get_resumable_sessions",
    "shell_create", "shell_input", "shell_close", "get_tasks", "kill_task",
    "fcm_token", "request_sessions_list", "browse_dir",
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


# ---------------------------------------------------------------------------
# Outbound event builders — typed constructors guarantee correct event shapes
# ---------------------------------------------------------------------------

# Session-scoped: sent via send_event(); session_id is injected automatically.
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

# WebSocket-level: sent directly via ws.send(); include all fields.
def _msg_pong() -> dict:
    return {"type": "pong"}

def _msg_error(message: str, session_id: str = "") -> dict:
    d: dict = {"type": "error", "message": message}
    if session_id:
        d["session_id"] = session_id
    return d

def _msg_sessions_list(sessions: list[dict]) -> dict:
    return {"type": "sessions_list", "sessions": sessions}

def _msg_session_created(session_id: str, name: str, created_at: float, cwd: str) -> dict:
    return {"type": "session_created", "session_id": session_id, "name": name,
            "created_at": created_at, "cwd": cwd}

def _msg_session_renamed(session_id: str, name: str) -> dict:
    return {"type": "session_renamed", "session_id": session_id, "name": name}

def _msg_session_history(session_id: str, messages: list[dict]) -> dict:
    return {"type": "session_history", "session_id": session_id, "messages": messages}

def _msg_resumable_sessions(sessions: list[dict]) -> dict:
    return {"type": "resumable_sessions", "sessions": sessions}

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
# HTTP media server (shared across all sessions)
# ---------------------------------------------------------------------------
_http_server_proc: "asyncio.subprocess.Process | None" = None


async def ensure_http_server() -> None:
    global _http_server_proc
    if _http_server_proc is not None and _http_server_proc.returncode is None:
        return
    log.info("Starting SimpleHTTPServer on port %d", HTTP_PORT)
    _http_server_proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "http.server", str(HTTP_PORT), "--directory", "/",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )


# ---------------------------------------------------------------------------
# Global session registry
# ---------------------------------------------------------------------------
_SESSIONS: Dict[str, "ClaudeSession"] = {}
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
class ClaudeSession:
    session_id: str
    name: str
    created_at: float
    cwd: str = "/Users/wulala"

    proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    stdout_task: Optional[asyncio.Task] = field(default=None, repr=False)
    stderr_task: Optional[asyncio.Task] = field(default=None, repr=False)
    watch_task: Optional[asyncio.Task] = field(default=None, repr=False)
    is_streaming: bool = False
    is_stopping: bool = False
    claude_session_uuid: Optional[str] = None
    last_activity: float = 0.0
    accumulated_text: str = ""
    tool_blocks: dict = field(default_factory=dict)
    restart_count: int = 0
    ws_ref: Optional[Any] = field(default=None, repr=False)
    pending_stop: bool = False
    offline_buffer: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# fetch_usage — query claude.ai/api/oauth/usage via Bun (same TLS fingerprint)
# ---------------------------------------------------------------------------
_BUN_USAGE_SCRIPT = r"""
const { execSync } = require('child_process');
const raw = execSync("security find-generic-password -s 'Claude Code-credentials' -g 2>&1").toString();
const creds = JSON.parse(raw.match(/password: "(.+)"/)[1]);
const token = creds.claudeAiOauth.accessToken;
const res = await fetch('https://claude.ai/api/oauth/usage', {
  headers: { 'Authorization': `Bearer ${token}` }
});
const data = await res.json();
console.log(JSON.stringify(data));
"""

async def fetch_usage(ws: ServerConnection) -> None:
    try:
        proc = await asyncio.create_subprocess_exec(
            BUN_BIN, "-e", _BUN_USAGE_SCRIPT,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        data = json.loads(stdout.decode().strip())

        def fmt(entry: dict | None) -> dict | None:
            if not entry:
                return None
            return {
                "utilization": entry.get("utilization"),
                "resets_at": entry.get("resets_at"),
            }

        await ws.send(json.dumps(_msg_usage_report(
            fmt(data.get("five_hour")),
            fmt(data.get("seven_day")),
            fmt(data.get("seven_day_sonnet")),
        )))
        log.info("Usage report sent")
    except Exception as exc:
        log.warning("fetch_usage failed: %s", exc)
        try:
            await ws.send(json.dumps(_msg_error(f"Usage fetch failed: {exc}")))
        except Exception:
            pass


# ---------------------------------------------------------------------------
# send_event — all session-scoped outbound events go through here
# ---------------------------------------------------------------------------
async def send_event(session: ClaudeSession, event: dict) -> None:
    payload = {**event, "session_id": session.session_id}
    if session.ws_ref is not None:
        try:
            await session.ws_ref.send(json.dumps(payload))
            return
        except Exception:
            session.ws_ref = None
    # buffer while disconnected
    session.offline_buffer.append(payload)
    if len(session.offline_buffer) > OFFLINE_BUFFER_MAX:
        session.offline_buffer.pop(0)


# ---------------------------------------------------------------------------
# stream_text — 4-char chunks, 20 ms delay
# ---------------------------------------------------------------------------
async def stream_text(text: str, session: ClaudeSession, chunk_size: int = 4) -> None:
    for i in range(0, len(text), chunk_size):
        await send_event(session, _evt_text_chunk(text[i:i + chunk_size]))
        await asyncio.sleep(0.02)


# ---------------------------------------------------------------------------
# scan_for_media
# ---------------------------------------------------------------------------
async def scan_for_media(text: str, session: ClaudeSession) -> None:
    matches = MEDIA_RE.findall(text)
    for path in matches:
        if not os.path.exists(path):
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext in IMAGE_EXTS:
            media_type = "image"
        elif ext in VIDEO_EXTS:
            media_type = "video"
        else:
            continue
        await ensure_http_server()
        encoded = urlquote(path)
        url = f"http://127.0.0.1:{HTTP_PORT}{encoded}"
        payload = _evt_media(media_type, path, url)
        log.info("Media detected: %s", payload)
        await send_event(session, payload)


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
# Saved sessions (for resume)
# ---------------------------------------------------------------------------
def _find_session_file(uuid: str) -> Optional[str]:
    """Search ~/.claude/projects/ for a JSONL file matching the given UUID."""
    try:
        for proj in os.scandir(CLAUDE_PROJECTS_DIR):
            if not proj.is_dir():
                continue
            candidate = os.path.join(proj.path, uuid + ".jsonl")
            if os.path.isfile(candidate):
                return candidate
    except Exception:
        pass
    return None


def _load_session_history(uuid: str, limit: int = 60) -> list:
    """Parse JSONL and return last `limit` user/assistant message dicts."""
    path = _find_session_file(uuid)
    if not path:
        return []
    messages = []
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            for raw in f:
                try:
                    d = json.loads(raw)
                    if d.get("isSidechain") or d.get("type") not in ("user", "assistant"):
                        continue
                    role = d["type"]
                    content = d.get("message", {}).get("content", "")
                    text = ""
                    if isinstance(content, str):
                        text = content
                    elif isinstance(content, list):
                        parts = []
                        for blk in content:
                            if not isinstance(blk, dict):
                                continue
                            if blk.get("type") == "text":
                                parts.append(blk.get("text", ""))
                        text = "\n".join(p for p in parts if p)
                    if not text or text.startswith("<") or text.startswith("[Request interrupted"):
                        continue
                    messages.append({"role": role, "content": text})
                except Exception:
                    pass
    except Exception as exc:
        log.warning("Failed to load session history: %s", exc)
    return messages[-limit:]


def _load_saved_sessions() -> dict:
    try:
        with open(SAVED_SESSIONS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _scan_local_sessions(limit: int = 100) -> list:
    """Scan ~/.claude/projects/ and return sessions sorted by last modified."""
    sessions = []
    saved_names = {v["claude_uuid"]: v["name"] for v in _load_saved_sessions().values()}
    try:
        for proj in os.scandir(CLAUDE_PROJECTS_DIR):
            if not proj.is_dir():
                continue
            try:
                cwd = "/" + proj.name[1:].replace("-", "/")
                if not os.path.isdir(cwd):
                    cwd = os.path.expanduser("~")
            except Exception:
                cwd = os.path.expanduser("~")

            for entry in os.scandir(proj.path):
                if not entry.name.endswith(".jsonl") or not entry.is_file():
                    continue
                uuid = entry.name[:-6]
                mtime = int(entry.stat().st_mtime)
                if uuid in saved_names:
                    name = saved_names[uuid]
                else:
                    name = proj.name.split("-")[-1] or uuid[:8]
                    try:
                        with open(entry.path, encoding="utf-8", errors="ignore") as f:
                            for raw in f:
                                try:
                                    d = json.loads(raw)
                                    if d.get("type") != "user":
                                        continue
                                    content = d.get("message", {}).get("content", "")
                                    text = ""
                                    if isinstance(content, str):
                                        text = content
                                    elif isinstance(content, list):
                                        for blk in content:
                                            if isinstance(blk, dict) and blk.get("type") == "text":
                                                text = blk.get("text", "")
                                                break
                                    if text and not text.startswith("<"):
                                        name = text[:50].strip()
                                        break
                                except Exception:
                                    pass
                    except Exception:
                        pass
                sessions.append({
                    "id": uuid,
                    "name": name,
                    "claude_uuid": uuid,
                    "last_used": mtime,
                    "cwd": cwd,
                })
    except Exception as exc:
        log.warning("Failed to scan local sessions: %s", exc)
    sessions.sort(key=lambda x: x["last_used"], reverse=True)
    return sessions[:limit]


def _persist_session(session: "ClaudeSession") -> None:
    if not session.claude_session_uuid:
        return
    saved = _load_saved_sessions()
    saved[session.session_id] = {
        "name": session.name,
        "claude_uuid": session.claude_session_uuid,
        "last_used": int(time.time()),
        "cwd": session.cwd,
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
            session = ClaudeSession(
                session_id=sid,
                name=data.get("name", sid[:8]),
                created_at=float(data.get("last_used", time.time())),
                cwd=data.get("cwd", "/Users/wulala"),
            )
            session.claude_session_uuid = data.get("claude_uuid") or None
            _SESSIONS[sid] = session
            count += 1
        except Exception as exc:
            log.warning("Failed to restore session %s: %s", sid, exc)
    if count:
        log.info("Restored %d session(s) from disk", count)


# ---------------------------------------------------------------------------
# stdout_reader — parses NDJSON from claude subprocess
# ---------------------------------------------------------------------------
async def stdout_reader(session: ClaudeSession) -> None:
    assert session.proc is not None
    async for line_bytes in session.proc.stdout:
        line = line_bytes.decode("utf-8", errors="replace").strip()
        if not line:
            continue

        try:
            evt = json.loads(line)
        except json.JSONDecodeError:
            log.debug("[%s] Non-JSON stdout: %s", session.session_id, line[:120])
            continue

        etype = evt.get("type", "")

        if etype == "assistant":
            message = evt.get("message", {})
            content_blocks = message.get("content", [])
            for block in content_blocks:
                btype = block.get("type", "")
                if btype == "text":
                    text = block.get("text", "")
                    if text:
                        session.accumulated_text += text
                        await stream_text(text, session)
                        await scan_for_media(text, session)
                elif btype == "tool_use":
                    tool_id = block.get("id", "")
                    name = block.get("name", "")
                    input_data = block.get("input", {})
                    command = input_data.get("command", json.dumps(input_data))
                    session.tool_blocks[tool_id] = {"name": name}
                    await send_event(session, _evt_tool_start(tool_id, name, command))

        elif etype == "tool_result":
            tool_id = evt.get("tool_use_id", "")
            output = evt.get("content", "")
            if isinstance(output, list):
                output = "\n".join(
                    b.get("text", "") for b in output if b.get("type") == "text"
                )
            await send_event(session, _evt_tool_result(tool_id, str(output)))
            await send_event(session, _evt_tool_end(tool_id))

        elif etype == "result":
            subtype = evt.get("subtype", "")
            new_uuid = evt.get("session_id")
            session.is_streaming = False
            if subtype == "success":
                log.info("[%s] result success, claude_uuid=%s", session.session_id, new_uuid)
                if new_uuid:
                    session.claude_session_uuid = new_uuid
                    _persist_session(session)
                asyncio.create_task(notify_fcm(session.name, session.accumulated_text, session.session_id))
                await send_event(session, _evt_done())
                session.accumulated_text = ""
                session.tool_blocks = {}
            else:
                err = evt.get("result", "Unknown error")
                log.error("[%s] result error: %s", session.session_id, err)
                await send_event(session, _evt_error(str(err)))
                session.accumulated_text = ""
                session.tool_blocks = {}

        elif etype == "system":
            log.debug("[%s] system subtype=%s", session.session_id, evt.get("subtype", ""))

        elif etype == "rate_limit_event":
            log.debug("[%s] rate_limit_event", session.session_id)

        else:
            log.debug("[%s] Unhandled event type: %s", session.session_id, etype)


# ---------------------------------------------------------------------------
# stderr_reader
# ---------------------------------------------------------------------------
async def stderr_reader(session: ClaudeSession) -> None:
    assert session.proc is not None
    async for line_bytes in session.proc.stderr:
        line = line_bytes.decode("utf-8", errors="replace").strip()
        if line:
            log.warning("[%s] claude stderr: %s", session.session_id, line)


# ---------------------------------------------------------------------------
# watch_proc — auto-restart on unexpected crash
# ---------------------------------------------------------------------------
async def watch_proc(session: ClaudeSession) -> None:
    assert session.proc is not None
    await session.proc.wait()

    if session.is_stopping:
        return

    rc = session.proc.returncode
    log.warning("[%s] Claude proc exited unexpectedly (rc=%s)", session.session_id, rc)

    if rc != 0 and session.restart_count < 3:
        session.restart_count += 1
        log.info("[%s] Auto-restarting (attempt %d/3)", session.session_id, session.restart_count)
        await send_event(session, _evt_session_warning(
            f"Claude process exited (rc={rc}), restarting ({session.restart_count}/3)…"
        ))
        await spawn_proc(session)
    else:
        log.error("[%s] Session died after %d restart(s)", session.session_id, session.restart_count)
        await send_event(session, _evt_session_died(
            f"Claude process exited (rc={rc}) and will not restart."
        ))


# ---------------------------------------------------------------------------
# spawn_proc — launch / re-launch claude subprocess for a session
# ---------------------------------------------------------------------------
async def spawn_proc(session: ClaudeSession) -> None:
    cmd = [
        CLAUDE_BIN,
        "--print",
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--verbose",
        "--dangerously-skip-permissions",
    ]
    if session.claude_session_uuid:
        cmd += ["--resume", session.claude_session_uuid]

    log.info("[%s] Spawning claude: %s (cwd=%s)", session.session_id, cmd, session.cwd)

    try:
        session.proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=session.cwd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except Exception as exc:
        log.error("[%s] Failed to spawn claude: %s", session.session_id, exc)
        await send_event(session, _evt_error(f"Failed to spawn claude: {exc}"))
        return

    session.is_stopping = False

    for attr in ("stdout_task", "stderr_task", "watch_task"):
        old = getattr(session, attr)
        if old and not old.done():
            old.cancel()

    session.stdout_task = asyncio.create_task(stdout_reader(session))
    session.stderr_task = asyncio.create_task(stderr_reader(session))
    session.watch_task  = asyncio.create_task(watch_proc(session))
    log.info("[%s] Claude process started (pid=%d)", session.session_id, session.proc.pid)


# ---------------------------------------------------------------------------
# send_message — write user input to claude stdin
# ---------------------------------------------------------------------------
async def send_message(session: ClaudeSession, content: str, images: list | None = None, files: list | None = None) -> None:
    if session.is_streaming:
        await send_event(session, _evt_error("Session is currently processing a request.", "session_busy"))
        return

    if session.proc is None or session.proc.returncode is not None:
        await send_event(session, _evt_error("Claude process is not running.", "session_dead"))
        return

    session.accumulated_text = ""
    session.tool_blocks = {}
    session.is_streaming = True
    session.last_activity = asyncio.get_event_loop().time()

    content_blocks: list = []
    for img in (images or []):
        content_blocks.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": img.get("media_type", "image/jpeg"),
                "data": img.get("data", ""),
            },
        })
    for f in (files or []):
        media_type = f.get("media_type", "text/plain")
        name = f.get("name", "file")
        file_content = f.get("content", "")
        if media_type == "application/pdf":
            content_blocks.append({
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": file_content,
                },
            })
        else:
            ext = name.rsplit(".", 1)[-1] if "." in name else ""
            fence = f"```{ext}\n{file_content}\n```" if ext else file_content
            content_blocks.append({"type": "text", "text": f"[File: {name}]\n{fence}"})
    if content:
        content_blocks.append({"type": "text", "text": content})

    payload = json.dumps({
        "type": "user",
        "message": {"role": "user", "content": content_blocks},
    }) + "\n"

    try:
        session.proc.stdin.write(payload.encode("utf-8"))
        await session.proc.stdin.drain()
        log.info("[%s] Message sent (%d chars, %d images)", session.session_id, len(content), len(images or []))
    except Exception as exc:
        session.is_streaming = False
        log.error("[%s] Failed to write to stdin: %s", session.session_id, exc)
        await send_event(session, _evt_error(f"stdin write failed: {exc}"))


# ---------------------------------------------------------------------------
# stop_session — send SIGTERM/SIGKILL, emit stopped
# ---------------------------------------------------------------------------
async def stop_session(session: ClaudeSession) -> None:
    if session.proc is None or session.proc.returncode is not None:
        await send_event(session, _evt_stopped())
        return

    session.is_stopping = True
    log.info("[%s] Stopping session (pid=%d)", session.session_id, session.proc.pid)

    try:
        session.proc.send_signal(signal.SIGTERM)
    except ProcessLookupError:
        pass

    await asyncio.sleep(1)

    try:
        if session.proc.returncode is None:
            session.proc.kill()
    except ProcessLookupError:
        pass

    session.is_streaming = False
    session.accumulated_text = ""
    session.tool_blocks = {}
    await send_event(session, _evt_stopped())


# ---------------------------------------------------------------------------
# close_session — stop proc + cancel tasks + remove from registry
# ---------------------------------------------------------------------------
async def close_session(session: ClaudeSession) -> None:
    session.is_stopping = True
    log.info("[%s] Closing session", session.session_id)

    if session.proc is not None and session.proc.returncode is None:
        try:
            session.proc.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(session.proc.wait(), timeout=2)
        except asyncio.TimeoutError:
            try:
                session.proc.kill()
            except ProcessLookupError:
                pass

    for attr in ("stdout_task", "stderr_task", "watch_task"):
        task = getattr(session, attr)
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async with _SESSIONS_LOCK:
        _SESSIONS.pop(session.session_id, None)

    await send_event(session, _evt_session_closed())


# ---------------------------------------------------------------------------
# clear_session — kill proc + respawn without --resume (fresh history)
# ---------------------------------------------------------------------------
async def clear_session(session: ClaudeSession) -> None:
    log.info("[%s] Clearing session history", session.session_id)
    session.is_stopping = True
    session.claude_session_uuid = None
    session.accumulated_text = ""
    session.tool_blocks = {}
    session.is_streaming = False

    if session.proc is not None and session.proc.returncode is None:
        try:
            session.proc.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(session.proc.wait(), timeout=2)
        except asyncio.TimeoutError:
            try:
                session.proc.kill()
            except ProcessLookupError:
                pass

    session.restart_count = 0
    await spawn_proc(session)
    await send_event(session, _evt_session_warning("Session history cleared."))


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
        }
        for s in _SESSIONS.values()
    ])


# ---------------------------------------------------------------------------
# WebSocket handler
# ---------------------------------------------------------------------------
async def handler(ws: ServerConnection) -> None:
    remote = ws.remote_address
    log.info("Client connected: %s", remote)

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
        await ws.send(json.dumps(build_sessions_list()))
    except Exception:
        pass

    try:
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

            # ------------------------------------------------------------------
            if mtype == "ping":
                try:
                    await ws.send(json.dumps(_msg_pong()))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "request_sessions_list":
                try:
                    await ws.send(json.dumps(build_sessions_list()))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "new_session":
                sid              = msg["session_id"]
                name             = msg["name"]
                cwd              = msg.get("cwd", "/Users/wulala")
                resume_claude_id = msg.get("resume_claude_id", "")

                async with _SESSIONS_LOCK:
                    if sid in _SESSIONS:
                        _SESSIONS[sid].ws_ref = ws
                        try:
                            await ws.send(json.dumps(_msg_session_created(
                                sid, _SESSIONS[sid].name,
                                _SESSIONS[sid].created_at, _SESSIONS[sid].cwd,
                            )))
                        except Exception:
                            pass
                        continue

                    if len(_SESSIONS) >= MAX_SESSIONS:
                        try:
                            await ws.send(json.dumps(_msg_error(
                                f"Maximum sessions ({MAX_SESSIONS}) reached."
                            )))
                        except Exception:
                            pass
                        continue

                    import time as _time
                    session = ClaudeSession(
                        session_id=sid,
                        name=name,
                        created_at=_time.time(),
                        cwd=cwd,
                        ws_ref=ws,
                        claude_session_uuid=resume_claude_id or None,
                    )
                    _SESSIONS[sid] = session

                await spawn_proc(session)

                try:
                    await ws.send(json.dumps(_msg_session_created(
                        sid, name, session.created_at, cwd
                    )))
                except Exception:
                    pass

                if resume_claude_id:
                    history = await asyncio.get_event_loop().run_in_executor(
                        None, _load_session_history, resume_claude_id
                    )
                    if history:
                        try:
                            await ws.send(json.dumps(_msg_session_history(sid, history)))
                        except Exception:
                            pass

                log.info("Session created: %s (%s)", sid, name)

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

                await send_message(session, content, images, files)

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
                asyncio.create_task(stop_session(session))

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
                asyncio.create_task(close_session(session))

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
                try:
                    await ws.send(json.dumps(_msg_session_renamed(sid, new_name)))
                except Exception:
                    pass

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
                asyncio.create_task(clear_session(session))

            # ------------------------------------------------------------------
            elif mtype == "get_usage":
                asyncio.create_task(fetch_usage(ws))

            # ------------------------------------------------------------------
            elif mtype == "get_resumable_sessions":
                resumable = await asyncio.get_event_loop().run_in_executor(
                    None, _scan_local_sessions, 100
                )
                active_uuids = {s.claude_session_uuid for s in _SESSIONS.values() if s.claude_session_uuid}
                resumable = [r for r in resumable if r["claude_uuid"] not in active_uuids]
                try:
                    await ws.send(json.dumps(_msg_resumable_sessions(resumable)))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "shell_create":
                if len(_SHELL_SESSIONS) >= MAX_SHELLS:
                    try:
                        await ws.send(json.dumps(_msg_error(f"Max {MAX_SHELLS} shell sessions reached")))
                    except Exception:
                        pass
                    continue
                cwd = msg.get("cwd", os.path.expanduser("~"))
                shell_id = "sh_" + os.urandom(4).hex()
                proc = await asyncio.create_subprocess_exec(
                    "/bin/bash", "-s",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    cwd=cwd if os.path.isdir(cwd) else os.path.expanduser("~"),
                    env={**os.environ, "TERM": "dumb"},
                )
                shell = ShellSession(shell_id=shell_id, proc=proc, ws_ref=ws, cwd=cwd)
                shell.read_task = asyncio.create_task(_shell_reader(shell))
                _SHELL_SESSIONS[shell_id] = shell
                try:
                    await ws.send(json.dumps(_msg_shell_created(shell_id)))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "shell_input":
                shell_id = msg["shell_id"]
                shell = _SHELL_SESSIONS.get(shell_id)
                if shell and shell.proc.returncode is None:
                    data = (msg["data"].rstrip("\n") + "\n").encode("utf-8")
                    shell.proc.stdin.write(data)
                    await shell.proc.stdin.drain()

            # ------------------------------------------------------------------
            elif mtype == "shell_close":
                shell_id = msg["shell_id"]
                shell = _SHELL_SESSIONS.pop(shell_id, None)
                if shell:
                    try:
                        shell.proc.terminate()
                    except Exception:
                        pass

            # ------------------------------------------------------------------
            elif mtype == "get_tasks":
                tasks = []
                for sid, s in list(_SESSIONS.items()):
                    tasks.append({
                        "id": sid,
                        "name": s.name,
                        "type": "claude",
                        "pid": s.proc.pid if s.proc else None,
                        "is_streaming": s.is_streaming,
                        "cwd": s.cwd,
                    })
                for shid, sh in list(_SHELL_SESSIONS.items()):
                    tasks.append({
                        "id": shid,
                        "name": f"Shell {shid[-4:]}",
                        "type": "shell",
                        "pid": sh.proc.pid if sh.proc else None,
                        "is_streaming": sh.proc.returncode is None,
                        "cwd": sh.cwd,
                    })
                try:
                    await ws.send(json.dumps(_msg_tasks_list(tasks)))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "kill_task":
                task_id = msg["id"]
                killed = False
                if task_id in _SESSIONS:
                    s = _SESSIONS[task_id]
                    if s.proc and s.proc.returncode is None:
                        s.proc.terminate()
                        killed = True
                elif task_id in _SHELL_SESSIONS:
                    sh = _SHELL_SESSIONS.pop(task_id, None)
                    if sh and sh.proc.returncode is None:
                        sh.proc.terminate()
                        killed = True
                try:
                    await ws.send(json.dumps(_msg_task_killed(task_id, killed)))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "browse_dir":
                req_path = msg.get("path") or "~"
                path = os.path.realpath(os.path.expanduser(req_path))
                entries: list[dict] = []
                if os.path.isdir(path):
                    try:
                        for entry in os.scandir(path):
                            try:
                                stat = entry.stat(follow_symlinks=False)
                                entries.append({
                                    "name": entry.name,
                                    "is_dir": entry.is_dir(follow_symlinks=True),
                                    "size": stat.st_size,
                                    "modified": int(stat.st_mtime),
                                })
                            except Exception:
                                pass
                    except PermissionError:
                        pass
                entries.sort(key=lambda e: (not e["is_dir"], e["name"].lower()))

                sessions_here: list[dict] = []
                for sid, s in list(_SESSIONS.items()):
                    try:
                        if os.path.realpath(s.cwd) == path:
                            sessions_here.append({
                                "id": sid,
                                "name": s.name,
                                "claude_uuid": s.claude_session_uuid or "",
                                "last_used": int(s.last_activity or s.created_at),
                                "is_active": True,
                            })
                    except Exception:
                        pass

                active_uuids = {s.claude_session_uuid for s in _SESSIONS.values() if s.claude_session_uuid}
                try:
                    resumable = await asyncio.get_event_loop().run_in_executor(None, _scan_local_sessions)
                    for r in resumable:
                        try:
                            if os.path.realpath(r["cwd"]) == path and r["claude_uuid"] not in active_uuids:
                                sessions_here.append({
                                    "id": r["id"],
                                    "name": r["name"],
                                    "claude_uuid": r["claude_uuid"],
                                    "last_used": r["last_used"],
                                    "is_active": False,
                                })
                        except Exception:
                            pass
                except Exception:
                    pass

                try:
                    await ws.send(json.dumps(_msg_dir_listing(path, entries, sessions_here)))
                except Exception:
                    pass

            # ------------------------------------------------------------------
            elif mtype == "fcm_token":
                token = msg.get("token", "").strip()
                if token:
                    try:
                        with open(FCM_TOKEN_FILE, "w") as f:
                            f.write(token)
                        log.info("FCM token registered: %s…", token[:20])
                    except Exception as exc:
                        log.warning("Failed to save FCM token: %s", exc)

    except Exception as exc:
        name = type(exc).__name__
        if "ConnectionClosed" in name:
            log.info("Client disconnected: %s (%s)", remote, exc)
        else:
            log.exception("Unhandled error in handler: %s", exc)
    finally:
        for session in list(_SESSIONS.values()):
            if session.ws_ref is ws:
                session.ws_ref = None
        for shell in list(_SHELL_SESSIONS.values()):
            if shell.ws_ref is ws:
                shell.ws_ref = None
        log.info("Client gone: %s", remote)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main(port: int) -> None:
    init_firebase()
    _restore_sessions_from_disk()
    log.info("Claude Bridge v2 starting on port %d", port)
    async with serve(
        handler,
        "0.0.0.0",
        port,
        ping_interval=30,
        ping_timeout=30,
    ):
        log.info("Bridge v2 listening on ws://0.0.0.0:%d", port)
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Claude WebSocket Bridge v2")
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()
    asyncio.run(main(args.port))
