"""
Gemini CLI backend — ACP (Agent Client Protocol) mode.
Protocol: JSON-RPC 2.0 over stdio (newline-delimited).
Spawn:    gemini --acp
History:  ~/.gemini/tmp/<project_hash>/chats/*.json
Resume:   gemini --acp --resume <session_id>
"""

import asyncio
import glob
import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from shutil import which
from typing import Optional, TYPE_CHECKING

from .base import Backend
from .events import (
    send_event, stream_text,
    _evt_error, _evt_stopped, _evt_done,
    _evt_tool_start, _evt_tool_result, _evt_tool_end,
    _evt_session_warning, _evt_session_closed,
    _msg_session_uuid,
)
from .history import complete_history_message, clamp_history_limit, slice_history

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")

_STREAM_READER_LIMIT = 128 * 1024 * 1024  # 128 MiB
_GEMINI_HOME = os.path.expanduser("~/.gemini")
_TURN_TIMEOUT_SECS = 600.0


@dataclass
class _GeminiState:
    proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    read_task: Optional[asyncio.Task] = field(default=None, repr=False)
    next_id: int = 1
    rpc_futures: dict = field(default_factory=dict)
    # Streaming state
    turn_done_event: asyncio.Event = field(default_factory=asyncio.Event)
    turn_error: Optional[str] = None
    turn_active: bool = False
    # Session identity from Gemini
    gemini_session_id: Optional[str] = None
    spawning: bool = False


class GeminiCliBackend(Backend):
    """One gemini --acp subprocess per bridge session."""

    def __init__(self, gemini_bin: str = "") -> None:
        self._gemini_bin = gemini_bin or _find_gemini_bin()
        self._states: dict[str, _GeminiState] = {}

    def _get_state(self, session: "Session") -> _GeminiState:
        if session.session_id not in self._states:
            self._states[session.session_id] = _GeminiState()
        return self._states[session.session_id]

    # ------------------------------------------------------------------ ACP helpers

    def _alloc_id(self, state: _GeminiState) -> int:
        rid = state.next_id
        state.next_id += 1
        return rid

    async def _send_rpc(self, state: _GeminiState, method: str,
                        params: dict | None, rid: int) -> None:
        assert state.proc and state.proc.stdin
        payload: dict = {"jsonrpc": "2.0", "id": rid, "method": method}
        if params is not None:
            payload["params"] = params
        state.proc.stdin.write((json.dumps(payload) + "\n").encode())
        await state.proc.stdin.drain()

    async def _send_notification(self, state: _GeminiState,
                                  method: str, params: dict | None = None) -> None:
        assert state.proc and state.proc.stdin
        msg: dict = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        state.proc.stdin.write((json.dumps(msg) + "\n").encode())
        await state.proc.stdin.drain()

    async def _rpc(self, state: _GeminiState, method: str,
                   params: dict | None, timeout: float = 30.0) -> dict:
        rid = self._alloc_id(state)
        loop = asyncio.get_event_loop()
        fut: asyncio.Future = loop.create_future()
        state.rpc_futures[rid] = fut
        await self._send_rpc(state, method, params, rid)
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        except asyncio.TimeoutError:
            state.rpc_futures.pop(rid, None)
            raise TimeoutError(f"gemini ACP RPC '{method}' timed out after {timeout}s")

    # ------------------------------------------------------------------ read loop

    async def _read_loop(self, session: "Session", state: _GeminiState) -> None:
        proc = state.proc
        if proc is None or proc.stdout is None:
            return
        try:
            async for raw in proc.stdout:
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except Exception:
                    log.debug("[gemini:%s] non-JSON line: %s", session.session_id[:8], line[:120])
                    continue
                await self._dispatch(session, state, msg)
        except Exception as exc:
            log.warning("[gemini:%s] read_loop error: %s", session.session_id[:8], exc)
        finally:
            log.info("[gemini:%s] read_loop exited (rc=%s)",
                     session.session_id[:8], proc.returncode)
            for fut in state.rpc_futures.values():
                if not fut.done():
                    fut.set_exception(RuntimeError("gemini process died"))
            state.rpc_futures.clear()
            if state.turn_active:
                state.turn_error = "gemini process exited unexpectedly"
                state.turn_done_event.set()
            state.proc = None

    async def _dispatch(self, session: "Session", state: _GeminiState, msg: dict) -> None:
        rid = msg.get("id")
        method = msg.get("method", "")

        # RPC response
        if rid is not None and "result" in msg:
            fut = state.rpc_futures.pop(rid, None)
            if fut and not fut.done():
                fut.set_result(msg["result"])
            return
        if rid is not None and "error" in msg:
            fut = state.rpc_futures.pop(rid, None)
            if fut and not fut.done():
                fut.set_exception(RuntimeError(str(msg["error"])))
            return

        # Server-side tool_use request: Gemini asks bridge to run a tool
        if rid is not None and method:
            await self._handle_tool_request(session, state, rid, method, msg.get("params", {}))
            return

        # Notifications (no id)
        params = msg.get("params", {}) or {}

        if method in ("message/delta", "turn/delta", "content/delta"):
            delta = params.get("delta") or params.get("text") or params.get("content") or ""
            if isinstance(delta, str) and delta:
                session.accumulated_text = (session.accumulated_text or "") + delta
                try:
                    await stream_text(delta, session)
                except Exception:
                    pass

        elif method in ("turn/complete", "turn/completed", "message/complete"):
            if state.turn_active:
                err = params.get("error")
                state.turn_error = err.get("message") if isinstance(err, dict) else (err or None)
                state.turn_done_event.set()

        elif method in ("session/created", "session/started"):
            sid = params.get("sessionId") or params.get("id")
            if sid:
                state.gemini_session_id = str(sid)
                session.resume_id = state.gemini_session_id
                if session.ws_ref:
                    try:
                        await session.ws_ref.send(json.dumps(
                            _msg_session_uuid(session.session_id, state.gemini_session_id)
                        ))
                    except Exception:
                        pass

        elif method == "error":
            err = params.get("message") or str(params)
            if state.turn_active:
                state.turn_error = err
                state.turn_done_event.set()
            else:
                log.warning("[gemini:%s] server error: %s", session.session_id[:8], err[:200])

        else:
            log.debug("[gemini:%s] unhandled notification: %s %s",
                      session.session_id[:8], method, str(params)[:120])

    async def _handle_tool_request(self, session: "Session", state: _GeminiState,
                                    rid: int, method: str, params: dict) -> None:
        """Intercept Gemini's tool_use RPC and route to bridge native handlers."""
        tool_name = method.split("/")[-1]
        tool_input = params if isinstance(params, dict) else {}
        tool_id = f"gemini_tool_{rid}"

        await send_event(session, _evt_tool_start(tool_id, tool_name, tool_input))

        try:
            result = await _execute_bridge_tool(tool_name, tool_input, session)
            await send_event(session, _evt_tool_result(tool_id, tool_name, str(result)))
            response = {"jsonrpc": "2.0", "id": rid, "result": {"output": str(result)}}
        except Exception as exc:
            error_msg = str(exc)
            await send_event(session, _evt_tool_result(tool_id, tool_name, f"Error: {error_msg}"))
            response = {"jsonrpc": "2.0", "id": rid, "error": {"code": -32000, "message": error_msg}}

        await send_event(session, _evt_tool_end(tool_id, tool_name))

        assert state.proc and state.proc.stdin
        state.proc.stdin.write((json.dumps(response) + "\n").encode())
        await state.proc.stdin.drain()

    # ------------------------------------------------------------------ Backend API

    async def spawn(self, session: "Session") -> None:
        state = self._get_state(session)
        if state.proc is not None and state.proc.returncode is None:
            return
        if state.spawning:
            return
        state.spawning = True
        try:
            await self._spawn_proc(session, state)
        finally:
            state.spawning = False

    async def _spawn_proc(self, session: "Session", state: _GeminiState) -> None:
        if not self._gemini_bin:
            raise RuntimeError("gemini binary not found — install with: npm install -g @google/gemini-cli")

        cmd = [self._gemini_bin, "--acp"]
        if session.resume_id:
            cmd += ["--resume", session.resume_id]

        cwd = session.cwd if os.path.isdir(session.cwd) else os.path.expanduser("~")

        log.info("[gemini:%s] spawning: %s (cwd=%s)", session.session_id[:8], " ".join(cmd), cwd)
        state.proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            limit=_STREAM_READER_LIMIT,
        )

        if state.read_task is not None:
            state.read_task.cancel()
        state.read_task = asyncio.create_task(self._read_loop(session, state))

        # ACP handshake
        try:
            result = await self._rpc(state, "initialize", {
                "clientInfo": {"name": "claude-bridge", "version": "1.0"},
                "capabilities": {},
            }, timeout=15.0)
            await self._send_notification(state, "initialized")
            log.info("[gemini:%s] ACP initialized: %s",
                     session.session_id[:8], str(result)[:80])
        except Exception as exc:
            log.error("[gemini:%s] ACP initialize failed: %s", session.session_id[:8], exc)
            raise

    async def send(self, session: "Session", content: str,
                   images: list | None = None, files: list | None = None) -> None:
        state = self._get_state(session)

        if session.is_streaming:
            await send_event(session, _evt_error("Session is currently processing a request.", "session_busy"))
            return

        if state.proc is None or state.proc.returncode is not None:
            try:
                await self.spawn(session)
            except Exception as exc:
                await send_event(session, _evt_error(f"Failed to start gemini: {exc}", "spawn_failed"))
                return

        session.is_streaming = True
        session.is_stopping = False
        session.accumulated_text = ""
        session.last_activity = time.time()

        # Build content list
        user_text = content or ""
        for f in (files or []):
            name = f.get("name", "file")
            body = f.get("content", "")
            user_text += f"\n\n[File: {name}]\n{body}"

        content_parts: list[dict] = [{"type": "text", "text": user_text}]
        for img in (images or []):
            content_parts.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": img.get("media_type", "image/jpeg"),
                    "data": img.get("data", ""),
                },
            })

        state.turn_done_event.clear()
        state.turn_error = None
        state.turn_active = True

        asyncio.create_task(self._run_turn(session, state, content_parts))

    async def _run_turn(self, session: "Session", state: _GeminiState,
                        content_parts: list[dict]) -> None:
        try:
            await self._rpc(state, "message", {
                "role": "user",
                "content": content_parts,
            }, timeout=30.0)

            try:
                await asyncio.wait_for(state.turn_done_event.wait(), timeout=_TURN_TIMEOUT_SECS)
            except asyncio.TimeoutError:
                state.turn_error = f"Gemini turn timed out after {_TURN_TIMEOUT_SECS}s"

            if session.is_stopping:
                await send_event(session, _evt_stopped())
            elif state.turn_error:
                log.warning("[gemini:%s] turn error: %s", session.session_id[:8], state.turn_error)
                await send_event(session, _evt_error(state.turn_error, "turn_error"))
            else:
                await send_event(session, _evt_done())

        except Exception as exc:
            if not session.is_stopping:
                await send_event(session, _evt_error(f"gemini turn failed: {exc}", "stream_error"))
        finally:
            state.turn_active = False
            session.is_streaming = False
            session.is_stopping = False

    async def stop(self, session: "Session") -> None:
        state = self._get_state(session)
        session.is_stopping = True

        # Try graceful cancel via ACP
        if state.proc and state.proc.returncode is None and state.turn_active:
            try:
                await self._send_notification(state, "cancel")
            except Exception:
                pass

        state.turn_error = "stopped"
        state.turn_done_event.set()
        session.is_streaming = False
        await send_event(session, _evt_stopped())

    async def clear(self, session: "Session") -> None:
        await self.stop(session)
        state = self._get_state(session)

        # Terminate process to clear in-memory history
        if state.proc and state.proc.returncode is None:
            try:
                state.proc.terminate()
                await asyncio.wait_for(state.proc.wait(), timeout=3.0)
            except Exception:
                try:
                    state.proc.kill()
                except Exception:
                    pass
        state.proc = None
        state.gemini_session_id = None
        session.resume_id = None
        await send_event(session, _evt_session_warning("Session history cleared."))

    async def close(self, session: "Session") -> None:
        await self.stop(session)
        state = self._states.pop(session.session_id, None)
        if state and state.proc and state.proc.returncode is None:
            try:
                state.proc.terminate()
            except Exception:
                pass
        await send_event(session, _evt_session_closed())

    def supports_resume(self) -> bool:
        return True

    async def get_resumable_sessions(self, limit: int = 100) -> list[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _load_gemini_sessions, limit)

    async def load_session_history(
        self,
        resume_id: str,
        limit: int = 120,
        known_last_source_message_id: str = "",
        mode: str = "snapshot",
        before_source_message_id: str = "",
    ) -> list[dict] | dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            _load_gemini_history,
            resume_id,
            clamp_history_limit(limit),
            known_last_source_message_id,
            mode,
            before_source_message_id,
        )


# ------------------------------------------------------------------ tool execution

async def _execute_bridge_tool(tool_name: str, params: dict, session: "Session") -> str:
    """Route Gemini tool_use requests to bridge native handlers."""
    if tool_name in ("execute_command", "run_shell", "shell"):
        cmd = params.get("command") or params.get("cmd") or ""
        if not cmd:
            return "Error: no command provided"
        cwd = params.get("cwd") or session.cwd or os.path.expanduser("~")
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=cwd,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=30.0)
        return stdout.decode("utf-8", errors="replace")[:8000]

    if tool_name in ("read_file",):
        path = os.path.expanduser(params.get("path") or "")
        if not path or not os.path.isfile(path):
            return f"Error: file not found: {path}"
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.read(32768)

    if tool_name in ("write_file",):
        path = os.path.expanduser(params.get("path") or "")
        content = params.get("content") or ""
        if not path:
            return "Error: no path provided"
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        return f"Written {len(content)} bytes to {path}"

    if tool_name in ("list_directory", "list_dir"):
        path = os.path.expanduser(params.get("path") or session.cwd or "~")
        if not os.path.isdir(path):
            return f"Error: not a directory: {path}"
        entries = os.listdir(path)
        return "\n".join(sorted(entries))

    return f"Error: unknown tool '{tool_name}'"


# ------------------------------------------------------------------ history helpers

def _gemini_history_dirs() -> list[str]:
    tmp_root = os.path.join(_GEMINI_HOME, "tmp")
    if not os.path.isdir(tmp_root):
        return []
    pattern = os.path.join(tmp_root, "*", "chats")
    return [d for d in glob.glob(pattern) if os.path.isdir(d)]


def _load_gemini_sessions(limit: int = 100) -> list[dict]:
    out: list[dict] = []
    for chat_dir in _gemini_history_dirs():
        for chat_file in sorted(glob.glob(os.path.join(chat_dir, "*.json")), reverse=True):
            try:
                with open(chat_file, "r", encoding="utf-8", errors="ignore") as f:
                    data = json.load(f)
            except Exception:
                continue
            sid = data.get("id") or os.path.splitext(os.path.basename(chat_file))[0]
            messages = data.get("messages") or data.get("turns") or []
            name = _infer_session_name(messages, sid[:8])
            mtime = int(os.path.getmtime(chat_file) * 1000)
            out.append({
                "id": f"gemini_{sid[:12]}",
                "name": name,
                "claude_uuid": sid,
                "last_used": mtime,
                "cwd": data.get("cwd") or os.path.expanduser("~"),
            })
            if len(out) >= limit:
                break
        if len(out) >= limit:
            break
    return out


def _load_gemini_history(
    resume_id: str,
    limit: int,
    known_last_source_message_id: str,
    mode: str,
    before_source_message_id: str,
) -> list[dict] | dict:
    chat_file = _find_gemini_chat_file(resume_id)
    if not chat_file:
        return []
    try:
        with open(chat_file, "r", encoding="utf-8", errors="ignore") as f:
            data = json.load(f)
    except Exception:
        return []

    raw_messages = data.get("messages") or data.get("turns") or []
    messages: list[dict] = []
    for i, item in enumerate(raw_messages):
        role = item.get("role") or item.get("author", "")
        if role == "model":
            role = "assistant"
        if role not in ("user", "assistant"):
            continue
        parts = item.get("parts") or item.get("content") or []
        if isinstance(parts, str):
            text = parts
        elif isinstance(parts, list):
            text = "\n".join(
                p.get("text", "") if isinstance(p, dict) else str(p)
                for p in parts
                if isinstance(p, dict) and p.get("text")
            ).strip()
        else:
            continue
        if not text:
            continue
        ts = item.get("timestamp") or item.get("created_at")
        ts_ms = int(ts * 1000) if isinstance(ts, (int, float)) else None
        source_id = f"gemini:{resume_id}:msg:{i}"
        messages.append(complete_history_message(
            source="gemini",
            source_session_id=resume_id,
            source_message_id=source_id,
            role=role,
            content=text,
            timestamp=ts_ms,
            blocks=[{"type": "text", "text": text}],
        ))

    return slice_history(
        messages,
        limit=limit,
        known_last_source_message_id=known_last_source_message_id,
        mode=mode,
        before_source_message_id=before_source_message_id,
    )


def _find_gemini_chat_file(session_id: str) -> str:
    for chat_dir in _gemini_history_dirs():
        # Exact match by file stem
        candidate = os.path.join(chat_dir, f"{session_id}.json")
        if os.path.isfile(candidate):
            return candidate
        # Prefix match
        for f in glob.glob(os.path.join(chat_dir, "*.json")):
            stem = os.path.splitext(os.path.basename(f))[0]
            if stem.startswith(session_id) or session_id.startswith(stem):
                return f
    return ""


def _infer_session_name(messages: list, fallback: str) -> str:
    for msg in messages:
        role = msg.get("role") or msg.get("author", "")
        if role != "user":
            continue
        parts = msg.get("parts") or msg.get("content") or []
        if isinstance(parts, str):
            text = parts
        elif isinstance(parts, list):
            text = next(
                (p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text")),
                "",
            )
        else:
            continue
        text = text.strip()
        if text:
            return text[:60] + ("…" if len(text) > 60 else "")
    return fallback


def _find_gemini_bin() -> str:
    for candidate in ("gemini", "gemini-cli"):
        path = which(candidate)
        if path:
            return path
    for p in (
        os.path.expanduser("~/.npm-global/bin/gemini"),
        os.path.expanduser("~/.local/bin/gemini"),
        "/usr/local/bin/gemini",
        "/opt/homebrew/bin/gemini",
    ):
        if os.path.isfile(p):
            return p
    return ""
