"""
Gemini CLI backend — ACP (Agent Client Protocol) mode.
Protocol: JSON-RPC 2.0 over stdio (newline-delimited).
Spec:     https://agentclientprotocol.com/protocol/
Spawn:    gemini --acp
History:  ~/.gemini/tmp/<project_hash>/chats/
"""

import asyncio
import glob
import json
import logging
import os
import time
from dataclasses import dataclass, field
from shutil import which
from typing import Optional, TYPE_CHECKING

from .base import Backend, _StatesMixin
from .jsonrpc import JsonRpcPlumber
from .events import (
    send_event, stream_text,
    _evt_error, _evt_stopped, _evt_done,
    _evt_tool_start, _evt_tool_result, _evt_tool_end,
    _evt_session_warning, _evt_session_closed,
    _msg_session_uuid,
)
from .history import complete_history_message, clamp_history_limit, slice_history
import client_manager
import task_manager

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")

_STREAM_READER_LIMIT = 128 * 1024 * 1024  # 128 MiB
_GEMINI_HOME = os.path.expanduser("~/.gemini")
_TURN_TIMEOUT_SECS = 600.0

# ACP protocol version (per spec)
_ACP_PROTOCOL_VERSION = 1


@dataclass
class _GeminiState:
    proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    read_task: Optional[asyncio.Task] = field(default=None, repr=False)
    plumber: JsonRpcPlumber = field(default_factory=lambda: JsonRpcPlumber("gemini"))
    # ACP session identity (assigned by Gemini after session/new or session/load)
    acp_session_id: Optional[str] = None
    # Current turn state
    turn_active: bool = False
    turn_stop_reason: Optional[str] = None  # "end_turn" | "cancelled" | etc.
    turn_error: Optional[str] = None
    spawning: bool = False


class GeminiCliBackend(Backend, _StatesMixin):
    """One gemini --acp subprocess per bridge session."""

    def __init__(self, gemini_bin: str = "") -> None:
        self._gemini_bin = gemini_bin or _find_gemini_bin()
        self._states: dict[str, _GeminiState] = {}

    def _state_factory(self) -> _GeminiState:
        return _GeminiState()

    # ------------------------------------------------------------------ JSON-RPC helpers

    async def _write(self, state: _GeminiState, obj: dict) -> None:
        assert state.proc and state.proc.stdin
        state.proc.stdin.write((json.dumps(obj) + "\n").encode())
        await state.proc.stdin.drain()

    async def _rpc(self, state: _GeminiState, method: str,
                   params: dict | None, timeout: float = 30.0) -> dict:
        """Send a request and wait for the response."""
        return await state.plumber.request(state.proc, method, params, timeout)

    async def _notify(self, state: _GeminiState,
                      method: str, params: dict | None = None) -> None:
        """Send a notification (no id, no response expected)."""
        await state.plumber.notify(state.proc, method, params)

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
                    log.debug("[gemini:%s] non-JSON: %s", session.session_id[:8], line[:120])
                    continue
                await self._dispatch(session, state, msg)
        except Exception as exc:
            log.warning("[gemini:%s] read_loop error: %s", session.session_id[:8], exc)
        finally:
            log.info("[gemini:%s] read_loop exited (rc=%s)",
                     session.session_id[:8], proc.returncode)
            state.plumber.fail_all(RuntimeError("gemini process died"))
            if state.turn_active:
                state.turn_error = "gemini process exited unexpectedly"
                # Resolve the pending session/prompt future via turn_stop_reason
                state.turn_stop_reason = "process_died"
                _resolve_prompt_future(state)
            state.proc = None

    async def _dispatch(self, session: "Session", state: _GeminiState, msg: dict) -> None:
        rid = msg.get("id")
        method = msg.get("method", "")

        # ── RPC response: {"id": N, "result": {...}} or {"id": N, "error": {...}}
        if rid is not None and ("result" in msg or "error" in msg):
            state.plumber.dispatch_response(msg)
            return

        # ── Server-side RPC request: {"id": N, "method": "...", "params": {...}}
        # (only session/request_permission per spec)
        if rid is not None and method:
            await self._handle_server_request(session, state, rid, method,
                                               msg.get("params", {}) or {})
            return

        # ── Notifications: {"method": "...", "params": {...}} (no id)
        params = msg.get("params", {}) or {}

        if method == "session/update":
            await self._handle_session_update(session, state, params)

        elif method in ("error", "session/error"):
            err = params.get("message") or str(params)
            if state.turn_active:
                state.turn_error = err
                _resolve_prompt_future(state)
            else:
                log.warning("[gemini:%s] server error: %s", session.session_id[:8], err[:200])

        else:
            log.debug("[gemini:%s] unhandled notification: %s %s",
                      session.session_id[:8], method, str(params)[:120])

    async def _handle_session_update(self, session: "Session", state: _GeminiState,
                                      params: dict) -> None:
        """Handle session/update notifications (streaming, tool calls, etc.)."""
        update = params.get("update", {}) or {}
        kind = update.get("sessionUpdate", "")

        if kind == "agent_message_chunk":
            # content may be a single ContentBlock dict OR a ContentBlock[]
            content = update.get("content") or []
            delta = ""
            if isinstance(content, dict):
                delta = content.get("text", "") if content.get("type") == "text" else ""
            elif isinstance(content, list):
                delta = "".join(
                    block.get("text", "") for block in content
                    if isinstance(block, dict) and block.get("type") == "text"
                )
            elif isinstance(content, str):
                delta = content
            if delta:
                session.accumulated_text = (session.accumulated_text or "") + delta
                try:
                    await stream_text(delta, session)
                except Exception:
                    pass

        elif kind == "agent_thought_chunk":
            # Extended reasoning / thinking — log but don't emit to frontend for now
            pass

        elif kind == "tool_call":
            # Agent is starting a tool call (informational; actual permission via request_permission)
            tool_call = update.get("toolCall", {}) or {}
            tool_name = tool_call.get("name") or tool_call.get("toolName") or "tool"
            tool_input = tool_call.get("input") or tool_call.get("args") or {}
            tool_id = tool_call.get("toolCallId") or tool_call.get("id") or "tc"
            await send_event(session, _evt_tool_start(tool_id, tool_name, json.dumps(tool_input) if isinstance(tool_input, (dict, list)) else str(tool_input)))

        elif kind == "tool_call_update":
            tool_call = update.get("toolCall", {}) or {}
            tool_id = tool_call.get("toolCallId") or tool_call.get("id") or "tc"
            tool_name = tool_call.get("name") or "tool"
            status = update.get("status") or ""
            if status in ("completed", "done", "success", "error"):
                output = update.get("output") or update.get("result") or ""
                await send_event(session, _evt_tool_result(tool_id, str(output)))
                await send_event(session, _evt_tool_end(tool_id))

        elif kind == "usage_update":
            usage = update.get("usage") or {}
            if isinstance(usage, dict):
                total = usage.get("totalTokens") or usage.get("total_tokens") or 0
                ctx_max = usage.get("contextWindow") or 0
                if total:
                    session.context_used = int(total)
                if ctx_max:
                    session.context_max = int(ctx_max)

    async def _handle_server_request(self, session: "Session", state: _GeminiState,
                                      rid: int, method: str, params: dict) -> None:
        """Handle RPC requests FROM the agent (e.g. session/request_permission)."""
        if method == "session/request_permission":
            # Auto-grant: find an "allow_once" or "allow_always" option and select it
            options: list = params.get("options") or []
            selected_id: str | None = None
            for opt in options:
                if isinstance(opt, dict) and opt.get("kind") in ("allow_always", "allow_once"):
                    selected_id = str(opt.get("optionId", ""))
                    break
            if selected_id:
                result = {"outcome": "selected", "optionId": selected_id}
            else:
                result = {"outcome": "cancelled"}
            await self._write(state, {"jsonrpc": "2.0", "id": rid, "result": result})
        else:
            # Unknown server request — return an error
            await self._write(state, {
                "jsonrpc": "2.0", "id": rid,
                "error": {"code": -32601, "message": f"unknown method: {method}"},
            })

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
            raise RuntimeError(
                "gemini binary not found — install: npm install -g @google/gemini-cli"
            )

        cwd = session.cwd if os.path.isdir(session.cwd) else os.path.expanduser("~")

        log.info("[gemini:%s] spawning %s (cwd=%s)", session.session_id[:8], self._gemini_bin, cwd)
        state.proc = await asyncio.create_subprocess_exec(
            self._gemini_bin, "--acp",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            limit=_STREAM_READER_LIMIT,
        )

        if state.read_task is not None:
            state.read_task.cancel()
        state.read_task = asyncio.create_task(self._read_loop(session, state))

        # ── ACP handshake: initialize
        try:
            result = await self._rpc(state, "initialize", {
                "protocolVersion": _ACP_PROTOCOL_VERSION,
                "clientInfo": {"name": "claude-bridge", "version": "1.0"},
                "clientCapabilities": {
                    "fs": {"readTextFile": True, "writeTextFile": True},
                    "terminal": True,
                },
            }, timeout=15.0)
            await self._notify(state, "initialized")
            log.info("[gemini:%s] ACP initialized (agent: %s)",
                     session.session_id[:8],
                     result.get("agentInfo", {}).get("name", "?"))
        except Exception as exc:
            log.error("[gemini:%s] initialize failed: %s", session.session_id[:8], exc)
            raise

        # ── Create or resume ACP session
        if session.resume_id:
            try:
                load_result = await self._rpc(state, "session/load", {
                    "sessionId": session.resume_id,
                    "cwd": cwd,
                    "mcpServers": [],
                }, timeout=15.0)
                acp_sid = (load_result.get("sessionId") or
                           load_result.get("session", {}).get("id") or
                           session.resume_id)
                state.acp_session_id = str(acp_sid)
                log.info("[gemini:%s] ACP session resumed: %s",
                         session.session_id[:8], state.acp_session_id[:8])
            except Exception as exc:
                log.warning("[gemini:%s] session/load failed (%s), creating new", session.session_id[:8], exc)
                session.resume_id = None
                state.acp_session_id = None

        if state.acp_session_id is None:
            new_result = await self._rpc(state, "session/new", {
                "cwd": cwd,
                "mcpServers": [],
            }, timeout=15.0)
            acp_sid = (new_result.get("sessionId") or
                       new_result.get("session", {}).get("id") or "")
            if not acp_sid:
                raise RuntimeError("gemini session/new returned no sessionId")
            state.acp_session_id = str(acp_sid)
            log.info("[gemini:%s] ACP session created: %s",
                     session.session_id[:8], state.acp_session_id[:8])

        # Propagate to bridge session metadata
        session.resume_id = state.acp_session_id
        if session.ws_ref:
            await client_manager.send_json(
                session.ws_ref,
                _msg_session_uuid(session.session_id, state.acp_session_id),
            )

    async def send(self, session: "Session", content: str,
                   images: list | None = None, files: list | None = None) -> None:
        if not await self._begin_send(session):
            return
        session.is_stopping = False

        state = self._get_state(session)

        if state.proc is None or state.proc.returncode is not None:
            try:
                await self.spawn(session)
            except Exception as exc:
                session.is_streaming = False
                session.turn_done_event.set()
                await send_event(session, _evt_error(f"Failed to start gemini: {exc}", "spawn_failed"))
                return

        # Build ACP ContentBlock[]
        prompt_blocks: list[dict] = []
        user_text = content or ""
        for f in (files or []):
            name = f.get("name", "file")
            body = f.get("content", "")
            user_text += f"\n\n[File: {name}]\n{body}"
        if user_text:
            prompt_blocks.append({"type": "text", "text": user_text})
        for img in (images or []):
            prompt_blocks.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": img.get("media_type", "image/jpeg"),
                    "data": img.get("data", ""),
                },
            })

        state.turn_active = True
        state.turn_stop_reason = None
        state.turn_error = None

        task_manager.spawn(
            f"gemini-turn:{session.session_id}",
            self._run_turn(session, state, prompt_blocks),
            owner=f"session:{session.session_id}",
        )

    async def _run_turn(self, session: "Session", state: _GeminiState,
                        prompt_blocks: list[dict]) -> None:
        try:
            # session/prompt is a blocking RPC — response arrives when the turn is fully done.
            # Streaming happens via session/update notifications in parallel.
            result = await self._rpc(state, "session/prompt", {
                "sessionId": state.acp_session_id,
                "prompt": prompt_blocks,
            }, timeout=_TURN_TIMEOUT_SECS)

            stop_reason = result.get("stopReason", "end_turn")
            state.turn_stop_reason = stop_reason

            if session.is_stopping or stop_reason == "cancelled":
                await send_event(session, _evt_stopped())
            else:
                await send_event(session, _evt_done())

        except asyncio.TimeoutError:
            if not session.is_stopping:
                await send_event(session, _evt_error(
                    f"Gemini turn timed out after {_TURN_TIMEOUT_SECS}s", "timeout"))
        except Exception as exc:
            if not session.is_stopping:
                await send_event(session, _evt_error(f"gemini turn failed: {exc}", "stream_error"))
        finally:
            state.turn_active = False
            session.is_streaming = False
            session.turn_done_event.set()
            session.is_stopping = False

    async def stop(self, session: "Session") -> None:
        state = self._get_state(session)
        session.is_stopping = True

        if state.acp_session_id and state.proc and state.proc.returncode is None:
            try:
                # session/cancel is a notification (no id, no response expected)
                await self._notify(state, "session/cancel", {
                    "sessionId": state.acp_session_id,
                })
            except Exception:
                pass

        session.is_streaming = False
        session.turn_done_event.set()
        await send_event(session, _evt_stopped())

    async def clear(self, session: "Session") -> None:
        await self.stop(session)
        state = self._get_state(session)
        # Terminate process; a new session/new will be issued on next send
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
        state.acp_session_id = None
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


# ------------------------------------------------------------------ helpers

def _resolve_prompt_future(state: _GeminiState) -> None:
    """Resolve the pending session/prompt future so _run_turn can unblock."""
    # The session/prompt future is identified as the most recently allocated RPC
    # with no result yet. We inject a synthetic result.
    for fut in list(state.plumber._futures.values()):
        if not fut.done():
            if state.turn_error:
                fut.set_exception(RuntimeError(state.turn_error))
            else:
                fut.set_result({"stopReason": state.turn_stop_reason or "cancelled"})
            break


# ------------------------------------------------------------------ history helpers

def _gemini_history_dirs() -> list[str]:
    tmp_root = os.path.join(_GEMINI_HOME, "tmp")
    if not os.path.isdir(tmp_root):
        return []
    return [
        d for d in glob.glob(os.path.join(tmp_root, "*", "chats"))
        if os.path.isdir(d)
    ]


def _load_gemini_sessions(limit: int = 100) -> list[dict]:
    out: list[dict] = []
    for chat_dir in _gemini_history_dirs():
        for chat_file in sorted(glob.glob(os.path.join(chat_dir, "*.json")), reverse=True):
            try:
                with open(chat_file, "r", encoding="utf-8", errors="ignore") as f:
                    data = json.load(f)
            except Exception:
                continue
            sid = data.get("sessionId") or data.get("id") or os.path.splitext(os.path.basename(chat_file))[0]
            messages = data.get("messages") or data.get("turns") or []
            name = _infer_session_name(messages, sid[:8])
            mtime = int(os.path.getmtime(chat_file) * 1000)
            out.append({
                "id": f"gemini_{sid[:12]}",
                "name": name,
                "claude_uuid": sid,
                "resume_id": sid,
                "last_used": mtime,
                "cwd": data.get("cwd") or os.path.expanduser("~"),
            })
            if len(out) >= limit:
                return out
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
        # Gemini CLI uses "type" field ("user"/"gemini"), not "role"
        role = item.get("role") or item.get("type") or item.get("author", "")
        if role in ("model", "gemini"):
            role = "assistant"
        if role not in ("user", "assistant"):
            continue
        parts = item.get("parts") or item.get("content") or []
        if isinstance(parts, str):
            text = parts
        elif isinstance(parts, list):
            # Gemini CLI parts are {"text": "..."} without a "type" field
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
        if isinstance(ts, (int, float)):
            ts_ms = int(ts * 1000)
        elif isinstance(ts, str):
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                ts_ms = int(dt.timestamp() * 1000)
            except Exception:
                ts_ms = None
        else:
            ts_ms = None
        messages.append(complete_history_message(
            source="gemini",
            source_session_id=resume_id,
            source_message_id=f"gemini:{resume_id}:msg:{i}",
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
        candidate = os.path.join(chat_dir, f"{session_id}.json")
        if os.path.isfile(candidate):
            return candidate
        for f in glob.glob(os.path.join(chat_dir, "*.json")):
            stem = os.path.splitext(os.path.basename(f))[0]
            if stem.startswith(session_id) or session_id.startswith(stem):
                return f
            # Gemini CLI embeds the first 8 chars of the session UUID in the filename
            # e.g. "session-2026-04-28T00-31-612f8c5a.json" for UUID "612f8c5a-..."
            if len(session_id) >= 8 and session_id[:8] in stem:
                return f
    return ""


def _infer_session_name(messages: list, fallback: str) -> str:
    for msg in messages:
        role = msg.get("role") or msg.get("type") or msg.get("author", "")
        if role != "user":
            continue
        parts = msg.get("parts") or msg.get("content") or []
        text = ""
        if isinstance(parts, str):
            text = parts
        elif isinstance(parts, list):
            text = next(
                (p.get("text", "") for p in parts
                 if isinstance(p, dict) and p.get("text")),
                "",
            )
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
