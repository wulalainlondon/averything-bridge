"""
Codex app-server backend.

Runs ONE `codex app-server` process for the entire bridge lifetime.
Each bridge session maps to one persistent Codex thread, so there is
no per-message process spawn overhead.

Protocol: newline-delimited JSON-RPC 2.0 over stdin/stdout.
"""

import asyncio
import base64
import json
import logging
import os
import re
import subprocess
import time
import uuid
from dataclasses import dataclass, field
from shutil import which
from typing import Optional, TYPE_CHECKING

from .base import Backend, _StatesMixin
from .jsonrpc import JsonRpcPlumber
from .events import (
    send_event, stream_text, emit_done,
    _evt_error, _evt_stopped, _evt_done, _evt_session_warning, _evt_session_closed,
    _evt_thinking_chunk, _evt_tool_start, _evt_tool_result, _evt_tool_end,
    _msg_session_uuid, _msg_usage_report,
)
from .history import complete_history_message, clamp_history_limit, load_indexed_jsonl_messages, slice_history, _JSONL_HISTORY_CACHE, DEFAULT_HISTORY_LIMIT
from interactions import REGISTRY as INTERACTIONS, normalize_questions
from push_registry import notify_fcm_user_input as _notify_fcm_user_input
import client_manager

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")

_STREAM_READER_LIMIT = 128 * 1024 * 1024  # 128 MiB
_COMPACT_THRESHOLD = 0.80
_CODEX_WRAPPER_CLOSED_RE = re.compile(
    r"<(turn_aborted)>.*?</\1>",
    re.IGNORECASE | re.DOTALL,
)
# Errors that mean the Codex thread no longer exists on the server side
# (e.g. app-server restarted). Detected here so we can respawn silently.
_STALE_THREAD_RE = re.compile(r"Unknown session", re.IGNORECASE)
CODEX_MODEL = "gpt-5.5"

# Matches a JSON block (in a fenced code block or inline) containing ask_user_question.
_ASK_USER_RE = re.compile(
    r"```(?:json)?\s*(\{[^`]*?\"(?:ask_user_question|AskUserQuestion)\"[^`]*?\})\s*```"
    r"|(\{[^{}]*\"(?:ask_user_question|AskUserQuestion)\"[^{}]*\})",
    re.DOTALL,
)


def _extract_ask_user_question(text: str) -> dict | None:
    """Return the first AskUserQuestion JSON block found in Codex output, or None."""
    for m in _ASK_USER_RE.finditer(text):
        raw = m.group(1) or m.group(2)
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and str(data.get("type", "")).lower() in (
                "ask_user_question", "askuserquestion"
            ):
                return data
        except Exception:
            continue
    return None


@dataclass
class _AppServerState:
    thread_id: Optional[str] = None
    current_turn_id: Optional[str] = None
    turn_active: bool = False
    turn_done_event: asyncio.Event = field(default_factory=asyncio.Event)
    turn_error: Optional[str] = None
    last_usage: dict = field(default_factory=dict)
    last_rate_limits: dict = field(default_factory=dict)
    usage_updated_at: float = 0.0
    temp_image_paths: list = field(default_factory=list)
    tool_outputs: dict[str, str] = field(default_factory=dict)
    compact_in_progress: bool = False
    compact_done_event: asyncio.Event = field(default_factory=asyncio.Event)
    compact_error: Optional[str] = None


class CodexAppServerBackend(Backend, _StatesMixin):
    """Persistent Codex app-server backend — one process, per-session threads."""

    def __init__(self, codex_bin: str,
                 broadcast_fn: "Callable[[dict], Coroutine] | None" = None,
                 notify_fcm_fn: "Callable[[str, str, str], Coroutine] | None" = None,
                 persist_session_fn: "Callable | None" = None):
        self._codex_bin = codex_bin
        self._broadcast_fn = broadcast_fn
        self._notify_fcm_fn = notify_fcm_fn
        self._persist_session_fn = persist_session_fn
        self._codex_home = os.path.expanduser("~/.codex")
        self._native_sessions_root = os.path.join(self._codex_home, "sessions")
        self._native_session_index_path = os.path.join(self._codex_home, "session_index.jsonl")
        self._session_path_index: dict[str, str] | None = None
        self._session_path_index_time: float = 0.0

        # Singleton app-server
        self._proc: Optional[asyncio.subprocess.Process] = None
        self._read_task: Optional[asyncio.Task] = None
        self._rpc_plumber = JsonRpcPlumber("codex")

        # Per-session state
        self._states: dict[str, _AppServerState] = {}
        # threadId -> Session for notification routing
        self._thread_to_session: dict[str, "Session"] = {}

        self._start_lock = asyncio.Lock()

    # ------------------------------------------------------------------ helpers

    def _state_factory(self) -> "_AppServerState":
        return _AppServerState()

    async def _send_notification(self, method: str, params: dict | None = None) -> None:
        await self._rpc_plumber.notify(self._proc, method, params)

    async def _rpc(self, method: str, params: dict | None, timeout: float = 30.0) -> dict:
        return await self._rpc_plumber.request(self._proc, method, params, timeout)

    async def _ensure_server(self) -> None:
        """Spawn and initialize the singleton app-server if it's not running."""
        async with self._start_lock:
            if self._proc is not None and self._proc.returncode is None:
                return  # already running

            log.info("[codex-appserver] spawning codex app-server")
            self._proc = await asyncio.create_subprocess_exec(
                self._codex_bin, "app-server",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=_STREAM_READER_LIMIT,
            )

            # Start the global read loop before sending any RPC.
            if self._read_task is not None:
                self._read_task.cancel()
            self._read_task = asyncio.create_task(self._read_loop())

            try:
                result = await self._rpc("initialize", {
                    "clientInfo": {"name": "claude-bridge", "version": "1.0"}
                }, timeout=30.0)
                await self._send_notification("initialized")
                log.info("[codex-appserver] initialized: %s", result.get("userAgent", "?"))
            except Exception as exc:
                log.error("[codex-appserver] initialize failed: %s", exc)
                raise

    async def _read_loop(self) -> None:
        """Continuous read loop: route RPC responses and stream notifications."""
        proc = self._proc
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
                    continue
                if self._rpc_plumber.dispatch_response(msg):
                    continue
                await self._dispatch(msg)
        except Exception as exc:
            log.warning("[codex-appserver] read_loop error: %s", exc)
        finally:
            log.info("[codex-appserver] read_loop exited (proc rc=%s)", proc.returncode)
            self._rpc_plumber.fail_all(RuntimeError("app-server process died"))
            # Mark proc gone so next call restarts it
            self._proc = None

    async def _dispatch(self, msg: dict) -> None:
        # --- Notifications (RPC responses are handled by _rpc_plumber in _read_loop) ---
        msg_id = msg.get("id")
        method = msg.get("method", "")
        params = msg.get("params", {}) or {}
        if msg_id is not None and method and "result" not in msg and "error" not in msg:
            await self._handle_server_request(msg_id, method, params)
            return

        thread_id = params.get("threadId")
        session = self._thread_to_session.get(thread_id) if thread_id else None

        if method == "turn/started" and session:
            state = self._states.get(session.session_id)
            if state:
                turn = params.get("turn", {})
                state.current_turn_id = turn.get("id")

        elif method == "item/agentMessage/delta" and session:
            phase = params.get("phase") or params.get("messagePhase") or params.get("item", {}).get("phase")
            delta = params.get("delta", "")
            if delta:
                if phase == "commentary":
                    try:
                        await send_event(session, _evt_thinking_chunk(delta))
                    except Exception:
                        pass
                    return
                session.accumulated_text = (session.accumulated_text or "") + delta
                try:
                    await stream_text(delta, session)
                except Exception:
                    pass

        elif method == "item/reasoning/textDelta" and session:
            delta = params.get("delta") or params.get("text") or ""
            if delta:
                try:
                    await send_event(session, _evt_thinking_chunk(str(delta)))
                except Exception:
                    pass

        elif method == "item/commandExecution/outputDelta" and session:
            item_id = str(params.get("itemId") or params.get("callId") or "codex_command")
            delta = params.get("delta") or params.get("output") or ""
            if delta:
                try:
                    state = self._states.get(session.session_id)
                    output = str(delta)
                    if state:
                        output = state.tool_outputs.get(item_id, "") + output
                        state.tool_outputs[item_id] = output
                    await send_event(session, _evt_tool_result(item_id, output))
                except Exception:
                    pass

        elif method == "item/fileChange/outputDelta" and session:
            item_id = str(params.get("itemId") or params.get("callId") or "codex_file_change")
            delta = params.get("delta") or params.get("output") or ""
            if delta:
                try:
                    state = self._states.get(session.session_id)
                    output = str(delta)
                    if state:
                        output = state.tool_outputs.get(item_id, "") + output
                        state.tool_outputs[item_id] = output
                    await send_event(session, _evt_tool_result(item_id, output))
                except Exception:
                    pass

        elif method == "item/started" and session:
            item = params.get("item", {}) if isinstance(params.get("item"), dict) else {}
            item_id = str(params.get("itemId") or item.get("id") or "codex_item")
            name = str(params.get("name") or item.get("name") or item.get("type") or "codex")
            command = params.get("command") or item.get("command") or item.get("input") or ""
            if isinstance(command, (dict, list)):
                command = json.dumps(command, ensure_ascii=False)
            try:
                state = self._states.get(session.session_id)
                if state:
                    state.tool_outputs[item_id] = ""
                await send_event(session, _evt_tool_start(item_id, name, str(command)))
            except Exception:
                pass

        elif method == "item/completed" and session:
            item = params.get("item", {}) if isinstance(params.get("item"), dict) else {}
            item_id = str(params.get("itemId") or item.get("id") or "codex_item")
            output = params.get("output") or item.get("output") or item.get("result") or ""
            try:
                state = self._states.get(session.session_id)
                if output:
                    if state:
                        state.tool_outputs[item_id] = str(output)
                    await send_event(session, _evt_tool_result(item_id, str(output)))
                await send_event(session, _evt_tool_end(item_id))
                if state:
                    state.tool_outputs.pop(item_id, None)
            except Exception:
                pass

        elif method == "item/commandExecution/terminalInteraction" and session:
            item_id = str(params.get("itemId") or params.get("callId") or "codex_terminal")
            text = params.get("text") or params.get("input") or params.get("message") or ""
            if text:
                try:
                    state = self._states.get(session.session_id)
                    output = str(text)
                    if state:
                        output = state.tool_outputs.get(item_id, "") + output
                        state.tool_outputs[item_id] = output
                    await send_event(session, _evt_tool_result(item_id, output))
                except Exception:
                    pass

        elif method == "turn/completed" and session:
            state = self._states.get(session.session_id)
            if state and state.turn_active:
                turn = params.get("turn", {})
                status = turn.get("status", "")
                if status == "failed":
                    err = turn.get("error", {}) or {}
                    state.turn_error = err.get("message", "turn failed")
                else:
                    state.turn_error = None
                state.turn_done_event.set()
            elif state and state.compact_in_progress:
                # Compact turn completion
                turn = params.get("turn", {})
                status = turn.get("status", "")
                if status == "failed":
                    err = turn.get("error", {}) or {}
                    state.compact_error = err.get("message", "compact turn failed")
                else:
                    state.compact_error = None
                state.compact_done_event.set()

        elif method == "thread/compacted" and session:
            state = self._states.get(session.session_id)
            if state and state.compact_in_progress:
                state.compact_error = None
                state.compact_done_event.set()

        elif method == "error" and session:
            state = self._states.get(session.session_id)
            if state and state.turn_active and not params.get("willRetry"):
                err = params.get("error", {}) or {}
                state.turn_error = err.get("message", "unknown codex error")
                state.turn_done_event.set()

        elif method == "thread/tokenUsage/updated" and session:
            # Parse usage into state
            state = self._states.get(session.session_id)
            if state:
                token_usage = params.get("tokenUsage", {}) or params.get("usage", {}) or {}
                usage = token_usage.get("last", {}) if isinstance(token_usage, dict) else {}
                state.last_usage = {
                    "total_tokens": int(usage.get("totalTokens") or usage.get("total_tokens") or 0),
                    "input_tokens": int(usage.get("inputTokens") or usage.get("input_tokens") or 0),
                    "output_tokens": int(usage.get("outputTokens") or usage.get("output_tokens") or 0),
                    "cached_input_tokens": int(usage.get("cachedInputTokens") or usage.get("cached_input_tokens") or 0),
                    "reasoning_output_tokens": int(usage.get("reasoningOutputTokens") or usage.get("reasoning_output_tokens") or 0),
                }
                state.usage_updated_at = time.time()
                session.context_used = state.last_usage["total_tokens"]
                context_window = token_usage.get("modelContextWindow") if isinstance(token_usage, dict) else None
                if context_window is not None:
                    try:
                        session.context_max = int(context_window)
                    except Exception:
                        pass

        elif method == "account/rateLimits/updated":
            rate_limits = params.get("rateLimits", {})
            if isinstance(rate_limits, dict):
                for state in self._states.values():
                    state.last_rate_limits = rate_limits

        elif method in ("warning", "error") and not session:
            log.debug("[codex-appserver] %s: %s", method, str(params)[:200])

    async def _handle_server_request(self, request_id, method: str, params: dict) -> None:
        approval_methods = {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "item/permissions/requestApproval",
            "applyPatchApproval",
            "execCommandApproval",
        }
        if method in approval_methods:
            result = {"decision": "accept"}
        elif method == "item/tool/requestUserInput":
            result = {"answers": []}
        elif method == "item/tool/call":
            await self._rpc_plumber.write(self._proc, {
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": "item/tool/call is not implemented by claude-bridge",
                },
            })
            return
        else:
            log.debug("[codex-appserver] unhandled ServerRequest: %s", method)
            await self._rpc_plumber.write(self._proc, {
                "id": request_id,
                "error": {"code": -32601, "message": f"unknown method: {method}"},
            })
            return

        await self._rpc_plumber.write(self._proc, {"id": request_id, "result": result})

    # ------------------------------------------------------------------ Backend API

    async def spawn(self, session: "Session") -> None:
        await self._ensure_server()
        state = self._get_state(session)

        if state.thread_id is not None:
            # Already has a thread; verify it's still known to the server.
            return

        cwd = session.cwd if os.path.isdir(session.cwd) else os.path.expanduser("~")

        if session.resume_id:
            # Resume an existing Codex thread.
            try:
                result = await self._rpc("thread/resume", {
                    "threadId": session.resume_id,
                    "cwd": cwd,
                    "approvalPolicy": "never",
                    "sandbox": self._sandbox_mode(session),
                }, timeout=15.0)
                thread = result.get("thread", {})
                state.thread_id = thread.get("id") or session.resume_id
            except Exception as exc:
                log.warning("[codex-appserver] thread/resume failed (%s), starting new thread: %s",
                            session.resume_id, exc)
                state.thread_id = None  # fall through to thread/start

        if state.thread_id is None:
            result = await self._rpc("thread/start", {
                "model": session.model or CODEX_MODEL,
                "cwd": cwd,
                "ephemeral": False,
                "approvalPolicy": "never",
                "sandbox": self._sandbox_mode(session),
            }, timeout=15.0)
            thread = result.get("thread", {})
            state.thread_id = thread.get("id")
            if not state.thread_id:
                raise RuntimeError("codex thread/start returned no thread id")

        # Register for notification routing.
        self._thread_to_session[state.thread_id] = session

        # Emit the native thread ID as the session UUID.
        if session.ws_ref:
            try:
                await session.ws_ref.send(json.dumps(
                    _msg_session_uuid(session.session_id, state.thread_id)
                ))
            except Exception:
                pass
        session.resume_id = state.thread_id
        if self._persist_session_fn is not None:
            self._persist_session_fn(session)
        log.info("[codex-appserver] session=%s thread=%s", session.session_id[:8], state.thread_id[:8])

    async def send(self, session: "Session", content: str,
                   images: list | None = None, files: list | None = None) -> None:
        if not await self._begin_send(session):
            return
        session.is_stopping = False

        state = self._get_state(session)

        # User-triggered /compact: route through the proper RPC path, same as auto-compact.
        if (content or "").strip() == "/compact" and not state.compact_in_progress:
            asyncio.create_task(self._auto_compact(session, state))
            return

        # Ensure we have an active thread. Also re-spawn if app-server died (proc gone).
        proc_alive = self._proc is not None and self._proc.returncode is None
        if state.thread_id is None or not proc_alive:
            if not proc_alive:
                # Server restarted — old thread_id is invalid; request a new thread.
                if state.thread_id:
                    self._thread_to_session.pop(state.thread_id, None)
                state.thread_id = None
            try:
                await self.spawn(session)
            except Exception as exc:
                session.is_streaming = False
                await send_event(session, _evt_error(f"Failed to start codex thread: {exc}", "spawn_failed"))
                return

        # Build input list.
        user_text = content or ""
        for f in (files or []):
            name = f.get("name", "file")
            body = f.get("content", "")
            user_text += f"\n\n[File: {name}]\n{body}"
        user_input: list[dict] = [{"type": "text", "text": user_text, "text_elements": []}]
        for img in (images or []):
            img_input = self._prepare_image_input(session, img, state)
            if img_input:
                user_input.append(img_input)

        # Prepare turn event.
        state.turn_done_event.clear()
        state.turn_error = None
        state.turn_active = True

        asyncio.create_task(self._run_turn(session, state, user_input))

    async def handle_user_input_response(self, session: "Session", interaction, response: dict) -> None:
        answers = response.get("answers") if isinstance(response.get("answers"), dict) else {}
        if not answers:
            answers = {
                k: v for k, v in response.items()
                if k not in {"type", "response_type", "request_id", "session_id"}
            }
        payload = json.dumps({
            "request_id": interaction.request_id,
            "answers": answers,
            "cancelled": bool(response.get("cancelled") or response.get("canceled")),
        }, ensure_ascii=False)
        await self.send(session, f"Structured user input response:\n{payload}")

    async def _run_turn(self, session: "Session", state: _AppServerState,
                        user_input: list[dict]) -> None:
        try:
            try:
                await self._rpc("turn/start", {
                    "threadId": state.thread_id,
                    "input": user_input,
                    "approvalPolicy": "never",
                }, timeout=30.0)
            except RuntimeError as rpc_exc:
                if not _STALE_THREAD_RE.search(str(rpc_exc)):
                    raise
                # turn/start rejected the thread (Codex restarted between spawn and send).
                # Respawn silently and retry once — the user sees no error.
                log.warning("[codex-appserver] stale thread on turn/start session=%s, respawning",
                            session.session_id[:8])
                self._thread_to_session.pop(state.thread_id or "", None)
                state.thread_id = None
                await self.spawn(session)
                await self._rpc("turn/start", {
                    "threadId": state.thread_id,
                    "input": user_input,
                    "approvalPolicy": "never",
                }, timeout=30.0)

            # Wait until turn completes or timeout.
            try:
                await asyncio.wait_for(state.turn_done_event.wait(), timeout=6000.0)
            except asyncio.TimeoutError:
                state.turn_error = "Codex turn timed out after 6000s"

            # Stale-thread error via notification path (no id, routed through _dispatch).
            # Respawn and retry once rather than surfacing a confusing error to the user.
            if state.turn_error and _STALE_THREAD_RE.search(state.turn_error) and not session.is_stopping:
                log.warning("[codex-appserver] stale thread (notification) session=%s, respawning: %s",
                            session.session_id[:8], state.turn_error)
                self._thread_to_session.pop(state.thread_id or "", None)
                state.thread_id = None
                state.turn_error = None
                state.turn_done_event.clear()
                state.turn_active = True
                await self.spawn(session)
                await self._rpc("turn/start", {
                    "threadId": state.thread_id,
                    "input": user_input,
                    "approvalPolicy": "never",
                }, timeout=30.0)
                try:
                    await asyncio.wait_for(state.turn_done_event.wait(), timeout=6000.0)
                except asyncio.TimeoutError:
                    state.turn_error = "Codex turn timed out after 6000s"

            if session.is_stopping:
                await send_event(session, _evt_stopped())
            elif state.turn_error:
                log.warning("[codex-appserver] turn error: %s", state.turn_error)
                await send_event(session, _evt_error(state.turn_error, "turn_error"))
            else:
                # Detect AskUserQuestion JSON block in Codex output.
                _ask_data = _extract_ask_user_question(session.accumulated_text or "")
                if _ask_data is not None:
                    try:
                        _questions = normalize_questions(_ask_data)
                        _header = str(_ask_data.get("header") or _ask_data.get("title") or "Question")
                        _interaction = await INTERACTIONS.create(
                            session_id=session.session_id,
                            source="codex",
                            kind="ask_user_question",
                            questions=_questions,
                            header=_header,
                            requesting_agent="AskUserQuestion",
                            raw_command=_ask_data,
                            broadcast_json=self._broadcast_fn,
                        )
                        asyncio.create_task(_notify_fcm_user_input(
                            session_name=session.name,
                            header=_header,
                            question_text=_questions[0].get("text", "") if _questions else "",
                            session_id=session.session_id,
                            request_id=_interaction.request_id,
                        ))
                    except Exception as _exc:
                        log.warning("[codex-appserver] AskUserQuestion extraction failed: %s", _exc)
                if self._notify_fcm_fn is not None and not client_manager.has_clients():
                    asyncio.create_task(
                        self._notify_fcm_fn(session.name, session.accumulated_text or "", session.session_id)
                    )
                await emit_done(session)
                if (session.context_max
                        and session.context_used >= int(session.context_max * _COMPACT_THRESHOLD)
                        and state.thread_id):
                    asyncio.create_task(self._auto_compact(session, state))

        except Exception as exc:
            if not session.is_stopping:
                await send_event(session, _evt_error(f"codex turn failed: {exc}", "stream_error"))
        finally:
            state.turn_active = False
            session.is_streaming = False
            session.is_stopping = False
            session.accumulated_text = ""
            self._cleanup_temp_images(state)

    async def _auto_compact(self, session: "Session", state: _AppServerState) -> None:
        if not state.thread_id or self._proc is None or self._proc.returncode is not None:
            return
        state.compact_done_event.clear()
        state.compact_in_progress = True
        state.compact_error = None
        session.is_streaming = True  # block new sends during compact

        log.info("[codex-appserver] auto-compact triggered session=%s context=%d/%d",
                 session.session_id[:8], session.context_used, session.context_max)

        if self._broadcast_fn is not None:
            asyncio.create_task(self._broadcast_fn({
                "type": "session_command_started",
                "session_id": session.session_id,
                "request_id": f"compact_{session.session_id}",
                "queue_length": 0,
            }))

        try:
            await self._rpc("thread/compact/start", {"threadId": state.thread_id}, timeout=30.0)

            try:
                await asyncio.wait_for(state.compact_done_event.wait(), timeout=120.0)
            except asyncio.TimeoutError:
                state.compact_error = "compact timed out after 120s"

            if state.compact_error:
                log.warning("[codex-appserver] compact failed session=%s: %s",
                            session.session_id[:8], state.compact_error)
                if self._broadcast_fn is not None:
                    asyncio.create_task(self._broadcast_fn({
                        "type": "session_command_failed",
                        "session_id": session.session_id,
                        "request_id": f"compact_{session.session_id}",
                        "error": state.compact_error,
                        "queue_length": 0,
                    }))
            else:
                log.info("[codex-appserver] compact done session=%s", session.session_id[:8])
                if self._broadcast_fn is not None:
                    asyncio.create_task(self._broadcast_fn({
                        "type": "session_command_done",
                        "session_id": session.session_id,
                        "request_id": f"compact_{session.session_id}",
                        "queue_length": 0,
                    }))
        except Exception as exc:
            log.warning("[codex-appserver] compact exception session=%s: %s",
                        session.session_id[:8], exc)
            if self._broadcast_fn is not None:
                asyncio.create_task(self._broadcast_fn({
                    "type": "session_command_failed",
                    "session_id": session.session_id,
                    "request_id": f"compact_{session.session_id}",
                    "error": str(exc),
                    "queue_length": 0,
                }))
        finally:
            state.compact_in_progress = False
            session.is_streaming = False

    async def stop(self, session: "Session") -> None:
        state = self._get_state(session)
        session.is_stopping = True

        if state.thread_id and state.current_turn_id and self._proc and self._proc.returncode is None:
            try:
                await self._rpc("turn/interrupt", {
                    "threadId": state.thread_id,
                    "turnId": state.current_turn_id,
                }, timeout=5.0)
            except Exception:
                pass

        # Signal any waiting _run_turn.
        state.turn_error = "stopped"
        state.turn_done_event.set()
        session.is_streaming = False
        session.accumulated_text = ""
        await send_event(session, _evt_stopped())

    async def clear(self, session: "Session") -> None:
        state = self._get_state(session)
        await self.stop(session)

        # Archive the old thread so history is wiped from the UI.
        if state.thread_id and self._proc and self._proc.returncode is None:
            try:
                await self._rpc("thread/archive", {"threadId": state.thread_id}, timeout=5.0)
            except Exception:
                pass
            self._thread_to_session.pop(state.thread_id, None)

        # Reset state — a new thread will be created on next send.
        state.thread_id = None
        state.last_usage = {}
        state.tool_outputs.clear()
        session.resume_id = None
        state.turn_done_event.clear()
        await send_event(session, _evt_session_warning("Session history cleared."))

    async def close(self, session: "Session") -> None:
        await self.stop(session)
        state = self._states.pop(session.session_id, None)
        if state and state.thread_id:
            self._thread_to_session.pop(state.thread_id, None)
        await send_event(session, _evt_session_closed())

    def get_pid(self, session: "Session") -> "int | None":
        if self._proc and self._proc.returncode is None:
            return self._proc.pid
        return None

    def kill_session_proc(self, session: "Session") -> bool:
        if self._proc and self._proc.returncode is None:
            self._proc.terminate()
            return True
        return False

    def find_session_file(self, resume_id: str) -> "str | None":
        path = self._find_native_session_file(resume_id)
        return path if path else None

    _TURN_END_STOP_REASONS = frozenset({"end_turn", "max_tokens", "stop_sequence"})

    def detect_turn_end(self, lines: list) -> bool:
        for d in lines:
            if d.get("type") != "response_item":
                continue
            payload = d.get("payload", {})
            if not isinstance(payload, dict):
                continue
            if (payload.get("role") == "assistant"
                    and payload.get("stop_reason") in self._TURN_END_STOP_REASONS):
                return True
        return False

    def supports_resume(self) -> bool:
        return True

    @staticmethod
    def _sandbox_mode(session: "Session") -> str:
        if session.sandbox in {"read-only", "workspace-write", "danger-full-access"}:
            return session.sandbox
        return "danger-full-access"

    async def get_resumable_sessions(self, limit: int = 100) -> list[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._load_native_codex_sessions, limit)

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
            self._load_native_session_history,
            resume_id,
            limit,
            known_last_source_message_id,
            mode,
            before_source_message_id,
        )

    async def warmup_history_cache(self, max_sessions: int = 30) -> None:
        """Bridge 啟動後在背景預建近期 session 的 history index。"""
        try:
            sessions = await self.get_resumable_sessions(limit=max_sessions)
        except Exception:
            return
        loop = asyncio.get_event_loop()
        warmed = 0
        for info in sessions:
            resume_id = info.get("resume_id") or ""
            if not resume_id or f"codex:{resume_id}" in _JSONL_HISTORY_CACHE:
                continue
            try:
                await loop.run_in_executor(
                    None,
                    lambda rid=resume_id: self._load_native_session_history(rid, DEFAULT_HISTORY_LIMIT, "", "snapshot"),
                )
                warmed += 1
                await asyncio.sleep(0.01)
            except Exception:
                pass
        if warmed:
            log.info("codex-appserver warmup_history_cache: pre-built index for %d sessions", warmed)

    async def fetch_usage(self, ws) -> None:
        from datetime import datetime, timezone as _tz

        rpc_rate_limits: dict = {}
        try:
            await self._ensure_server()
            result = await self._rpc("account/rateLimits/read", None, timeout=10.0)
            rpc_rate_limits = result.get("rateLimits") or result.get("rate_limits") or {}
            if isinstance(rpc_rate_limits, dict):
                for state in self._states.values():
                    state.last_rate_limits = rpc_rate_limits
        except Exception as exc:
            log.debug("[codex-appserver] account/rateLimits/read failed: %s", exc)

        # Use fresh RPC result; fall back to cached state only if RPC failed
        rate_limits: dict = rpc_rate_limits
        if not rate_limits:
            latest: _AppServerState | None = None
            for state in self._states.values():
                if latest is None or state.usage_updated_at > latest.usage_updated_at:
                    latest = state
            rate_limits = latest.last_rate_limits if latest else {}

        def fmt_window(window: dict | None) -> dict | None:
            if not isinstance(window, dict):
                return None
            used_pct = window.get("usedPercent")
            if used_pct is None:
                used_pct = window.get("used_percent")
            utilization = None
            if used_pct is not None:
                try:
                    utilization = (100.0 - float(used_pct)) / 100.0
                except (TypeError, ValueError):
                    utilization = None
            resets_at = window.get("resetsAt") or window.get("resets_at")
            if isinstance(resets_at, (int, float)) and resets_at > 1e9:
                resets_at = datetime.fromtimestamp(resets_at, tz=_tz.utc).isoformat()
            return {
                "utilization": utilization,
                "resets_at": str(resets_at) if resets_at is not None else None,
            }
        five_hour = fmt_window(rate_limits.get("primary") if isinstance(rate_limits, dict) else None)
        seven_day = fmt_window(rate_limits.get("secondary") if isinstance(rate_limits, dict) else None)

        if five_hour is None and latest and latest.last_usage:
            total = latest.last_usage.get("total_tokens", 0)
            if total:
                five_hour = {"utilization": total, "resets_at": None}

        payload = _msg_usage_report(five_hour, seven_day, None)
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            pass

    # ------------------------------------------------------------------ image helpers

    def _prepare_image_input(self, session: "Session", img: dict,
                             state: _AppServerState) -> dict | None:
        media_type = str(img.get("media_type", "image/jpeg"))
        raw_b64 = str(img.get("data", "")).strip()
        if not raw_b64:
            return None
        if "," in raw_b64 and raw_b64.lower().startswith("data:"):
            raw_b64 = raw_b64.split(",", 1)[1]
        try:
            blob = base64.b64decode(raw_b64, validate=False)
        except Exception:
            return None

        ext = _ext_for_media_type(media_type)
        tmp_path = self._write_session_image(session, blob, ext)
        if not tmp_path:
            return None
        state.temp_image_paths.append(tmp_path)
        return {"type": "localImage", "path": tmp_path}

    @staticmethod
    def _resolve_image_dir(session: "Session") -> str:
        configured = (session.image_dir or "").strip()
        if configured:
            return os.path.abspath(os.path.expanduser(configured))
        base = session.cwd if os.path.isdir(session.cwd) else os.path.expanduser("~")
        return os.path.join(base, ".bridge_images")

    def _write_session_image(self, session: "Session", blob: bytes, ext: str) -> str | None:
        try:
            root = self._resolve_image_dir(session)
            os.makedirs(root, exist_ok=True)
            request_id = (session.current_request_id or f"r_{uuid.uuid4().hex[:8]}").replace("/", "_")
            filename = f"{session.session_id}_{request_id}_{uuid.uuid4().hex[:8]}{ext}"
            path = os.path.join(root, filename)
            with open(path, "wb") as f:
                f.write(blob)
            return path
        except Exception:
            return None

    @staticmethod
    def _cleanup_temp_images(state: _AppServerState) -> None:
        for p in state.temp_image_paths:
            try:
                os.remove(p)
            except Exception:
                pass
        state.temp_image_paths.clear()

    # ------------------------------------------------------------------ native session helpers

    def _get_session_path_index(self) -> dict[str, str]:
        now = time.time()
        if self._session_path_index is not None and now - self._session_path_index_time < 300.0:
            return self._session_path_index
        index: dict[str, str] = {}
        if os.path.isdir(self._native_sessions_root):
            for root, _dirs, files in os.walk(self._native_sessions_root):
                for fn in files:
                    if fn.endswith(".jsonl"):
                        uid = fn[:-6][-36:]
                        index[uid] = os.path.join(root, fn)
        self._session_path_index = index
        self._session_path_index_time = now
        return index

    def _find_native_session_file(self, session_id: str) -> str:
        if not session_id:
            return ""
        return self._get_session_path_index().get(session_id, "")

    def _read_native_session_cwd(self, session_id: str) -> str:
        path = self._find_native_session_file(session_id)
        if not path:
            return os.path.expanduser("~")
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue
                    data = json.loads(line)
                    if data.get("type") != "session_meta":
                        continue
                    payload = data.get("payload", {})
                    if isinstance(payload, dict):
                        cwd = payload.get("cwd")
                        if isinstance(cwd, str) and cwd.strip():
                            return cwd
                    break
        except Exception:
            pass
        return os.path.expanduser("~")

    @staticmethod
    def _parse_iso_to_epoch(value: str | None) -> int:
        from datetime import datetime, timezone
        if not value:
            return 0
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return 0

    def _load_native_codex_sessions(self, limit: int = 200) -> list[dict]:
        out: list[dict] = []
        seen_ids: set[str] = set()

        # Legacy path: session_index.jsonl written by Codex versions before ~0.128.
        if os.path.isfile(self._native_session_index_path):
            try:
                with open(self._native_session_index_path, "r", encoding="utf-8", errors="ignore") as f:
                    for raw in f:
                        line = raw.strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                        except Exception:
                            continue
                        sid = str(row.get("id") or "").strip()
                        if not sid or sid in seen_ids:
                            continue
                        seen_ids.add(sid)
                        raw_name = str(row.get("thread_name") or sid[:8])
                        out.append({
                            "id": f"native_{sid[:12]}",
                            "name": _sanitize_session_name(raw_name, sid[:8]),
                            "claude_uuid": sid,
                            "resume_id": sid,
                            "last_used": self._parse_iso_to_epoch(str(row.get("updated_at") or "")),
                            "cwd": self._read_native_session_cwd(sid),
                        })
            except Exception:
                pass

        # Newer path: Codex >=0.128 writes rollout-{DATETIME}-{UUID}.jsonl files
        # directly under ~/.codex/sessions/YYYY/MM/DD/ and no longer updates
        # session_index.jsonl.  Scan the directory tree for these files.
        if os.path.isdir(self._native_sessions_root):
            rollout_candidates: list[tuple[str, str, str]] = []
            for root, _dirs, files in os.walk(self._native_sessions_root):
                for fn in files:
                    if not (fn.startswith("rollout-") and fn.endswith(".jsonl")):
                        continue
                    uid = fn[:-6][-36:]  # last 36 chars of stem = UUID
                    if uid in seen_ids:
                        continue
                    rollout_candidates.append((uid, os.path.join(root, fn), fn))

            # Sort by filename descending (filename encodes the creation timestamp).
            rollout_candidates.sort(key=lambda x: x[2], reverse=True)

            for uid, filepath, fn in rollout_candidates:
                if uid in seen_ids:
                    continue
                # Parse timestamp from filename:
                # rollout-2026-05-03T22-59-01-<UUID>.jsonl
                last_used = 0
                try:
                    date_part = fn[8:18]           # "2026-05-03"
                    time_part = fn[19:27].replace("-", ":")  # "22:59:01"
                    last_used = self._parse_iso_to_epoch(f"{date_part}T{time_part}Z")
                except Exception:
                    pass

                cwd = os.path.expanduser("~")
                name = uid[:8]
                try:
                    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                        for raw in f:
                            raw = raw.strip()
                            if not raw:
                                continue
                            try:
                                d = json.loads(raw)
                            except Exception:
                                continue
                            t = d.get("type", "")
                            p = d.get("payload", {})
                            if t == "session_meta" and isinstance(p, dict):
                                cwd = str(p.get("cwd") or cwd)
                            elif t == "event_msg" and isinstance(p, dict) and p.get("type") == "user_message":
                                msg = p.get("message", "")
                                if isinstance(msg, str) and msg.strip():
                                    name = msg.strip()
                                break
                except Exception:
                    pass

                seen_ids.add(uid)
                out.append({
                    "id": f"native_{uid[:12]}",
                    "name": _sanitize_session_name(name, uid[:8]),
                    "claude_uuid": uid,
                    "resume_id": uid,
                    "last_used": last_used,
                    "cwd": cwd,
                })

        out.sort(key=lambda x: x.get("last_used", 0), reverse=True)
        return out[:limit]

    def _extract_text_from_content(self, content: object) -> str:
        if isinstance(content, str):
            text = content
        elif isinstance(content, list):
            chunks: list[str] = []
            for item in content:
                if not isinstance(item, dict):
                    continue
                txt = item.get("text")
                if isinstance(txt, str) and txt.strip():
                    chunks.append(txt)
            text = "\n".join(chunks).strip()
        else:
            return ""
        text = _CODEX_WRAPPER_CLOSED_RE.sub("", text)
        return text.strip()

    @staticmethod
    def _is_codex_bootstrap_text(text: str) -> bool:
        stripped = text.lstrip()
        return (
            stripped.startswith("# AGENTS.md instructions")
            and "<environment_context>" in stripped
            and "<INSTRUCTIONS>" in stripped
        )

    def _load_native_session_history(
        self,
        resume_id: str,
        limit: int = 120,
        known_last_source_message_id: str = "",
        mode: str = "snapshot",
        before_source_message_id: str = "",
    ) -> list[dict] | dict:
        path = self._find_native_session_file(resume_id)
        if not path or not os.path.isfile(path):
            return []
        def parse(row: dict, line_no: int, offset: int) -> dict | None:
            if row.get("type") != "response_item":
                return None
            payload = row.get("payload", {})
            if not isinstance(payload, dict) or payload.get("type") != "message":
                return None
            role = payload.get("role")
            if role not in {"user", "assistant"}:
                return None
            if role == "assistant" and payload.get("phase") == "commentary":
                return None
            text = self._extract_text_from_content(payload.get("content"))
            if not text:
                return None
            if role == "user" and self._is_codex_bootstrap_text(text):
                return None
            ts = self._parse_iso_to_epoch(str(row.get("timestamp") or "")) * 1000
            return complete_history_message(
                source="codex",
                source_session_id=resume_id,
                source_message_id=f"codex:{resume_id}:line:{line_no}",
                role=role,
                content=text,
                timestamp=ts or None,
                blocks=[{"type": "text", "text": text}],
            )
        try:
            index = load_indexed_jsonl_messages(cache_name=f"codex:{resume_id}", path=path, parse_line=parse)
        except Exception:
            return []
        return slice_history(
            index.messages,
            limit=clamp_history_limit(limit),
            known_last_source_message_id=known_last_source_message_id,
            mode=mode,
            before_source_message_id=before_source_message_id,
        )


# ------------------------------------------------------------------ module helpers

def _ext_for_media_type(media_type: str) -> str:
    mt = media_type.lower()
    if "png" in mt:
        return ".png"
    if "webp" in mt:
        return ".webp"
    if "gif" in mt:
        return ".gif"
    return ".jpg"


def _sanitize_session_name(raw: str, fallback: str) -> str:
    s = "".join(ch for ch in (raw or "") if ch.isprintable())
    s = " ".join(s.split())
    spill_markers = [" Wait ", " needs ", " no quotes", "----", "{\"", "\"}"]
    for marker in spill_markers:
        idx = s.find(marker)
        if idx > 0:
            s = s[:idx].strip()
            break
    s = s.strip("`'\"[]{}()<>")
    if not s:
        return fallback
    if len(s) > 80:
        s = s[:80].rstrip()
    return s or fallback
