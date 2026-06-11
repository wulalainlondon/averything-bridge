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
    send_event, stream_text,
    _evt_session_warning, _evt_session_closed,
    _evt_thinking_chunk,
    _evt_todo_update, _msg_session_uuid, _msg_usage_report,
)
from .todo_state import normalize_full_list
from .turn_lifecycle import emit_turn_done, emit_turn_error, emit_turn_stopped, settle_turn_state
from .history import complete_history_message, clamp_history_limit, load_indexed_jsonl_messages, slice_history, _JSONL_HISTORY_CACHE, DEFAULT_HISTORY_LIMIT
from interactions import REGISTRY as INTERACTIONS, normalize_questions
from push_registry import notify_fcm_user_input as _notify_fcm_user_input
import client_manager
import task_manager
from .codex_common import _AppServerState
from .codex_native import _CodexNativeSessionMixin
from .codex_images import _CodexImageMixin
from .codex_tools import normalize_codex_live_tool
if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")

_STREAM_READER_LIMIT = 128 * 1024 * 1024  # 128 MiB
_COMPACT_THRESHOLD = 0.80
# Errors that mean the Codex thread no longer exists on the server side
# (e.g. app-server restarted). Detected here so we can respawn silently.
_STALE_THREAD_RE = re.compile(r"(?:Unknown session|thread not found)", re.IGNORECASE)
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


def _request_thread_id(params: dict) -> str:
    thread_id = params.get("threadId") or params.get("thread_id")
    if isinstance(thread_id, str) and thread_id:
        return thread_id
    thread = params.get("thread")
    if isinstance(thread, dict):
        thread_id = thread.get("id")
        if isinstance(thread_id, str) and thread_id:
            return thread_id
    return ""


def _request_item_id(params: dict, fallback: str) -> str:
    item = params.get("item")
    if not isinstance(item, dict):
        item = {}
    value = (
        params.get("itemId")
        or params.get("callId")
        or params.get("toolCallId")
        or params.get("toolUseId")
        or item.get("id")
        or fallback
    )
    return str(value)


def _request_tool_name(params: dict) -> str:
    tool = params.get("tool")
    if isinstance(tool, dict):
        value = tool.get("name") or tool.get("type")
        if value:
            return str(value)
    item = params.get("item")
    if isinstance(item, dict):
        value = item.get("name") or item.get("type")
        if value:
            return str(value)
    return str(params.get("name") or params.get("toolName") or "codex_tool")


def _response_answers(response: dict) -> dict:
    answers = response.get("answers") if isinstance(response.get("answers"), dict) else {}
    if answers:
        return answers
    return {
        k: v for k, v in response.items()
        if k not in {"type", "response_type", "request_id", "session_id", "cancelled", "canceled"}
    }


class CodexAppServerBackend(Backend, _StatesMixin, _CodexNativeSessionMixin, _CodexImageMixin):
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
        # Per-file cache for _load_native_codex_sessions: path -> (key, cwd, name)
        # where key = (st_mtime_ns, st_size). Avoids re-opening every rollout file
        # (1000s of them, GBs) on each resumable-sessions poll, which otherwise
        # saturates the executor and starves WS keepalive pings.
        self._codex_scan_cache: dict[str, tuple] = {}

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
            self._invalidate_live_threads()
            # Mark proc gone so next call restarts it
            self._proc = None

    def _invalidate_live_threads(self) -> None:
        """Drop live thread routing after the singleton app-server exits."""
        self._thread_to_session.clear()
        for state in self._states.values():
            state.thread_id = None
            state.current_turn_id = None

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
                    if state:
                        await state.tool_lifecycle.result(session, item_id, output)
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
                    if state:
                        await state.tool_lifecycle.result(session, item_id, output)
                except Exception:
                    pass

        elif method == "item/started" and session:
            tool = normalize_codex_live_tool(params)
            if not tool:
                return
            try:
                state = self._states.get(session.session_id)
                if state:
                    state.tool_outputs[tool.tool_use_id] = ""
                if state:
                    await state.tool_lifecycle.start(session, tool.tool_use_id, tool.name, tool.command)
            except Exception:
                pass

        elif method == "item/completed" and session:
            item = params.get("item", {}) if isinstance(params.get("item"), dict) else {}
            item_id = str(params.get("itemId") or item.get("id") or "codex_item")
            output = params.get("output") or item.get("output") or item.get("result") or ""
            try:
                state = self._states.get(session.session_id)
                if state and output:
                    if state:
                        state.tool_outputs[item_id] = str(output)
                    await state.tool_lifecycle.result(session, item_id, str(output))
                if state:
                    await state.tool_lifecycle.end(session, item_id)
                if state:
                    state.tool_outputs.pop(item_id, None)
            except Exception:
                pass

        elif method == "turn/plan/updated" and session:
            # Codex update_plan → normalized todo panel. Full replace; step→content.
            todos = normalize_full_list(params.get("plan"), content_key="step")
            try:
                await send_event(session, _evt_todo_update(todos))
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
                    if state:
                        await state.tool_lifecycle.result(session, item_id, output)
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
        thread_id = _request_thread_id(params)
        session = self._thread_to_session.get(thread_id) if thread_id else None
        if method in approval_methods:
            await self._emit_approval_event(session, method, params)
            result = {"decision": "accept"}
        elif method == "item/tool/requestUserInput":
            if session is None:
                result = {"answers": []}
            else:
                await self._create_jsonrpc_user_input_request(request_id, session, params)
                return
        elif method == "item/tool/call":
            await self._emit_unsupported_tool_call(session, params)
            await self._rpc_plumber.write(self._proc, {
                "id": request_id,
                "error": {
                    "code": -32000,
                    "message": (
                        f"Codex hosted tool '{_request_tool_name(params)}' is not "
                        "implemented by claude-bridge"
                    ),
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

    async def _emit_approval_event(self, session, method: str, params: dict) -> None:
        environment_id = str(params.get("environmentId") or params.get("environment_id") or "")
        item_id = _request_item_id(params, f"codex_approval_{uuid.uuid4().hex[:8]}")
        command = params.get("command") or params.get("changes") or params.get("permission") or ""
        if isinstance(command, (dict, list)):
            command = json.dumps(command, ensure_ascii=False)
        summary = {
            "method": method,
            "environmentId": environment_id,
            "cwd": params.get("cwd") or params.get("workingDirectory") or "",
        }
        log.info("[codex-appserver] approval accepted: %s", summary)
        if session is None:
            return
        try:
            state = self._get_state(session)
            await state.tool_lifecycle.start(session, item_id, "codex_approval", str(command))
            await state.tool_lifecycle.result(session, item_id, json.dumps(summary, ensure_ascii=False))
            await state.tool_lifecycle.end(session, item_id)
        except Exception:
            pass

    async def _emit_unsupported_tool_call(self, session, params: dict) -> None:
        tool_name = _request_tool_name(params)
        item_id = _request_item_id(params, f"codex_tool_{uuid.uuid4().hex[:8]}")
        raw_input = params.get("input") or params.get("arguments") or params.get("args") or {}
        command = json.dumps(raw_input, ensure_ascii=False) if isinstance(raw_input, (dict, list)) else str(raw_input)
        log.warning("[codex-appserver] unsupported hosted tool call: %s", tool_name)
        if session is None:
            return
        try:
            state = self._get_state(session)
            await state.tool_lifecycle.start(session, item_id, tool_name, command)
            await state.tool_lifecycle.result(session, item_id, f"Unsupported Codex hosted tool: {tool_name}")
            await state.tool_lifecycle.end(session, item_id)
        except Exception:
            pass

    async def _create_jsonrpc_user_input_request(self, request_id, session: "Session", params: dict) -> None:
        command = dict(params)
        raw_questions = (
            params.get("questions")
            or params.get("input")
            or params.get("prompt")
            or params.get("message")
            or params.get("question")
            or params
        )
        if isinstance(raw_questions, dict):
            question_command = raw_questions
        elif isinstance(raw_questions, list):
            question_command = {"questions": raw_questions}
        else:
            question_command = {"questions": [{"text": str(raw_questions or "Codex needs input.")}]}
        questions = normalize_questions(question_command)
        header = str(params.get("header") or params.get("title") or "Codex needs input")
        tool_use_id = _request_item_id(params, str(request_id))

        async def _resolve_jsonrpc(interaction, response: dict) -> None:
            payload = {
                "answers": _response_answers(response),
                "cancelled": bool(response.get("cancelled") or response.get("canceled")),
            }
            await self._rpc_plumber.write(self._proc, {"id": request_id, "result": payload})

        await INTERACTIONS.create(
            session_id=session.session_id,
            source="codex",
            kind="request_user_input",
            questions=questions,
            header=header,
            tool_use_id=tool_use_id,
            requesting_agent=str(params.get("requestingAgent") or params.get("requesting_agent") or "Codex"),
            raw_command={**command, "codex_jsonrpc_request_id": request_id},
            resolve_callback=_resolve_jsonrpc,
            broadcast_json=self._broadcast_fn,
        )

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
                }, timeout=30.0)
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
            }, timeout=30.0)
            thread = result.get("thread", {})
            state.thread_id = thread.get("id")
            if not state.thread_id:
                raise RuntimeError("codex thread/start returned no thread id")

        # Register for notification routing.
        self._thread_to_session[state.thread_id] = session

        # Emit the native thread ID as the session UUID.
        if session.ws_ref:
            await client_manager.send_json(
                session.ws_ref,
                _msg_session_uuid(session.session_id, state.thread_id),
            )
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
            task_manager.spawn(
                f"codex-auto-compact:{session.session_id}",
                self._auto_compact(session, state),
                owner=f"session:{session.session_id}",
            )
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
                await emit_turn_error(session, f"Failed to start codex thread: {exc}", "spawn_failed")
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

        task_manager.spawn(
            f"codex-turn:{session.session_id}",
            self._run_turn(session, state, user_input),
            owner=f"session:{session.session_id}",
        )

    async def handle_user_input_response(self, session: "Session", interaction, response: dict) -> None:
        raw_command = getattr(interaction, "raw_command", None)
        if isinstance(raw_command, dict) and raw_command.get("codex_jsonrpc_request_id") is not None:
            return
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
                await emit_turn_stopped(session, tool_lifecycle=state.tool_lifecycle)
            elif state.turn_error:
                log.warning("[codex-appserver] turn error: %s", state.turn_error)
                await emit_turn_error(
                    session,
                    state.turn_error,
                    "turn_error",
                    tool_lifecycle=state.tool_lifecycle,
                )
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
                        task_manager.spawn(
                            f"codex-fcm-user-input:{session.session_id}",
                            _notify_fcm_user_input(
                                session_name=session.name,
                                header=_header,
                                question_text=_questions[0].get("text", "") if _questions else "",
                                session_id=session.session_id,
                                request_id=_interaction.request_id,
                            ),
                            owner=f"session:{session.session_id}",
                        )
                    except Exception as _exc:
                        log.warning("[codex-appserver] AskUserQuestion extraction failed: %s", _exc)
                if self._notify_fcm_fn is not None and not client_manager.has_clients():
                    task_manager.spawn(
                        f"codex-fcm-done:{session.session_id}",
                        self._notify_fcm_fn(session.name, session.accumulated_text or "", session.session_id),
                        owner=f"session:{session.session_id}",
                    )
                await emit_turn_done(session, tool_lifecycle=state.tool_lifecycle)
                if session.ws_ref is not None:
                    task_manager.spawn(
                        f"codex-fetch-usage:{session.session_id}",
                        self.fetch_usage(session.ws_ref),
                        owner=f"session:{session.session_id}",
                    )
                if (session.context_max
                        and session.context_used >= int(session.context_max * _COMPACT_THRESHOLD)
                        and state.thread_id):
                    task_manager.spawn(
                        f"codex-auto-compact:{session.session_id}",
                        self._auto_compact(session, state),
                        owner=f"session:{session.session_id}",
                    )

        except Exception as exc:
            if not session.is_stopping:
                await emit_turn_error(
                    session,
                    f"codex turn failed: {exc}",
                    "stream_error",
                    tool_lifecycle=state.tool_lifecycle,
                )
        finally:
            state.turn_active = False
            settle_turn_state(session, clear_accumulated=True, clear_stopping=True)
            self._cleanup_temp_images(state)

    async def _auto_compact(self, session: "Session", state: _AppServerState) -> None:
        if not state.thread_id or self._proc is None or self._proc.returncode is not None:
            settle_turn_state(session, clear_accumulated=False)
            return
        state.compact_done_event.clear()
        state.compact_in_progress = True
        state.compact_error = None
        session.is_streaming = True  # block new sends during compact

        log.info("[codex-appserver] auto-compact triggered session=%s context=%d/%d",
                 session.session_id[:8], session.context_used, session.context_max)

        if self._broadcast_fn is not None:
            task_manager.spawn(
                f"codex-compact-started:{session.session_id}",
                self._broadcast_fn({
                    "type": "session_command_started",
                    "session_id": session.session_id,
                    "request_id": f"compact_{session.session_id}",
                    "queue_length": 0,
                }),
                owner=f"session:{session.session_id}",
            )

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
                    task_manager.spawn(
                        f"codex-compact-failed:{session.session_id}",
                        self._broadcast_fn({
                            "type": "session_command_failed",
                            "session_id": session.session_id,
                            "request_id": f"compact_{session.session_id}",
                            "error": state.compact_error,
                            "queue_length": 0,
                        }),
                        owner=f"session:{session.session_id}",
                    )
            else:
                log.info("[codex-appserver] compact done session=%s", session.session_id[:8])
                if self._broadcast_fn is not None:
                    task_manager.spawn(
                        f"codex-compact-done:{session.session_id}",
                        self._broadcast_fn({
                            "type": "session_command_done",
                            "session_id": session.session_id,
                            "request_id": f"compact_{session.session_id}",
                            "queue_length": 0,
                        }),
                        owner=f"session:{session.session_id}",
                    )
        except Exception as exc:
            log.warning("[codex-appserver] compact exception session=%s: %s",
                        session.session_id[:8], exc)
            if self._broadcast_fn is not None:
                task_manager.spawn(
                    f"codex-compact-exception:{session.session_id}",
                    self._broadcast_fn({
                        "type": "session_command_failed",
                        "session_id": session.session_id,
                        "request_id": f"compact_{session.session_id}",
                        "error": str(exc),
                        "queue_length": 0,
                    }),
                    owner=f"session:{session.session_id}",
                )
        finally:
            state.compact_in_progress = False
            settle_turn_state(session, clear_accumulated=False)

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
        await emit_turn_stopped(session, tool_lifecycle=state.tool_lifecycle)

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
        state.tool_lifecycle.clear()
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
            self._invalidate_live_threads()
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
                    utilization = float(used_pct) / 100.0
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
        await client_manager.send_json(ws, payload)
