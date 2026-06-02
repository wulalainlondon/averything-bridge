"""
Claude CLI backend — wraps the claude subprocess and handles NDJSON streaming.
All Claude-specific state is managed here; Session only carries generic fields.
"""

import asyncio
import datetime
import json
import logging
import os
import signal
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional, TYPE_CHECKING

from .base import Backend, _StatesMixin
from utils.uuid_helper import is_valid_uuid
from .events import (
    send_event, stream_text, emit_done,
    _evt_error, _evt_stopped, _evt_done,
    _evt_tool_start, _evt_tool_result, _evt_tool_end, _evt_thinking_chunk,
    _evt_session_warning, _evt_session_died, _evt_session_closed,
    _msg_session_uuid, _msg_usage_report, _msg_error,
)
from .history import (
    complete_history_message, clamp_history_limit, slice_history,
    _JSONL_HISTORY_CACHE, _file_cache_key, HistoryIndex,
    HISTORY_INDEX_TTL_SECONDS, DEFAULT_HISTORY_LIMIT,
)
from .history_sqlite import sqlite_load, sqlite_save_background
from interactions import REGISTRY as INTERACTIONS, normalize_questions
from push_registry import notify_fcm_user_input as _notify_fcm_user_input
from push_registry import notify_fcm_session_died as _notify_fcm_session_died
import client_manager
from .claude_common import _ClaudeState, _get_context_limit
from .claude_history import _ClaudeHistoryMixin
from .claude_stream import _ClaudeProcessMixin
from .claude_interactions import _ClaudeUserInputMixin

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")

class ClaudeCliBackend(_StatesMixin, _ClaudeHistoryMixin, _ClaudeProcessMixin, _ClaudeUserInputMixin, Backend):
    def __init__(
        self,
        claude_bin: str = "",
        bun_bin: str = "bun",
        notify_fcm_fn: "Callable[[str, str, str], Coroutine] | None" = None,
        persist_session_fn: "Callable[[Session], None] | None" = None,
        claude_projects_dir: str = "",
        load_saved_sessions_fn: "Callable[[], dict] | None" = None,
        broadcast_fn: "Callable[[dict], Coroutine] | None" = None,
    ) -> None:
        self._claude_bin = claude_bin
        self._bun_bin = bun_bin
        self._notify_fcm_fn = notify_fcm_fn
        self._persist_session_fn = persist_session_fn
        self._claude_projects_dir = claude_projects_dir
        self._load_saved_sessions_fn = load_saved_sessions_fn
        self._broadcast_fn = broadcast_fn
        self._states: dict[str, _ClaudeState] = {}
        # Per-file cache for _scan_local_sessions_sync: path -> (key, file_cwd, content_name)
        # where key = (st_mtime_ns, st_size). Lets the resumable-sessions scan skip
        # re-reading unchanged .jsonl files — otherwise every call re-parses thousands
        # of files / GBs, blocking the executor and starving WS keepalive pings.
        self._scan_file_cache: dict[str, tuple] = {}

    def _state_factory(self) -> _ClaudeState:
        return _ClaudeState()

    # ------------------------------------------------------------------
    # Public Backend interface
    # ------------------------------------------------------------------

    async def spawn(self, session: "Session") -> None:
        await self._spawn_proc(session)

    async def send(self, session: "Session", content: str,
                   images: list | None = None, files: list | None = None) -> None:
        if not await self._begin_send(session):
            return

        state = self._get_state(session)

        if state.proc is None or state.proc.returncode is not None:
            # Trigger spawn if nothing is running yet
            if not state.spawning:
                if state.proc_ready_event is None:
                    state.proc_ready_event = asyncio.Event()
                else:
                    state.proc_ready_event.clear()
                asyncio.create_task(self._spawn_proc(session))
            elif state.proc_ready_event is None:
                state.proc_ready_event = asyncio.Event()
            # Wait up to 30s for the process to become ready
            try:
                await asyncio.wait_for(state.proc_ready_event.wait(), timeout=30.0)
            except asyncio.TimeoutError:
                pass
            if state.proc is None or state.proc.returncode is not None:
                session.is_streaming = False
                await send_event(session, _evt_error("Claude process failed to start.", "session_dead"))
                return

        state.tool_blocks = {}

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

        if content.strip() == "/compact" and not state.compact_in_progress:
            state.compact_in_progress = True
            log.info("[%s] user-triggered /compact, setting compact_in_progress", session.session_id)
            # Mirror auto-compact: tell the frontend a compact turn started so it renders
            # the CompactingBanner instead of an ordinary user/assistant exchange. The
            # matching session_command_done is broadcast from the result handler below.
            if self._broadcast_fn is not None:
                asyncio.create_task(self._broadcast_fn({
                    "type": "session_command_started",
                    "session_id": session.session_id,
                    "request_id": f"compact_{session.session_id}",
                    "queue_length": 0,
                }))

        try:
            state.proc.stdin.write(payload.encode("utf-8"))
            await state.proc.stdin.drain()
            log.info("[%s] Message sent (%d chars, %d images)", session.session_id, len(content), len(images or []))
        except Exception as exc:
            session.is_streaming = False
            log.error("[%s] Failed to write to stdin: %s", session.session_id, exc)
            await send_event(session, _evt_error(f"stdin write failed: {exc}"))
            return

        if state.timeout_task and not state.timeout_task.done():
            state.timeout_task.cancel()
        state.timeout_task = asyncio.create_task(self._idle_watchdog(session))

        if state.tree_poll_task and not state.tree_poll_task.done():
            state.tree_poll_task.cancel()
        state.tree_poll_task = asyncio.create_task(self._agent_tree_poller(session))

    async def stop(self, session: "Session") -> None:
        state = self._get_state(session)

        if state.proc is None or state.proc.returncode is not None:
            await send_event(session, _evt_stopped())
            return

        session.is_stopping = True
        if state.timeout_task and not state.timeout_task.done():
            state.timeout_task.cancel()
        log.info("[%s] Stopping session (pid=%d)", session.session_id, state.proc.pid)

        # Resolve any dangling AskUserQuestion tool_use BEFORE killing the
        # process, so the session JSONL doesn't keep an orphan tool_use that
        # would poison the next --resume.
        if state.tool_waiting_events:
            await self._cancel_pending_user_input(session)

        try:
            state.proc.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            pass

        await asyncio.sleep(1)

        try:
            if state.proc.returncode is None:
                state.proc.kill()
        except ProcessLookupError:
            pass

        session.is_streaming = False
        if state.tree_poll_task and not state.tree_poll_task.done():
            state.tree_poll_task.cancel()
            state.tree_poll_task = None
        session.accumulated_text = ""
        state.tool_blocks = {}
        for ev in state.tool_waiting_events.values():
            ev.set()
        state.tool_waiting_events.clear()
        state.tool_waiting_interactions.clear()
        await send_event(session, _evt_stopped())
        await self._spawn_proc(session)

    async def clear(self, session: "Session") -> None:
        state = self._get_state(session)

        log.info("[%s] Clearing session history", session.session_id)
        if state.timeout_task and not state.timeout_task.done():
            state.timeout_task.cancel()
        session.is_stopping = True
        session.resume_id = None
        session.accumulated_text = ""
        state.tool_blocks = {}
        for ev in state.tool_waiting_events.values():
            ev.set()
        state.tool_waiting_events.clear()
        state.tool_waiting_interactions.clear()
        session.is_streaming = False
        if state.tree_poll_task and not state.tree_poll_task.done():
            state.tree_poll_task.cancel()
            state.tree_poll_task = None

        if state.proc is not None and state.proc.returncode is None:
            try:
                state.proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(state.proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                try:
                    state.proc.kill()
                except ProcessLookupError:
                    pass

        state.restart_count = 0
        await self._spawn_proc(session)
        await send_event(session, _evt_session_warning("Session history cleared."))

    async def close(self, session: "Session") -> None:
        state = self._get_state(session)

        session.is_stopping = True
        if state.timeout_task and not state.timeout_task.done():
            state.timeout_task.cancel()
        log.info("[%s] Closing session", session.session_id)

        if state.proc is not None and state.proc.returncode is None:
            try:
                state.proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(state.proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                try:
                    state.proc.kill()
                except ProcessLookupError:
                    pass

        for task in (state.stdout_task, state.stderr_task, state.watch_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

        if state.tree_poll_task and not state.tree_poll_task.done():
            state.tree_poll_task.cancel()
            try:
                await state.tree_poll_task
            except (asyncio.CancelledError, Exception):
                pass

        # Removal from _SESSIONS is the bridge handler's responsibility
        self._states.pop(session.session_id, None)
        await send_event(session, _evt_session_closed())

    # ------------------------------------------------------------------
    # Task management helpers (used by get_tasks / kill_task handlers)
    # ------------------------------------------------------------------

    def get_pid(self, session: "Session") -> "int | None":
        state = self._states.get(session.session_id)
        if state and state.proc:
            return state.proc.pid
        return None

    def kill_session_proc(self, session: "Session") -> bool:
        state = self._states.get(session.session_id)
        if state and state.proc and state.proc.returncode is None:
            state.proc.terminate()
            return True
        return False

    _TURN_END_STOP_REASONS = frozenset({"end_turn", "max_tokens", "stop_sequence"})

    def detect_turn_end(self, lines: list) -> bool:
        for d in lines:
            if (d.get("type") == "assistant"
                    and not d.get("isSidechain")
                    and d.get("message", {}).get("stop_reason") in self._TURN_END_STOP_REASONS):
                return True
        return False

    # ------------------------------------------------------------------
    # Resume / usage capabilities (Claude-specific overrides)
    # ------------------------------------------------------------------

    def supports_resume(self) -> bool:
        return True

    async def fetch_usage(self, ws: Any) -> None:
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
        try:
            proc = await asyncio.create_subprocess_exec(
                self._bun_bin, "-e", _BUN_USAGE_SCRIPT,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            data = json.loads(stdout.decode().strip())

            def fmt(entry):
                if not entry:
                    return None
                util = entry.get("utilization")
                if util is not None:
                    try:
                        util = float(util) / 100.0
                    except (TypeError, ValueError):
                        util = None
                return {"utilization": util, "resets_at": entry.get("resets_at")}

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
