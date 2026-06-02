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

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")

# ---------------------------------------------------------------------------
# Agent-tree mtime-based result cache
# ---------------------------------------------------------------------------
@dataclass
class _AgentTreeCacheEntry:
    main_mtime_ns: int
    agent_mtimes: dict  # agent_id → mtime_ns
    result: dict        # the full return value of _build_agent_tree_sync

_AGENT_TREE_CACHE: OrderedDict = OrderedDict()   # key=main_jsonl_path → _AgentTreeCacheEntry
_AGENT_TREE_CACHE_LOCK: threading.Lock = threading.Lock()
_AGENT_TREE_CACHE_MAX = 200
# ---------------------------------------------------------------------------

TOOL_IDLE_TIMEOUT_SECS = 6000  # kill claude if no stdout for this many seconds (was 300)

# Upper bound on how long the stdout reader will pause waiting for an
# AskUserQuestion answer.  Generous (a human is in the loop) but bounded so a
# never-answered question can't pin the reader — and the turn — forever.  On
# timeout the dangling tool_use is cancelled so --resume history stays clean.
ASK_USER_QUESTION_MAX_WAIT_SECS = 1800  # 30 minutes

_STREAM_READER_LIMIT = 128 * 1024 * 1024  # 128 MiB — matches codex_appserver; prevents LimitOverrunError on large tool outputs

# Compact when context_used exceeds this fraction of the model's context window.
_COMPACT_THRESHOLD = 0.80

# All current Claude models share a 200k input context window.
# Unknown / non-Claude models return 0 (auto-compact disabled).
def _get_context_limit(model: str) -> int:
    m = (model or "").lower()
    if "claude" not in m:
        return 0
    # [1m] suffix or 1m-context variants → 1,000,000 tokens
    if "[1m]" in m or "-1m" in m or "1000000" in m:
        return 1_000_000
    return 200_000


@dataclass
class _ClaudeState:
    proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    stdout_task: Optional[asyncio.Task] = field(default=None, repr=False)
    stderr_task: Optional[asyncio.Task] = field(default=None, repr=False)
    watch_task: Optional[asyncio.Task] = field(default=None, repr=False)
    timeout_task: Optional[asyncio.Task] = field(default=None, repr=False)
    tree_poll_task: Optional[asyncio.Task] = field(default=None, repr=False)
    timed_out: bool = False
    spawning: bool = False
    proc_ready_event: Optional[asyncio.Event] = field(default=None, repr=False)
    tool_blocks: dict = field(default_factory=dict)
    tool_waiting_events: dict = field(default_factory=dict)  # tool_use_id → asyncio.Event
    tool_waiting_interactions: dict = field(default_factory=dict)  # tool_use_id → request_id
    restart_count: int = 0
    pending_stop: bool = False
    bad_resume: bool = False
    compact_in_progress: bool = False


class ClaudeCliBackend(Backend, _StatesMixin):
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

    async def _write_stream_json(self, session: "Session", payload: dict) -> None:
        state = self._get_state(session)
        if state.proc is None or state.proc.returncode is not None or state.proc.stdin is None:
            await self._spawn_proc(session, allow_resume_fallback=True)
        if state.proc is None or state.proc.returncode is not None or state.proc.stdin is None:
            raise RuntimeError("Claude process is not running")
        state.proc.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
        await state.proc.stdin.drain()
        session.last_activity = time.time()

    async def handle_user_input_response(self, session: "Session", interaction: Any, response: dict) -> None:
        answers = response.get("answers") if isinstance(response.get("answers"), dict) else {}
        if not answers:
            answers = {
                k: v for k, v in response.items()
                if k not in {"type", "response_type", "request_id", "session_id"}
            }
        output = json.dumps({
            "request_id": interaction.request_id,
            "answers": answers,
            "cancelled": bool(response.get("cancelled") or response.get("canceled")),
        }, ensure_ascii=False)
        content_block: dict
        if interaction.tool_use_id:
            content_block = {
                "type": "tool_result",
                "tool_use_id": interaction.tool_use_id,
                "content": output,
            }
        else:
            content_block = {
                "type": "text",
                "text": f"Structured user input response:\n{output}",
            }
        await self._write_stream_json(session, {
            "type": "user",
            "message": {"role": "user", "content": [content_block]},
        })
        session.is_streaming = True
        # Unblock the stdout reader that's waiting for this AskUserQuestion response.
        state = self._get_state(session)
        state.tool_waiting_interactions.pop(interaction.tool_use_id, None)
        ev = state.tool_waiting_events.pop(interaction.tool_use_id, None)
        if ev is not None:
            ev.set()

    # ------------------------------------------------------------------
    # AskUserQuestion blocking helpers
    # ------------------------------------------------------------------

    def has_pending_user_input(self, session: "Session") -> bool:
        """True if the stdout reader is currently paused on an AskUserQuestion."""
        state = self._states.get(session.session_id)
        return bool(state and state.tool_waiting_events)

    @staticmethod
    def _build_tool_result_content(content: str, images: list | None = None,
                                   files: list | None = None) -> Any:
        """Build the `content` field of a tool_result block from a user message.

        Returns a plain string for text-only replies (the simplest valid form),
        or a list of content blocks when images/files are attached.
        """
        blocks: list = []
        if content:
            blocks.append({"type": "text", "text": content})
        for img in (images or []):
            blocks.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": img.get("media_type", "image/jpeg"),
                    "data": img.get("data", ""),
                },
            })
        for f in (files or []):
            name = f.get("name", "file")
            file_content = f.get("content", "")
            media_type = f.get("media_type", "text/plain")
            if media_type == "application/pdf":
                blocks.append({"type": "text", "text": f"[File attached (pdf, omitted from answer): {name}]"})
            else:
                ext = name.rsplit(".", 1)[-1] if "." in name else ""
                fence = f"```{ext}\n{file_content}\n```" if ext else file_content
                blocks.append({"type": "text", "text": f"[File: {name}]\n{fence}"})
        if not blocks:
            return ""
        if len(blocks) == 1 and blocks[0]["type"] == "text":
            return blocks[0]["text"]
        return blocks

    async def handle_message_during_user_input(
        self, session: "Session", content: str,
        images: list | None = None, files: list | None = None,
    ) -> bool:
        """Consume a plain message that arrived while a turn is paused on an
        AskUserQuestion: feed it back as the dangling tool_use's tool_result and
        unblock the stdout reader.  Returns False (message untouched) if nothing
        is actually pending."""
        state = self._get_state(session)
        pending_ids = list(state.tool_waiting_events.keys())
        if not pending_ids:
            return False

        first_content = self._build_tool_result_content(content, images, files)
        blocks: list = []
        for idx, tid in enumerate(pending_ids):
            blocks.append({
                "type": "tool_result",
                "tool_use_id": tid,
                # The first dangling ask receives the user's free-text reply;
                # any others (multiple AskUserQuestion in one turn) are cancelled
                # since a single message can only answer one.
                "content": first_content if idx == 0
                else json.dumps({"cancelled": True}, ensure_ascii=False),
            })

        await self._write_stream_json(session, {
            "type": "user",
            "message": {"role": "user", "content": blocks},
        })
        session.is_streaming = True
        session.last_activity = time.time()

        # Drop the resolved interactions from the registry so they no longer
        # appear as pending (and a stale user_input_response can't double-answer
        # the same tool_use).
        for tid in pending_ids:
            rid = state.tool_waiting_interactions.pop(tid, None)
            if rid:
                try:
                    await INTERACTIONS.resolve(rid)
                except Exception:
                    pass

        # Unblock the stdout reader.
        for tid in pending_ids:
            ev = state.tool_waiting_events.pop(tid, None)
            if ev is not None:
                ev.set()

        log.info("[%s] Plain message consumed as AskUserQuestion answer (%d tool_use resolved)",
                 session.session_id, len(pending_ids))
        return True

    async def _cancel_pending_user_input(self, session: "Session") -> None:
        """Write a {"cancelled": true} tool_result for every dangling
        AskUserQuestion tool_use so the in-flight process can persist a clean
        history (no orphan tool_use to poison --resume), then unblock the reader.

        Writes straight to the live stdin — must NOT respawn — and gives Claude a
        brief beat to flush the tool_result into its JSONL before the caller
        terminates the process."""
        state = self._get_state(session)
        pending_ids = list(state.tool_waiting_events.keys())
        if not pending_ids:
            return

        blocks = [{
            "type": "tool_result",
            "tool_use_id": tid,
            "content": json.dumps({"cancelled": True}, ensure_ascii=False),
        } for tid in pending_ids]

        try:
            if (state.proc is not None and state.proc.returncode is None
                    and state.proc.stdin is not None):
                payload = json.dumps({
                    "type": "user",
                    "message": {"role": "user", "content": blocks},
                }) + "\n"
                state.proc.stdin.write(payload.encode("utf-8"))
                await state.proc.stdin.drain()
                # Let Claude persist the tool_result to its session JSONL.
                await asyncio.sleep(0.3)
        except Exception as exc:
            log.warning("[%s] failed to write cancelled tool_result(s): %s",
                        session.session_id, exc)

        for tid in pending_ids:
            rid = state.tool_waiting_interactions.pop(tid, None)
            if rid:
                try:
                    await INTERACTIONS.resolve(rid)
                except Exception:
                    pass

        for tid in pending_ids:
            ev = state.tool_waiting_events.pop(tid, None)
            if ev is not None:
                ev.set()
        log.info("[%s] Cancelled %d dangling AskUserQuestion tool_use(s)",
                 session.session_id, len(pending_ids))

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

    def find_session_file(self, resume_id: str) -> "str | None":
        return self._find_session_file_sync(resume_id)

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

    async def get_resumable_sessions(self, limit: int = 100) -> list[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._scan_local_sessions_sync, limit)

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
            self._load_session_history_sync,
            resume_id,
            limit,
            known_last_source_message_id,
            mode,
            before_source_message_id,
        )

    # ------------------------------------------------------------------
    # Private sync helpers (Claude session file scanning)
    # ------------------------------------------------------------------

    def _find_session_file_sync(self, uuid: str) -> Optional[str]:
        try:
            for proj in os.scandir(self._claude_projects_dir):
                if not proj.is_dir():
                    continue
                candidate = os.path.join(proj.path, uuid + ".jsonl")
                if os.path.isfile(candidate):
                    return candidate
        except Exception:
            pass
        return None

    def _load_session_history_sync(
        self,
        resume_id: str,
        limit: int = 120,
        known_last_source_message_id: str = "",
        mode: str = "snapshot",
        before_source_message_id: str = "",
    ) -> list | dict:
        path = self._find_session_file_sync(resume_id)
        if not path:
            return []

        import time
        cache_name = f"claude:{resume_id}"
        try:
            cache_key = _file_cache_key(path)
            cached = _JSONL_HISTORY_CACHE.get(cache_name)
            if cached and cached.key == cache_key and time.time() - cached.built_at < HISTORY_INDEX_TTL_SECONDS:
                return slice_history(
                    cached.messages,
                    limit=clamp_history_limit(limit),
                    known_last_source_message_id=known_last_source_message_id,
                    mode=mode,
                    before_source_message_id=before_source_message_id,
                )
        except Exception:
            cache_key = None

        # SQLite persistent cache — survives bridge restarts / Mac sleep-wake.
        # Only reached on memory miss; key (mtime_ns, size) guards staleness.
        if cache_key is not None:
            try:
                cached_messages = sqlite_load(cache_name, cache_key)
                if cached_messages is not None:
                    import time as _time
                    idx = HistoryIndex(
                        key=cache_key,
                        built_at=_time.time(),
                        messages=cached_messages,
                        by_source_id={
                            str(m.get("source_message_id")): i
                            for i, m in enumerate(cached_messages)
                            if m.get("source_message_id")
                        },
                    )
                    _JSONL_HISTORY_CACHE[cache_name] = idx
                    return slice_history(
                        cached_messages,
                        limit=clamp_history_limit(limit),
                        known_last_source_message_id=known_last_source_message_id,
                        mode=mode,
                        before_source_message_id=before_source_message_id,
                    )
            except Exception:
                pass

        _MAX_OUTPUT = 256 * 1024

        def _flatten_tool_result_content(c) -> str:
            if isinstance(c, str):
                return c
            if isinstance(c, list):
                parts = []
                for item in c:
                    if isinstance(item, dict):
                        parts.append(item.get("text", "") or item.get("content", ""))
                    elif isinstance(item, str):
                        parts.append(item)
                return "\n".join(p for p in parts if p)
            return ""

        # Single pass: collect all records, then derive tool_outputs and build messages.
        raw_records: list[tuple[int, dict]] = []  # (line_no, parsed_dict)
        try:
            with open(path, encoding="utf-8", errors="ignore") as f:
                for line_no, raw in enumerate(f, start=1):
                    try:
                        d = json.loads(raw)
                        raw_records.append((line_no, d))
                    except Exception:
                        pass
        except Exception as exc:
            log.warning("Failed to load session history: %s", exc)
            return []

        # Derive tool_use_id -> output mapping (equivalent to former Pass 1)
        tool_outputs: dict = {}
        for _ln, d in raw_records:
            content = d.get("message", {}).get("content", "")
            if not isinstance(content, list):
                continue
            for blk in content:
                if not isinstance(blk, dict):
                    continue
                if blk.get("type") == "tool_result":
                    tid = blk.get("tool_use_id", "")
                    if tid:
                        output = _flatten_tool_result_content(blk.get("content", ""))
                        if len(output) > _MAX_OUTPUT:
                            output = output[:_MAX_OUTPUT] + "\n…(truncated)"
                        tool_outputs[tid] = output

        # Build message list with blocks (equivalent to former Pass 2)
        messages = []
        try:
            file_mtime_ms = int(os.path.getmtime(path) * 1000)
        except Exception:
            file_mtime_ms = int(time.time() * 1000)
        for line_no, d in raw_records:
            try:
                if (
                    d.get("isSidechain")
                    or d.get("type") not in ("user", "assistant")
                    or d.get("isCompactSummary")
                    or d.get("isVisibleInTranscriptOnly")
                ):
                    continue
                role = d["type"]
                content = d.get("message", {}).get("content", "")
                text = ""
                blocks = []
                if isinstance(content, str):
                    text = content
                elif isinstance(content, list):
                    text_parts = []
                    for blk in content:
                        if not isinstance(blk, dict):
                            continue
                        btype = blk.get("type")
                        if btype == "text":
                            t = blk.get("text", "")
                            if t:
                                text_parts.append(t)
                                blocks.append({"type": "text", "text": t})
                        elif btype == "tool_use" and role == "assistant":
                            tid = blk.get("id", "")
                            name = blk.get("name", "")
                            inp = blk.get("input", {})
                            command = inp.get("command", json.dumps(inp)) if isinstance(inp, dict) else json.dumps(inp)
                            output = tool_outputs.get(tid, "")
                            blocks.append({
                                "type": "tool_call",
                                "tool_use_id": tid,
                                "name": name,
                                "command": command,
                                "output": output,
                            })
                    text = "\n".join(text_parts)
                if not text or text.startswith("<") or text.startswith("[Request interrupted"):
                    continue
                # Filter system-injected skill instructions (injected as user text by Claude Code harness).
                if text.startswith("Base directory for this skill:"):
                    continue
                # If no blocks built (e.g. plain-string content), synthesise a text block
                if not blocks:
                    blocks = [{"type": "text", "text": text}]
                ts_ms = 0
                ts_str = d.get("timestamp", "")
                if ts_str:
                    try:
                        ts_ms = int(datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp() * 1000)
                    except Exception:
                        pass
                if not ts_ms:
                    ts_ms = file_mtime_ms
                messages.append(complete_history_message(
                    source="claude",
                    source_session_id=resume_id,
                    source_message_id=f"claude:{resume_id}:line:{line_no}",
                    role=role,
                    content=text,
                    timestamp=ts_ms,
                    blocks=blocks,
                ))
            except Exception:
                pass

        if cache_key is not None:
            import time as _time
            _JSONL_HISTORY_CACHE[cache_name] = HistoryIndex(
                key=cache_key,
                built_at=_time.time(),
                messages=messages,
                by_source_id={
                    str(m.get("source_message_id")): i
                    for i, m in enumerate(messages)
                    if m.get("source_message_id")
                },
            )
            sqlite_save_background(cache_name, cache_key, messages)

        return slice_history(
            messages,
            limit=clamp_history_limit(limit),
            known_last_source_message_id=known_last_source_message_id,
            mode=mode,
            before_source_message_id=before_source_message_id,
        )

    @staticmethod
    def _scan_main_jsonl_once(main_path: str) -> dict:
        """Single-pass scan of a main session JSONL.

        Returns:
            main_prompt_ids: set of all promptIds from user records
            latest_prompt_id: promptId of the most-recent non-sidechain user record
        """
        main_prompt_ids: set = set()
        latest_prompt_id: "str | None" = None
        latest_ts = 0
        try:
            with open(main_path, encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        rec = json.loads(raw)
                    except Exception:
                        continue
                    if rec.get("type") == "user":
                        pid = rec.get("promptId")
                        if pid:
                            main_prompt_ids.add(pid)
                        if not rec.get("isSidechain") and pid:
                            ts_str = rec.get("timestamp", "")
                            ts_ms = 0
                            if ts_str:
                                try:
                                    ts_ms = int(datetime.datetime.fromisoformat(
                                        ts_str.replace("Z", "+00:00")
                                    ).timestamp() * 1000)
                                except Exception:
                                    pass
                            if ts_ms >= latest_ts:
                                latest_ts = ts_ms
                                latest_prompt_id = pid
        except Exception:
            pass
        return {"main_prompt_ids": main_prompt_ids, "latest_prompt_id": latest_prompt_id}

    @staticmethod
    def _scan_agent_jsonl_once(agent_path: str) -> dict:
        """Single-pass scan of a subagent JSONL.

        Returns:
            prompt_ids: set of all promptIds from user records (for parent-linking)
            first_prompt_id: promptId from the first user record
            start_ts, end_ts: epoch-ms timestamps
            description: first 150 chars of first user message text
            tool_calls: list of {name, ts} dicts (capped at 50)
            last_assistant_record: raw dict of the last assistant record
        """
        prompt_ids: set = set()
        first_prompt_id: "str | None" = None
        start_ts: "int | None" = None
        end_ts: "int | None" = None
        description = ""
        tool_calls: list = []
        last_assistant_record: "dict | None" = None
        first_user_found = False

        try:
            with open(agent_path, encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        rec = json.loads(raw)
                    except Exception:
                        continue

                    if start_ts is None:
                        ts_str = rec.get("timestamp", "")
                        if ts_str:
                            try:
                                start_ts = int(datetime.datetime.fromisoformat(
                                    ts_str.replace("Z", "+00:00")
                                ).timestamp() * 1000)
                            except Exception:
                                pass

                    ts_str = rec.get("timestamp", "")
                    if ts_str:
                        try:
                            end_ts = int(datetime.datetime.fromisoformat(
                                ts_str.replace("Z", "+00:00")
                            ).timestamp() * 1000)
                        except Exception:
                            pass

                    rtype = rec.get("type", "")

                    if rtype == "user":
                        pid = rec.get("promptId")
                        if pid:
                            prompt_ids.add(pid)
                        if not first_user_found:
                            first_user_found = True
                            first_prompt_id = pid
                            content = rec.get("message", {}).get("content", "")
                            text = ""
                            if isinstance(content, str):
                                text = content
                            elif isinstance(content, list):
                                for blk in content:
                                    if isinstance(blk, dict) and blk.get("type") == "text":
                                        text = blk.get("text", "")
                                        break
                            description = text[:150]

                    if rtype == "assistant":
                        last_assistant_record = rec
                        if len(tool_calls) < 50:
                            rec_ts_str = rec.get("timestamp", "")
                            rec_ts: "int | None" = None
                            if rec_ts_str:
                                try:
                                    rec_ts = int(datetime.datetime.fromisoformat(
                                        rec_ts_str.replace("Z", "+00:00")
                                    ).timestamp() * 1000)
                                except Exception:
                                    pass
                            content = rec.get("message", {}).get("content", [])
                            if isinstance(content, list):
                                for blk in content:
                                    if isinstance(blk, dict) and blk.get("type") == "tool_use":
                                        if len(tool_calls) < 50:
                                            tool_calls.append({
                                                "name": blk.get("name", ""),
                                                "ts": rec_ts,
                                            })
        except Exception:
            pass

        return {
            "prompt_ids": prompt_ids,
            "first_prompt_id": first_prompt_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "description": description,
            "tool_calls": tool_calls,
            "last_assistant_record": last_assistant_record,
        }

    def _build_agent_tree_sync(self, resume_id: str) -> dict:
        _empty = {"resume_id": resume_id, "total_agents": 0, "tree": []}
        try:
            main_path = self._find_session_file_sync(resume_id)
            if not main_path:
                return _empty
            subagent_dir = os.path.join(os.path.dirname(main_path), resume_id, "subagents")
            if not os.path.isdir(subagent_dir):
                return _empty
        except Exception:
            return _empty

        # --- mtime-based cache check ------------------------------------------
        try:
            main_mtime_ns = os.stat(main_path).st_mtime_ns
        except OSError:
            main_mtime_ns = 0

        agent_mtimes: dict = {}
        try:
            for _e in os.scandir(subagent_dir):
                if _e.name.startswith("agent-") and _e.name.endswith(".jsonl") and _e.is_file():
                    _agent_id = _e.name[len("agent-"):-len(".jsonl")]
                    try:
                        agent_mtimes[_agent_id] = _e.stat().st_mtime_ns
                    except OSError:
                        agent_mtimes[_agent_id] = 0
        except OSError:
            pass

        with _AGENT_TREE_CACHE_LOCK:
            entry = _AGENT_TREE_CACHE.get(main_path)
            if (
                entry is not None
                and entry.main_mtime_ns == main_mtime_ns
                and entry.agent_mtimes == agent_mtimes
            ):
                # Move to end (LRU: most-recently-used stays at tail)
                _AGENT_TREE_CACHE.move_to_end(main_path)
                return entry.result
        # --- end cache check --------------------------------------------------

        # Single scan of main JSONL — covers Steps 2 and the latest-turn filter.
        main_scan = self._scan_main_jsonl_once(main_path)
        main_prompt_ids: set = main_scan["main_prompt_ids"]
        latest_prompt_id: "str | None" = main_scan["latest_prompt_id"]

        # Step 1: scan all agent-{id}.jsonl files in subagent_dir (one pass each).
        agents: dict[str, dict] = {}  # agent_id → node dict (without children yet)
        subagent_prompt_ids: dict[str, str] = {}  # promptId → agent_id (for parent-linking)

        for entry in os.scandir(subagent_dir):
            if not entry.name.endswith(".jsonl") or not entry.is_file():
                continue
            if not entry.name.startswith("agent-"):
                continue
            agent_id = entry.name[len("agent-"):-len(".jsonl")]
            try:
                # Read meta
                meta_path = os.path.join(subagent_dir, f"agent-{agent_id}.meta.json")
                agent_type = "unknown"
                try:
                    with open(meta_path, encoding="utf-8", errors="ignore") as mf:
                        meta = json.loads(mf.read())
                        agent_type = meta.get("agentType", "unknown") or "unknown"
                except Exception:
                    pass

                # Single-pass scan of this agent's JSONL.
                scan = self._scan_agent_jsonl_once(entry.path)

                # Register all this agent's promptIds for parent-linking.
                for pid in scan["prompt_ids"]:
                    subagent_prompt_ids[pid] = agent_id

                # output_preview: last text block of last assistant record
                output_preview = ""
                last_rec = scan["last_assistant_record"]
                if last_rec is not None:
                    content = last_rec.get("message", {}).get("content", [])
                    if isinstance(content, list):
                        last_text = ""
                        for blk in content:
                            if isinstance(blk, dict) and blk.get("type") == "text":
                                last_text = blk.get("text", "")
                        output_preview = last_text[:200]

                start_ts = scan["start_ts"]
                end_ts = scan["end_ts"]
                duration_ms: "int | None" = None
                if start_ts is not None and end_ts is not None:
                    duration_ms = end_ts - start_ts

                agents[agent_id] = {
                    "agent_id": agent_id,
                    "agent_type": agent_type,
                    "description": scan["description"],
                    "prompt_id": scan["first_prompt_id"],
                    "parent_agent_id": None,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "duration_ms": duration_ms,
                    "tool_calls": scan["tool_calls"],
                    "output_preview": output_preview,
                    "children": [],
                }
            except Exception:
                pass

        if not agents:
            return _empty

        # Step 4: determine parent_agent_id for each agent
        for agent_id, node in agents.items():
            fp = node["prompt_id"]
            if fp is None:
                node["parent_agent_id"] = None
            elif fp in main_prompt_ids:
                node["parent_agent_id"] = None
            else:
                parent = subagent_prompt_ids.get(fp)
                # Avoid self-reference
                node["parent_agent_id"] = parent if parent and parent != agent_id else None

        # Step 5: build tree recursively
        root_nodes = []
        children_map: dict[str, list] = {aid: [] for aid in agents}
        for agent_id, node in agents.items():
            p = node["parent_agent_id"]
            if p is not None and p in children_map:
                children_map[p].append(agent_id)
            else:
                root_nodes.append(agent_id)

        # Iterative BFS build to avoid recursion limit and cycle crashes.
        built: dict[str, dict] = {}
        queue = list(root_nodes)
        visited: set[str] = set()
        while queue:
            aid = queue.pop(0)
            if aid in visited:
                continue
            visited.add(aid)
            node = dict(agents[aid])
            node["children"] = []
            built[aid] = node
            queue.extend(c for c in children_map.get(aid, []) if c not in visited)

        # Attach children in reverse BFS order so parents are populated last.
        for aid in reversed(list(visited)):
            node = built[aid]
            node["children"] = [built[c] for c in children_map.get(aid, []) if c in built]

        tree = [built[aid] for aid in root_nodes if aid in built]

        # Filter to latest conversation turn only.
        if latest_prompt_id:
            filtered = [n for n in tree if n.get("prompt_id") == latest_prompt_id]
            if filtered:
                tree = filtered

        result = {
            "resume_id": resume_id,
            "total_agents": len(agents),
            "tree": tree,
        }

        # --- write to cache ---------------------------------------------------
        with _AGENT_TREE_CACHE_LOCK:
            _AGENT_TREE_CACHE[main_path] = _AgentTreeCacheEntry(
                main_mtime_ns=main_mtime_ns,
                agent_mtimes=agent_mtimes,
                result=result,
            )
            _AGENT_TREE_CACHE.move_to_end(main_path)
            # Evict oldest entries beyond the cap
            while len(_AGENT_TREE_CACHE) > _AGENT_TREE_CACHE_MAX:
                _AGENT_TREE_CACHE.popitem(last=False)
        # --- end cache write --------------------------------------------------

        return result

    async def build_agent_tree(self, resume_id: str) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._build_agent_tree_sync, resume_id)

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
            if not resume_id:
                continue
            if f"claude:{resume_id}" in _JSONL_HISTORY_CACHE:
                continue
            try:
                await loop.run_in_executor(
                    None,
                    self._load_session_history_sync,
                    resume_id, DEFAULT_HISTORY_LIMIT, "", "snapshot",
                )
                warmed += 1
                await asyncio.sleep(0.01)
            except Exception:
                pass
        if warmed:
            log.info("warmup_history_cache: pre-built index for %d sessions", warmed)

    @staticmethod
    def _decode_project_path(proj_name: str) -> str:
        """Decode a Claude project directory name back to filesystem path.

        Claude encodes paths by replacing '/' with '-' and prepending '-'.
        Since '-' is ambiguous (separator vs literal hyphen in dir names),
        we use exhaustive search with OS stat checks to find the real path.
        """
        home = os.path.expanduser("~")
        if not proj_name.startswith("-"):
            return home
        atoms = proj_name[1:].split("-")

        def candidates(component: str) -> list[str]:
            variants = [component]
            underscore = component.replace("-", "_")
            if underscore != component:
                variants.append(underscore)
            return variants

        def search(cur: str, idx: int) -> str | None:
            if idx >= len(atoms):
                return cur if os.path.isdir(cur) else None
            component = ""
            for end in range(idx, len(atoms)):
                component = component + ("-" if component else "") + atoms[end]
                for variant in candidates(component):
                    candidate = os.path.join(cur, variant)
                    if os.path.isdir(candidate):
                        result = search(candidate, end + 1)
                        if result is not None:
                            return result
            return None

        return search("/", 0) or home

    _OVERRIDES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "path_overrides.json")

    @staticmethod
    def _load_path_overrides() -> dict[str, str]:
        try:
            with open(ClaudeCliBackend._OVERRIDES_FILE, encoding="utf-8") as f:
                data = json.load(f)
            return {str(k): str(v) for k, v in data.items() if k and v}
        except FileNotFoundError:
            return {}
        except Exception as exc:
            log.warning("path_overrides.json load failed: %s", exc)
            return {}

    def _scan_local_sessions_sync(self, limit: int = 100) -> list:
        sessions = []
        saved_names: dict = {}
        if self._load_saved_sessions_fn is not None:
            # Accept both "resume_id" (new canonical) and legacy "claude_uuid" key.
            saved_names = {
                (v.get("resume_id") or v.get("claude_uuid", "")): v["name"]
                for v in self._load_saved_sessions_fn().values()
                if v.get("resume_id") or v.get("claude_uuid")
            }
        path_overrides = self._load_path_overrides()
        # Build a fresh cache each scan, reusing entries for unchanged files. Reading
        # the old cache via .get() is thread-safe; we never mutate the shared dict in
        # place (multiple scans may run concurrently in the executor). Rebuilding also
        # naturally prunes entries for deleted session files.
        old_cache = self._scan_file_cache
        new_cache: dict[str, tuple] = {}
        try:
            for proj in os.scandir(self._claude_projects_dir):
                if not proj.is_dir():
                    continue

                # cwd is resolved per-project: read from JSONL (authoritative),
                # fall back to directory-name decoding for old/empty files.
                proj_cwd: str | None = None

                for entry in os.scandir(proj.path):
                    if not entry.name.endswith(".jsonl") or not entry.is_file():
                        continue
                    uuid = entry.name[:-6]
                    st = entry.stat()
                    mtime = int(st.st_mtime)
                    file_key = (st.st_mtime_ns, st.st_size)

                    # Fast path: file unchanged since last scan — reuse the parsed
                    # cwd/name without opening it. This is the whole point of the cache.
                    cached = old_cache.get(entry.path)
                    if cached is not None and cached[0] == file_key:
                        file_cwd, content_name = cached[1], cached[2]
                    else:
                        file_cwd = None
                        content_name = ""
                        try:
                            with open(entry.path, encoding="utf-8", errors="ignore") as f:
                                for raw in f:
                                    try:
                                        d = json.loads(raw)
                                        # Pick up cwd from any record that carries it.
                                        if file_cwd is None:
                                            raw_cwd = d.get("cwd")
                                            if isinstance(raw_cwd, str) and raw_cwd.strip():
                                                file_cwd = raw_cwd.strip()
                                        # Pick up name from first non-empty user text.
                                        if not content_name and d.get("type") == "user":
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
                                                content_name = text[:50].strip()
                                        if file_cwd and content_name:
                                            break
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                    new_cache[entry.path] = (file_key, file_cwd, content_name)

                    # Saved (user-renamed) name takes precedence over content-derived name.
                    name = saved_names.get(uuid) or content_name
                    if not name:
                        name = proj.name.split("-")[-1] or uuid[:8]

                    # Cache the first cwd we found for this project directory.
                    if file_cwd and proj_cwd is None:
                        proj_cwd = file_cwd

                    cwd = file_cwd or proj_cwd or self._decode_project_path(proj.name)
                    cwd = path_overrides.get(cwd, cwd)
                    sessions.append({
                        "id": uuid,
                        "name": name,
                        "claude_uuid": uuid,
                        "resume_id": uuid,
                        "last_used": mtime,
                        "cwd": cwd,
                    })
        except FileNotFoundError:
            log.info("Local Claude sessions dir not found yet; skipping scan")
        except OSError as exc:
            # Windows first-run commonly raises WinError 3 (path not found).
            if getattr(exc, "winerror", None) == 3:
                log.info("Local Claude sessions path not found yet; skipping scan")
            else:
                log.warning("Failed to scan local sessions: %s", exc)
        except Exception as exc:
            log.warning("Failed to scan local sessions: %s", exc)
        # Commit the rebuilt cache (atomic reference swap; safe vs. concurrent scans).
        # On an early exception above, new_cache may be partial — only commit when we
        # actually walked the tree (i.e. it has entries) to avoid wiping a good cache.
        if new_cache:
            self._scan_file_cache = new_cache
        sessions.sort(key=lambda x: x["last_used"], reverse=True)
        return sessions[:limit]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _find_newest_jsonl_uuid(self, cwd: str, exclude: "str | None" = None) -> "str | None":
        """Scan ~/.claude/projects/<mangled-cwd>/ for the newest .jsonl and return
        its stem (UUID) if it differs from *exclude* and is a valid UUID.

        cwd mangling rule: replace '/' with '-' and prepend '-'.
        Example: /Users/alice/foo → -Users-alice-foo
        """
        if not cwd:
            return None
        try:
            mangled = "-" + cwd.lstrip("/").replace("/", "-")
            proj_dir = os.path.join(self._claude_projects_dir, mangled)
            if not os.path.isdir(proj_dir):
                return None
            best_mtime = -1.0
            best_uuid: "str | None" = None
            for entry in os.scandir(proj_dir):
                if not entry.name.endswith(".jsonl") or not entry.is_file():
                    continue
                stem = entry.name[:-6]
                if not is_valid_uuid(stem):
                    continue
                if stem == exclude:
                    continue
                mtime = entry.stat().st_mtime
                if mtime > best_mtime:
                    best_mtime = mtime
                    best_uuid = stem
            return best_uuid
        except Exception as exc:
            log.warning("_find_newest_jsonl_uuid failed for cwd=%s: %s", cwd, exc)
            return None

    async def _spawn_proc(self, session: "Session", allow_resume_fallback: bool = False) -> None:
        state = self._get_state(session)
        if state.proc is not None and state.proc.returncode is None:
            return  # already running
        if state.spawning:
            return  # spawn already in progress, caller should wait
        state.spawning = True

        # Guard: reject non-UUID resume IDs before touching the subprocess.
        # Claude CLI requires a canonical UUID; anything else causes immediate
        # failure followed by 3 auto-restart cycles and a supervisor kill loop.
        if session.resume_id and not is_valid_uuid(session.resume_id):
            log.warning(
                "[%s] refused spawn: resume_id %r is not a valid UUID — dropping",
                session.session_id, session.resume_id,
            )
            # Clear the bad resume_id so the session can still start fresh.
            session.resume_id = None
            if self._persist_session_fn is not None:
                self._persist_session_fn(session)
            await send_event(session, _evt_session_warning(
                f"Invalid claude_uuid (not a UUID format) — starting fresh session."
            ))

        # BUG-00d fallback: if we have no resume_id yet (e.g. idle-timeout killed
        # the proc before the first `init` event arrived) but we know the cwd,
        # scan ~/.claude/projects/<mangled-cwd>/ for the newest .jsonl and use
        # its stem as a candidate resume_id so context is not lost.
        # Only active when allow_resume_fallback=True (idle-timeout respawn path).
        # Must NOT fire on user-initiated new sessions to prevent self-loop.
        if allow_resume_fallback and session.resume_id is None and session.cwd:
            candidate = self._find_newest_jsonl_uuid(session.cwd)
            if candidate:
                log.info(
                    "[%s] Spawning claude with fallback resume_id=%s (cwd=%s)",
                    session.session_id, candidate, session.cwd,
                )
                session.resume_id = candidate
                if self._persist_session_fn is not None:
                    self._persist_session_fn(session)

        cmd = [
            self._claude_bin,
            "--print",
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--verbose",
        ]
        model_str = session.model or ""
        if model_str == "opusplan":
            # Plan mode: Claude plans but does not execute tools; no permission bypass needed
            cmd += ["--model", "opus", "--permission-mode", "plan"]
        else:
            if session.sandbox == "read-only":
                cmd += [
                    "--dangerously-skip-permissions",
                    "--allowedTools", "Read,Glob,Grep,WebSearch,WebFetch",
                ]
            elif session.sandbox == "workspace-write":
                cmd += [
                    "--dangerously-skip-permissions",
                    "--disallowedTools", "Bash",
                ]
            else:
                cmd.append("--dangerously-skip-permissions")
            if model_str:
                cmd += ["--model", model_str]
        if session.resume_id:
            cmd += ["--resume", session.resume_id]
        if session.effort and session.effort != "auto":
            cmd += ["--effort", session.effort]

        log.info("[%s] Spawning claude: %s (cwd=%s)", session.session_id, cmd, session.cwd)

        try:
            state.proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=session.cwd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=_STREAM_READER_LIMIT,
            )
        except Exception as exc:
            log.error("[%s] Failed to spawn claude: %s", session.session_id, exc)
            await send_event(session, _evt_error(f"Failed to spawn claude: {exc}"))
            state.spawning = False
            if state.proc_ready_event is not None:
                state.proc_ready_event.set()
            return

        state.spawning = False
        session.is_stopping = False
        if state.proc_ready_event is not None:
            state.proc_ready_event.set()

        for task in (state.stdout_task, state.stderr_task, state.watch_task):
            if task and not task.done():
                task.cancel()

        state.stdout_task = asyncio.create_task(self._stdout_reader(session))
        state.stderr_task = asyncio.create_task(self._stderr_reader(session))
        state.watch_task  = asyncio.create_task(self._watch_proc(session))
        log.info("[%s] Claude process started (pid=%d)", session.session_id, state.proc.pid)

        if state.timed_out:
            state.timed_out = False
            context_msg = json.dumps({
                "type": "user",
                "message": {"role": "user", "content": [{
                    "type": "text",
                    "text": (
                        "[系統自動注入] 你上一輪的工具執行超過5分鐘無回應，"
                        "session 已被自動終止並以 --resume 重啟。"
                        "請簡短說明你剛才在做什麼、最後執行的是什麼工具，以及可能的原因。"
                        "不要自動重試任何操作，等待用戶指示。"
                    ),
                }]},
            }) + "\n"
            session.is_streaming = True
            session.last_activity = time.time()
            try:
                state.proc.stdin.write(context_msg.encode("utf-8"))
                await state.proc.stdin.drain()
                log.info("[%s] Injected timeout context message", session.session_id)
            except Exception as exc:
                session.is_streaming = False
                log.error("[%s] Failed to inject timeout context: %s", session.session_id, exc)

    async def _stdout_reader(self, session: "Session") -> None:
        state = self._get_state(session)
        assert state.proc is not None

        while True:
            try:
                line_bytes = await state.proc.stdout.readline()
            except asyncio.LimitOverrunError as exc:
                log.error("[%s] stdout line too long (%d bytes), discarding", session.session_id, exc.consumed)
                await state.proc.stdout.read(exc.consumed)
                continue
            if not line_bytes:
                break

            session.last_activity = time.time()
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
                _pending_ask_events: list[tuple[str, asyncio.Event]] = []
                for block in content_blocks:
                    btype = block.get("type", "")
                    if btype == "thinking":
                        thinking_text = block.get("thinking", "")
                        if thinking_text:
                            await send_event(session, _evt_thinking_chunk(thinking_text))
                    elif btype == "text":
                        text = block.get("text", "")
                        if text:
                            session.accumulated_text += text
                            await stream_text(text, session)
                    elif btype == "tool_use":
                        tool_id = block.get("id", "")
                        name = block.get("name", "")
                        input_data = block.get("input", {})
                        command = input_data.get("command", json.dumps(input_data))
                        state.tool_blocks[tool_id] = {"name": name}
                        if name == "AskUserQuestion":
                            try:
                                _questions = normalize_questions(input_data)
                                _header = str(input_data.get("header") or input_data.get("title") or "Question")
                                interaction = await INTERACTIONS.create(
                                    session_id=session.session_id,
                                    source="claude",
                                    kind="ask_user_question",
                                    questions=_questions,
                                    header=_header,
                                    tool_use_id=tool_id,
                                    requesting_agent=name,
                                    raw_command=input_data,
                                    broadcast_json=self._broadcast_fn,
                                )
                                _q_text = _questions[0].get("text", "") if _questions else ""
                                asyncio.create_task(_notify_fcm_user_input(
                                    session_name=session.name,
                                    header=_header,
                                    question_text=_q_text,
                                    session_id=session.session_id,
                                    request_id=interaction.request_id,
                                ))
                                _wait_ev = asyncio.Event()
                                state.tool_waiting_events[tool_id] = _wait_ev
                                state.tool_waiting_interactions[tool_id] = interaction.request_id
                                _pending_ask_events.append((tool_id, _wait_ev))
                            except Exception as exc:
                                log.warning("[%s] AskUserQuestion bridge conversion failed: %s", session.session_id, exc)
                        await send_event(session, _evt_tool_start(tool_id, name, command))
                # Pause stdout reader until user answers all AskUserQuestion calls.
                # This ensures tool_result is written to Claude's stdin BEFORE readline()
                # is called, preventing Claude Code from timing out the interaction.
                if _pending_ask_events:
                    log.info("[%s] Waiting for %d AskUserQuestion response(s) (max %ds)",
                             session.session_id, len(_pending_ask_events), ASK_USER_QUESTION_MAX_WAIT_SECS)
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*[ev.wait() for _, ev in _pending_ask_events]),
                            timeout=ASK_USER_QUESTION_MAX_WAIT_SECS,
                        )
                        log.info("[%s] AskUserQuestion response(s) received, resuming stdout reader",
                                 session.session_id)
                    except asyncio.TimeoutError:
                        log.warning("[%s] AskUserQuestion wait exceeded %ds — cancelling "
                                    "dangling tool_use(s) and resuming",
                                    session.session_id, ASK_USER_QUESTION_MAX_WAIT_SECS)
                        # Any still-unanswered asks remain in state; cancel them so
                        # the turn can proceed and --resume history stays clean.
                        await self._cancel_pending_user_input(session)

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
                if state.tree_poll_task and not state.tree_poll_task.done():
                    state.tree_poll_task.cancel()
                    state.tree_poll_task = None
                if subtype == "success":
                    usage = evt.get("usage", {})
                    # context_used = actual context window occupied = new input + cache creation.
                    # Do NOT include cache_read_input_tokens — that accumulates with each tool round
                    # and inflates to many times the true context size, falsely triggering compact.
                    session.context_used = (
                        usage.get("input_tokens", 0)
                        + usage.get("cache_creation_input_tokens", 0)
                    )
                    context_limit = _get_context_limit(session.model)
                    if context_limit > 0:
                        session.context_max = context_limit
                    if session.context_used > session.context_max * 1.5:
                        log.error("[%s] context_used=%d > context_max=%d * 1.5 — formula likely wrong, skipping compact",
                                  session.session_id, session.context_used, session.context_max)
                        session.context_used = min(session.context_used, session.context_max)
                    log.info("[%s] result success, claude_uuid=%s, context_used=%d/%d", session.session_id, new_uuid, session.context_used, session.context_max)
                    if new_uuid:
                        first_uuid = session.resume_id is None
                        if session.resume_id and session.resume_id != new_uuid:
                            session.historical_resume_ids.add(session.resume_id)
                        session.resume_id = new_uuid
                        # Clear latest_source_line so the next request_history does a
                        # fresh JSONL read.  The in-memory cache may have been built
                        # before the final message was appended to the file (race between
                        # the result event and an earlier concurrent request_history task),
                        # so reading the cache here would set latest_source_line to the
                        # second-to-last message ID, causing all subsequent request_history
                        # calls to hit the fast-path and return an empty delta — the final
                        # message would never reach the client.
                        session.latest_source_line = ""
                        if self._persist_session_fn is not None:
                            self._persist_session_fn(session)
                        if first_uuid:
                            try:
                                if session.ws_ref and session.ws_ref.open:
                                    await session.ws_ref.send(json.dumps(
                                        _msg_session_uuid(session.session_id, new_uuid)
                                    ))
                            except Exception:
                                pass
                    if self._notify_fcm_fn is not None and not client_manager.has_clients():
                        asyncio.create_task(self._notify_fcm_fn(session.name, session.accumulated_text, session.session_id))
                    await emit_done(session)
                    if session.ws_ref is not None:
                        asyncio.create_task(self.fetch_usage(session.ws_ref))
                    state.tool_blocks = {}

                    # Auto-compact: if context exceeds threshold and compact not already running,
                    # write /compact directly to stdin before the next user message arrives.
                    # The compact streams as a normal assistant turn; the frontend handles it
                    # naturally via resolveAssistantMsgId's createIfMissing=true path.
                    compact_was_in_progress = state.compact_in_progress
                    state.compact_in_progress = False  # reset after each result
                    if compact_was_in_progress and self._broadcast_fn is not None:
                        # Compact just finished — signal the frontend's loading indicator.
                        asyncio.create_task(self._broadcast_fn({
                            "type": "session_command_done",
                            "session_id": session.session_id,
                            "request_id": f"compact_{session.session_id}",
                            "queue_length": 0,
                        }))
                    if (
                        not compact_was_in_progress
                        and context_limit > 0
                        and session.context_used >= int(context_limit * _COMPACT_THRESHOLD)
                        and state.proc is not None
                        and state.proc.returncode is None
                    ):
                        log.info(
                            "[%s] auto-compact triggered: context_used=%d >= %d (%.0f%% of %d)",
                            session.session_id, session.context_used,
                            int(context_limit * _COMPACT_THRESHOLD),
                            100 * session.context_used / context_limit,
                            context_limit,
                        )
                        session.is_streaming = True
                        state.compact_in_progress = True
                        if self._broadcast_fn is not None:
                            asyncio.create_task(self._broadcast_fn({
                                "type": "session_command_started",
                                "session_id": session.session_id,
                                "request_id": f"compact_{session.session_id}",
                                "queue_length": 0,
                            }))
                        compact_payload = json.dumps({
                            "type": "user",
                            "message": {"role": "user", "content": [{"type": "text", "text": "/compact"}]},
                        }) + "\n"
                        try:
                            state.proc.stdin.write(compact_payload.encode("utf-8"))
                            await state.proc.stdin.drain()
                        except Exception as exc:
                            log.warning("[%s] auto-compact stdin write failed: %s", session.session_id, exc)
                            session.is_streaming = False
                            state.compact_in_progress = False
                            if self._broadcast_fn is not None:
                                asyncio.create_task(self._broadcast_fn({
                                    "type": "session_command_failed",
                                    "session_id": session.session_id,
                                    "request_id": f"compact_{session.session_id}",
                                    "message": str(exc),
                                    "queue_length": 0,
                                }))
                else:
                    err = evt.get("result", "Unknown error")
                    log.error("[%s] result error: %s", session.session_id, err)
                    await send_event(session, _evt_error(str(err)))
                    session.accumulated_text = ""
                    state.tool_blocks = {}

            elif etype == "system":
                subtype = evt.get("subtype", "")
                log.debug("[%s] system subtype=%s", session.session_id, subtype)
                if subtype == "init":
                    model = evt.get("model", "")
                    if model:
                        session.model = model
                    # Capture claude session_id at init so we can --resume even if
                    # the first turn is killed by the idle watchdog before result.
                    init_uuid = evt.get("session_id")
                    if init_uuid and init_uuid != session.resume_id:
                        first_uuid = session.resume_id is None
                        if session.resume_id and session.resume_id != init_uuid:
                            session.historical_resume_ids.add(session.resume_id)
                        session.resume_id = init_uuid
                        if self._persist_session_fn is not None:
                            self._persist_session_fn(session)
                        log.info("[%s] captured claude_uuid=%s at init", session.session_id, init_uuid)
                        if first_uuid:
                            try:
                                if session.ws_ref and session.ws_ref.open:
                                    await session.ws_ref.send(json.dumps(
                                        _msg_session_uuid(session.session_id, init_uuid)
                                    ))
                            except Exception:
                                pass

            elif etype == "rate_limit_event":
                log.debug("[%s] rate_limit_event", session.session_id)

            elif etype == "user":
                pass

            else:
                log.debug("[%s] Unhandled event type: %s", session.session_id, etype)

    async def _stderr_reader(self, session: "Session") -> None:
        state = self._get_state(session)
        assert state.proc is not None

        async for line_bytes in state.proc.stderr:
            line = line_bytes.decode("utf-8", errors="replace").strip()
            if line:
                log.warning("[%s] claude stderr: %s", session.session_id, line)
                if "No conversation found" in line:
                    state.bad_resume = True

    async def _watch_proc(self, session: "Session") -> None:
        state = self._get_state(session)
        assert state.proc is not None

        await state.proc.wait()

        if session.is_stopping:
            return

        rc = state.proc.returncode
        log.warning("[%s] Claude proc exited unexpectedly (rc=%s)", session.session_id, rc)

        # A process exit while a turn is streaming means the current response
        # cannot complete. Close that turn explicitly so queue_runner and the
        # mobile UI do not stay in a stale "processing" state until the next
        # process/session event arrives.
        if session.is_streaming:
            session.is_streaming = False
            session.accumulated_text = ""
            state.tool_blocks = {}
            if state.timeout_task and not state.timeout_task.done():
                state.timeout_task.cancel()
            if state.tree_poll_task and not state.tree_poll_task.done():
                state.tree_poll_task.cancel()
                state.tree_poll_task = None
            await send_event(session, _evt_error(
                f"Claude process exited (rc={rc}); current response was stopped.",
                "process_exited",
            ))

        # If compact was in progress when the proc died, clear the flag and notify frontend.
        if state.compact_in_progress:
            state.compact_in_progress = False
            if self._broadcast_fn is not None:
                asyncio.create_task(self._broadcast_fn({
                    "type": "session_command_failed",
                    "session_id": session.session_id,
                    "request_id": f"compact_{session.session_id}",
                    "message": "Process exited during compact",
                    "queue_length": 0,
                }))

        if state.bad_resume:
            state.bad_resume = False
            old_id = session.resume_id

            # --- BUG-00d fallback: try to recover a valid resume_id from disk ---
            # Before discarding the session, scan ~/.claude/projects/<mangled-cwd>/
            # for the newest .jsonl file.  If a candidate UUID differs from the
            # dead one, retry with it (one attempt) instead of starting fresh.
            candidate_id = self._find_newest_jsonl_uuid(session.cwd, exclude=old_id)
            if candidate_id:
                log.info(
                    "[%s] bad_resume: trying candidate resume_id %s (was %s)",
                    session.session_id, candidate_id, old_id,
                )
                session.resume_id = candidate_id
                from bridge_v2 import _persist_session as _ps
                _ps(session)
                await send_event(session, _evt_session_warning(
                    f"Resume session not found; retrying with nearest candidate…"
                ))
                await self._spawn_proc(session)
            else:
                session.resume_id = None
                log.info("[%s] Resume ID %s not found, restarting fresh", session.session_id, old_id)
                # Persist the cleared resume_id immediately so a subsequent bridge
                # restart does not retry the same dead uuid.
                from bridge_v2 import _persist_session as _ps
                _ps(session)
                await send_event(session, _evt_session_warning(
                    "Resume session not found, starting fresh…"
                ))
                await self._spawn_proc(session)
        elif rc != 0 and state.restart_count < 3:
            state.restart_count += 1
            log.info("[%s] Auto-restarting (attempt %d/3)", session.session_id, state.restart_count)
            await send_event(session, _evt_session_warning(
                f"Claude process exited (rc={rc}), restarting ({state.restart_count}/3)…"
            ))
            await self._spawn_proc(session)
        else:
            log.error("[%s] Session died after %d restart(s)", session.session_id, state.restart_count)
            await send_event(session, _evt_session_died(
                f"Claude process exited (rc={rc}) and will not restart."
            ))
            asyncio.create_task(_notify_fcm_session_died(
                session_name=session.name,
                session_id=session.session_id,
            ))

    async def _agent_tree_poller(self, session: "Session") -> None:
        await asyncio.sleep(3)  # 初始延遲，等 subagent 有機會出現
        last_fingerprint: tuple = (-1, -1)  # (total_agents, completed_count)
        while session.is_streaming:
            try:
                if session.resume_id:
                    loop = asyncio.get_event_loop()
                    tree_data = await loop.run_in_executor(
                        None, self._build_agent_tree_sync, session.resume_id
                    )
                    total = tree_data.get("total_agents", 0)
                    if total > 0:
                        # 計算「已完成的 agent 數」作為指紋，有變化才推送
                        def _count_done(nodes: list) -> int:
                            cnt = 0
                            for n in nodes:
                                if n.get("end_ts") is not None:
                                    cnt += 1
                                cnt += _count_done(n.get("children", []))
                            return cnt
                        done_count = _count_done(tree_data.get("tree", []))
                        fingerprint = (total, done_count)
                        if fingerprint != last_fingerprint:
                            last_fingerprint = fingerprint
                            msg = json.dumps({
                                "type": "agent_tree",
                                "session_id": session.session_id,
                                **tree_data,
                            })
                            if session.ws_ref and getattr(session.ws_ref, "open", False):
                                await session.ws_ref.send(msg)
            except Exception:
                pass
            await asyncio.sleep(2)

    async def _idle_watchdog(self, session: "Session") -> None:
        state = self._get_state(session)
        while True:
            await asyncio.sleep(30)
            if not session.is_streaming:
                return
            elapsed = time.time() - session.last_activity
            if elapsed < TOOL_IDLE_TIMEOUT_SECS:
                continue
            log.warning("[%s] Tool idle timeout (%.0fs) — killing claude (pid=%s)",
                        session.session_id, elapsed,
                        state.proc.pid if state.proc else "?")
            await send_event(session, _evt_session_warning(
                f"⚠️ 工具執行超過 {int(elapsed) // 60} 分 {int(elapsed) % 60} 秒無回應，"
                "已自動終止並重新啟動 Claude…"
            ))
            session.is_stopping = True
            session.is_streaming = False
            if state.tree_poll_task and not state.tree_poll_task.done():
                state.tree_poll_task.cancel()
                state.tree_poll_task = None
            session.accumulated_text = ""
            state.tool_blocks = {}
            state.timed_out = True
            try:
                state.proc.send_signal(signal.SIGTERM)
            except (ProcessLookupError, AttributeError):
                pass
            await asyncio.sleep(1)
            try:
                if state.proc and state.proc.returncode is None:
                    state.proc.kill()
            except (ProcessLookupError, AttributeError):
                pass
            await self._spawn_proc(session, allow_resume_fallback=True)
            return
