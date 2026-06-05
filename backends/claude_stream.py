"""
Claude backend — subprocess lifecycle & NDJSON streaming mixin.

Owns the `claude --print --input-format stream-json` child process: spawning
(with resume / fallback), the stdout NDJSON reader that emits normalized
events, the stderr drain, the process watchdog (turn-complete detection, FCM
notify, auto-compact), the agent-tree poller, and the idle watchdog.
"""

import asyncio
import json
import logging
import os
import signal
import time
from typing import Any, Optional, TYPE_CHECKING

from utils.uuid_helper import is_valid_uuid
from .events import (
    send_event, stream_text,
    _evt_session_warning, _evt_session_died, _evt_session_closed,
    _evt_thinking_chunk,
    _evt_todo_update, _msg_session_uuid,
)
from .turn_lifecycle import emit_turn_done, emit_turn_error, settle_turn_state
from interactions import REGISTRY as INTERACTIONS, normalize_questions
from push_registry import notify_fcm_user_input as _notify_fcm_user_input
from push_registry import notify_fcm_session_died as _notify_fcm_session_died
import client_manager
import task_manager
from .claude_common import _get_context_limit

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")

TOOL_IDLE_TIMEOUT_SECS = 6000  # kill claude if no stdout for this many seconds (was 300)

# Upper bound on how long the stdout reader will pause waiting for an
# AskUserQuestion answer.  Generous (a human is in the loop) but bounded so a
# never-answered question can't pin the reader — and the turn — forever.  On
# timeout the dangling tool_use is cancelled so --resume history stays clean.
ASK_USER_QUESTION_MAX_WAIT_SECS = 1800  # 30 minutes

_STREAM_READER_LIMIT = 128 * 1024 * 1024  # 128 MiB — matches codex_appserver; prevents LimitOverrunError on large tool outputs

# Compact when context_used exceeds this fraction of the model's context window.
_COMPACT_THRESHOLD = 0.80

# Task/plan tools normalized into todo_update events instead of rendered as tool cards.
_TODO_TOOLS = frozenset({"TodoWrite", "TaskCreate", "TaskUpdate", "TaskDelete"})


class _ClaudeProcessMixin:
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
            await emit_turn_error(session, f"Failed to spawn claude: {exc}")
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
                settle_turn_state(session, clear_accumulated=False)
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
                        # Task/plan tools → normalized todo_update panel, not a tool card.
                        # Suppress the tool_start (and later its result/end) so they don't
                        # clutter the message stream. TaskCreate's server-assigned id is
                        # resolved later from its tool_result text.
                        if name in _TODO_TOOLS:
                            if name == "TodoWrite":
                                changed = state.todo_store.apply_todowrite(input_data)
                            elif name == "TaskCreate":
                                changed = state.todo_store.note_create(tool_id, input_data)
                            elif name == "TaskUpdate":
                                changed = state.todo_store.apply_update(input_data)
                            else:  # TaskDelete
                                changed = state.todo_store.apply_delete(input_data)
                            if tool_id:
                                state.todo_suppressed_ids.add(tool_id)
                            if changed:
                                await send_event(session, _evt_todo_update(state.todo_store.as_list()))
                            continue
                        command = input_data.get("command", json.dumps(input_data))
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
                                task_manager.spawn(
                                    f"claude-fcm-user-input:{session.session_id}",
                                    _notify_fcm_user_input(
                                        session_name=session.name,
                                        header=_header,
                                        question_text=_q_text,
                                        session_id=session.session_id,
                                        request_id=interaction.request_id,
                                    ),
                                    owner=f"session:{session.session_id}",
                                )
                                _wait_ev = asyncio.Event()
                                state.tool_waiting_events[tool_id] = _wait_ev
                                state.tool_waiting_interactions[tool_id] = interaction.request_id
                                _pending_ask_events.append((tool_id, _wait_ev))
                            except Exception as exc:
                                log.warning("[%s] AskUserQuestion bridge conversion failed: %s", session.session_id, exc)
                        await state.tool_lifecycle.start(session, tool_id, name, command)
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
                # Swallow results for normalized task/todo tools. For TaskCreate this
                # is where the server-assigned id (#N) arrives — resolve it so later
                # TaskUpdate(taskId) can match, then re-emit the snapshot.
                if tool_id in state.todo_suppressed_ids:
                    state.todo_suppressed_ids.discard(tool_id)
                    if state.todo_store.resolve_create(tool_id, str(output)):
                        await send_event(session, _evt_todo_update(state.todo_store.as_list()))
                    continue
                await state.tool_lifecycle.result(session, tool_id, str(output))
                await state.tool_lifecycle.end(session, tool_id)

            elif etype == "result":
                subtype = evt.get("subtype", "")
                new_uuid = evt.get("session_id")
                settle_turn_state(session, clear_accumulated=False)
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
                            if session.ws_ref and getattr(session.ws_ref, "open", True):
                                await client_manager.send_json(
                                    session.ws_ref,
                                    _msg_session_uuid(session.session_id, new_uuid),
                                )
                    if self._notify_fcm_fn is not None and not client_manager.has_clients():
                        task_manager.spawn(
                            f"claude-fcm-done:{session.session_id}",
                            self._notify_fcm_fn(session.name, session.accumulated_text, session.session_id),
                            owner=f"session:{session.session_id}",
                        )
                    await emit_turn_done(session, tool_lifecycle=state.tool_lifecycle)
                    if session.ws_ref is not None:
                        task_manager.spawn(
                            f"claude-fetch-usage:{session.session_id}",
                            self.fetch_usage(session.ws_ref),
                            owner=f"session:{session.session_id}",
                        )
                    state.tool_lifecycle.clear()

                    # Auto-compact: if context exceeds threshold and compact not already running,
                    # write /compact directly to stdin before the next user message arrives.
                    # The compact streams as a normal assistant turn; the frontend handles it
                    # naturally via resolveAssistantMsgId's createIfMissing=true path.
                    compact_was_in_progress = state.compact_in_progress
                    state.compact_in_progress = False  # reset after each result
                    if compact_was_in_progress and self._broadcast_fn is not None:
                        # Compact just finished — signal the frontend's loading indicator.
                        task_manager.spawn(
                            f"claude-compact-done:{session.session_id}",
                            self._broadcast_fn({
                                "type": "session_command_done",
                                "session_id": session.session_id,
                                "request_id": f"compact_{session.session_id}",
                                "queue_length": 0,
                            }),
                            owner=f"session:{session.session_id}",
                        )
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
                            task_manager.spawn(
                                f"claude-compact-started:{session.session_id}",
                                self._broadcast_fn({
                                    "type": "session_command_started",
                                    "session_id": session.session_id,
                                    "request_id": f"compact_{session.session_id}",
                                    "queue_length": 0,
                                }),
                                owner=f"session:{session.session_id}",
                            )
                        compact_payload = json.dumps({
                            "type": "user",
                            "message": {"role": "user", "content": [{"type": "text", "text": "/compact"}]},
                        }) + "\n"
                        try:
                            state.proc.stdin.write(compact_payload.encode("utf-8"))
                            await state.proc.stdin.drain()
                        except Exception as exc:
                            log.warning("[%s] auto-compact stdin write failed: %s", session.session_id, exc)
                            settle_turn_state(session, clear_accumulated=False)
                            state.compact_in_progress = False
                            if self._broadcast_fn is not None:
                                task_manager.spawn(
                                    f"claude-compact-failed:{session.session_id}",
                                    self._broadcast_fn({
                                        "type": "session_command_failed",
                                        "session_id": session.session_id,
                                        "request_id": f"compact_{session.session_id}",
                                        "message": str(exc),
                                        "queue_length": 0,
                                    }),
                                    owner=f"session:{session.session_id}",
                                )
                else:
                    err = evt.get("result", "Unknown error")
                    log.error("[%s] result error: %s", session.session_id, err)
                    await emit_turn_error(session, str(err), tool_lifecycle=state.tool_lifecycle)
                    state.tool_lifecycle.clear()

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
                            if session.ws_ref and getattr(session.ws_ref, "open", True):
                                await client_manager.send_json(
                                    session.ws_ref,
                                    _msg_session_uuid(session.session_id, init_uuid),
                                )

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
            await emit_turn_error(
                session,
                f"Claude process exited (rc={rc}); current response was stopped.",
                "process_exited",
                tool_lifecycle=state.tool_lifecycle,
                reason="process_exited",
            )
            state.tool_lifecycle.clear()
            if state.timeout_task and not state.timeout_task.done():
                state.timeout_task.cancel()
            if state.tree_poll_task and not state.tree_poll_task.done():
                state.tree_poll_task.cancel()
                state.tree_poll_task = None

        # If compact was in progress when the proc died, clear the flag and notify frontend.
        if state.compact_in_progress:
            state.compact_in_progress = False
            if self._broadcast_fn is not None:
                task_manager.spawn(
                    f"claude-compact-process-exited:{session.session_id}",
                    self._broadcast_fn({
                        "type": "session_command_failed",
                        "session_id": session.session_id,
                        "request_id": f"compact_{session.session_id}",
                        "message": "Process exited during compact",
                        "queue_length": 0,
                    }),
                    owner=f"session:{session.session_id}",
                )

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
            task_manager.spawn(
                f"claude-fcm-session-died:{session.session_id}",
                _notify_fcm_session_died(
                    session_name=session.name,
                    session_id=session.session_id,
                ),
                owner=f"session:{session.session_id}",
            )

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
                            payload = {
                                "type": "agent_tree",
                                "session_id": session.session_id,
                                **tree_data,
                            }
                            if session.ws_ref and getattr(session.ws_ref, "open", False):
                                await client_manager.send_json(session.ws_ref, payload)
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
            settle_turn_state(session, clear_accumulated=True)
            if state.tree_poll_task and not state.tree_poll_task.done():
                state.tree_poll_task.cancel()
                state.tree_poll_task = None
            await state.tool_lifecycle.end_all(session, "idle_timeout")
            state.tool_lifecycle.clear()
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
