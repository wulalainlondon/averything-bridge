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
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional, TYPE_CHECKING

from .base import Backend
from .events import (
    send_event, stream_text, scan_for_media,
    _evt_error, _evt_stopped, _evt_done,
    _evt_tool_start, _evt_tool_result, _evt_tool_end,
    _evt_session_warning, _evt_session_died, _evt_session_closed,
    _msg_session_uuid, _msg_usage_report, _msg_error,
)

if TYPE_CHECKING:
    from claude_bridge_v2 import Session

log = logging.getLogger("bridge_v2")


@dataclass
class _ClaudeState:
    proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    stdout_task: Optional[asyncio.Task] = field(default=None, repr=False)
    stderr_task: Optional[asyncio.Task] = field(default=None, repr=False)
    watch_task: Optional[asyncio.Task] = field(default=None, repr=False)
    tool_blocks: dict = field(default_factory=dict)
    restart_count: int = 0
    pending_stop: bool = False
    bad_resume: bool = False


class ClaudeCliBackend(Backend):
    def __init__(
        self,
        claude_bin: str = "",
        bun_bin: str = "bun",
        notify_fcm_fn: "Callable[[str, str, str], Coroutine] | None" = None,
        persist_session_fn: "Callable[[Session], None] | None" = None,
        claude_projects_dir: str = "",
        load_saved_sessions_fn: "Callable[[], dict] | None" = None,
    ) -> None:
        self._claude_bin = claude_bin
        self._bun_bin = bun_bin
        self._notify_fcm_fn = notify_fcm_fn
        self._persist_session_fn = persist_session_fn
        self._claude_projects_dir = claude_projects_dir
        self._load_saved_sessions_fn = load_saved_sessions_fn
        self._states: dict[str, _ClaudeState] = {}

    def _get_state(self, session: "Session") -> _ClaudeState:
        if session.session_id not in self._states:
            self._states[session.session_id] = _ClaudeState()
        return self._states[session.session_id]

    # ------------------------------------------------------------------
    # Public Backend interface
    # ------------------------------------------------------------------

    async def spawn(self, session: "Session") -> None:
        await self._spawn_proc(session)

    async def send(self, session: "Session", content: str,
                   images: list | None = None, files: list | None = None) -> None:
        state = self._get_state(session)

        if session.is_streaming:
            await send_event(session, _evt_error("Session is currently processing a request.", "session_busy"))
            return

        if state.proc is None or state.proc.returncode is not None:
            await send_event(session, _evt_error("Claude process is not running.", "session_dead"))
            return

        session.accumulated_text = ""
        state.tool_blocks = {}
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
            state.proc.stdin.write(payload.encode("utf-8"))
            await state.proc.stdin.drain()
            log.info("[%s] Message sent (%d chars, %d images)", session.session_id, len(content), len(images or []))
        except Exception as exc:
            session.is_streaming = False
            log.error("[%s] Failed to write to stdin: %s", session.session_id, exc)
            await send_event(session, _evt_error(f"stdin write failed: {exc}"))

    async def stop(self, session: "Session") -> None:
        state = self._get_state(session)

        if state.proc is None or state.proc.returncode is not None:
            await send_event(session, _evt_stopped())
            return

        session.is_stopping = True
        log.info("[%s] Stopping session (pid=%d)", session.session_id, state.proc.pid)

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
        session.accumulated_text = ""
        state.tool_blocks = {}
        await send_event(session, _evt_stopped())

    async def clear(self, session: "Session") -> None:
        state = self._get_state(session)

        log.info("[%s] Clearing session history", session.session_id)
        session.is_stopping = True
        session.resume_id = None
        session.accumulated_text = ""
        state.tool_blocks = {}
        session.is_streaming = False

        if state.proc is not None and state.proc.returncode is None:
            try:
                state.proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(state.proc.wait(), timeout=2)
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
        log.info("[%s] Closing session", session.session_id)

        if state.proc is not None and state.proc.returncode is None:
            try:
                state.proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(state.proc.wait(), timeout=2)
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
                return {"utilization": entry.get("utilization"), "resets_at": entry.get("resets_at")}

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

    async def load_session_history(self, resume_id: str, limit: int = 60) -> list[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._load_session_history_sync, resume_id, limit)

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

    def _load_session_history_sync(self, resume_id: str, limit: int = 60) -> list:
        path = self._find_session_file_sync(resume_id)
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
                        ts_ms = 0
                        ts_str = d.get("timestamp", "")
                        if ts_str:
                            try:
                                ts_ms = int(datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp() * 1000)
                            except Exception:
                                pass
                        messages.append({"role": role, "content": text, "timestamp": ts_ms})
                    except Exception:
                        pass
        except Exception as exc:
            log.warning("Failed to load session history: %s", exc)
        return messages[-limit:]

    def _scan_local_sessions_sync(self, limit: int = 100) -> list:
        sessions = []
        saved_names: dict = {}
        if self._load_saved_sessions_fn is not None:
            saved_names = {v["claude_uuid"]: v["name"] for v in self._load_saved_sessions_fn().values()}
        try:
            for proj in os.scandir(self._claude_projects_dir):
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

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _spawn_proc(self, session: "Session") -> None:
        state = self._get_state(session)
        if state.proc is not None and state.proc.returncode is None:
            return  # already running

        cmd = [
            self._claude_bin,
            "--print",
            "--input-format", "stream-json",
            "--output-format", "stream-json",
            "--verbose",
            "--dangerously-skip-permissions",
        ]
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
            )
        except Exception as exc:
            log.error("[%s] Failed to spawn claude: %s", session.session_id, exc)
            await send_event(session, _evt_error(f"Failed to spawn claude: {exc}"))
            return

        session.is_stopping = False

        for task in (state.stdout_task, state.stderr_task, state.watch_task):
            if task and not task.done():
                task.cancel()

        state.stdout_task = asyncio.create_task(self._stdout_reader(session))
        state.stderr_task = asyncio.create_task(self._stderr_reader(session))
        state.watch_task  = asyncio.create_task(self._watch_proc(session))
        log.info("[%s] Claude process started (pid=%d)", session.session_id, state.proc.pid)

    async def _stdout_reader(self, session: "Session") -> None:
        state = self._get_state(session)
        assert state.proc is not None

        async for line_bytes in state.proc.stdout:
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
                        state.tool_blocks[tool_id] = {"name": name}
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
                    usage = evt.get("usage", {})
                    session.context_used = usage.get("input_tokens", 0) + usage.get("cache_read_input_tokens", 0)
                    log.info("[%s] result success, claude_uuid=%s, context_used=%d", session.session_id, new_uuid, session.context_used)
                    if new_uuid:
                        first_uuid = session.resume_id is None
                        session.resume_id = new_uuid
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
                    if self._notify_fcm_fn is not None:
                        asyncio.create_task(self._notify_fcm_fn(session.name, session.accumulated_text, session.session_id))
                    await send_event(session, _evt_done())
                    session.accumulated_text = ""
                    state.tool_blocks = {}
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

            elif etype == "rate_limit_event":
                log.debug("[%s] rate_limit_event", session.session_id)

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

        if state.bad_resume:
            state.bad_resume = False
            old_id = session.resume_id
            session.resume_id = None
            log.info("[%s] Resume ID %s not found, restarting fresh", session.session_id, old_id)
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
