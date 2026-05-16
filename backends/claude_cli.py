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
    _evt_tool_start, _evt_tool_result, _evt_tool_end, _evt_thinking_chunk,
    _evt_session_warning, _evt_session_died, _evt_session_closed,
    _msg_session_uuid, _msg_usage_report, _msg_error,
)
from .history import (
    complete_history_message, clamp_history_limit, slice_history,
    _JSONL_HISTORY_CACHE, _file_cache_key, HistoryIndex,
    HISTORY_INDEX_TTL_SECONDS, DEFAULT_HISTORY_LIMIT,
)

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")

TOOL_IDLE_TIMEOUT_SECS = 300  # kill claude if no stdout for this many seconds

# Compact when context_used exceeds this fraction of the model's context window.
_COMPACT_THRESHOLD = 0.80

# All current Claude models share a 200k input context window.
# Unknown / non-Claude models return 0 (auto-compact disabled).
def _get_context_limit(model: str) -> int:
    if "claude" in (model or "").lower():
        return 200_000
    return 0


@dataclass
class _ClaudeState:
    proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    stdout_task: Optional[asyncio.Task] = field(default=None, repr=False)
    stderr_task: Optional[asyncio.Task] = field(default=None, repr=False)
    watch_task: Optional[asyncio.Task] = field(default=None, repr=False)
    timeout_task: Optional[asyncio.Task] = field(default=None, repr=False)
    timed_out: bool = False
    tool_blocks: dict = field(default_factory=dict)
    restart_count: int = 0
    pending_stop: bool = False
    bad_resume: bool = False
    compact_in_progress: bool = False


class ClaudeCliBackend(Backend):
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
            return

        if state.timeout_task and not state.timeout_task.done():
            state.timeout_task.cancel()
        state.timeout_task = asyncio.create_task(self._idle_watchdog(session))

    async def stop(self, session: "Session") -> None:
        state = self._get_state(session)

        if state.proc is None or state.proc.returncode is not None:
            await send_event(session, _evt_stopped())
            return

        session.is_stopping = True
        if state.timeout_task and not state.timeout_task.done():
            state.timeout_task.cancel()
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
        if state.timeout_task and not state.timeout_task.done():
            state.timeout_task.cancel()
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

        _MAX_OUTPUT = 64 * 1024

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

        # Pass 1: collect tool_use_id -> output mapping from all lines (including isSidechain)
        tool_outputs: dict = {}
        try:
            with open(path, encoding="utf-8", errors="ignore") as f:
                for line_no, raw in enumerate(f, start=1):
                    try:
                        d = json.loads(raw)
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
                    except Exception:
                        pass
        except Exception as exc:
            log.warning("Failed to load session history (pass 1): %s", exc)
            return []

        # Pass 2: build message list with blocks
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
                        messages.append(complete_history_message(
                            source="claude",
                            source_session_id=resume_id,
                            source_message_id=f"claude:{resume_id}:line:{line_no}",
                            role=role,
                            content=text,
                            timestamp=ts_ms or None,
                            blocks=blocks,
                        ))
                    except Exception:
                        pass
        except Exception as exc:
            log.warning("Failed to load session history (pass 2): %s", exc)

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

        return slice_history(
            messages,
            limit=clamp_history_limit(limit),
            known_last_source_message_id=known_last_source_message_id,
            mode=mode,
            before_source_message_id=before_source_message_id,
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
            saved_names = {v["claude_uuid"]: v["name"] for v in self._load_saved_sessions_fn().values()}
        path_overrides = self._load_path_overrides()
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
                    mtime = int(entry.stat().st_mtime)
                    name = saved_names.get(uuid, "")
                    file_cwd: str | None = None

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
                                    # Pick up session name from first non-empty user text.
                                    if not name and d.get("type") == "user":
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
                                    if file_cwd and name:
                                        break
                                except Exception:
                                    pass
                    except Exception:
                        pass

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
            session.last_activity = asyncio.get_event_loop().time()
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

        async for line_bytes in state.proc.stdout:
            session.last_activity = asyncio.get_event_loop().time()
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
                    if btype == "thinking":
                        thinking_text = block.get("thinking", "")
                        if thinking_text:
                            await send_event(session, _evt_thinking_chunk(thinking_text))
                    elif btype == "text":
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
                    context_limit = _get_context_limit(session.model)
                    if context_limit > 0:
                        session.context_max = context_limit
                    log.info("[%s] result success, claude_uuid=%s, context_used=%d/%d", session.session_id, new_uuid, session.context_used, session.context_max)
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

        # If compact was in progress when the proc died, clear the flag and notify frontend.
        if state.compact_in_progress:
            state.compact_in_progress = False
            if self._broadcast_fn is not None:
                asyncio.create_task(self._broadcast_fn({
                    "type": "session_command_failed",
                    "session_id": session.session_id,
                    "request_id": f"compact_{session.session_id}",
                    "error": "Process exited during compact",
                    "queue_length": 0,
                }))

        if state.bad_resume:
            state.bad_resume = False
            old_id = session.resume_id
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

    async def _idle_watchdog(self, session: "Session") -> None:
        state = self._get_state(session)
        loop = asyncio.get_event_loop()
        while True:
            await asyncio.sleep(30)
            if not session.is_streaming:
                return
            elapsed = loop.time() - session.last_activity
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
            await self._spawn_proc(session)
            return
