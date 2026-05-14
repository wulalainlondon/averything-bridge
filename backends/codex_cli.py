"""
Codex CLI backend — runs `codex exec --json` and maps events to bridge events.
This backend keeps a simple in-memory text history per session.
"""

import asyncio
import base64
import json
import logging
import os
import signal
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field
from shutil import which
from typing import Optional, TYPE_CHECKING

from .base import Backend
from .events import (
    send_event, stream_text,
    _evt_error, _evt_stopped, _evt_done, _evt_session_warning, _evt_session_closed,
    _msg_session_uuid, _msg_usage_report,
)

if TYPE_CHECKING:
    from claude_bridge_v2 import Session

log = logging.getLogger("bridge_v2")


@dataclass
class _CodexState:
    proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    read_task: Optional[asyncio.Task] = field(default=None, repr=False)
    history: list[dict] = field(default_factory=list)
    last_usage: dict = field(default_factory=dict)
    last_rate_limits: dict = field(default_factory=dict)
    usage_updated_at: float = 0.0
    temp_image_paths: list[str] = field(default_factory=list)


class CodexCliBackend(Backend):
    # Codex can emit large single-line JSON events. With asyncio's default
    # 64KiB StreamReader limit, readline() may raise:
    # "Separator is not found, and chunk exceed the limit".
    _STREAM_READER_LIMIT = 1024 * 1024  # 1 MiB

    def __init__(self, codex_bin: str):
        self._codex_bin = codex_bin
        self._states: dict[str, _CodexState] = {}
        self._saved_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "saved_sessions_codex.json",
        )
        self._codex_home = os.path.expanduser("~/.codex")
        self._native_session_index_path = os.path.join(self._codex_home, "session_index.jsonl")
        self._native_history_path = os.path.join(self._codex_home, "history.jsonl")
        self._native_sessions_root = os.path.join(self._codex_home, "sessions")
        self._session_path_index: dict[str, str] | None = None
        self._session_path_index_time: float = 0.0

    def _get_state(self, session: "Session") -> _CodexState:
        if session.session_id not in self._states:
            self._states[session.session_id] = _CodexState()
        return self._states[session.session_id]

    async def spawn(self, session: "Session") -> None:
        self._get_state(session)

    async def send(self, session: "Session", content: str,
                   images: list | None = None, files: list | None = None) -> None:

        state = self._get_state(session)
        if session.is_streaming:
            await send_event(session, _evt_error("Session is currently processing a request.", "session_busy"))
            return

        session.is_streaming = True
        session.is_stopping = False
        session.accumulated_text = ""
        session.last_activity = asyncio.get_event_loop().time()

        user_text = content or ""
        for f in (files or []):
            name = f.get("name", "file")
            body = f.get("content", "")
            user_text += f"\n\n[File: {name}]\n{body}"
        image_notes = self._prepare_image_notes(images or [], state)
        if image_notes:
            user_text += "\n\n" + "\n\n".join(image_notes)

        state.history.append({"role": "user", "content": user_text})
        prompt = self._build_prompt(state.history)

        cmd = [self._codex_bin, "exec"]
        if session.resume_id:
            # `codex exec resume` supports a narrower flag set than plain `codex exec`.
            # Passing `--sandbox` / `--cd` here causes parse error and exit code 2.
            cmd += [
                "resume",
                session.resume_id,
                "--json",
                "--skip-git-repo-check",
                "-",
            ]
        else:
            sandbox = session.sandbox if session.sandbox in {"read-only", "workspace-write", "danger-full-access"} else "workspace-write"
            cmd += [
                "--json",
                "--skip-git-repo-check",
                "--sandbox",
                sandbox,
                "--cd",
                session.cwd if os.path.isdir(session.cwd) else os.path.expanduser("~"),
                "-",
            ]
        if session.model:
            cmd += ["--model", session.model]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=self._STREAM_READER_LIMIT,
                cwd=session.cwd if os.path.isdir(session.cwd) else os.path.expanduser("~"),
            )
        except Exception as exc:
            session.is_streaming = False
            await send_event(session, _evt_error(f"Failed to spawn codex: {exc}", "spawn_failed"))
            return

        state.proc = proc
        assert proc.stdin is not None
        proc.stdin.write(prompt.encode("utf-8"))
        await proc.stdin.drain()
        proc.stdin.close()

        state.read_task = asyncio.create_task(self._consume_output(session))

    async def stop(self, session: "Session") -> None:
        state = self._get_state(session)
        session.is_stopping = True

        if state.proc is not None and state.proc.returncode is None:
            try:
                state.proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
            await asyncio.sleep(0.5)
            if state.proc.returncode is None:
                try:
                    state.proc.kill()
                except ProcessLookupError:
                    pass

        session.is_streaming = False
        await send_event(session, _evt_stopped())

    async def clear(self, session: "Session") -> None:
        state = self._get_state(session)
        await self.stop(session)
        state.history = []
        state.last_usage = {}
        session.resume_id = None
        self._delete_saved_session(session.session_id)
        await send_event(session, _evt_session_warning("Session history cleared."))

    async def close(self, session: "Session") -> None:
        await self.stop(session)
        # Removal from _SESSIONS is the bridge handler's responsibility
        self._states.pop(session.session_id, None)
        await send_event(session, _evt_session_closed())

    def get_pid(self, session: "Session") -> "int | None":
        state = self._states.get(session.session_id)
        if state and state.proc and state.proc.returncode is None:
            return state.proc.pid
        return None

    def kill_session_proc(self, session: "Session") -> bool:
        state = self._states.get(session.session_id)
        if state and state.proc and state.proc.returncode is None:
            state.proc.terminate()
            return True
        return False

    def supports_resume(self) -> bool:
        return True

    async def get_resumable_sessions(self, limit: int = 100) -> list[dict]:
        saved = self._load_saved_sessions()
        out: list[dict] = []
        seen: set[str] = set()
        for sid, entry in saved.items():
            rid = entry.get("resume_id", "")
            if rid:
                seen.add(rid)
            out.append({
                "id": sid,
                "name": entry.get("name", sid[:8]),
                "claude_uuid": rid,
                "last_used": int(entry.get("last_used", 0)),
                "cwd": entry.get("cwd", os.path.expanduser("~")),
            })

        for native in self._load_native_codex_sessions(limit=limit * 3):
            rid = native.get("claude_uuid", "")
            if not rid or rid in seen:
                continue
            seen.add(rid)
            out.append(native)

        out.sort(key=lambda x: x["last_used"], reverse=True)
        return out[:limit]

    async def load_session_history(self, resume_id: str, limit: int = 60) -> list[dict]:
        saved = self._load_saved_sessions()
        for entry in saved.values():
            if entry.get("resume_id") == resume_id:
                history = entry.get("history", [])
                if isinstance(history, list):
                    items = [m for m in history if isinstance(m, dict) and m.get("role") in ("user", "assistant")]
                    return [{"role": m["role"], "content": str(m.get("content", ""))} for m in items[-limit:]]
        native = self._load_native_session_history(resume_id=resume_id, limit=limit)
        if native:
            return native
        return []

    async def fetch_usage(self, ws) -> None:
        latest_state: _CodexState | None = None
        for state in self._states.values():
            if latest_state is None:
                latest_state = state
                continue
            if state.usage_updated_at > latest_state.usage_updated_at:
                latest_state = state

        five_hour = None
        seven_day = None
        if latest_state and latest_state.last_rate_limits:
            primary = latest_state.last_rate_limits.get("primary", {}) or {}
            secondary = latest_state.last_rate_limits.get("secondary", {}) or {}
            if primary:
                five_hour = {
                    "utilization": float(primary.get("used_percent")) if primary.get("used_percent") is not None else None,
                    "resets_at": self._to_iso8601(primary.get("resets_at")),
                }
            if secondary:
                seven_day = {
                    "utilization": float(secondary.get("used_percent")) if secondary.get("used_percent") is not None else None,
                    "resets_at": self._to_iso8601(secondary.get("resets_at")),
                }

        # Fallback: if rate-limit windows are unavailable, at least show latest token usage.
        if five_hour is None and latest_state and latest_state.last_usage:
            total = int(latest_state.last_usage.get("input_tokens", 0)) + int(latest_state.last_usage.get("cached_input_tokens", 0))
            five_hour = {"utilization": total, "resets_at": None}

        payload = _msg_usage_report(five_hour, seven_day, None)
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            pass

    async def _consume_output(self, session: "Session") -> None:

        state = self._get_state(session)
        proc = state.proc
        if proc is None or proc.stdout is None:
            session.is_streaming = False
            return

        assistant_texts: list[str] = []
        try:
            async for line_bytes in proc.stdout:
                line = line_bytes.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                except json.JSONDecodeError:
                    continue

                etype = evt.get("type", "")
                payload = evt.get("payload", {}) if isinstance(evt.get("payload"), dict) else {}
                if etype == "event_msg" and payload:
                    etype = payload.get("type", "")
                if etype == "thread.started":
                    thread_id = evt.get("thread_id")
                    if isinstance(thread_id, str) and thread_id:
                        first_uuid = session.resume_id is None
                        session.resume_id = thread_id
                        if first_uuid:
                            try:
                                if session.ws_ref:
                                    await session.ws_ref.send(json.dumps(
                                        _msg_session_uuid(session.session_id, thread_id)
                                    ))
                            except Exception:
                                pass
                if etype == "item.completed":
                    item = evt.get("item", {})
                    if item.get("type") == "agent_message":
                        text = item.get("text", "")
                        if text:
                            assistant_texts.append(text)
                            session.accumulated_text += text
                            await stream_text(text, session)
                elif etype == "turn.completed":
                    usage = evt.get("usage", {}) or {}
                    state.last_usage = {
                        "input_tokens": int(usage.get("input_tokens") or 0),
                        "cached_input_tokens": int(usage.get("cached_input_tokens") or 0),
                        "output_tokens": int(usage.get("output_tokens") or 0),
                        "reasoning_output_tokens": int(usage.get("reasoning_output_tokens") or 0),
                    }
                    state.usage_updated_at = time.time()
                    session.context_used = int(usage.get("input_tokens") or 0) + int(usage.get("cached_input_tokens") or 0)
                elif etype == "token_count":
                    info = payload.get("info", {}) if payload else {}
                    last = info.get("last_token_usage", {}) or {}
                    state.last_usage = {
                        "input_tokens": int(last.get("input_tokens") or 0),
                        "cached_input_tokens": int(last.get("cached_input_tokens") or 0),
                        "output_tokens": int(last.get("output_tokens") or 0),
                        "reasoning_output_tokens": int(last.get("reasoning_output_tokens") or 0),
                    }
                    state.last_rate_limits = payload.get("rate_limits", {}) or {}
                    state.usage_updated_at = time.time()
                    session.context_used = int(last.get("input_tokens") or 0) + int(last.get("cached_input_tokens") or 0)
                    session.context_max = int(info.get("model_context_window") or 0)

            rc = await proc.wait()
            if rc == 0:
                full = "\n\n".join(t for t in assistant_texts if t).strip()
                if full:
                    state.history.append({"role": "assistant", "content": full})
                self._save_session_snapshot(session, state)
                await send_event(session, _evt_done())
            elif not session.is_stopping:
                stderr_tail = ""
                if proc.stderr is not None:
                    try:
                        err_raw = await proc.stderr.read()
                        err_text = err_raw.decode("utf-8", errors="replace").strip()
                        if err_text:
                            lines = err_text.splitlines()
                            stderr_tail = "\n".join(lines[-4:])
                    except Exception:
                        pass
                detail = f"codex process exited with code {rc}"
                if stderr_tail:
                    detail += f": {stderr_tail}"
                await send_event(session, _evt_error(detail, "process_exit"))
        except Exception as exc:
            if not session.is_stopping:
                await send_event(session, _evt_error(f"codex stream failed: {exc}", "stream_error"))
        finally:
            session.is_streaming = False
            session.is_stopping = False
            self._cleanup_temp_images(state)

    @staticmethod
    def _to_iso8601(ts: object) -> str | None:
        if ts is None:
            return None
        try:
            return datetime.fromtimestamp(float(ts), timezone.utc).isoformat()
        except Exception:
            return None

    @staticmethod
    def _build_prompt(history: list[dict]) -> str:
        lines = [
            "You are helping through a websocket bridge chat session.",
            "Reply directly to the user's latest request.",
            "",
        ]
        for msg in history[-40:]:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            lines.append(f"{role.upper()}:")
            lines.append(str(content))
            lines.append("")
        return "\n".join(lines)

    def _prepare_image_notes(self, images: list, state: _CodexState) -> list[str]:
        notes: list[str] = []
        for i, img in enumerate(images, start=1):
            media_type = str(img.get("media_type", "image/jpeg"))
            raw_b64 = str(img.get("data", "")).strip()
            if not raw_b64:
                notes.append(f"[Image {i}] (missing data)")
                continue
            if "," in raw_b64 and raw_b64.lower().startswith("data:"):
                raw_b64 = raw_b64.split(",", 1)[1]
            try:
                blob = base64.b64decode(raw_b64, validate=False)
            except Exception:
                notes.append(f"[Image {i}] (invalid base64 data)")
                continue

            ext = self._ext_for_media_type(media_type)
            tmp_path = self._write_temp_image(blob, ext)
            if not tmp_path:
                notes.append(f"[Image {i}] (failed to persist temp image)")
                continue
            state.temp_image_paths.append(tmp_path)

            ocr_text = self._extract_image_text(tmp_path)
            section = [
                f"[Image {i}]",
                "priority: inspect the original image file at saved_path first; use ocr_text only as fallback",
                f"media_type: {media_type}",
                f"saved_path: {tmp_path}",
            ]
            if ocr_text:
                section.append("ocr_text:")
                section.append(ocr_text)
            else:
                section.append("ocr_text: (none)")
            notes.append("\n".join(section))
        return notes

    @staticmethod
    def _ext_for_media_type(media_type: str) -> str:
        mt = media_type.lower()
        if "png" in mt:
            return ".png"
        if "webp" in mt:
            return ".webp"
        if "gif" in mt:
            return ".gif"
        return ".jpg"

    @staticmethod
    def _write_temp_image(blob: bytes, ext: str) -> str | None:
        try:
            fd, path = tempfile.mkstemp(prefix="cb_codex_img_", suffix=ext)
            with os.fdopen(fd, "wb") as f:
                f.write(blob)
            return path
        except Exception:
            return None

    @staticmethod
    def _extract_image_text(path: str) -> str:
        if which("tesseract") is None:
            return ""
        try:
            proc = subprocess.run(
                ["tesseract", path, "stdout", "-l", "eng"],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if proc.returncode != 0:
                return ""
            text = (proc.stdout or "").strip()
            if not text:
                return ""
            if len(text) > 4000:
                return text[:4000].rstrip() + "\n...(truncated)"
            return text
        except Exception:
            return ""

    @staticmethod
    def _cleanup_temp_images(state: _CodexState) -> None:
        if not state.temp_image_paths:
            return
        for p in state.temp_image_paths:
            try:
                os.remove(p)
            except Exception:
                pass
        state.temp_image_paths = []

    def _load_saved_sessions(self) -> dict:
        try:
            with open(self._saved_path, encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except (FileNotFoundError, json.JSONDecodeError):
            pass
        return {}

    def _write_saved_sessions(self, data: dict) -> None:
        try:
            with open(self._saved_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            log.warning("Failed to write codex saved sessions: %s", exc)

    def _save_session_snapshot(self, session: "Session", state: _CodexState) -> None:
        saved = self._load_saved_sessions()
        saved[session.session_id] = {
            "name": session.name,
            "resume_id": session.resume_id or "",
            "last_used": int(time.time()),
            "cwd": session.cwd,
            "history": state.history[-200:],
        }
        cutoff = int(time.time()) - 90 * 24 * 3600
        saved = {k: v for k, v in saved.items() if int(v.get("last_used", 0)) > cutoff}
        if len(saved) > 200:
            saved = dict(sorted(saved.items(), key=lambda kv: int(kv[1].get("last_used", 0)), reverse=True)[:200])
        self._write_saved_sessions(saved)

    def _parse_iso_to_epoch(self, value: str | None) -> int:
        if not value:
            return 0
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return 0

    def _get_session_path_index(self) -> dict[str, str]:
        now = time.time()
        if self._session_path_index is not None and now - self._session_path_index_time < 300.0:
            return self._session_path_index
        index: dict[str, str] = {}
        if os.path.isdir(self._native_sessions_root):
            for root, _dirs, files in os.walk(self._native_sessions_root):
                for fn in files:
                    if fn.endswith(".jsonl"):
                        # filename: rollout-TIMESTAMP-{uuid}.jsonl, uuid is last 36 chars
                        uuid = fn[:-6][-36:]
                        index[uuid] = os.path.join(root, fn)
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

    def _load_native_codex_sessions(self, limit: int = 200) -> list[dict]:
        out: list[dict] = []
        if not os.path.isfile(self._native_session_index_path):
            return out
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
                    if not sid:
                        continue
                    raw_name = str(row.get("thread_name") or sid[:8])
                    out.append({
                        "id": f"native_{sid[:12]}",
                        "name": self._sanitize_session_name(raw_name, sid[:8]),
                        "claude_uuid": sid,
                        "last_used": self._parse_iso_to_epoch(str(row.get("updated_at") or "")),
                        "cwd": self._read_native_session_cwd(sid),
                    })
                    if len(out) >= limit:
                        break
        except Exception:
            return []
        return out

    def _extract_text_from_content(self, content: object) -> str:
        if isinstance(content, str):
            return content
        if not isinstance(content, list):
            return ""
        chunks: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            txt = item.get("text")
            if isinstance(txt, str) and txt.strip():
                chunks.append(txt)
        return "\n".join(chunks).strip()

    def _sanitize_session_name(self, raw: str, fallback: str) -> str:
        s = "".join(ch for ch in (raw or "") if ch.isprintable())
        s = " ".join(s.split())
        # Trim obvious spillover fragments from malformed prompt text.
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

    def _load_native_session_history(self, resume_id: str, limit: int = 60) -> list[dict]:
        path = self._find_native_session_file(resume_id)
        if not path or not os.path.isfile(path):
            return []
        items: list[dict] = []
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if row.get("type") != "response_item":
                        continue
                    payload = row.get("payload", {})
                    if not isinstance(payload, dict):
                        continue
                    if payload.get("type") != "message":
                        continue
                    role = payload.get("role")
                    if role not in {"user", "assistant"}:
                        continue
                    text = self._extract_text_from_content(payload.get("content"))
                    if not text:
                        continue
                    items.append({"role": role, "content": text})
        except Exception:
            return []
        return items[-limit:]

    def _delete_saved_session(self, session_id: str) -> None:
        saved = self._load_saved_sessions()
        if session_id in saved:
            saved.pop(session_id, None)
            self._write_saved_sessions(saved)
