"""Fork session handler — copies a session's JSONL history into a new independent session."""
from __future__ import annotations

import json
import logging
import os
import shutil
import time
import uuid
from typing import Any

log = logging.getLogger(__name__)

_CLAUDE_PROJECTS_DIR = os.path.join(os.path.expanduser("~"), ".claude", "projects")
_CODEX_SESSIONS_DIR = os.path.join(os.path.expanduser("~"), ".codex", "sessions")


def _resolve_jsonl_path(session: Any) -> str | None:
    """Return the absolute .jsonl path for *session*, or None if unsupported / not found."""
    backend = getattr(session, "backend_name", "claude")
    resume_id = getattr(session, "resume_id", None)

    if backend == "claude":
        if not resume_id:
            return None
        cwd = getattr(session, "cwd", "") or ""
        mangled = "-" + cwd.lstrip("/").replace("/", "-")
        path = os.path.join(_CLAUDE_PROJECTS_DIR, mangled, resume_id + ".jsonl")
        return path if os.path.isfile(path) else None

    if backend == "codex":
        if not resume_id:
            return None
        # Codex stores sessions under ~/.codex/sessions/<...>/<resume_id[-36:]>.jsonl
        uid = resume_id[-36:]
        if not os.path.isdir(_CODEX_SESSIONS_DIR):
            return None
        for dirpath, _dirs, files in os.walk(_CODEX_SESSIONS_DIR):
            for fn in files:
                if fn.endswith(".jsonl") and fn[:-6][-36:] == uid:
                    return os.path.join(dirpath, fn)
        return None

    # ollama and other backends have no JSONL to fork
    return None


def find_fork_byte_offset(jsonl_path: str, source_message_id: str) -> int | None:
    """
    Return the byte offset immediately after the target line in the JSONL.

    source_message_id uses the bridge-internal format "claude:<uuid>:line:<N>"
    (1-indexed). Extract N and count bytes up to that line.
    """
    # Parse "claude:<uuid>:line:<N>" format used by claude_cli backend
    if ":line:" in source_message_id:
        try:
            target_line = int(source_message_id.rsplit(":line:", 1)[1])
        except (ValueError, IndexError):
            return None
        offset = 0
        with open(jsonl_path, "rb") as f:
            for line_no, line in enumerate(f, start=1):
                offset += len(line)
                if line_no == target_line:
                    return offset
        return None

    # Fallback: match by uuid field in JSONL content (other backends)
    offset = 0
    with open(jsonl_path, "rb") as f:
        for line in f:
            offset += len(line)
            try:
                data = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            uuid_val = (
                data.get("uuid")
                or data.get("message", {}).get("id")
                or data.get("id")
            )
            if uuid_val == source_message_id:
                return offset
    return None


async def handle_fork_message(
    *,
    mtype: str,
    msg: dict,
    ws: Any,
    client: Any,
    ctx: Any,
) -> bool:
    if mtype != "fork_session":
        return False

    parent_id: str = msg["session_id"]
    fork_name: str | None = msg.get("name") or None
    fork_after_message_id: str | None = msg.get("fork_after_message_id") or None

    async with ctx.sessions_lock:
        parent = ctx.sessions.get(parent_id)
        if not parent:
            from route_utils import safe_send_json
            await safe_send_json(ws, ctx.msg_error(f"Unknown session: {parent_id}", parent_id))
            return True

        if parent.is_streaming or parent.processing:
            from route_utils import safe_send_json
            await safe_send_json(ws, {
                "type": "fork_error",
                "session_id": parent_id,
                "reason": "parent_busy",
            })
            return True

        src_path = _resolve_jsonl_path(parent)
        if src_path is None:
            from route_utils import safe_send_json
            await safe_send_json(ws, {
                "type": "fork_error",
                "session_id": parent_id,
                "reason": "no_history_file",
            })
            return True

        # Build destination path: same directory as source, new UUID filename
        new_resume_id = str(uuid.uuid4())
        dst_dir = os.path.dirname(src_path)
        dst_path = os.path.join(dst_dir, new_resume_id + ".jsonl")
        tmp_path = dst_path + ".tmp"

        if fork_after_message_id:
            byte_offset = find_fork_byte_offset(src_path, fork_after_message_id)
            if byte_offset is None:
                from route_utils import safe_send_json
                await safe_send_json(ws, {
                    "type": "fork_error",
                    "session_id": parent_id,
                    "reason": "fork_point_not_found",
                })
                return True
            try:
                with open(src_path, "rb") as src_f, open(tmp_path, "wb") as dst_f:
                    remaining = byte_offset
                    while remaining > 0:
                        chunk = src_f.read(min(65536, remaining))
                        if not chunk:
                            break
                        dst_f.write(chunk)
                        remaining -= len(chunk)
                os.rename(tmp_path, dst_path)
            except Exception as exc:
                log.warning("[fork] Failed to partial-copy JSONL for session %s: %s", parent_id, exc)
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                from route_utils import safe_send_json
                await safe_send_json(ws, {
                    "type": "fork_error",
                    "session_id": parent_id,
                    "reason": f"copy_failed: {exc}",
                })
                return True
        else:
            try:
                shutil.copyfile(src_path, tmp_path)
                os.rename(tmp_path, dst_path)
            except Exception as exc:
                log.warning("[fork] Failed to copy JSONL for session %s: %s", parent_id, exc)
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                from route_utils import safe_send_json
                await safe_send_json(ws, {
                    "type": "fork_error",
                    "session_id": parent_id,
                    "reason": f"copy_failed: {exc}",
                })
                return True

        new_sid = f"s_{uuid.uuid4().hex[:8]}"
        name = fork_name or f"{parent.name} (fork)"
        now = time.time()

        fork = ctx.session_cls(
            session_id=new_sid,
            name=name,
            created_at=now,
            cwd=parent.cwd,
            backend_name=parent.backend_name,
            model=parent.model,
            effort=parent.effort,
            sandbox=parent.sandbox,
        )
        fork.resume_id = new_resume_id
        fork.parent_session_id = parent.session_id
        fork.forked_at = now

        ctx.sessions[new_sid] = fork
        ctx.persist_session(fork)

    await ctx.broadcast_json({
        "type": "session_forked",
        "session_id": new_sid,
        "parent_session_id": parent_id,
        "name": fork.name,
        "created_at": fork.created_at,
    })
    await ctx.broadcast_json(ctx.build_sessions_list())
    return True
