"""Prompt and stop WebSocket routes."""
from __future__ import annotations

import os
import re
import uuid
import time
from datetime import UTC, datetime
from typing import Any

from route_utils import safe_send_json

_AT_FILE_RE = re.compile(r'(?<!\S)@((?:\.\.?/|/)?[\w][\w.\-/]*\.\w+)')
_MAX_INJECT_BYTES = 100 * 1024
_MAX_INJECT_FILES = 8


def _inject_file_refs(text: str, cwd: str) -> str:
    injections: list[str] = []
    seen: set[str] = set()
    for m in _AT_FILE_RE.finditer(text):
        if len(injections) >= _MAX_INJECT_FILES:
            break
        ref = m.group(1)
        if ref in seen:
            continue
        seen.add(ref)
        full = ref if ref.startswith('/') else os.path.join(cwd, ref)
        try:
            with open(full, 'r', encoding='utf-8', errors='replace') as fh:
                raw = fh.read(_MAX_INJECT_BYTES + 1)
            truncated = len(raw) > _MAX_INJECT_BYTES
            if truncated:
                raw = raw[:_MAX_INJECT_BYTES]
            lang = os.path.splitext(ref)[1].lstrip('.')
            suffix = '\n...(truncated)' if truncated else ''
            injections.append(f"--- @{ref} ---\n```{lang}\n{raw}{suffix}\n```")
        except OSError:
            pass
    if not injections:
        return text
    return text + "\n\n" + "\n\n".join(injections)


async def handle_prompt_message(
    *,
    mtype: str,
    msg: dict,
    ws: Any,
    client: Any,
    ctx: Any,
) -> bool:
    if mtype == "message":
        sid = msg["session_id"]
        content = ctx.strip_turn_aborted_notice(msg.get("content", ""))
        session = ctx.sessions.get(sid)

        if not session:
            await safe_send_json(ws, ctx.msg_error(f"Unknown session: {sid}", sid))
            return True

        if content and session.cwd:
            content = _inject_file_refs(content, session.cwd)

        session.ws_ref = ws

        images = msg.get("images")
        files = msg.get("files")
        request_id = str(msg.get("request_id") or f"r_{uuid.uuid4().hex[:10]}")
        if not content and not images and not files:
            ctx.log_prompt_lifecycle("rejected_empty", session, request_id, client_id=client.client_id)
            await safe_send_json(
                ws,
                {**ctx.evt_error("Empty content"), "session_id": session.session_id, "request_id": request_id},
            )
            return True
        if (
            any(cmd.request_id == request_id for cmd in session.queue)
            or session.current_request_id == request_id
            or request_id in session.recent_request_ids
        ):
            ctx.log_prompt_lifecycle("duplicate", session, request_id, client_id=client.client_id)
            await safe_send_json(ws, {
                "type": "message_ack",
                "session_id": sid,
                "request_id": request_id,
                "status": "duplicate",
            })
            return True
        await safe_send_json(ws, {
            "type": "message_ack",
            "session_id": sid,
            "request_id": request_id,
            "status": "queued",
        })
        session.queue.append(ctx.queued_command_cls(
            request_id=request_id,
            device_id=client.device_id,
            client_id=client.client_id,
            content=content,
            images=images,
            files=files,
            enqueued_at=time.time(),
        ))
        ctx.log_prompt_lifecycle(
            "queued",
            session,
            request_id,
            client_id=client.client_id,
            device_id=client.device_id,
            content_len=len(content),
            image_count=len(images) if isinstance(images, list) else 0,
            file_count=len(files) if isinstance(files, list) else 0,
        )
        await ctx.broadcast_json({
            "type": "session_command_queued",
            "session_id": sid,
            "request_id": request_id,
            "device_id": client.device_id,
            "queue_position": len(session.queue),
            "queue_length": len(session.queue),
        })
        ctx.spawn_task(f"session-queue:{sid}:{request_id}", ctx.run_session_queue(session))

        if ctx.search_enabled and not session._fts_first_msg_indexed and content:
            session._fts_first_msg_indexed = True
            msg_uuid = f"live_{uuid.uuid4().hex}"
            msg_ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            content_snap = content

            async def _index_first_msg(
                _sid: str = sid,
                _muuid: str = msg_uuid,
                _ts: str = msg_ts,
                _text: str = content_snap,
            ) -> None:
                try:
                    worker = ctx.get_search_worker()
                    if worker is not None:
                        worker.upsert_first_user_message(
                            session_id=_sid,
                            content=_text,
                            msg_uuid=_muuid,
                            ts=_ts,
                        )
                except Exception as exc:
                    ctx.log_debug("FTS5 first-msg index failed (non-fatal): %s", exc)

            ctx.spawn_task(f"search-first-message:{sid}:{request_id}", _index_first_msg())
        return True

    if mtype == "stop":
        sid = msg["session_id"]
        session = ctx.sessions.get(sid)
        if not session:
            await safe_send_json(ws, ctx.msg_error(f"Unknown session: {sid}", sid))
            return True
        session.ws_ref = ws

        async def _do_stop(s: Any) -> None:
            queued_before = list(s.queue)
            s.queue.clear()
            pending = queued_before[1:] if s.processing else queued_before
            remain = len(pending)
            for cmd in pending:
                remain = max(0, remain - 1)
                await ctx.broadcast_json({
                    "type": "session_command_failed",
                    "session_id": s.session_id,
                    "request_id": cmd.request_id,
                    "message": "Cancelled by stop",
                    "queue_length": remain,
                })
            await ctx.session_backend(s).stop(s)

        ctx.spawn_task(f"session-stop:{sid}", _do_stop(session))
        return True

    return False
