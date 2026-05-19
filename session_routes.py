"""Session lifecycle WebSocket routes."""
from __future__ import annotations

import os
import time
import uuid
from typing import Any

from route_utils import safe_send_json


async def handle_session_message(
    *,
    mtype: str,
    msg: dict,
    ws: Any,
    client: Any,
    ctx: Any,
) -> bool:
    if mtype == "new_session":
        sid = msg["session_id"]
        name = msg["name"]
        cwd = os.path.expanduser(msg.get("cwd") or ctx.default_cwd)
        resume_claude_id = msg.get("resume_claude_id", "")
        backend_name = ctx.normalize_backend_name(msg.get("backend"))
        effort = msg.get("effort", "")
        sandbox = str(msg.get("sandbox") or "danger-full-access")
        image_dir = str(msg.get("image_dir") or "")

        async with ctx.sessions_lock:
            if sid in ctx.sessions:
                existing = ctx.sessions[sid]
                existing.ws_ref = ws
                await safe_send_json(
                    ws,
                    ctx.msg_session_created(
                        sid,
                        existing.name,
                        existing.created_at,
                        existing.cwd,
                        existing.backend_name,
                        existing.model,
                        existing.sandbox,
                        existing.image_dir,
                    ),
                )
                return True

            if ctx.max_sessions > 0 and len(ctx.sessions) >= ctx.max_sessions:
                await safe_send_json(
                    ws,
                    ctx.msg_error(f"Maximum sessions ({ctx.max_sessions}) reached."),
                )
                return True

            session = ctx.session_cls(
                session_id=sid,
                name=name,
                created_at=time.time(),
                cwd=cwd,
                ws_ref=ws,
                resume_id=resume_claude_id or None,
                effort=effort,
                backend_name=backend_name,
                sandbox=sandbox,
                image_dir=image_dir,
            )
            ctx.sessions[sid] = session
            ctx.invalidate_sessions_cache()
            ctx.spawn_task(
                "preload-sessions-cache:new-session",
                ctx.preload_sessions_cache(ctx.backends),
            )

        if ctx.search_enabled:
            try:
                worker = ctx.get_search_worker()
                if worker is not None:
                    worker.upsert_session_metadata(
                        session_id=sid,
                        source=backend_name if backend_name in ("claude", "codex", "ollama") else "claude",
                        cwd=cwd,
                        display_name=name,
                    )
            except Exception as exc:
                ctx.log_debug("FTS5 early upsert failed (non-fatal): %s", exc)

        backend = ctx.session_backend(session)
        if resume_claude_id:
            await ctx.emit_resume_progress(session, "resume_started", 5, "Resume started")
            await ctx.emit_resume_progress(session, "resume_spawning_backend", 20, "Spawning backend")
            await backend.spawn(session)
        else:
            ctx.spawn_task(f"backend-spawn:{sid}", backend.spawn(session))

        await safe_send_json(
            ws,
            ctx.msg_session_created(
                sid,
                name,
                session.created_at,
                cwd,
                session.backend_name,
                session.model,
                session.sandbox,
                session.image_dir,
            ),
        )

        if resume_claude_id and backend.supports_resume():
            try:
                await ctx.emit_resume_progress(session, "resume_loading_history", 65, "Loading history")
                await ctx.send_session_history_response(ws, session, limit=None, mode="snapshot")
                await ctx.emit_resume_progress(session, "resume_ready", 100, "Resume ready")
            except Exception as exc:
                await ctx.emit_resume_progress(session, "resume_failed", 100, f"Resume failed: {exc}")

        ctx.persist_session_meta()
        await ctx.broadcast_json(ctx.build_sessions_list())
        return True

    if mtype == "close_session":
        sid = msg["session_id"]
        session = ctx.sessions.get(sid)
        if not session:
            await safe_send_json(ws, ctx.msg_error(f"Unknown session: {sid}", sid))
            return True
        session.ws_ref = ws

        async def _do_close(s: Any) -> None:
            await ctx.session_backend(s).close(s)
            async with ctx.sessions_lock:
                ctx.sessions.pop(s.session_id, None)
            ctx.read_cursors.pop(s.session_id, None)
            ctx.persist_read_cursors()
            ctx.persist_session_meta()
            ctx.remove_saved_session(s.session_id)
            ctx.invalidate_sessions_cache()
            ctx.spawn_task(
                "preload-sessions-cache:close-session",
                ctx.preload_sessions_cache(ctx.backends),
            )

        ctx.spawn_task(f"session-close:{sid}", _do_close(session))
        return True

    if mtype == "rename_session":
        sid = msg["session_id"]
        new_name = msg["name"]
        session = ctx.sessions.get(sid)
        if not session:
            await safe_send_json(ws, ctx.msg_error(f"Unknown session: {sid}", sid))
            return True
        session.name = new_name
        session.ws_ref = ws
        ctx.persist_session(session)
        await ctx.broadcast_json(ctx.msg_session_renamed(sid, new_name))
        return True

    if mtype == "clear_session":
        sid = msg["session_id"]
        session = ctx.sessions.get(sid)
        if not session:
            await safe_send_json(ws, ctx.msg_error(f"Unknown session: {sid}", sid))
            return True
        session.ws_ref = ws
        ctx.spawn_task(f"session-clear:{sid}", ctx.session_backend(session).clear(session))
        return True

    if mtype == "set_effort":
        sid = msg.get("session_id", "")
        effort = msg.get("effort", "")
        session = ctx.sessions.get(sid)
        if not session:
            return True
        session.effort = effort
        session.ws_ref = ws
        label = effort or "auto"
        await ctx.send_event(session, ctx.evt_session_warning(f"Effort set to {label}, restarting…"))

        async def _restart_effort(s: Any) -> None:
            backend = ctx.session_backend(s)
            await backend.stop(s)
            await backend.spawn(s)

        ctx.spawn_task(f"session-restart-effort:{sid}", _restart_effort(session))
        return True

    if mtype == "switch_session_config":
        sid = msg.get("session_id", "")
        source = ctx.sessions.get(sid)
        if not source:
            await safe_send_json(ws, ctx.msg_error(f"Unknown session: {sid}", sid))
            return True
        if source.is_streaming or source.processing:
            await ctx.send_event(source, ctx.evt_error("Session is currently processing a request.", "session_busy"))
            return True

        target_backend = ctx.normalize_backend_name(msg.get("backend") or source.backend_name)
        target_model = str(msg.get("model") or source.model or "")
        target_effort = str(msg.get("effort") if "effort" in msg else source.effort or "")
        requested_sandbox = str(msg.get("sandbox") or "")
        target_sandbox = requested_sandbox or source.sandbox or "danger-full-access"
        target_image_dir = str(msg.get("image_dir") or source.image_dir or "")
        if requested_sandbox:
            await ctx.send_event(source, ctx.evt_session_warning(
                f"Sandbox change requested ({requested_sandbox}) — will apply by creating a new session."
            ))

        transfer_history = await ctx.load_session_history_for_transfer(source, 80)
        new_sid = f"s_{uuid.uuid4().hex[:8]}"
        carry_resume = target_backend == source.backend_name
        if target_backend == "codex" and (
            target_model != (source.model or "")
            or target_effort != (source.effort or "")
            or target_sandbox != (source.sandbox or "danger-full-access")
            or target_image_dir != (source.image_dir or "")
        ):
            carry_resume = False
        new_session = ctx.session_cls(
            session_id=new_sid,
            name=f"{source.name} (switch)",
            created_at=time.time(),
            cwd=source.cwd,
            ws_ref=ws,
            resume_id=(source.resume_id if carry_resume else None),
            effort=target_effort,
            backend_name=target_backend,
            model=target_model,
            sandbox=target_sandbox,
            image_dir=target_image_dir,
        )

        async with ctx.sessions_lock:
            ctx.sessions[new_sid] = new_session

        await ctx.emit_resume_progress(new_session, "resume_spawning_backend", 20, "Spawning backend")
        await ctx.session_backend(new_session).spawn(new_session)
        await safe_send_json(
            ws,
            ctx.msg_session_created(
                new_sid,
                new_session.name,
                new_session.created_at,
                new_session.cwd,
                new_session.backend_name,
                new_session.model,
                new_session.sandbox,
                new_session.image_dir,
            ),
        )
        await ctx.broadcast_json(ctx.build_sessions_list())

        if transfer_history:
            transfer_request_id = f"r_handoff_{uuid.uuid4().hex[:8]}"
            new_session.queue.append(ctx.queued_command_cls(
                request_id=transfer_request_id,
                device_id=client.device_id,
                client_id=client.client_id,
                content=ctx.build_handoff_prompt(transfer_history),
                images=None,
                files=None,
                enqueued_at=time.time(),
            ))
            await ctx.broadcast_json({
                "type": "session_command_queued",
                "session_id": new_sid,
                "request_id": transfer_request_id,
                "device_id": client.device_id,
                "queue_position": 1,
                "queue_length": 1,
            })
            ctx.spawn_task(
                f"session-queue:{new_sid}:{transfer_request_id}",
                ctx.run_session_queue(new_session),
            )

        await safe_send_json(ws, {
            "type": "session_switched",
            "from_session_id": sid,
            "to_session_id": new_sid,
        })
        return True

    return False
