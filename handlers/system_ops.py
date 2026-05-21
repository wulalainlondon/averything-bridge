from __future__ import annotations

import json
import os
import time
import platform


async def handle_system_msg(mtype: str, msg: dict, ws, ctx: dict) -> bool:
    if mtype == "get_usage":
        sessions = ctx["sessions"]
        session_backend = ctx["session_backend"]
        if sessions:
            backends = {session_backend(s) for s in sessions.values()}
            for backend in backends:
                ctx["asyncio"].create_task(backend.fetch_usage(ws))
        return True

    if mtype == "get_resumable_sessions":
        backends = ctx["backends"]
        sessions = ctx["sessions"]
        msg_resumable = ctx["msg_resumable_sessions"]
        resumable = []
        for bname, backend in backends.items():
            if not backend.supports_resume():
                continue
            items = await backend.get_resumable_sessions(100)
            for item in items:
                enriched = dict(item)
                enriched.setdefault("backend", bname)
                resumable.append(enriched)
        resumable.sort(key=lambda x: int(x.get("last_used", 0)), reverse=True)
        active_uuids = {s.resume_id for s in sessions.values() if s.resume_id}
        resumable = [r for r in resumable if r.get("claude_uuid") not in active_uuids]
        try:
            await ws.send(json.dumps(msg_resumable(resumable)))
        except Exception:
            pass
        return True

    if mtype == "request_status":
        sessions = ctx["sessions"]
        streaming = sum(1 for s in sessions.values() if getattr(s, "is_streaming", False))
        queued = sum(len(getattr(s, "queue", [])) for s in sessions.values())
        payload = {
            "type": "status_result",
            "session_id": str(msg.get("session_id") or ""),
            "status": {
                "server_time_ms": int(time.time() * 1000),
                "platform": platform.platform(),
                "python_version": platform.python_version(),
                "sessions_total": len(sessions),
                "sessions_streaming": streaming,
                "queued_commands": queued,
                "permission_mode": str(ctx.get("permission_mode", "enforce")),
            },
        }
        try:
            await ws.send(json.dumps(payload))
        except Exception:
            pass
        return True

    if mtype == "restart_bridge":
        trigger_path = ctx.get("restart_trigger_path", "")
        if not trigger_path:
            try:
                await ws.send(json.dumps({"type": "error", "message": "Restart not configured on this bridge"}))
            except Exception:
                pass
            return True
        try:
            with open(trigger_path, "w") as f:
                f.write(str(time.time()))
            try:
                await ws.send(json.dumps({"type": "restart_ack"}))
            except Exception:
                pass
        except Exception as exc:
            try:
                await ws.send(json.dumps({"type": "error", "message": f"Failed to trigger restart: {exc}"}))
            except Exception:
                pass
        return True

    return False
