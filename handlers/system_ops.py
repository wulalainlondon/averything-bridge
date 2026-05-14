from __future__ import annotations

import json


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

    return False
