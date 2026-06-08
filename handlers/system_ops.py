from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
import platform

_RESUMABLE_CACHE: tuple[float, list[dict]] = (0.0, [])
_RESUMABLE_TTL_SEC = 5.0

_GITIGNORE_DEFAULTS = """\
node_modules/
dist/
.next/
build/
out/
.env
.env.*
__pycache__/
*.pyc
.venv/
venv/
*.log
.DS_Store
.gradle/
.idea/
*.class
"""


async def handle_system_msg(mtype: str, msg: dict, ws, ctx: dict) -> bool:
    if mtype == "get_usage":
        sessions = ctx["sessions"]
        session_backend = ctx["session_backend"]
        session_id = msg.get("session_id")
        if session_id and session_id in sessions:
            backend = session_backend(sessions[session_id])
            ctx["asyncio"].create_task(backend.fetch_usage(ws))
        elif sessions:
            backends = {session_backend(s) for s in sessions.values()}
            for backend in backends:
                ctx["asyncio"].create_task(backend.fetch_usage(ws))
        return True

    if mtype == "get_resumable_sessions":
        global _RESUMABLE_CACHE
        backends = ctx["backends"]
        sessions = ctx["sessions"]
        msg_resumable = ctx["msg_resumable_sessions"]
        now = time.time()
        cached_until, cached_items = _RESUMABLE_CACHE
        if cached_until > now:
            resumable = [dict(item) for item in cached_items]
        else:
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
            _RESUMABLE_CACHE = (now + _RESUMABLE_TTL_SEC, [dict(item) for item in resumable])
        active_uuids = {s.resume_id for s in sessions.values() if s.resume_id}
        resumable = [r for r in resumable if r.get("claude_uuid") not in active_uuids]
        root_dir = ctx.get("root_dir", "")
        if root_dir:
            from utils.path_jail import is_inside_jail
            real_root = os.path.realpath(root_dir)
            resumable = [
                r for r in resumable
                if is_inside_jail(os.path.realpath(os.path.expanduser(r.get("cwd") or "~")), real_root)
            ]
        if not ctx.get("is_current_client", lambda: True)():
            return True
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

    if mtype == "get_agent_tree":
        session_id = msg.get("session_id", "")
        sessions = ctx["sessions"]
        session = sessions.get(session_id)
        if not session:
            return True
        resume_id = session.resume_id
        if not resume_id:
            return True
        backends = ctx["backends"]
        backend = backends.get(session.backend_name or "claude")
        if backend is None or not hasattr(backend, "build_agent_tree"):
            return True
        tree_data = await backend.build_agent_tree(resume_id)
        msg_builder = ctx["msg_agent_tree"]
        try:
            await ws.send(json.dumps(msg_builder(session_id, tree_data)))
        except Exception:
            pass
        return True

    if mtype == "get_git_diff":
        session_id = msg.get("session_id", "")
        sessions = ctx["sessions"]
        session = sessions.get(session_id)
        cwd = session.cwd if session else ""

        if not cwd or not os.path.isdir(cwd):
            payload = {"type": "git_diff_result", "session_id": session_id, "diff": "", "error": "no_cwd", "initialized": False}
        elif shutil.which("git") is None:
            payload = {"type": "git_diff_result", "session_id": session_id, "diff": "", "error": "git_not_found", "initialized": False}
        else:
            initialized = False
            if not os.path.isdir(os.path.join(cwd, ".git")):
                # Auto-init: write .gitignore if absent, then create baseline commit
                try:
                    gitignore_path = os.path.join(cwd, ".gitignore")
                    if not os.path.exists(gitignore_path):
                        with open(gitignore_path, "w", encoding="utf-8") as f:
                            f.write(_GITIGNORE_DEFAULTS)
                    for args in [
                        ["git", "init"],
                        ["git", "add", "-A"],
                        ["git", "-c", "user.email=bridge@local", "-c", "user.name=claude-bridge",
                         "commit", "-m", "baseline (claude-bridge)", "--allow-empty"],
                    ]:
                        proc = await asyncio.create_subprocess_exec(
                            *args, cwd=cwd,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                        )
                        await asyncio.wait_for(proc.communicate(), timeout=30)
                    initialized = True
                except Exception as exc:
                    payload = {"type": "git_diff_result", "session_id": session_id, "diff": "", "error": str(exc), "initialized": False}
                    try:
                        await ws.send(json.dumps(payload))
                    except Exception:
                        pass
                    return True

            try:
                proc = await asyncio.create_subprocess_exec(
                    "git", "diff", "HEAD",
                    cwd=cwd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
                payload = {"type": "git_diff_result", "session_id": session_id, "diff": stdout.decode("utf-8", errors="replace"), "error": None, "initialized": initialized}
            except asyncio.TimeoutError:
                payload = {"type": "git_diff_result", "session_id": session_id, "diff": "", "error": "timeout", "initialized": False}
            except Exception as exc:
                payload = {"type": "git_diff_result", "session_id": session_id, "diff": "", "error": str(exc), "initialized": False}

        try:
            await ws.send(json.dumps(payload))
        except Exception:
            pass
        return True

    return False
