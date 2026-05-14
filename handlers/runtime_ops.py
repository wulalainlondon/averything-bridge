from __future__ import annotations

import asyncio
import json
import os


async def _collect_processes_async(limit: int = 200) -> list[dict]:
    proc = await asyncio.create_subprocess_exec(
        "ps", "-axo", "pid=,pcpu=,rss=,user=,comm=,args=",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=3)
    processes: list[dict] = []
    text = stdout.decode("utf-8", errors="replace")
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split(None, 5)
        if len(parts) < 5:
            continue
        try:
            pid = int(float(parts[0]))
            cpu = float(parts[1])
            mem = int(float(parts[2]))
        except Exception:
            continue
        if pid <= 0:
            continue
        user = parts[3] if len(parts) > 3 else ""
        comm = parts[4] if len(parts) > 4 else ""
        args = parts[5] if len(parts) > 5 else comm
        processes.append({
            "pid": pid,
            "cpu_percent": cpu,
            "mem_rss_kb": mem,
            "user": user,
            "command": comm,
            "args": args,
        })
    processes.sort(key=lambda p: (p.get("cpu_percent", 0.0), p.get("mem_rss_kb", 0)), reverse=True)
    return processes[:limit]


async def handle_runtime_msg(mtype: str, msg: dict, ws, ctx: dict) -> bool:
    if mtype == "shell_create":
        if len(ctx["shell_sessions"]) >= ctx["max_shells"]:
            try:
                await ws.send(json.dumps(ctx["msg_error"](f"Max {ctx['max_shells']} shell sessions reached")))
            except Exception:
                pass
            return True

        cwd = msg.get("cwd", os.path.expanduser("~"))
        shell_id = "sh_" + os.urandom(4).hex()
        proc = await asyncio.create_subprocess_exec(
            "/bin/bash", "-s",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=cwd if os.path.isdir(cwd) else os.path.expanduser("~"),
            env={**os.environ, "TERM": "dumb"},
        )
        shell = ctx["shell_cls"](shell_id=shell_id, proc=proc, ws_ref=ws, cwd=cwd)
        shell.read_task = asyncio.create_task(ctx["shell_reader"](shell))
        ctx["shell_sessions"][shell_id] = shell
        try:
            await ws.send(json.dumps(ctx["msg_shell_created"](shell_id)))
        except Exception:
            pass
        return True

    if mtype == "shell_input":
        shell = ctx["shell_sessions"].get(msg["shell_id"])
        if shell and shell.proc.returncode is None:
            data = (msg["data"].rstrip("\n") + "\n").encode("utf-8")
            shell.proc.stdin.write(data)
            await shell.proc.stdin.drain()
        return True

    if mtype == "shell_close":
        shell = ctx["shell_sessions"].pop(msg["shell_id"], None)
        if shell:
            try:
                shell.proc.terminate()
            except Exception:
                pass
        return True

    if mtype == "get_tasks":
        tasks = []
        for sid, s in list(ctx["sessions"].items()):
            backend = ctx["session_backend"](s)
            pid = backend.get_pid(s) if hasattr(backend, "get_pid") else None
            tasks.append({
                "id": sid,
                "name": s.name,
                "type": s.backend_name,
                "pid": pid,
                "is_streaming": s.is_streaming,
                "cwd": s.cwd,
            })
        for shid, sh in list(ctx["shell_sessions"].items()):
            tasks.append({
                "id": shid,
                "name": f"Shell {shid[-4:]}",
                "type": "shell",
                "pid": sh.proc.pid if sh.proc else None,
                "is_streaming": sh.proc.returncode is None,
                "cwd": sh.cwd,
            })
        try:
            await ws.send(json.dumps(ctx["msg_tasks_list"](tasks)))
        except Exception:
            pass
        return True

    if mtype == "kill_task":
        task_id = msg["id"]
        killed = False
        if task_id in ctx["sessions"]:
            s = ctx["sessions"][task_id]
            backend = ctx["session_backend"](s)
            if hasattr(backend, "kill_session_proc"):
                killed = backend.kill_session_proc(s)
        elif task_id in ctx["shell_sessions"]:
            sh = ctx["shell_sessions"].pop(task_id, None)
            if sh and sh.proc.returncode is None:
                sh.proc.terminate()
                killed = True
        try:
            await ws.send(json.dumps(ctx["msg_task_killed"](task_id, killed)))
        except Exception:
            pass
        return True

    if mtype == "get_processes":
        try:
            items = await _collect_processes_async(limit=200)
            await ws.send(json.dumps(ctx["msg_processes_list"](items)))
        except Exception:
            try:
                await ws.send(json.dumps(ctx["msg_processes_list"]([])))
            except Exception:
                pass
        return True

    if mtype == "kill_process":
        pid = int(msg["pid"])
        force = bool(msg.get("force", False))
        ok = False
        error_msg = ""
        try:
            os.kill(pid, 9 if force else 15)
            ok = True
        except ProcessLookupError:
            error_msg = "process_not_found"
        except PermissionError:
            error_msg = "permission_denied"
        except Exception as exc:
            error_msg = f"kill_failed: {type(exc).__name__}"
        try:
            await ws.send(json.dumps(ctx["msg_process_killed"](pid, ok, error_msg)))
        except Exception:
            pass
        return True

    return False
