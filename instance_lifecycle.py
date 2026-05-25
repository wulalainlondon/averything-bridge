from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import time

BRIDGE_DIR = os.path.dirname(os.path.abspath(__file__))
SUPERVISOR_SCRIPT = os.path.join(BRIDGE_DIR, "supervisor_instance.sh")

_BRIDGE_CMDLINE_MARKERS = ("bridge_v2.py", "supervisor_instance.sh", "supervisor_instance")


def _read_pid(path: str) -> int | None:
    """Read an integer PID from a file; return None on any error."""
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError, OSError):
        return None


def _pid_alive(pid: int) -> bool:
    """Return True if the process is alive (os.kill(pid, 0) succeeds)."""
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _is_bridge_process(pid: int) -> bool:
    """Return True if PID's command line contains a bridge-related marker."""
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "args="],
            capture_output=True, text=True, timeout=2,
        )
        cmdline = result.stdout
        return any(m in cmdline for m in _BRIDGE_CMDLINE_MARKERS)
    except Exception:
        return False


def _atomic_write(path: str, content: str) -> None:
    """Write content to path atomically via tmpfile + os.replace()."""
    dir_name = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(dir=dir_name)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(content)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _kill_with_escalation(pids: list[int]) -> None:
    """SIGTERM all pids; wait up to 5 s; SIGKILL survivors."""
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if not any(_pid_alive(p) for p in pids):
            break
        time.sleep(0.5)
    for pid in pids:
        if _pid_alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass


def start_instance(item: dict) -> tuple[bool, str | None]:
    """
    Ensure a bridge instance is running.

    1. Write <data_dir>/.bridge_state = "enabled" (atomically)
    2. If supervisor PID is alive, no-op.
    3. Otherwise fork supervisor_instance.sh (detached, start_new_session=True).

    Returns (True, None) on success or (False, error_code) on failure.
    """
    data_dir: str = item["data_dir"]

    if not os.path.isdir(data_dir):
        return False, "data_dir_missing"

    if not os.path.isfile(SUPERVISOR_SCRIPT):
        return False, "supervisor_not_found"

    # Mark instance as enabled (atomic write).
    state_path = os.path.join(data_dir, ".bridge_state")
    try:
        _atomic_write(state_path, "enabled")
    except OSError:
        return False, "state_write_failed"

    # Check if supervisor is already alive.
    pid_path = os.path.join(data_dir, "bridge.pid")
    existing_pid = _read_pid(pid_path)
    if existing_pid is not None and _pid_alive(existing_pid):
        return True, None

    # Build argument list.
    argv = [
        "bash",
        SUPERVISOR_SCRIPT,
        "--name", str(item["name"]),
        "--port", str(item["port"]),
        "--data-dir", str(data_dir),
    ]
    root_dir = item.get("root_dir") or ""
    if root_dir:
        argv += ["--root-dir", root_dir]
    backend = item.get("backend") or ""
    if backend:
        argv += ["--backend", backend]
    model = item.get("model") or ""
    if model:
        argv += ["--model", model]
    ollama_host = item.get("ollama_host") or ""
    if ollama_host:
        argv += ["--ollama-host", ollama_host]

    log_path = os.path.join(data_dir, "bridge.log")
    err_path = os.path.join(data_dir, "bridge.err")

    try:
        log_fh = open(log_path, "a")
        err_fh = open(err_path, "a")
        try:
            proc = subprocess.Popen(
                argv,
                stdout=log_fh,
                stderr=err_fh,
                start_new_session=True,
            )
        finally:
            # Close parent-side handles after Popen inherits them.
            log_fh.close()
            err_fh.close()
        del proc  # intentionally detached
    except Exception:
        return False, "spawn_failed"

    return True, None


def stop_instance(name: str, item: dict) -> tuple[bool, str | None]:
    """
    Stop a bridge instance.

    1. Write <data_dir>/.bridge_state = "disabled" (atomically)
    2. SIGTERM the supervisor PID; wait up to 5 s; SIGKILL if needed.
    3. Kill any verified bridge process still listening on the port.

    Returns (True, None) on success or (False, error_code) on failure.
    """
    data_dir: str = item["data_dir"]
    port: int = item["port"]

    # Mark instance as disabled (best-effort; ignore write errors).
    state_path = os.path.join(data_dir, ".bridge_state")
    try:
        _atomic_write(state_path, "disabled")
    except OSError:
        pass

    pid_path = os.path.join(data_dir, "bridge.pid")
    pid = _read_pid(pid_path)

    if pid is None or not _pid_alive(pid):
        # No supervisor — look for verified bridge process on the port.
        result = subprocess.run(
            ["lsof", "-t", f"-i:{port}", "-sTCP:LISTEN"],
            capture_output=True,
            text=True,
        )
        port_pids = [
            int(p) for p in result.stdout.split()
            if p.strip().isdigit() and _is_bridge_process(int(p))
        ]
        if not port_pids:
            return False, "not_found"
        _kill_with_escalation(port_pids)
        return True, None

    # Kill the supervisor (SIGTERM → 5 s → SIGKILL).
    try:
        _kill_with_escalation([pid])
    except Exception as exc:
        return False, "kill_failed"

    # Kill any verified bridge process still on the port.
    result = subprocess.run(
        ["lsof", "-t", f"-i:{port}", "-sTCP:LISTEN"],
        capture_output=True,
        text=True,
    )
    port_pids = [
        int(p) for p in result.stdout.split()
        if p.strip().isdigit() and _is_bridge_process(int(p))
    ]
    if port_pids:
        _kill_with_escalation(port_pids)

    return True, None


def instance_status(item: dict) -> dict:
    """
    Return the status dict for a single instance.

    State logic:
    - .bridge_state == "disabled"  → "stopped"
    - .bridge_state == "enabled" (or absent) AND supervisor PID alive → "running"
    - .bridge_state == "enabled" AND state file exists AND PID dead → "crashed"
    - pid file absent → "stopped"
    """
    data_dir: str = item["data_dir"]

    state_path = os.path.join(data_dir, ".bridge_state")
    pid_path = os.path.join(data_dir, "bridge.pid")
    bridge_v2_pid_path = os.path.join(data_dir, "bridge_v2.pid")

    try:
        with open(state_path) as f:
            bridge_state = f.read().strip()
    except FileNotFoundError:
        bridge_state = None

    supervisor_pid = _read_pid(pid_path)
    bridge_pid = _read_pid(bridge_v2_pid_path)

    if bridge_state == "disabled":
        state = "stopped"
    elif supervisor_pid is not None and _pid_alive(supervisor_pid):
        state = "running"
    elif bridge_state == "enabled" and os.path.exists(state_path):
        state = "crashed"
    else:
        state = "stopped"

    return {
        "name": item.get("name", ""),
        "port": item.get("port", 0),
        "root_dir": item.get("root_dir", ""),
        "data_dir": data_dir,
        "state": state,
        "supervisor_pid": supervisor_pid,
        "bridge_pid": bridge_pid,
    }


def list_status(items: list[dict]) -> list[dict]:
    """Return status dicts for all items."""
    return [instance_status(item) for item in items]
