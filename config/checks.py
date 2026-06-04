"""
Capability checks for claude-bridge startup.
Each check returns a CheckResult named tuple.
"""
from __future__ import annotations

import platform
import json
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Literal, NamedTuple


Severity = Literal["error", "warning", "info"]


class CheckResult(NamedTuple):
    name: str
    ok: bool
    message: str
    severity: Severity


def check_python_version() -> CheckResult:
    major, minor = sys.version_info[:2]
    version_str = f"{major}.{minor}.{sys.version_info[2]}"
    if (major, minor) >= (3, 11):
        return CheckResult(
            name="Python version",
            ok=True,
            message=f"Python {version_str}",
            severity="info",
        )
    return CheckResult(
        name="Python version",
        ok=False,
        message=(
            f"Python {version_str} is too old. claude-bridge requires >= 3.11. "
            "Upgrade Python or use pyenv/conda."
        ),
        severity="error",
    )


def check_sqlite_fts5() -> CheckResult:
    sqlite_ver = sqlite3.sqlite_version
    try:
        con = sqlite3.connect(":memory:")
        con.execute("CREATE VIRTUAL TABLE _fts5_probe USING fts5(x)")
        con.close()
        return CheckResult(
            name="SQLite FTS5",
            ok=True,
            message=f"SQLite {sqlite_ver} with FTS5",
            severity="info",
        )
    except sqlite3.OperationalError:
        return CheckResult(
            name="SQLite FTS5",
            ok=False,
            message=(
                f"SQLite {sqlite_ver} does NOT have FTS5 compiled in. "
                "Fix: install pysqlite3-binary (`pip install pysqlite3-binary`) "
                "or use Homebrew Python (`brew install python`)."
            ),
            severity="error",
        )


def check_pysqlite3_fallback() -> CheckResult:
    """Check if pysqlite3-binary is importable (relevant when stdlib FTS5 is absent)."""
    try:
        import pysqlite3  # type: ignore  # noqa: F401
        return CheckResult(
            name="pysqlite3 fallback",
            ok=True,
            message="pysqlite3-binary is available as FTS5 fallback",
            severity="info",
        )
    except ImportError:
        arch = platform.machine().lower()
        system = platform.system().lower()
        if system == "linux" and ("aarch64" in arch or "arm64" in arch):
            note = (
                "Linux aarch64 has no pre-built wheel — "
                "run `pip install pysqlite3 --no-binary :all:` to build from source."
            )
        else:
            note = "Install with: pip install pysqlite3-binary"
        return CheckResult(
            name="pysqlite3 fallback",
            ok=False,
            message=f"pysqlite3-binary not installed. {note}",
            severity="warning",
        )


def check_watchdog() -> CheckResult:
    try:
        import watchdog  # type: ignore  # noqa: F401
        version = getattr(watchdog, "__version__", "unknown")
        system = platform.system()
        observer_name = {
            "Darwin": "FSEventsObserver",
            "Linux": "InotifyObserver",
            "Windows": "WindowsApiObserver",
        }.get(system, "PollingObserver")
        return CheckResult(
            name="watchdog",
            ok=True,
            message=f"watchdog {version} (using {observer_name} on {system})",
            severity="info",
        )
    except ImportError:
        return CheckResult(
            name="watchdog",
            ok=False,
            message=(
                "watchdog is not installed. File watching will be unavailable. "
                "Install with: pip install watchdog"
            ),
            severity="error",
        )


def check_inotify_limit() -> CheckResult:
    if platform.system() != "Linux":
        return CheckResult(
            name="inotify limit",
            ok=True,
            message="inotify limit not checked (not on Linux)",
            severity="info",
        )
    watch_limit_path = Path("/proc/sys/fs/inotify/max_user_watches")
    try:
        limit = int(watch_limit_path.read_text().strip())
        if limit < 8192:
            return CheckResult(
                name="inotify limit",
                ok=False,
                message=(
                    f"inotify max_user_watches={limit} is very low and may cause "
                    "missed file events. Raise it: "
                    "echo fs.inotify.max_user_watches=65536 | "
                    "sudo tee /etc/sysctl.d/inotify.conf && sudo sysctl -p /etc/sysctl.d/inotify.conf"
                ),
                severity="warning",
            )
        return CheckResult(
            name="inotify limit",
            ok=True,
            message=f"inotify max_user_watches={limit}",
            severity="info",
        )
    except Exception as exc:
        return CheckResult(
            name="inotify limit",
            ok=False,
            message=f"Could not read inotify limit: {exc}",
            severity="warning",
        )


def check_claude_projects_exists() -> CheckResult:
    projects_dir = Path("~/.claude/projects").expanduser()
    if projects_dir.exists() and projects_dir.is_dir():
        try:
            file_count = sum(1 for _ in projects_dir.rglob("*") if _.is_file())
            return CheckResult(
                name="Claude projects dir",
                ok=True,
                message=f"Claude projects dir: {file_count:,} files at {projects_dir}",
                severity="info",
            )
        except Exception:
            return CheckResult(
                name="Claude projects dir",
                ok=True,
                message=f"Claude projects dir exists at {projects_dir}",
                severity="info",
            )
    return CheckResult(
        name="Claude projects dir",
        ok=False,
        message=f"Claude projects dir not found at {projects_dir} (Claude source disabled)",
        severity="warning",
    )


def check_codex_sessions_exists() -> CheckResult:
    sessions_dir = Path("~/.codex/sessions").expanduser()
    if sessions_dir.exists() and sessions_dir.is_dir():
        try:
            file_count = sum(1 for _ in sessions_dir.rglob("*") if _.is_file())
            return CheckResult(
                name="Codex sessions dir",
                ok=True,
                message=f"Codex sessions dir: {file_count:,} files at {sessions_dir}",
                severity="info",
            )
        except Exception:
            return CheckResult(
                name="Codex sessions dir",
                ok=True,
                message=f"Codex sessions dir exists at {sessions_dir}",
                severity="info",
            )
    return CheckResult(
        name="Codex sessions dir",
        ok=False,
        message=f"Codex sessions dir not found at {sessions_dir} (Codex source disabled)",
        severity="warning",
    )


def check_codex_plugins_json() -> CheckResult:
    codex = shutil.which("codex")
    if not codex:
        return CheckResult(
            name="Codex plugins",
            ok=False,
            message="codex binary not found; cannot inspect `codex plugin list --json`",
            severity="warning",
        )
    try:
        proc = subprocess.run(
            [codex, "plugin", "list", "--json"],
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except subprocess.TimeoutExpired:
        return CheckResult(
            name="Codex plugins",
            ok=False,
            message="`codex plugin list --json` timed out",
            severity="warning",
        )
    except Exception as exc:
        return CheckResult(
            name="Codex plugins",
            ok=False,
            message=f"Could not run `codex plugin list --json`: {exc}",
            severity="warning",
        )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip().splitlines()
        suffix = f": {detail[0][:160]}" if detail else ""
        return CheckResult(
            name="Codex plugins",
            ok=False,
            message=f"`codex plugin list --json` failed{suffix}",
            severity="warning",
        )
    try:
        data = json.loads(proc.stdout or "null")
    except Exception as exc:
        return CheckResult(
            name="Codex plugins",
            ok=False,
            message=f"`codex plugin list --json` returned invalid JSON: {exc}",
            severity="warning",
        )
    if isinstance(data, list):
        count = len(data)
    elif isinstance(data, dict):
        plugins = data.get("plugins") or data.get("items")
        if isinstance(plugins, list):
            count = len(plugins)
        else:
            installed = data.get("installed")
            available = data.get("available")
            count = (
                (len(installed) if isinstance(installed, list) else 0)
                + (len(available) if isinstance(available, list) else 0)
            )
    else:
        count = 0
    return CheckResult(
        name="Codex plugins",
        ok=True,
        message=f"Codex plugin JSON diagnostics available ({count} plugins)",
        severity="info",
    )


def check_runtime_dir_writable() -> CheckResult:
    runtime_dir = Path("~/.claude-bridge-runtime").expanduser()
    try:
        runtime_dir.mkdir(parents=True, exist_ok=True)
        probe = runtime_dir / ".write_probe"
        probe.write_text("ok")
        probe.unlink()
        return CheckResult(
            name="Runtime dir writable",
            ok=True,
            message=f"Runtime dir writable: {runtime_dir}",
            severity="info",
        )
    except Exception as exc:
        return CheckResult(
            name="Runtime dir writable",
            ok=False,
            message=(
                f"Runtime dir {runtime_dir} is not writable: {exc}. "
                "Check permissions or set BRIDGE_SEARCH__INDEX_PATH to a writable location."
            ),
            severity="error",
        )


def check_disk_space() -> CheckResult:
    runtime_dir = Path("~/.claude-bridge-runtime").expanduser()
    target = runtime_dir if runtime_dir.exists() else Path.home()
    try:
        usage = shutil.disk_usage(target)
        free_mb = usage.free // (1024 * 1024)
        free_gb = free_mb / 1024
        if free_mb < 200:
            return CheckResult(
                name="Disk space",
                ok=False,
                message=(
                    f"Only {free_mb} MB free at {target}. "
                    "At least 200 MB is recommended for the search index."
                ),
                severity="warning",
            )
        display = f"{free_gb:.0f} GB" if free_gb >= 1 else f"{free_mb} MB"
        return CheckResult(
            name="Disk space",
            ok=True,
            message=f"Disk space: {display} free",
            severity="info",
        )
    except Exception as exc:
        return CheckResult(
            name="Disk space",
            ok=False,
            message=f"Could not check disk space: {exc}",
            severity="warning",
        )


ALL_CHECKS = [
    check_python_version,
    check_sqlite_fts5,
    check_pysqlite3_fallback,
    check_watchdog,
    check_inotify_limit,
    check_claude_projects_exists,
    check_codex_sessions_exists,
    check_codex_plugins_json,
    check_runtime_dir_writable,
    check_disk_space,
]
