"""
Binary / network discovery helpers.

Locate the claude / bun / codex executables and the Tailscale IP. Pure
functions — they touch no bridge_v2 module state (callers assign the resolved
paths into the CLAUDE_BIN / CODEX_BIN / BUN_BIN globals).
"""

import os
import shutil
import subprocess
import sys


def _find_claude_bin() -> str:
    env = os.environ.get("CLAUDE_PATH")
    if env and os.path.isfile(env):
        return env
    found = shutil.which("claude")
    if found:
        return found
    candidates = [
        "~/.npm-global/bin/claude",
        "~/.local/bin/claude",
        "~/.bun/bin/claude",
        r"%APPDATA%\npm\claude.cmd",
        r"%APPDATA%\npm\claude.exe",
        r"%USERPROFILE%\AppData\Roaming\npm\claude.cmd",
        r"%USERPROFILE%\AppData\Roaming\npm\claude.exe",
        "/usr/local/bin/claude",
        "/opt/homebrew/bin/claude",
    ]
    for c in candidates:
        p = os.path.expanduser(os.path.expandvars(c))
        if os.path.isfile(p):
            return p
    print("ERROR: claude binary not found. Set CLAUDE_PATH env var or ensure claude is on PATH.")
    sys.exit(1)


def _find_bun_bin() -> str:
    env = os.environ.get("BUN_PATH")
    if env and os.path.isfile(env):
        return env
    found = shutil.which("bun")
    if found:
        return found
    candidates = [
        "/opt/homebrew/bin/bun",
        "~/.bun/bin/bun",
        r"%USERPROFILE%\.bun\bin\bun.exe",
        "/usr/local/bin/bun",
    ]
    for c in candidates:
        p = os.path.expanduser(os.path.expandvars(c))
        if os.path.isfile(p):
            return p
    return "bun"


def _find_codex_bin() -> str:
    env = os.environ.get("CODEX_PATH")
    if env and os.path.isfile(env):
        return env
    found = shutil.which("codex")
    if found:
        return found
    candidates = [
        "~/.npm-global/bin/codex",
        "~/.local/bin/codex",
        r"%APPDATA%\npm\codex.cmd",
        r"%APPDATA%\npm\codex.exe",
        r"%USERPROFILE%\AppData\Roaming\npm\codex.cmd",
        r"%USERPROFILE%\AppData\Roaming\npm\codex.exe",
        "/usr/local/bin/codex",
        "/opt/homebrew/bin/codex",
    ]
    for c in candidates:
        p = os.path.expanduser(os.path.expandvars(c))
        if os.path.isfile(p):
            return p
    print("ERROR: codex binary not found. Set CODEX_PATH env var or ensure codex is on PATH.")
    sys.exit(1)


def _detect_tailscale_ip() -> "str | None":
    ts = shutil.which("tailscale")
    if not ts:
        return None
    try:
        result = subprocess.run([ts, "ip", "-4"], capture_output=True, text=True, timeout=3)
        ip = result.stdout.strip().split("\n")[0]
        return ip if ip else None
    except Exception:
        return None
