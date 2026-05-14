#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent
MAIN = ROOT / "claude_bridge_v2.py"
MAX_MAIN_LINES = 1700

BANNED_INLINE_TYPES = {
    "get_usage",
    "get_resumable_sessions",
    "shell_create",
    "shell_input",
    "shell_close",
    "get_tasks",
    "kill_task",
    "get_processes",
    "kill_process",
    "browse_dir",
    "fcm_token",
}


def fail(msg: str) -> None:
    print(f"[governance] FAIL: {msg}")
    sys.exit(1)


def main() -> None:
    text = MAIN.read_text(encoding="utf-8")
    lines = text.count("\n") + 1
    if lines > MAX_MAIN_LINES:
        fail(f"{MAIN} has {lines} lines (budget {MAX_MAIN_LINES})")

    for mtype in sorted(BANNED_INLINE_TYPES):
        pat = re.compile(rf"elif\s+mtype\s*==\s*\"{re.escape(mtype)}\"")
        if pat.search(text):
            fail(f"inline handler branch remains for '{mtype}' in {MAIN}")

    print("[governance] PASS")


if __name__ == "__main__":
    main()
