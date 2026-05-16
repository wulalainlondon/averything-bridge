#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent
MAIN = ROOT / "bridge_v2.py"
WARN_MAIN_LINES = 1700   # emit warning; handler extraction recommended
MAX_MAIN_LINES = 1750   # hard fail; no new code may be added inline

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
        fail(f"{MAIN} has {lines} lines (hard limit {MAX_MAIN_LINES})")
    elif lines > WARN_MAIN_LINES:
        print(f"[governance] WARN: {MAIN.name} has {lines} lines (alert threshold {WARN_MAIN_LINES}; extract handlers before adding more)")

    for mtype in sorted(BANNED_INLINE_TYPES):
        pat = re.compile(rf"elif\s+mtype\s*==\s*\"{re.escape(mtype)}\"")
        if pat.search(text):
            fail(f"inline handler branch remains for '{mtype}' in {MAIN}")

    print("[governance] PASS")


if __name__ == "__main__":
    main()
