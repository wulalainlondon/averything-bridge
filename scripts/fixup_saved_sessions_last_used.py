"""
One-shot fixup: rewrite saved_sessions.json with real last_used from jsonl content.

For each entry in saved_sessions.json:
1. Locate the jsonl file: ~/.claude/projects/*/{claude_uuid}.jsonl (claude)
   or ~/.codex/sessions/**/{...}.jsonl (codex)
2. Tail-read the last 32 KB to find the most recent record containing a
   'timestamp' field (ISO-8601 string).
3. Parse ISO-8601 → Unix epoch float.
4. Overwrite the entry's last_used with the real timestamp.
5. After all entries are updated, prune those older than 30 days.

Writes atomically via a .json.tmp file so a crash never corrupts the original.

Usage (from the bridge/ source directory or anywhere with Python 3.9+):
    python3 bridge/scripts/fixup_saved_sessions_last_used.py

Or after install.sh has synced the runtime:
    python3 ~/.claude-bridge-runtime/scripts/fixup_saved_sessions_last_used.py
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

SAVED = Path("~/.claude-bridge-runtime/saved_sessions.json").expanduser()
CLAUDE_ROOT = Path("~/.claude/projects").expanduser()
CODEX_ROOT = Path("~/.codex/sessions").expanduser()
CUTOFF_DAYS = 30
TAIL_BYTES = 32 * 1024  # read last 32 KB to find recent message timestamps


# ---------------------------------------------------------------------------
# Pure-function helpers (also importable by tests)
# ---------------------------------------------------------------------------

def find_jsonl(claude_uuid: str, backend: str) -> Optional[Path]:
    """Return the first matching jsonl Path for the given uuid + backend."""
    if backend == "claude":
        matches = list(CLAUDE_ROOT.glob(f"*/{claude_uuid}.jsonl"))
        return matches[0] if matches else None
    elif backend == "codex":
        # Codex stems may carry a rollout-DATE prefix; search broadly.
        matches = list(CODEX_ROOT.rglob(f"*{claude_uuid}.jsonl"))
        return matches[0] if matches else None
    return None


def last_message_ts_in_jsonl(path: Path) -> Optional[str]:
    """Tail-read *path* and return the most recently seen 'timestamp' value.

    Reads at most TAIL_BYTES from the end of the file to avoid loading huge
    conversation histories into memory.  Returns the last match (i.e., the
    most recent timestamp string), or None when no match is found.
    """
    try:
        size = path.stat().st_size
        with path.open("rb") as fh:
            fh.seek(max(0, size - TAIL_BYTES))
            tail = fh.read().decode("utf-8", errors="replace")
        matches = re.findall(r'"timestamp"\s*:\s*"([^"]+)"', tail)
        return matches[-1] if matches else None
    except Exception:
        return None


def iso_to_epoch(ts: str) -> Optional[float]:
    """Convert an ISO-8601 string (with or without trailing Z) to epoch float."""
    if not ts:
        return None
    try:
        s = ts.rstrip("Zz")
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Core fixup logic (extracted so tests can call it without touching real files)
# ---------------------------------------------------------------------------

def run_fixup(
    saved_path: Path,
    claude_root: Path = CLAUDE_ROOT,
    codex_root: Path = CODEX_ROOT,
    cutoff_days: int = CUTOFF_DAYS,
) -> dict:
    """Load *saved_path*, rewrite last_used from jsonl content, prune stale entries.

    Returns a summary dict with keys: before, updated, pruned, remaining.
    """
    try:
        with saved_path.open(encoding="utf-8") as f:
            data: dict = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: {saved_path} not found — nothing to fix.", file=sys.stderr)
        return {"before": 0, "updated": 0, "pruned": 0, "remaining": 0}
    except json.JSONDecodeError as exc:
        print(f"ERROR: cannot parse {saved_path}: {exc}", file=sys.stderr)
        return {"before": 0, "updated": 0, "pruned": 0, "remaining": 0}

    before = len(data)
    cutoff_epoch = time.time() - cutoff_days * 86400
    updated = 0
    pruned_sids: list[str] = []

    for sid in list(data.keys()):
        m = data[sid]
        claude_uuid = m.get("claude_uuid", "")
        backend = m.get("backend", "claude")
        if not claude_uuid:
            continue

        # Resolve jsonl path using the appropriate root directories.
        if backend == "claude":
            matches = list(claude_root.glob(f"*/{claude_uuid}.jsonl"))
            jsonl_path = matches[0] if matches else None
        elif backend == "codex":
            matches = list(codex_root.rglob(f"*{claude_uuid}.jsonl"))
            jsonl_path = matches[0] if matches else None
        else:
            jsonl_path = None

        if jsonl_path is None:
            continue

        ts_str = last_message_ts_in_jsonl(jsonl_path)
        epoch = iso_to_epoch(ts_str) if ts_str else None
        if epoch is None:
            continue

        m["last_used"] = epoch
        updated += 1

        if epoch < cutoff_epoch:
            pruned_sids.append(sid)

    for sid in pruned_sids:
        del data[sid]

    # Atomic write: write to .tmp then rename.
    tmp = saved_path.with_suffix(".json.tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        tmp.replace(saved_path)
    except Exception as exc:
        print(f"ERROR: failed to write {saved_path}: {exc}", file=sys.stderr)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass

    return {
        "before": before,
        "updated": updated,
        "pruned": len(pruned_sids),
        "remaining": len(data),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    result = run_fixup(SAVED)
    before = result["before"]
    updated = result["updated"]
    pruned = result["pruned"]
    remaining = result["remaining"]
    print(
        f"fixup: scanned {before} entries, "
        f"updated {updated} last_used from jsonl, "
        f"pruned {pruned} stale (older than {CUTOFF_DAYS}d)"
    )
    print(f"remaining: {remaining} entries")


if __name__ == "__main__":
    main()
