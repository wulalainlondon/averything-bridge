"""
auto_register.py — persist CLI-discovered sessions into saved_sessions.json.

Called from search/ingest/single_file.py after each successful ingest so that
sessions run via Claude Code CLI (never opened from the app) appear in the
dashboard session list.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


def parse_iso8601_to_epoch(ts: str) -> Optional[float]:
    """Parse an ISO-8601 timestamp string to a Unix epoch float.

    Handles the common ``'2026-05-17T08:41:31.732Z'`` format produced by
    Claude/Codex JSONL files.  Returns ``None`` on any parse failure so
    callers can fall back gracefully.
    """
    if not ts:
        return None
    try:
        from datetime import datetime, timezone
        # Strip trailing 'Z' / 'z' so fromisoformat() (Python < 3.11) can parse.
        s = ts.rstrip('Zz')
        dt = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

# _SAVED_LOCK kept as an alias for backward compatibility (nothing external imports it,
# but keeping it avoids surprises if someone references it in tests).
# The authoritative lock lives in session_registry._SAVED_SESSIONS_LOCK; we import it
# lazily to avoid a circular import at module load time.
def _get_saved_lock() -> threading.Lock:
    from session_registry import _SAVED_SESSIONS_LOCK
    return _SAVED_SESSIONS_LOCK

# Only register sessions whose last activity is within this window.  Sessions
# older than this stay in search.db (searchable) but are NOT added to
# saved_sessions.json (won't show in dashboard, won't be resumed on startup).
_RECENT_CUTOFF_DAYS = 30
_RECENT_CUTOFF_SEC = _RECENT_CUTOFF_DAYS * 86400


def auto_register_session(
    saved_path: Path,
    *,
    claude_uuid: str,
    name: str,
    cwd: Optional[str],
    backend: str,
    last_used: float,
    cutoff_seconds: float = _RECENT_CUTOFF_SEC,
) -> bool:
    """Add session to saved_sessions.json if not already present.

    Returns True if a new entry was added, False if already registered
    (or skipped due to invalid input or age cutoff).  Updates last_used if
    the stored value is older than the supplied one (subject to cutoff).

    Sessions whose last_used is more than cutoff_seconds in the past are
    silently skipped — they remain searchable in search.db but are not
    added to saved_sessions.json, so they won't be resumed on startup.
    """
    from utils.uuid_helper import is_valid_uuid

    if backend not in ('claude', 'codex'):
        return False
    if not is_valid_uuid(claude_uuid):
        # Subagent IDs (agent-XXX stems) and other non-UUID paths are skipped.
        return False
    if not name:
        return False

    # Reject sessions whose last activity is too old.
    age_sec = time.time() - last_used
    if age_sec > cutoff_seconds:
        return False  # silently skip; still indexed in search.db

    from session_registry import _saved_sessions_file_lock
    with _saved_sessions_file_lock(str(saved_path)):
        if not saved_path.exists():
            data: dict = {}
        else:
            try:
                with saved_path.open(encoding='utf-8') as f:
                    data = json.load(f)
            except Exception:
                data = {}

        # Check for existing entry by claude_uuid match
        for sid, meta in data.items():
            if meta.get('claude_uuid') == claude_uuid:
                # Already registered — update last_used if this ingest is newer,
                # but only when the new value itself is within the cutoff window.
                if last_used > meta.get('last_used', 0) and age_sec <= cutoff_seconds:
                    meta['last_used'] = last_used
                    _atomic_write_json(saved_path, data)
                return False

        # Not present — add a new entry
        prefix = 'jl_c' if backend == 'claude' else 'jl_x'
        sid = f'{prefix}_{claude_uuid[:11]}'
        # Avoid sid collision (unlikely but possible with short prefixes)
        if sid in data:
            sid = f'{prefix}_{claude_uuid[:20]}'

        data[sid] = {
            'name': name[:80],
            'claude_uuid': claude_uuid,
            'last_used': last_used,
            'cwd': cwd or '~',
            'backend': backend,
            'model': '',
            'sandbox': 'danger-full-access',
            'image_dir': '',
        }
        _atomic_write_json(saved_path, data)
        return True


def prune_old_saved_sessions(saved_path: Path, days: int = 30) -> int:
    """Drop entries from saved_sessions.json whose last_used is older than N days.

    Returns the count of removed entries.  Writes atomically via a temp file.
    Safe to call even when the file does not exist.
    """
    if not saved_path.exists():
        return 0
    from session_registry import _saved_sessions_file_lock
    with _saved_sessions_file_lock(str(saved_path)):
        try:
            with saved_path.open(encoding='utf-8') as f:
                data = json.load(f)
        except Exception:
            return 0

        cutoff = time.time() - days * 86400
        before = len(data)
        pruned = {sid: m for sid, m in data.items() if (m.get('last_used') or 0) > cutoff}
        removed = before - len(pruned)
        if removed > 0:
            import os
            import tempfile
            fd, tmp_str = tempfile.mkstemp(dir=saved_path.parent, prefix='.tmp_', suffix='.json')
            tmp = Path(tmp_str)
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    json.dump(pruned, f, indent=2, ensure_ascii=False)
                tmp.replace(saved_path)
                log.info(
                    'pruned %d stale sessions from saved_sessions.json (kept %d)',
                    removed, len(pruned),
                )
            except Exception as exc:
                log.warning('Failed to write pruned saved_sessions.json: %s', exc)
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
        return removed


def _atomic_write_json(path: Path, data: dict) -> None:
    """Write *data* to *path* via a temp file to avoid partial writes.

    Uses mkstemp() for a unique temp filename so concurrent writers in
    different processes don't clobber each other's .tmp file.
    """
    import os
    import tempfile
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_str = tempfile.mkstemp(dir=path.parent, prefix='.tmp_', suffix='.json')
    tmp = Path(tmp_str)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        tmp.replace(path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise
