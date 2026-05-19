"""
uuid_helper.py — UUID validation for bridge session IDs.

Claude CLI and Codex both require a canonical UUID (8-4-4-4-12 hex) when
resuming a session.  Sessions whose claude_uuid does not match this format
will be rejected before spawning the subprocess to prevent infinite retry
loops.
"""
from __future__ import annotations

import re

_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE,
)


def is_valid_uuid(s: "str | None") -> bool:
    """Return True iff *s* is a canonical UUID (8-4-4-4-12 hex groups)."""
    return bool(s and _UUID_RE.match(s))
