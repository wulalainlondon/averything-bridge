"""
display_name.py — helpers for picking a clean, human-readable display_name
from a stream of chat messages.

Framework-generated noise (caveats, system prompts, slash commands) is filtered
out so that only genuine user-typed text is used as the session display_name.
"""
from __future__ import annotations

import re
from typing import Iterator, Optional

# ---------------------------------------------------------------------------
# Patterns that identify framework-generated content (NOT real user input)
# ---------------------------------------------------------------------------

_NOISE_PATTERNS = [
    re.compile(r'^<local-command-caveat>', re.IGNORECASE),
    re.compile(r'^This session is being continued from', re.IGNORECASE),
    re.compile(r'^#\s*AGENTS\.md\b', re.IGNORECASE),
    re.compile(r'^<environment_context>', re.IGNORECASE),
    re.compile(r'^<command-name>', re.IGNORECASE),
    re.compile(r'^<command-output>', re.IGNORECASE),
    re.compile(r'^<local-command-stdout>', re.IGNORECASE),
    re.compile(r'^<local-command-stderr>', re.IGNORECASE),
    re.compile(r'^<system-reminder>', re.IGNORECASE),
    re.compile(r'^Caveat:', re.IGNORECASE),
]

# Slash commands that are UI meta-commands, not real user messages
_SLASH_COMMANDS = {
    '/clear', '/exit', '/compact', '/init', '/help', '/quit',
    '/cost', '/model', '/status', '/login', '/logout',
}


def is_framework_noise(text: str) -> bool:
    """Return True if *text* looks like framework-generated content, not user typing.

    A message is considered noise when:
    - It is empty or whitespace-only.
    - Its stripped content matches one of the known framework-prefix patterns.
    - Its first non-empty line is a bare slash command (e.g. ``/clear``).
    """
    s = text.strip()
    if not s:
        return True
    if any(p.match(s) for p in _NOISE_PATTERNS):
        return True
    first_line = s.split('\n', 1)[0].strip()
    if first_line in _SLASH_COMMANDS:
        return True
    return False


def pick_display_name(
    messages: Iterator[dict],
    max_len: int = 80,
) -> Optional[str]:
    """Walk *messages* and return the first non-noise user message, truncated.

    Each element of *messages* must be a dict with at least:
    - ``'role'``: ``'user'`` | ``'assistant'`` | …
    - ``'text'``: the message text (may be empty or None)

    Returns ``None`` when every user message in the iterator is framework noise.
    """
    for msg in messages:
        if msg.get('role') != 'user':
            continue
        text = (msg.get('text') or '').strip()
        if is_framework_noise(text):
            continue
        cleaned = ' '.join(text.split())  # collapse internal whitespace / newlines
        if cleaned:
            return cleaned[:max_len]
    return None
