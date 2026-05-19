"""
_strip_wrappers.py — Remove framework-injected XML wrapper blocks from message text.

Claude's injection mechanism wraps context in tags like <system-reminder>…</system-reminder>
and <local-command-stdout>…</local-command-stdout>. These are noise for FTS indexing.
"""
from __future__ import annotations

import re

# Complete closed-form tags: <tag>…</tag>
_WRAPPER_CLOSED = re.compile(
    r'<(system-reminder|local-command-stdout|local-command-stderr|'
    r'command-name|command-message|command-args|command-output|'
    r'command-stdout|command-stderr|environment_details|environment_context|'
    r'local-command-caveat|turn_aborted)>.*?</\1>',
    re.IGNORECASE | re.DOTALL,
)

# Unclosed / standalone opening tags: strip from the tag to the next blank line or EOF
_WRAPPER_OPEN_LINES = re.compile(
    r'^<(?:system-reminder|local-command-stdout|local-command-stderr|'
    r'command-name|command-output|local-command-caveat|environment_context|turn_aborted)[^\n]*'
    r'(?:\n(?![\r\n]).*)*',
    re.IGNORECASE | re.MULTILINE,
)


def strip_framework_wrappers(text: str) -> str:
    """Remove framework-injected wrapper blocks from message text.

    Handles both closed <tag>…</tag> and unclosed <tag>… forms.
    Returns stripped and re-stripped result; never raises.
    """
    if not text:
        return text
    out = _WRAPPER_CLOSED.sub('', text)
    out = _WRAPPER_OPEN_LINES.sub('', out)
    # Collapse multiple blank lines left behind by stripping
    out = re.sub(r'\n{3,}', '\n\n', out)
    return out.strip()
