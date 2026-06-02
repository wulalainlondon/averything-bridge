"""
Codex backend shared leaf module.

Holds the per-session state dataclass and pure helpers used by both the core
CodexAppServerBackend and its native-session / image mixins. Dependency-free so
all three can import it without cycles.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class _AppServerState:
    thread_id: Optional[str] = None
    current_turn_id: Optional[str] = None
    turn_active: bool = False
    turn_done_event: asyncio.Event = field(default_factory=asyncio.Event)
    turn_error: Optional[str] = None
    last_usage: dict = field(default_factory=dict)
    last_rate_limits: dict = field(default_factory=dict)
    usage_updated_at: float = 0.0
    temp_image_paths: list = field(default_factory=list)
    tool_outputs: dict[str, str] = field(default_factory=dict)
    compact_in_progress: bool = False
    compact_done_event: asyncio.Event = field(default_factory=asyncio.Event)
    compact_error: Optional[str] = None


def _ext_for_media_type(media_type: str) -> str:
    mt = media_type.lower()
    if "png" in mt:
        return ".png"
    if "webp" in mt:
        return ".webp"
    if "gif" in mt:
        return ".gif"
    return ".jpg"


def _sanitize_session_name(raw: str, fallback: str) -> str:
    s = "".join(ch for ch in (raw or "") if ch.isprintable())
    s = " ".join(s.split())
    spill_markers = [" Wait ", " needs ", " no quotes", "----", "{\"", "\"}"]
    for marker in spill_markers:
        idx = s.find(marker)
        if idx > 0:
            s = s[:idx].strip()
            break
    s = s.strip("`'\"[]{}()<>")
    if not s:
        return fallback
    if len(s) > 80:
        s = s[:80].rstrip()
    return s or fallback
