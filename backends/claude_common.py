"""
Claude backend shared leaf module.

Holds symbols needed by both the core ClaudeCliBackend and its mixins (and by
the test-suite, which imports `_ClaudeState` / `_get_context_limit` directly
from `backends.claude_cli`, which re-exports them from here). Kept dependency-
free so the mixin modules and the core module can all import it without cycles.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Optional


# All current Claude models share a 200k input context window.
# Unknown / non-Claude models return 0 (auto-compact disabled).
def _get_context_limit(model: str) -> int:
    m = (model or "").lower()
    if "claude" not in m:
        return 0
    # [1m] suffix or 1m-context variants → 1,000,000 tokens
    if "[1m]" in m or "-1m" in m or "1000000" in m:
        return 1_000_000
    return 200_000


@dataclass
class _ClaudeState:
    proc: Optional[asyncio.subprocess.Process] = field(default=None, repr=False)
    stdout_task: Optional[asyncio.Task] = field(default=None, repr=False)
    stderr_task: Optional[asyncio.Task] = field(default=None, repr=False)
    watch_task: Optional[asyncio.Task] = field(default=None, repr=False)
    timeout_task: Optional[asyncio.Task] = field(default=None, repr=False)
    tree_poll_task: Optional[asyncio.Task] = field(default=None, repr=False)
    timed_out: bool = False
    spawning: bool = False
    proc_ready_event: Optional[asyncio.Event] = field(default=None, repr=False)
    tool_blocks: dict = field(default_factory=dict)
    tool_waiting_events: dict = field(default_factory=dict)  # tool_use_id → asyncio.Event
    tool_waiting_interactions: dict = field(default_factory=dict)  # tool_use_id → request_id
    restart_count: int = 0
    pending_stop: bool = False
    bad_resume: bool = False
    compact_in_progress: bool = False
