"""Backend-agnostic tool lifecycle tracker.

Contract: every emitted tool_start must eventually be followed by tool_end.
Backends may emit zero or more tool_result events in between.
"""

from dataclasses import dataclass, field

from .events import send_event, _evt_tool_start, _evt_tool_result, _evt_tool_end

_MAX_TOOL_OUTPUT = 256 * 1024  # 256 KB — mirrors claude_history.py


@dataclass
class ToolLifecycleTracker:
    active: dict[str, dict] = field(default_factory=dict)
    suppressed: set[str] = field(default_factory=set)

    async def start(self, session, tool_id: str, name: str, command: str) -> None:
        if not tool_id:
            return
        self.active[tool_id] = {"name": name, "command": command}
        if tool_id not in self.suppressed:
            await send_event(session, _evt_tool_start(tool_id, name, command))

    async def result(self, session, tool_id: str, output: str) -> None:
        if not tool_id or tool_id in self.suppressed:
            return
        if len(output) > _MAX_TOOL_OUTPUT:
            output = output[:_MAX_TOOL_OUTPUT] + "\n…(truncated)"
        await send_event(session, _evt_tool_result(tool_id, output))

    async def end(self, session, tool_id: str) -> None:
        if not tool_id:
            return
        was_active = tool_id in self.active
        self.active.pop(tool_id, None)
        suppressed = tool_id in self.suppressed
        self.suppressed.discard(tool_id)
        if was_active and not suppressed:
            await send_event(session, _evt_tool_end(tool_id))

    def suppress(self, tool_id: str) -> None:
        if tool_id:
            self.suppressed.add(tool_id)

    async def end_all(self, session, reason: str = "") -> None:
        del reason
        active_ids = list(self.active.keys())
        for tool_id in active_ids:
            await self.end(session, tool_id)
        self.suppressed.clear()

    def clear(self) -> None:
        self.active.clear()
        self.suppressed.clear()
