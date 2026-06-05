"""Transport-agnostic event sinks for bridge backend events."""

from __future__ import annotations

from typing import Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from bridge_v2 import Session


class EventSink(Protocol):
    async def emit(self, payload: dict, session: "Session") -> bool:
        """Deliver an already-stamped bridge event.

        Return True when delivered. Return False to let the caller fall back to
        the normal bridge dispatcher/offline buffer path.
        """


class MemoryEventSink:
    """Test sink that records delivered events without requiring WebSocket."""

    def __init__(self) -> None:
        self.events: list[dict] = []

    async def emit(self, payload: dict, session: "Session") -> bool:
        del session
        self.events.append(dict(payload))
        return True
