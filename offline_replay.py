"""Offline event replay helpers for reconnecting clients."""
from __future__ import annotations

import json
from typing import Any, Iterable


async def replay_offline_buffers(ws: Any, sessions: Iterable[Any]) -> int:
    """Replay buffered session events to a reconnecting client.

    Buffers are cleared only after their events are sent.  If the socket fails
    midway, the unsent tail is restored ahead of any events buffered during the
    replay attempt.
    """
    replayed = 0
    for session in list(sessions):
        if not session.offline_buffer:
            continue
        buf = session.offline_buffer[:]
        session.offline_buffer.clear()
        for idx, evt in enumerate(buf):
            try:
                await ws.send(json.dumps(evt))
                replayed += 1
            except Exception:
                session.offline_buffer = buf[idx:] + session.offline_buffer
                return replayed
    return replayed
