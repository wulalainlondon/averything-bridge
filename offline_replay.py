"""Offline event replay helpers for reconnecting clients."""
from __future__ import annotations

import json
from typing import Any, Iterable


async def replay_offline_buffers(ws: Any, sessions: Iterable[Any]) -> int:
    """Replay buffered session events to a reconnecting client.

    Takes a snapshot of each session's offline_buffer before sending so that
    events appended by concurrent producers during replay are not lost.
    Only the successfully-sent events are removed from the live buffer;
    any events appended after the snapshot was taken are preserved.
    If the socket fails mid-replay, the unsent portion of the snapshot is
    re-prepended so the next reconnect sees them.

    All sends are serialised under session._ws_send_lock so that live events
    emitted concurrently (e.g. from an ongoing Claude stream) cannot interleave
    with the replayed frames and corrupt text_chunk ordering.
    """
    replayed = 0
    for session in list(sessions):
        if not session.offline_buffer:
            continue
        # Snapshot without clearing — other producers may still append.
        snapshot = session.offline_buffer[:]
        sent_count = 0
        async with session._ws_send_lock:
            for idx, evt in enumerate(snapshot):
                try:
                    await ws.send(json.dumps(evt))
                    sent_count += 1
                    replayed += 1
                except Exception:
                    # Remove only the events we already sent from the live buffer.
                    # The remaining front already starts with the unsent tail of
                    # the snapshot, followed by any events appended during replay.
                    del session.offline_buffer[:sent_count]
                    return replayed
        # All snapshot events sent successfully.  Remove exactly those entries
        # from the front of the live buffer (new appends stay intact).
        del session.offline_buffer[:sent_count]
    return replayed
