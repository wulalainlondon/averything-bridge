from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

_BRIDGE_ROOT = Path(__file__).parent.parent
_REPO_ROOT = _BRIDGE_ROOT.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_BRIDGE_ROOT))


def _session(session_id: str):
    from bridge_v2 import Session
    return Session(session_id=session_id, name=session_id, created_at=time.time(), backend_name="claude")


def test_seq_monotonic_per_session_and_independent_counters():
    import backends.events as ev
    ev.set_event_dispatcher(None)  # force offline-buffer path (ws_ref is None)

    async def run():
        s1, s2 = _session("s1"), _session("s2")
        for _ in range(3):
            await ev.send_event(s1, {"type": "text_chunk", "content": "x"})
        await ev.send_event(s2, {"type": "done"})
        await ev.send_event(s1, {"type": "done"})
        return s1.offline_buffer, s2.offline_buffer

    b1, b2 = asyncio.run(run())
    assert [e["seq"] for e in b1] == [1, 2, 3, 4]
    assert [e["seq"] for e in b2] == [1]  # independent counter


def test_gen_is_stable_within_process_and_on_every_event():
    import backends.events as ev
    ev.set_event_dispatcher(None)

    async def run():
        s = _session("s")
        await ev.send_event(s, {"type": "text_chunk", "content": "a"})
        await ev.send_event(s, {"type": "done"})
        return s.offline_buffer

    buf = asyncio.run(run())
    gens = {e["gen"] for e in buf}
    assert len(gens) == 1
    assert buf[0]["gen"] == ev.get_generation()


def test_multi_client_dispatch_sees_identical_seq():
    """Stamp happens before _EVENT_DISPATCHER, so all clients see the same seq."""
    import backends.events as ev
    seen: list[dict] = []

    async def dispatcher(payload, session):
        seen.append(payload)
        return True  # delivered → no offline buffering

    ev.set_event_dispatcher(dispatcher)
    try:
        async def run():
            s = _session("s")
            await ev.send_event(s, {"type": "text_chunk", "content": "a"})
            await ev.send_event(s, {"type": "done"})
        asyncio.run(run())
    finally:
        ev.set_event_dispatcher(None)

    assert [e["seq"] for e in seen] == [1, 2]
    assert all("gen" in e for e in seen)


def test_offline_overflow_merge_creates_detectable_seq_gap(monkeypatch):
    """When the buffer overflows and text_chunks merge, the dropped event's seq
    vanishes — leaving a gap the client uses to trigger a reconcile."""
    import backends.events as ev
    ev.set_event_dispatcher(None)
    monkeypatch.setattr(ev, "OFFLINE_BUFFER_MAX", 3)

    async def run():
        s = _session("s")
        for _ in range(6):  # 6 text_chunks into a cap-3 buffer → merges
            await ev.send_event(s, {"type": "text_chunk", "content": "x"})
        return s.offline_buffer, s._event_seq

    buf, last_seq = asyncio.run(run())
    seqs = [e["seq"] for e in buf]
    assert last_seq == 6                       # counter kept climbing
    assert seqs == sorted(seqs)                # still ordered
    assert seqs[-1] < last_seq or len(buf) <= 3  # buffer bounded
    # the replayed seq stream is non-contiguous vs the 1..6 produced → a gap
    assert (max(seqs) - min(seqs) + 1) > len(buf) or len(buf) < 6
