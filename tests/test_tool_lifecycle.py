from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

_REPO_ROOT = Path(__file__).parent.parent.parent
_BRIDGE_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_BRIDGE_ROOT))


def test_tool_lifecycle_emits_start_result_end(monkeypatch):
    from backends.tool_lifecycle import ToolLifecycleTracker
    import backends.tool_lifecycle as lifecycle

    sent: list[dict] = []

    async def fake_send_event(session, event):
        sent.append({**event, "session_id": session.session_id})

    monkeypatch.setattr(lifecycle, "send_event", fake_send_event)

    async def run():
        session = SimpleNamespace(session_id="s1")
        tracker = ToolLifecycleTracker()
        await tracker.start(session, "t1", "Bash", "pwd")
        await tracker.result(session, "t1", "ok")
        await tracker.end(session, "t1")
        assert tracker.active == {}

    asyncio.run(run())

    assert [event["type"] for event in sent] == ["tool_start", "tool_result", "tool_end"]
    assert sent[0]["tool_use_id"] == "t1"
    assert sent[1]["output"] == "ok"


def test_tool_lifecycle_end_all_closes_active_tools(monkeypatch):
    from backends.tool_lifecycle import ToolLifecycleTracker
    import backends.tool_lifecycle as lifecycle

    sent: list[dict] = []

    async def fake_send_event(session, event):
        sent.append(event)

    monkeypatch.setattr(lifecycle, "send_event", fake_send_event)

    async def run():
        session = SimpleNamespace(session_id="s1")
        tracker = ToolLifecycleTracker()
        await tracker.start(session, "t1", "Read", "a.txt")
        await tracker.start(session, "t2", "Bash", "ls")
        await tracker.end_all(session, "done")
        assert tracker.active == {}

    asyncio.run(run())

    assert [event["type"] for event in sent] == [
        "tool_start",
        "tool_start",
        "tool_end",
        "tool_end",
    ]
    assert [event["tool_use_id"] for event in sent if event["type"] == "tool_end"] == ["t1", "t2"]


def test_tool_lifecycle_suppressed_tools_do_not_emit_cards(monkeypatch):
    from backends.tool_lifecycle import ToolLifecycleTracker
    import backends.tool_lifecycle as lifecycle

    sent: list[dict] = []

    async def fake_send_event(session, event):
        sent.append(event)

    monkeypatch.setattr(lifecycle, "send_event", fake_send_event)

    async def run():
        session = SimpleNamespace(session_id="s1")
        tracker = ToolLifecycleTracker()
        tracker.suppress("todo1")
        await tracker.start(session, "todo1", "TodoWrite", "{}")
        await tracker.result(session, "todo1", "ignored")
        await tracker.end(session, "todo1")
        assert tracker.active == {}
        assert tracker.suppressed == set()

    asyncio.run(run())

    assert sent == []
