from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path
from unittest.mock import patch


_BRIDGE_ROOT = Path(__file__).parent.parent
_REPO_ROOT = _BRIDGE_ROOT.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_BRIDGE_ROOT))


def _make_session(session_id: str = "s_prompt"):
    from bridge_v2 import Session

    return Session(
        session_id=session_id,
        name="Prompt lifecycle",
        created_at=time.time(),
        backend_name="claude",
    )


def _make_cmd(request_id: str, content: str = "hello"):
    from bridge_v2 import QueuedCommand

    return QueuedCommand(
        request_id=request_id,
        device_id="device_a",
        client_id="client_a",
        content=content,
        images=None,
        files=None,
        enqueued_at=time.time(),
    )


class _Backend:
    def __init__(self, *, fail: Exception | None = None) -> None:
        self.fail = fail
        self.sent: list[tuple[str, str]] = []

    async def send(self, session, content, images, files) -> None:
        self.sent.append((session.current_request_id, content))
        if self.fail is not None:
            raise self.fail


def test_run_session_queue_sends_commands_in_order_and_marks_done():
    import bridge_v2 as bv2

    async def run():
        session = _make_session()
        session.queue.append(_make_cmd("r_1", "first"))
        session.queue.append(_make_cmd("r_2", "second"))
        backend = _Backend()
        broadcasts: list[dict] = []

        async def fake_broadcast(payload: dict) -> None:
            broadcasts.append(payload)

        with patch.object(bv2, "_session_backend", return_value=backend), \
             patch.object(bv2, "_broadcast_json", side_effect=fake_broadcast):
            await bv2._run_session_queue(session)

        return session, backend, broadcasts

    session, backend, broadcasts = asyncio.run(run())

    assert backend.sent == [("r_1", "first"), ("r_2", "second")]
    assert list(session.queue) == []
    assert session.processing is False
    assert session.current_request_id == ""
    assert session.recent_request_ids == {"r_1", "r_2"}
    assert [b["type"] for b in broadcasts] == [
        "session_command_started",
        "session_command_done",
        "session_command_started",
        "session_command_done",
    ]
    assert [b["request_id"] for b in broadcasts] == ["r_1", "r_1", "r_2", "r_2"]


def test_run_session_queue_failure_emits_error_and_failed_event_then_pops_queue():
    import bridge_v2 as bv2
    import queue_runner

    async def run():
        session = _make_session()
        session.queue.append(_make_cmd("r_fail", "explode"))
        backend = _Backend(fail=RuntimeError("backend exploded"))
        broadcasts: list[dict] = []
        sent_events: list[dict] = []

        async def fake_broadcast(payload: dict) -> None:
            broadcasts.append(payload)

        async def fake_send_event(session_arg, event: dict) -> None:
            sent_events.append({**event, "session_id": session_arg.session_id})

        with patch.object(bv2, "_session_backend", return_value=backend), \
             patch.object(bv2, "_broadcast_json", side_effect=fake_broadcast), \
             patch.object(queue_runner, "send_event", side_effect=fake_send_event):
            await bv2._run_session_queue(session)

        return session, backend, broadcasts, sent_events

    session, backend, broadcasts, sent_events = asyncio.run(run())

    assert backend.sent == [("r_fail", "explode")]
    assert list(session.queue) == []
    assert session.processing is False
    assert session.current_request_id == ""
    assert session.is_streaming is False
    assert sent_events == [
        {"type": "error", "message": "backend exploded", "session_id": "s_prompt"}
    ]
    assert [b["type"] for b in broadcasts] == [
        "session_command_started",
        "session_command_failed",
    ]
    assert broadcasts[-1]["request_id"] == "r_fail"
    assert broadcasts[-1]["message"] == "backend exploded"


def test_run_session_queue_returns_without_starting_second_runner_when_processing():
    import bridge_v2 as bv2

    async def run():
        session = _make_session()
        session.processing = True
        session.queue.append(_make_cmd("r_pending", "pending"))
        backend = _Backend()
        broadcasts: list[dict] = []

        async def fake_broadcast(payload: dict) -> None:
            broadcasts.append(payload)

        with patch.object(bv2, "_session_backend", return_value=backend), \
             patch.object(bv2, "_broadcast_json", side_effect=fake_broadcast):
            await bv2._run_session_queue(session)

        return session, backend, broadcasts

    session, backend, broadcasts = asyncio.run(run())

    assert backend.sent == []
    assert len(session.queue) == 1
    assert session.current_request_id == ""
    assert broadcasts == []


def test_send_event_injects_current_request_id_for_prompt_events():
    from backends.events import send_event, set_event_dispatcher

    async def run():
        session = _make_session()
        session.current_request_id = "r_active"
        delivered: list[dict] = []

        async def dispatcher(payload: dict, session_arg) -> bool:
            delivered.append(payload)
            assert session_arg is session
            return True

        set_event_dispatcher(dispatcher)
        try:
            await send_event(session, {"type": "text_chunk", "content": "hi"})
            await send_event(session, {"type": "done"})
        finally:
            set_event_dispatcher(None)
        return delivered

    delivered = asyncio.run(run())

    assert delivered == [
        {"type": "text_chunk", "content": "hi", "session_id": "s_prompt", "request_id": "r_active"},
        {"type": "done", "session_id": "s_prompt", "request_id": "r_active"},
    ]


def test_validate_client_msg_covers_prompt_message_contract():
    import bridge_v2 as bv2

    assert bv2.validate_client_msg({"type": "message", "session_id": "s1"}) is None
    assert bv2.validate_client_msg({"type": "user_input_response", "request_id": "ui_1"}) is None
    assert bv2.validate_client_msg({"type": "request_user_input_response", "request_id": "ui_1"}) is None
    assert bv2.validate_client_msg({"type": "choice_response", "request_id": "ui_1"}) is None
    assert bv2.validate_client_msg({"type": "form_response", "request_id": "ui_1"}) is None
    assert bv2.validate_client_msg({"type": "questions_response", "request_id": "ui_1"}) is None
    assert bv2.validate_client_msg({"type": "pending_decision_result", "request_id": "ui_1"}) is None
    assert bv2.validate_client_msg({"type": "request_user_input"}) is None
    assert bv2.validate_client_msg({"type": "choice_request"}) is None
    assert bv2.validate_client_msg({"type": "pending_interactions_list"}) is None
    assert bv2.validate_client_msg({"type": "message"}) == "'message' missing required field 'session_id'"
    assert bv2.validate_client_msg({"type": "user_input_response"}) == (
        "'user_input_response' missing required field 'request_id'"
    )
    assert bv2.validate_client_msg({"type": "message", "session_id": 123}) == (
        "'message.session_id' must be str, got int"
    )
    assert bv2.validate_client_msg({"type": "unknown"}) == "unknown message type 'unknown'"
