from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path


_BRIDGE_ROOT = Path(__file__).parent.parent
_REPO_ROOT = _BRIDGE_ROOT.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_BRIDGE_ROOT))


class _Ws:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))


class _Client:
    client_id = "client_a"
    device_id = "device_a"
    last_seen = 0.0


def _ctx(**overrides):
    from message_router import RouterContext

    calls = {
        "broadcasts": [],
        "closed_duplicates": [],
        "history_requests": [],
        "invalidated": 0,
        "lifecycle": [],
        "marked_read": [],
        "persisted": 0,
        "persisted_cursors": 0,
        "persisted_sessions": [],
        "removed_saved": [],
        "resume_progress": [],
        "spawned": [],
        "sent_events": [],
        "unread_client": [],
        "unread_snapshot": [],
        "warnings": [],
        "debug": [],
    }

    async def broadcast(payload: dict) -> int:
        calls["broadcasts"].append(payload)
        return 1

    def persist() -> None:
        calls["persisted"] += 1

    def spawn(name, coro):
        calls["spawned"].append((name, coro))
        coro.close()
        return None

    async def push_file(ws, path: str, device_id: str) -> None:
        return None

    async def push_ack(file_id: str, device_id: str) -> None:
        return None

    async def send_all(ws) -> None:
        return None

    async def send_unread_snapshot(ws, client) -> None:
        calls["unread_snapshot"].append((client.client_id, client.device_id))

    async def send_unread_for_client_session(ws, client, session) -> None:
        calls["unread_client"].append((client.client_id, client.device_id, session.session_id))

    def mark_read(session_id: str, device_id: str, seq: int) -> None:
        calls["marked_read"].append((session_id, device_id, seq))

    def persist_read_cursors() -> None:
        calls["persisted_cursors"] += 1

    async def send_history_response(ws, session, **kwargs) -> None:
        calls["history_requests"].append((session.session_id, kwargs))
        await ws.send(json.dumps({"type": "session_history", "session_id": session.session_id, "messages": ["loaded"]}))

    def history_runtime_payload(session):
        return {"streaming": bool(session.is_streaming or session.processing)}

    async def emit_resume_progress(session, stage, progress=None, message=None) -> None:
        calls["resume_progress"].append((session.session_id, stage, progress, message))

    async def close_duplicate_device_clients(ws, device_id: str) -> int:
        calls["closed_duplicates"].append(device_id)
        return 0

    class _Backend:
        async def spawn(self, session) -> None:
            calls.setdefault("backend_spawned", []).append(session.session_id)

        async def stop(self, session) -> None:
            calls.setdefault("backend_stopped", []).append(session.session_id)

        async def close(self, session) -> None:
            calls.setdefault("backend_closed", []).append(session.session_id)

        async def clear(self, session) -> None:
            calls.setdefault("backend_cleared", []).append(session.session_id)

        def supports_resume(self) -> bool:
            return True

    class _SearchWorker:
        def upsert_first_user_message(self, **kwargs) -> None:
            calls.setdefault("indexed_first_messages", []).append(kwargs)

    async def send_event(session, event: dict) -> None:
        calls["sent_events"].append((session.session_id, event))

    def persist_session(session) -> None:
        calls["persisted_sessions"].append(session.session_id)

    def remove_saved_session(session_id: str) -> None:
        calls["removed_saved"].append(session_id)

    def invalidate_sessions_cache() -> None:
        calls["invalidated"] += 1

    async def preload_sessions_cache(backends) -> None:
        calls.setdefault("preloaded", 0)
        calls["preloaded"] += 1

    async def load_session_history_for_transfer(session, limit: int):
        calls.setdefault("transfer_loads", []).append((session.session_id, limit))
        return []

    async def run_session_queue(session) -> None:
        calls.setdefault("queue_runs", []).append(session.session_id)

    def build_handoff_prompt(history):
        calls.setdefault("handoffs", []).append(history)
        return "handoff"

    from session_registry import QueuedCommand, Session

    ctx = RouterContext(
        sessions={},
        sessions_lock=asyncio.Lock(),
        build_sessions_list=lambda: {"type": "sessions_list", "sessions": []},
        broadcast_json=broadcast,
        persist_session_meta=persist,
        send_all_sessions=send_all,
        spawn_task=spawn,
        handle_push_file=push_file,
        handle_file_push_ack=push_ack,
        msg_pong=lambda: {"type": "pong"},
        msg_session_history=lambda session_id, messages, **kw: {
            "type": "session_history",
            "session_id": session_id,
            "messages": messages,
            **kw,
        },
        send_unread_snapshot=send_unread_snapshot,
        send_unread_for_client_session=send_unread_for_client_session,
        mark_read=mark_read,
        persist_read_cursors=persist_read_cursors,
        send_session_history_response=send_history_response,
        history_runtime_payload=history_runtime_payload,
        emit_resume_progress=emit_resume_progress,
        close_duplicate_device_clients=close_duplicate_device_clients,
        log_warning=lambda *args: calls["warnings"].append(args),
        log_debug=lambda *args: calls["debug"].append(args),
        max_sessions=0,
        default_cwd="/tmp",
        normalize_backend_name=lambda raw: str(raw or "claude"),
        session_cls=Session,
        queued_command_cls=QueuedCommand,
        msg_session_created=lambda sid, name, created_at, cwd, backend, model, sandbox, image_dir: {
            "type": "session_created",
            "session_id": sid,
            "name": name,
            "created_at": created_at,
            "cwd": cwd,
            "backend": backend,
            "model": model,
            "sandbox": sandbox,
            "image_dir": image_dir,
        },
        msg_error=lambda message, session_id=None: {
            "type": "error",
            "message": message,
            **({"session_id": session_id} if session_id else {}),
        },
        msg_session_renamed=lambda sid, name: {"type": "session_renamed", "session_id": sid, "name": name},
        session_backend=lambda session: _Backend(),
        send_event=send_event,
        evt_session_warning=lambda message: {"type": "session_warning", "message": message},
        evt_error=lambda message, code=None: {"type": "error", "message": message, "code": code},
        persist_session=persist_session,
        read_cursors={},
        remove_saved_session=remove_saved_session,
        invalidate_sessions_cache=invalidate_sessions_cache,
        preload_sessions_cache=preload_sessions_cache,
        backends={},
        load_session_history_for_transfer=load_session_history_for_transfer,
        build_handoff_prompt=build_handoff_prompt,
        run_session_queue=run_session_queue,
        search_enabled=False,
        get_search_worker=lambda: _SearchWorker(),
        strip_turn_aborted_notice=lambda text: text.replace("<turn_aborted>old</turn_aborted>", ""),
        log_prompt_lifecycle=lambda stage, session, request_id, **fields: calls["lifecycle"].append(
            (stage, session.session_id, request_id, fields)
        ),
    )
    for key, value in overrides.items():
        setattr(ctx, key, value)
    return ctx, calls


def test_router_handles_ping_and_updates_last_seen():
    from message_router import handle_low_coupling_message

    async def run():
        ws = _Ws()
        client = _Client()
        client.last_seen = 1.0
        ctx, _calls = _ctx()
        handled = await handle_low_coupling_message(
            mtype="ping",
            msg={"type": "ping"},
            ws=ws,
            client=client,
            ctx=ctx,
        )
        return handled, ws.sent, client.last_seen

    handled, sent, last_seen = asyncio.run(run())

    assert handled is True
    assert sent == [{"type": "pong"}]
    assert last_seen > 1.0


def test_router_handles_request_sessions_list():
    from message_router import handle_low_coupling_message

    async def run():
        ws = _Ws()
        ctx, _calls = _ctx(build_sessions_list=lambda: {"type": "sessions_list", "sessions": [{"id": "s1"}]})
        handled = await handle_low_coupling_message(
            mtype="request_sessions_list",
            msg={"type": "request_sessions_list"},
            ws=ws,
            client=_Client(),
            ctx=ctx,
        )
        return handled, ws.sent

    handled, sent = asyncio.run(run())

    assert handled is True
    assert sent == [{"type": "sessions_list", "sessions": [{"id": "s1"}]}]


def test_router_handles_hello_updates_client_and_sends_ack_unread_snapshot():
    from message_router import handle_low_coupling_message

    async def run():
        ws = _Ws()
        client = _Client()
        ctx, calls = _ctx()
        handled = await handle_low_coupling_message(
            mtype="hello",
            msg={"type": "hello", "device_id": "phone", "device_name": "Pixel"},
            ws=ws,
            client=client,
            ctx=ctx,
        )
        return handled, ws.sent, client.device_id, client.device_name, calls

    handled, sent, device_id, device_name, calls = asyncio.run(run())

    assert handled is True
    assert device_id == "phone"
    assert device_name == "Pixel"
    assert sent == [{
        "type": "hello_ack",
        "client_id": "client_a",
        "device_id": "phone",
        "device_name": "Pixel",
        "is_locked": False,
        "locked_to_me": False,
        "instance_name": "",
        "root_dir": "",
        "data_dir": "",
    }]
    assert calls["closed_duplicates"] == ["phone"]
    assert calls["unread_snapshot"] == []
    assert [name for name, _coro in calls["spawned"]] == ["unread-snapshot:hello:client_a"]


def test_router_request_history_missing_session_sends_empty_history():
    from message_router import handle_low_coupling_message

    async def run():
        ws = _Ws()
        ctx, calls = _ctx()
        handled = await handle_low_coupling_message(
            mtype="request_history",
            msg={"type": "request_history", "session_id": "missing"},
            ws=ws,
            client=_Client(),
            ctx=ctx,
        )
        return handled, ws.sent, calls

    handled, sent, calls = asyncio.run(run())

    assert handled is True
    assert sent == [{
        "type": "session_history",
        "session_id": "missing",
        "messages": [],
        "source_count": 0,
        "has_more_before": False,
        "runtime": None,
    }]
    assert calls["history_requests"] == []


def test_router_request_history_marks_read_and_loads_resumable_history():
    from message_router import handle_low_coupling_message
    from session_registry import Session

    async def run():
        ws = _Ws()
        session = Session(
            session_id="s1",
            name="One",
            created_at=time.time(),
            resume_id="12345678-1234-4234-9234-123456789abc",
        )
        session.message_seq = 8
        ctx, calls = _ctx(sessions={"s1": session})
        handled = await handle_low_coupling_message(
            mtype="request_history",
            msg={
                "type": "request_history",
                "session_id": "s1",
                "limit": 20,
                "known_last_source_message_id": "m1",
                "mode": "delta",
                "before_source_message_id": "",
            },
            ws=ws,
            client=_Client(),
            ctx=ctx,
        )
        return handled, ws.sent, calls

    handled, sent, calls = asyncio.run(run())

    assert handled is True
    assert calls["marked_read"] == [("s1", "device_a", 8)]
    assert calls["persisted_cursors"] == 1
    assert calls["unread_client"] == [("client_a", "device_a", "s1")]
    assert calls["history_requests"][0][0] == "s1"
    assert calls["history_requests"][0][1]["limit"] == 20
    assert calls["resume_progress"] == [
        ("s1", "resume_started", 5, "Resume started"),
        ("s1", "resume_loading_history", 35, "Loading history"),
        ("s1", "resume_ready", 100, "Resume ready"),
    ]
    assert sent == [{"type": "session_history", "session_id": "s1", "messages": ["loaded"]}]


def test_router_handles_set_session_meta_and_broadcasts_update():
    from message_router import handle_low_coupling_message
    from session_registry import Session

    async def run():
        session = Session(session_id="s1", name="One", created_at=time.time())
        ctx, calls = _ctx(
            sessions={"s1": session},
            build_sessions_list=lambda: {"type": "sessions_list", "sessions": [{"id": "s1"}]},
        )
        handled = await handle_low_coupling_message(
            mtype="set_session_meta",
            msg={"type": "set_session_meta", "session_id": "s1", "pinned": True, "hidden": True},
            ws=_Ws(),
            client=_Client(),
            ctx=ctx,
        )
        return handled, session, calls

    handled, session, calls = asyncio.run(run())

    assert handled is True
    assert session.pinned is True
    assert session.hidden is True
    assert calls["persisted"] == 1
    assert calls["broadcasts"] == [
        {"type": "session_meta_updated", "session_id": "s1", "pinned": True, "hidden": True},
        {"type": "sessions_list", "sessions": [{"id": "s1"}]},
    ]


def test_router_creates_new_session_and_defers_backend_spawn():
    from message_router import handle_low_coupling_message

    async def run():
        ws = _Ws()
        sessions = {}
        ctx, calls = _ctx(
            sessions=sessions,
            build_sessions_list=lambda: {"type": "sessions_list", "sessions": [{"id": "s_new"}]},
        )
        handled = await handle_low_coupling_message(
            mtype="new_session",
            msg={"type": "new_session", "session_id": "s_new", "name": "New", "backend": "codex"},
            ws=ws,
            client=_Client(),
            ctx=ctx,
        )
        return handled, ws.sent, sessions, calls

    handled, sent, sessions, calls = asyncio.run(run())

    assert handled is True
    assert "s_new" in sessions
    assert sent[0]["type"] == "session_created"
    assert sent[0]["session_id"] == "s_new"
    assert sessions["s_new"].backend_name == "codex"
    assert calls["invalidated"] == 1
    assert calls["persisted"] == 1
    assert [name for name, _coro in calls["spawned"]] == [
        "preload-sessions-cache:new-session",
        "backend-spawn:s_new",
    ]
    assert calls["broadcasts"] == [{"type": "sessions_list", "sessions": [{"id": "s_new"}]}]


def test_router_rejects_new_session_when_limit_reached():
    from message_router import handle_low_coupling_message
    from session_registry import Session

    async def run():
        ws = _Ws()
        ctx, calls = _ctx(
            sessions={"existing": Session(session_id="existing", name="Existing", created_at=time.time())},
            max_sessions=1,
        )
        handled = await handle_low_coupling_message(
            mtype="new_session",
            msg={"type": "new_session", "session_id": "s_new", "name": "New"},
            ws=ws,
            client=_Client(),
            ctx=ctx,
        )
        return handled, ws.sent, calls

    handled, sent, calls = asyncio.run(run())

    assert handled is True
    assert sent == [{"type": "error", "message": "Maximum sessions (1) reached."}]
    assert calls["spawned"] == []


def test_router_renames_and_persists_session():
    from message_router import handle_low_coupling_message
    from session_registry import Session

    async def run():
        session = Session(session_id="s1", name="Old", created_at=time.time())
        ctx, calls = _ctx(sessions={"s1": session})
        handled = await handle_low_coupling_message(
            mtype="rename_session",
            msg={"type": "rename_session", "session_id": "s1", "name": "New"},
            ws=_Ws(),
            client=_Client(),
            ctx=ctx,
        )
        return handled, session, calls

    handled, session, calls = asyncio.run(run())

    assert handled is True
    assert session.name == "New"
    assert calls["persisted_sessions"] == ["s1"]
    assert calls["broadcasts"] == [{"type": "session_renamed", "session_id": "s1", "name": "New"}]


def test_router_set_effort_sends_warning_and_restarts_backend():
    from message_router import handle_low_coupling_message
    from session_registry import Session

    async def run():
        session = Session(session_id="s1", name="One", created_at=time.time())
        ctx, calls = _ctx(sessions={"s1": session})
        held_coros = []

        def hold_spawn(name, coro):
            calls["spawned"].append((name, coro))
            held_coros.append(coro)
            return None

        ctx.spawn_task = hold_spawn
        handled = await handle_low_coupling_message(
            mtype="set_effort",
            msg={"type": "set_effort", "session_id": "s1", "effort": "high"},
            ws=_Ws(),
            client=_Client(),
            ctx=ctx,
        )
        await held_coros[0]
        return handled, session, calls

    handled, session, calls = asyncio.run(run())

    assert handled is True
    assert session.effort == "high"
    assert calls["sent_events"] == [("s1", {"type": "session_warning", "message": "Effort set to high, restarting…"})]
    assert calls["backend_stopped"] == ["s1"]
    assert calls["backend_spawned"] == ["s1"]


def test_router_switch_session_config_creates_handoff_session():
    from message_router import handle_low_coupling_message
    from session_registry import Session

    async def run():
        ws = _Ws()
        source = Session(
            session_id="s1",
            name="One",
            created_at=time.time(),
            cwd="/work",
            resume_id="resume-1",
            backend_name="codex",
            model="gpt-5",
            effort="low",
        )
        sessions = {"s1": source}
        ctx, calls = _ctx(
            sessions=sessions,
            build_sessions_list=lambda: {"type": "sessions_list", "sessions": [{"id": sid} for sid in sessions]},
        )

        async def load_transfer(session, limit: int):
            calls.setdefault("transfer_loads", []).append((session.session_id, limit))
            return [{"role": "user", "content": "previous"}]

        ctx.load_session_history_for_transfer = load_transfer
        handled = await handle_low_coupling_message(
            mtype="switch_session_config",
            msg={"type": "switch_session_config", "session_id": "s1", "model": "gpt-5.1", "effort": "high"},
            ws=ws,
            client=_Client(),
            ctx=ctx,
        )
        new_sid = next(sid for sid in sessions if sid != "s1")
        return handled, ws.sent, sessions[new_sid], calls

    handled, sent, new_session, calls = asyncio.run(run())

    assert handled is True
    assert new_session.name == "One (switch)"
    assert new_session.resume_id is None
    assert new_session.model == "gpt-5.1"
    assert new_session.effort == "high"
    assert new_session.queue[0].content == "handoff"
    assert sent[-1]["type"] == "session_switched"
    assert sent[-1]["to_session_id"] == new_session.session_id
    assert calls["backend_spawned"] == [new_session.session_id]
    assert any(payload["type"] == "session_command_queued" for payload in calls["broadcasts"])


def test_router_message_enqueues_prompt_and_spawns_queue_runner():
    from message_router import handle_low_coupling_message
    from session_registry import Session

    async def run():
        ws = _Ws()
        session = Session(session_id="s1", name="One", created_at=time.time())
        ctx, calls = _ctx(sessions={"s1": session})
        handled = await handle_low_coupling_message(
            mtype="message",
            msg={
                "type": "message",
                "session_id": "s1",
                "request_id": "r_1",
                "content": "<turn_aborted>old</turn_aborted>hello",
            },
            ws=ws,
            client=_Client(),
            ctx=ctx,
        )
        return handled, ws.sent, session, calls

    handled, sent, session, calls = asyncio.run(run())

    assert handled is True
    assert sent == [{
        "type": "message_ack",
        "session_id": "s1",
        "request_id": "r_1",
        "status": "queued",
    }]
    assert len(session.queue) == 1
    assert session.queue[0].content == "hello"
    assert calls["broadcasts"] == [{
        "type": "session_command_queued",
        "session_id": "s1",
        "request_id": "r_1",
        "device_id": "device_a",
        "queue_position": 1,
        "queue_length": 1,
    }]
    assert [name for name, _coro in calls["spawned"]] == ["session-queue:s1:r_1"]
    assert calls["lifecycle"][0][0] == "queued"


def test_router_message_rejects_empty_content_after_strip():
    from message_router import handle_low_coupling_message
    from session_registry import Session

    async def run():
        ws = _Ws()
        session = Session(session_id="s1", name="One", created_at=time.time())
        ctx, calls = _ctx(sessions={"s1": session})
        handled = await handle_low_coupling_message(
            mtype="message",
            msg={
                "type": "message",
                "session_id": "s1",
                "request_id": "r_empty",
                "content": "<turn_aborted>old</turn_aborted>",
            },
            ws=ws,
            client=_Client(),
            ctx=ctx,
        )
        return handled, ws.sent, session, calls

    handled, sent, session, calls = asyncio.run(run())

    assert handled is True
    assert list(session.queue) == []
    assert sent == [{
        "type": "error",
        "message": "Empty content",
        "code": None,
        "session_id": "s1",
        "request_id": "r_empty",
    }]
    assert calls["lifecycle"][0][0] == "rejected_empty"


def test_router_message_duplicate_request_acknowledges_without_enqueue():
    from message_router import handle_low_coupling_message
    from session_registry import QueuedCommand, Session

    async def run():
        ws = _Ws()
        session = Session(session_id="s1", name="One", created_at=time.time())
        session.queue.append(QueuedCommand(
            request_id="r_dup",
            device_id="device_a",
            client_id="client_a",
            content="queued",
            images=None,
            files=None,
            enqueued_at=time.time(),
        ))
        ctx, calls = _ctx(sessions={"s1": session})
        handled = await handle_low_coupling_message(
            mtype="message",
            msg={"type": "message", "session_id": "s1", "request_id": "r_dup", "content": "again"},
            ws=ws,
            client=_Client(),
            ctx=ctx,
        )
        return handled, ws.sent, session, calls

    handled, sent, session, calls = asyncio.run(run())

    assert handled is True
    assert len(session.queue) == 1
    assert sent == [{
        "type": "message_ack",
        "session_id": "s1",
        "request_id": "r_dup",
        "status": "duplicate",
    }]
    assert calls["spawned"] == []
    assert calls["lifecycle"][0][0] == "duplicate"


def test_router_message_indexes_first_user_message_when_search_enabled():
    from message_router import handle_low_coupling_message
    from session_registry import Session

    async def run():
        ws = _Ws()
        session = Session(session_id="s1", name="One", created_at=time.time())
        ctx, calls = _ctx(sessions={"s1": session}, search_enabled=True)
        held_coros = []

        def hold_spawn(name, coro):
            calls["spawned"].append((name, coro))
            held_coros.append(coro)
            return None

        ctx.spawn_task = hold_spawn
        handled = await handle_low_coupling_message(
            mtype="message",
            msg={"type": "message", "session_id": "s1", "request_id": "r_1", "content": "hello"},
            ws=ws,
            client=_Client(),
            ctx=ctx,
        )
        await held_coros[1]
        held_coros[0].close()
        return handled, session, calls

    handled, session, calls = asyncio.run(run())

    assert handled is True
    assert session._fts_first_msg_indexed is True
    assert [name for name, _coro in calls["spawned"]] == [
        "session-queue:s1:r_1",
        "search-first-message:s1:r_1",
    ]
    assert calls["indexed_first_messages"][0]["session_id"] == "s1"
    assert calls["indexed_first_messages"][0]["content"] == "hello"


def test_router_stop_clears_pending_queue_and_stops_backend():
    from message_router import handle_low_coupling_message
    from session_registry import QueuedCommand, Session

    def cmd(request_id: str):
        return QueuedCommand(
            request_id=request_id,
            device_id="device_a",
            client_id="client_a",
            content=request_id,
            images=None,
            files=None,
            enqueued_at=time.time(),
        )

    async def run():
        session = Session(session_id="s1", name="One", created_at=time.time())
        session.processing = True
        session.queue.append(cmd("r_active"))
        session.queue.append(cmd("r_pending"))
        ctx, calls = _ctx(sessions={"s1": session})
        held_coros = []

        def hold_spawn(name, coro):
            calls["spawned"].append((name, coro))
            held_coros.append(coro)
            return None

        ctx.spawn_task = hold_spawn
        handled = await handle_low_coupling_message(
            mtype="stop",
            msg={"type": "stop", "session_id": "s1"},
            ws=_Ws(),
            client=_Client(),
            ctx=ctx,
        )
        await held_coros[0]
        return handled, session, calls

    handled, session, calls = asyncio.run(run())

    assert handled is True
    assert list(session.queue) == []
    assert calls["spawned"][0][0] == "session-stop:s1"
    assert calls["backend_stopped"] == ["s1"]
    assert calls["broadcasts"] == [{
        "type": "session_command_failed",
        "session_id": "s1",
        "request_id": "r_pending",
        "message": "Cancelled by stop",
        "queue_length": 0,
    }]


def test_router_spawn_tasks_for_file_and_session_bulk_commands():
    from message_router import handle_low_coupling_message

    async def run():
        ctx, calls = _ctx()
        client = _Client()
        ws = _Ws()
        handled_push = await handle_low_coupling_message(
            mtype="push_file",
            msg={"type": "push_file", "path": "/tmp/a.txt"},
            ws=ws,
            client=client,
            ctx=ctx,
        )
        handled_ack = await handle_low_coupling_message(
            mtype="file_push_ack",
            msg={"type": "file_push_ack", "file_id": "file_1"},
            ws=ws,
            client=client,
            ctx=ctx,
        )
        handled_all = await handle_low_coupling_message(
            mtype="get_all_sessions",
            msg={"type": "get_all_sessions"},
            ws=ws,
            client=client,
            ctx=ctx,
        )
        return handled_push, handled_ack, handled_all, calls["spawned"]

    handled_push, handled_ack, handled_all, spawned = asyncio.run(run())

    assert (handled_push, handled_ack, handled_all) == (True, True, True)
    assert [name for name, _coro in spawned] == [
        "push-file:device_a",
        "file-push-ack:file_1:device_a",
        "send-all-sessions:client_a",
    ]


def test_router_returns_false_for_unhandled_type():
    from message_router import handle_low_coupling_message

    async def run():
        ctx, _calls = _ctx()
        return await handle_low_coupling_message(
            mtype="unknown_after_validation",
            msg={"type": "unknown_after_validation"},
            ws=_Ws(),
            client=_Client(),
            ctx=ctx,
        )

    assert asyncio.run(run()) is False
