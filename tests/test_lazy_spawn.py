"""
Tests for lazy spawn behaviour: bridge startup must not spawn any claude
subprocess, and spawn must only happen on demand (send message / explicit
resume via new_session with resume_claude_id).

Run: ~/.claude-bridge-runtime/venv/bin/python -m pytest bridge/tests/test_lazy_spawn.py -v
"""
from __future__ import annotations

import asyncio
import gzip
import json
import sys
import time
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Ensure the project root and bridge package are importable.
_REPO_ROOT = Path(__file__).parent.parent.parent
_BRIDGE_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_BRIDGE_ROOT))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _random_uuid() -> str:
    return str(uuid.uuid4())


def _make_session(sid: str, resume_id: str | None = None, backend_name: str = "claude"):
    """Return a bridge_v2.Session with minimal fields set."""
    from bridge_v2 import Session
    s = Session(
        session_id=sid,
        name=f"Test {sid}",
        created_at=time.time(),
        backend_name=backend_name,
    )
    s.resume_id = resume_id
    return s


def _fake_stdin():
    stdin = MagicMock()
    stdin.write = MagicMock()
    stdin.drain = AsyncMock()
    return stdin


# ---------------------------------------------------------------------------
# 1. _restore_sessions_from_disk must not spawn
# ---------------------------------------------------------------------------

class TestRestoreSessionsNoSpawn:
    """_restore_sessions_from_disk loads metadata only — no subprocess."""

    def test_restore_populates_sessions_without_spawn(self, tmp_path, monkeypatch):
        """Five saved sessions load into _SESSIONS with proc=None."""
        import bridge_v2 as bv2
        import jsonl_sessions
        from backends.claude_cli import ClaudeCliBackend

        # Build a fake saved_sessions.json with 5 entries.
        # Use distinct SIDs and UUIDs so none collide.
        entries = {}
        expected_sids = []
        for i in range(5):
            uid = _random_uuid()
            sid = f"sid_{i:04d}_{uid[:8]}"
            expected_sids.append(sid)
            entries[sid] = {
                "name": f"Session {i}",
                "cwd": "/tmp",
                "backend": "claude",
                "claude_uuid": uid,
                "last_used": time.time() - i * 3600,
            }
        saved_file = tmp_path / "saved_sessions.json"
        saved_file.write_text(json.dumps(entries), encoding="utf-8")

        spawn_calls: list = []

        async def fake_spawn(self_b, session):
            spawn_calls.append(session.session_id)

        monkeypatch.setattr(ClaudeCliBackend, "_spawn_proc", fake_spawn)
        monkeypatch.setattr(bv2, "SAVED_SESSIONS_FILE", str(saved_file))
        monkeypatch.setattr(bv2, "_SESSIONS", {})

        # Patch prune so it's a no-op (we control the file directly).
        import auto_register as ar
        monkeypatch.setattr(ar, "prune_old_saved_sessions", lambda path, days: 0)

        bv2._restore_sessions_from_disk()

        assert len(bv2._SESSIONS) == 5, f"Expected 5 sessions, got {len(bv2._SESSIONS)}"
        assert spawn_calls == [], f"Expected 0 spawn calls, got {spawn_calls}"
        for sid in expected_sids:
            assert sid in bv2._SESSIONS, f"SID {sid} missing from _SESSIONS"

    def test_restore_sessions_have_no_active_proc(self, tmp_path, monkeypatch):
        """After restore, backend state for each session has proc=None."""
        import bridge_v2 as bv2
        from backends.claude_cli import ClaudeCliBackend

        uid = _random_uuid()
        sid = f"solo_{uid[:8]}"
        entries = {
            sid: {
                "name": "Solo session",
                "cwd": "/tmp",
                "backend": "claude",
                "claude_uuid": uid,
                "last_used": time.time(),
            }
        }
        saved_file = tmp_path / "saved_sessions.json"
        saved_file.write_text(json.dumps(entries), encoding="utf-8")

        monkeypatch.setattr(bv2, "SAVED_SESSIONS_FILE", str(saved_file))
        monkeypatch.setattr(bv2, "_SESSIONS", {})
        import auto_register as ar
        monkeypatch.setattr(ar, "prune_old_saved_sessions", lambda path, days: 0)

        bv2._restore_sessions_from_disk()

        session = bv2._SESSIONS.get(sid)
        assert session is not None, f"Session {sid} not in _SESSIONS after restore"
        # The backend only allocates _ClaudeState on first send(); so either the
        # state doesn't exist yet (fully lazy) or proc is explicitly None.
        backend = bv2._get_or_create_backend("claude")
        state = backend._states.get(sid)
        if state is not None:
            assert state.proc is None, "proc must be None before first send"


def test_claude_proc_exit_closes_active_stream_before_restart(monkeypatch):
    """rc=185 or similar exits must not leave the prompt queue/UI streaming forever."""
    from backends.claude_cli import ClaudeCliBackend

    class DeadProc:
        returncode = 185

        async def wait(self):
            return 185

    async def run():
        session = _make_session("s_rc185")
        session.is_streaming = True
        session.current_request_id = "r_rc185"
        backend = ClaudeCliBackend()
        state = backend._get_state(session)
        state.proc = DeadProc()
        sent_events: list[dict] = []
        spawn_calls: list[str] = []

        async def fake_send_event(session_arg, event: dict) -> None:
            sent_events.append({**event, "session_id": session_arg.session_id})

        async def fake_spawn(session_arg) -> None:
            spawn_calls.append(session_arg.session_id)

        monkeypatch.setattr("backends.claude_stream.send_event", fake_send_event)
        monkeypatch.setattr("backends.turn_lifecycle.send_event", fake_send_event)
        monkeypatch.setattr(backend, "_spawn_proc", fake_spawn)

        await backend._watch_proc(session)
        return session, state, sent_events, spawn_calls

    session, state, sent_events, spawn_calls = asyncio.run(run())

    assert session.is_streaming is False
    assert session.accumulated_text == ""
    assert state.tool_lifecycle.active == {}
    assert sent_events[0]["type"] == "error"
    assert sent_events[0]["code"] == "process_exited"
    assert "rc=185" in sent_events[0]["message"]
    assert spawn_calls == ["s_rc185"]


# ---------------------------------------------------------------------------
# 2. build_sessions_list must not spawn
# ---------------------------------------------------------------------------

class TestBuildSessionsListNoSpawn:
    """build_sessions_list reads metadata only — no subprocess."""

    def test_sessions_list_no_spawn_50_entries(self, monkeypatch):
        """Calling build_sessions_list() with 50 sessions spawns nothing."""
        import bridge_v2 as bv2
        from backends.claude_cli import ClaudeCliBackend

        spawn_calls: list = []

        async def fake_spawn(self_b, session):
            spawn_calls.append(session.session_id)

        monkeypatch.setattr(ClaudeCliBackend, "_spawn_proc", fake_spawn)

        # Populate _SESSIONS with 50 valid entries using distinct SIDs/UUIDs.
        sessions: dict = {}
        for i in range(50):
            uid = _random_uuid()
            sid = f"sess_{i:04d}_{uid[:8]}"
            s = _make_session(sid, resume_id=uid)
            sessions[sid] = s
        monkeypatch.setattr(bv2, "_SESSIONS", sessions)

        result = bv2.build_sessions_list()

        assert spawn_calls == [], f"build_sessions_list triggered spawn: {spawn_calls}"
        assert result["type"] == "sessions_list"
        # Top-50 cap means we get all 50 entries.
        assert len(result["sessions"]) == 50

    def test_sessions_list_returns_metadata_fields(self, monkeypatch):
        """Each summary dict contains the required metadata fields."""
        import bridge_v2 as bv2

        uid = _random_uuid()
        sid = f"meta_{uid[:8]}"
        s = _make_session(sid, resume_id=uid)
        s.name = "My session"
        s.cwd = "/tmp/project"
        monkeypatch.setattr(bv2, "_SESSIONS", {sid: s})

        result = bv2.build_sessions_list()

        assert len(result["sessions"]) == 1
        summary = result["sessions"][0]
        assert summary["id"] == sid
        assert summary["name"] == "My session"
        assert summary["cwd"] == "/tmp/project"
        assert summary["backend"] == "claude"

    def test_codex_rollout_jsonl_registers_with_native_uuid(self, tmp_path, monkeypatch):
        """Codex rollout filenames must register by trailing UUID, not full stem."""
        import bridge_v2 as bv2
        import jsonl_sessions

        uid = _random_uuid()
        root = tmp_path / "codex" / "sessions"
        day = root / "2026" / "05" / "18"
        day.mkdir(parents=True)
        path = day / f"rollout-2026-05-18T01-27-37-{uid}.jsonl"
        records = [
            {
                "timestamp": "2026-05-17T17:27:37.210Z",
                "type": "session_meta",
                "payload": {"id": uid, "cwd": "/tmp/lucky3"},
            },
            {
                "timestamp": "2026-05-17T17:27:38.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "# AGENTS.md instructions for /tmp/lucky3"}],
                },
            },
            {
                "timestamp": "2026-05-17T17:27:39.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "檢查 Lucky3 solver 狀態"}],
                },
            },
        ]
        path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")

        sessions = {}
        monkeypatch.setattr(jsonl_sessions, "CODEX_SESSIONS_DIR", str(root))
        jsonl_sessions.configure(
            sessions=sessions,
            default_cwd=bv2.DEFAULT_CWD,
            claude_projects_dir=bv2.CLAUDE_PROJECTS_DIR,
            session_backend=bv2._session_backend,
            broadcast_json=bv2._broadcast_json,
            build_sessions_list=lambda: bv2.session_registry.build_sessions_list(
                sessions,
                recent_messages=jsonl_sessions.get_recent_messages_sync,
            ),
            dispatch_event=bv2._dispatch_event,
            evt_done=bv2._evt_done,
            log=bv2.log,
        )

        assert jsonl_sessions._register_jsonl_session(str(path)) is True
        result = bv2.session_registry.build_sessions_list(
            sessions,
            recent_messages=jsonl_sessions.get_recent_messages_sync,
        )

        assert len(result["sessions"]) == 1
        summary = result["sessions"][0]
        assert summary["backend"] == "codex"
        assert summary["cwd"] == "/tmp/lucky3"
        assert summary["name"] == "檢查 Lucky3 solver 狀態"
        session = sessions[summary["id"]]
        assert session.resume_id == uid

    def test_codex_history_skips_commentary_phase(self, tmp_path):
        """Codex history should expose final answers, not internal commentary messages."""
        from backends.codex_appserver import CodexAppServerBackend

        uid = _random_uuid()
        day = tmp_path / "sessions" / "2026" / "05" / "18"
        day.mkdir(parents=True)
        path = day / f"rollout-2026-05-18T01-27-37-{uid}.jsonl"
        records = [
            {
                "timestamp": "2026-05-17T17:27:37.210Z",
                "type": "session_meta",
                "payload": {"id": uid, "cwd": "/tmp/project"},
            },
            {
                "timestamp": "2026-05-17T17:27:38.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "回報 cwd"}],
                },
            },
            {
                "timestamp": "2026-05-17T17:27:39.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "phase": "commentary",
                    "content": [{"type": "output_text", "text": "我先確認目前工作目錄。"}],
                },
            },
            {
                "timestamp": "2026-05-17T17:27:40.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "phase": "final_answer",
                    "content": [{"type": "output_text", "text": "目前目錄是 `/tmp/project`。"}],
                },
            },
        ]
        path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")

        backend = CodexAppServerBackend("codex")
        backend._native_sessions_root = str(tmp_path / "sessions")
        history = backend._load_native_session_history(uid, limit=20, mode="snapshot")

        assert history["kind"] == "snapshot"
        assert [m["role"] for m in history["messages"]] == ["user", "assistant"]
        assert history["messages"][-1]["content"] == "目前目錄是 `/tmp/project`。"
        assert all("我先確認" not in m["content"] for m in history["messages"])

    def test_codex_live_commentary_delta_emits_thinking_chunk(self):
        """Live Codex commentary should render as an AI process block in chat."""
        import asyncio
        from unittest.mock import MagicMock

        from backends.codex_appserver import CodexAppServerBackend
        from backends.events import flush_session_events, set_event_dispatcher

        async def run():
            backend = CodexAppServerBackend("codex")
            session = MagicMock()
            session.session_id = "s_codex"
            session.current_request_id = "r_codex"
            session.ws_ref = None
            session.offline_buffer = []
            backend._thread_to_session["thread_1"] = session

            delivered: list[dict] = []

            async def dispatcher(payload: dict, session_arg) -> bool:
                delivered.append(payload)
                assert session_arg is session
                return True

            set_event_dispatcher(dispatcher)
            try:
                await backend._dispatch({
                    "method": "item/agentMessage/delta",
                    "params": {
                        "threadId": "thread_1",
                        "phase": "commentary",
                        "delta": "我先檢查目前的 bridge stream。",
                    },
                })
                await flush_session_events(session)
            finally:
                set_event_dispatcher(None)
            return delivered

        delivered = asyncio.run(run())

        stripped = [{k: v for k, v in e.items() if k not in ("seq", "gen")} for e in delivered]
        assert stripped == [{
            "type": "thinking_chunk",
            "content": "我先檢查目前的 bridge stream。",
            "session_id": "s_codex",
            "request_id": "r_codex",
        }]

    def test_codex_server_request_approval_is_answered(self):
        """Codex app-server requests must receive JSON-RPC responses or turns can hang."""
        import asyncio
        from unittest.mock import MagicMock

        from backends.codex_appserver import CodexAppServerBackend

        async def run():
            backend = CodexAppServerBackend("codex")
            proc = MagicMock()
            proc.stdin = _fake_stdin()
            backend._proc = proc

            await backend._dispatch({
                "id": 42,
                "method": "item/commandExecution/requestApproval",
                "params": {"threadId": "thread_1"},
            })

            raw = proc.stdin.write.call_args.args[0].decode()
            return json.loads(raw)

        assert asyncio.run(run()) == {
            "id": 42,
            "result": {"decision": "accept"},
        }

    def test_codex_approval_event_includes_environment_id(self):
        """0.137 permission requests carry environment identity; bridge should expose it."""
        import asyncio
        from unittest.mock import MagicMock

        from backends.codex_appserver import CodexAppServerBackend
        from backends.events import flush_session_events, set_event_dispatcher

        async def run():
            backend = CodexAppServerBackend("codex")
            proc = MagicMock()
            proc.stdin = _fake_stdin()
            backend._proc = proc
            session = MagicMock()
            session.session_id = "s_codex"
            session.current_request_id = "r_codex"
            session.ws_ref = None
            session.offline_buffer = []
            backend._thread_to_session["thread_1"] = session
            delivered: list[dict] = []

            async def dispatcher(payload: dict, session_arg) -> bool:
                delivered.append(payload)
                assert session_arg is session
                return True

            set_event_dispatcher(dispatcher)
            try:
                await backend._dispatch({
                    "id": 42,
                    "method": "item/permissions/requestApproval",
                    "params": {
                        "threadId": "thread_1",
                        "environmentId": "env_abc",
                        "permission": {"name": "network"},
                    },
                })
                await flush_session_events(session)
            finally:
                set_event_dispatcher(None)

            raw = proc.stdin.write.call_args.args[0].decode()
            return json.loads(raw), delivered

        response, delivered = asyncio.run(run())
        assert response == {"id": 42, "result": {"decision": "accept"}}
        assert any(e.get("type") == "tool_result" and "env_abc" in e.get("output", "") for e in delivered)

    def test_codex_request_user_input_waits_for_frontend_response(self):
        """Native Codex requestUserInput should become a pending bridge interaction."""
        import asyncio
        from unittest.mock import MagicMock

        from backends.codex_appserver import CodexAppServerBackend
        from interactions import REGISTRY

        async def run():
            backend = CodexAppServerBackend("codex")
            proc = MagicMock()
            proc.stdin = _fake_stdin()
            backend._proc = proc
            session = MagicMock()
            session.session_id = "s_codex"
            session.current_request_id = "r_codex"
            session.ws_ref = None
            session.offline_buffer = []
            backend._thread_to_session["thread_1"] = session
            broadcasts: list[dict] = []

            async def broadcast(payload: dict) -> int:
                broadcasts.append(payload)
                return 1

            backend._broadcast_fn = broadcast
            await backend._dispatch({
                "id": 77,
                "method": "item/tool/requestUserInput",
                "params": {
                    "threadId": "thread_1",
                    "itemId": "ask_1",
                    "questions": [{
                        "id": "choice",
                        "question": "Pick one",
                        "options": [{"id": "yes", "label": "Yes"}],
                    }],
                },
            })
            assert proc.stdin.write.call_args is None
            request_id = broadcasts[-1]["request_id"]
            item = await REGISTRY.resolve(
                {"type": "user_input_response", "request_id": request_id, "answers": {"choice": "yes"}},
                broadcast_json=broadcast,
            )
            await backend.handle_user_input_response(
                session,
                item,
                {"request_id": request_id, "answers": {"choice": "yes"}},
            )

            raw = proc.stdin.write.call_args.args[0].decode()
            return broadcasts, json.loads(raw)

        broadcasts, response = asyncio.run(run())
        assert broadcasts[0]["type"] == "user_input_request"
        assert broadcasts[0]["tool_use_id"] == "ask_1"
        assert response == {"id": 77, "result": {"answers": {"choice": "yes"}, "cancelled": False}}

    def test_codex_server_tool_call_gets_safe_error(self):
        """Hosted tool calls should be observable, then safely rejected until implemented."""
        import asyncio
        from unittest.mock import MagicMock

        from backends.codex_appserver import CodexAppServerBackend
        from backends.events import flush_session_events, set_event_dispatcher

        async def run():
            backend = CodexAppServerBackend("codex")
            proc = MagicMock()
            proc.stdin = _fake_stdin()
            backend._proc = proc
            session = MagicMock()
            session.session_id = "s_codex"
            session.current_request_id = "r_codex"
            session.ws_ref = None
            session.offline_buffer = []
            backend._thread_to_session["thread_1"] = session
            delivered: list[dict] = []

            async def dispatcher(payload: dict, session_arg) -> bool:
                delivered.append(payload)
                assert session_arg is session
                return True

            set_event_dispatcher(dispatcher)
            try:
                await backend._dispatch({
                    "id": 99,
                    "method": "item/tool/call",
                    "params": {
                        "threadId": "thread_1",
                        "tool": {"name": "web_search"},
                        "input": {"q": "codex"},
                    },
                })
                await flush_session_events(session)
            finally:
                set_event_dispatcher(None)

            raw = proc.stdin.write.call_args.args[0].decode()
            return json.loads(raw), delivered

        response, delivered = asyncio.run(run())
        assert response["id"] == 99
        assert response["error"]["code"] == -32000
        assert "web_search" in response["error"]["message"]
        assert any(e.get("type") == "tool_start" and e.get("name") == "web_search" for e in delivered)

    def test_codex_server_request_unknown_method_gets_error(self):
        """Unhandled ServerRequest frames should fail explicitly instead of being dropped."""
        import asyncio
        from unittest.mock import MagicMock

        from backends.codex_appserver import CodexAppServerBackend

        async def run():
            backend = CodexAppServerBackend("codex")
            proc = MagicMock()
            proc.stdin = _fake_stdin()
            backend._proc = proc

            await backend._dispatch({
                "id": 99,
                "method": "item/unknown/request",
                "params": {"threadId": "thread_1"},
            })

            raw = proc.stdin.write.call_args.args[0].decode()
            return json.loads(raw)

        response = asyncio.run(run())
        assert response["id"] == 99
        assert response["error"]["code"] == -32601
        assert "item/unknown/request" in response["error"]["message"]

    def test_codex_output_delta_sends_accumulated_tool_result(self):
        """Frontend replaces tool_result output, so Codex deltas must be accumulated first."""
        import asyncio
        from unittest.mock import MagicMock

        from backends.codex_appserver import CodexAppServerBackend
        from backends.events import flush_session_events, set_event_dispatcher

        async def run():
            backend = CodexAppServerBackend("codex")
            session = MagicMock()
            session.session_id = "s_codex"
            session.current_request_id = "r_codex"
            session.ws_ref = None
            session.offline_buffer = []
            backend._states[session.session_id] = backend._state_factory()
            backend._thread_to_session["thread_1"] = session

            delivered: list[dict] = []

            async def dispatcher(payload: dict, session_arg) -> bool:
                delivered.append(payload)
                assert session_arg is session
                return True

            set_event_dispatcher(dispatcher)
            try:
                await backend._dispatch({
                    "method": "item/started",
                    "params": {
                        "threadId": "thread_1",
                        "itemId": "cmd_1",
                        "name": "shell",
                        "command": "printf",
                    },
                })
                await backend._dispatch({
                    "method": "item/commandExecution/outputDelta",
                    "params": {"threadId": "thread_1", "itemId": "cmd_1", "delta": "hel"},
                })
                await backend._dispatch({
                    "method": "item/commandExecution/outputDelta",
                    "params": {"threadId": "thread_1", "itemId": "cmd_1", "delta": "lo"},
                })
                await flush_session_events(session)
            finally:
                set_event_dispatcher(None)
            return delivered

        delivered = asyncio.run(run())

        assert [event["type"] for event in delivered] == [
            "tool_start",
            "tool_result",
            "tool_result",
        ]
        assert delivered[-2]["output"] == "hel"
        assert delivered[-1]["output"] == "hello"

    def test_codex_spawn_uses_session_model(self, tmp_path):
        """thread/start should respect the model selected on the bridge session."""
        import asyncio
        from unittest.mock import AsyncMock

        from backends.codex_appserver import CodexAppServerBackend

        async def run():
            backend = CodexAppServerBackend("codex")
            session = _make_session("s_codex_model", backend_name="codex")
            session.cwd = str(tmp_path)
            session.model = "codex-mini"

            backend._ensure_server = AsyncMock()
            backend._rpc = AsyncMock(return_value={"thread": {"id": "thread_model"}})
            await backend.spawn(session)
            return backend._rpc.call_args.args

        method, params = asyncio.run(run())
        assert method == "thread/start"
        assert params["model"] == "codex-mini"

    def test_codex_history_strips_turn_aborted_notice(self, tmp_path):
        """Codex history should not render framework abort notices as user text."""
        from backends.codex_appserver import CodexAppServerBackend

        uid = _random_uuid()
        day = tmp_path / "sessions" / "2026" / "05" / "18"
        day.mkdir(parents=True)
        path = day / f"rollout-2026-05-18T10-52-00-{uid}.jsonl"
        records = [
            {
                "timestamp": "2026-05-18T02:52:00.000Z",
                "type": "session_meta",
                "payload": {"id": uid, "cwd": "/tmp/project"},
            },
            {
                "timestamp": "2026-05-18T02:52:01.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": (
                                "打包taildrop給我\n"
                                "<turn_aborted>\n"
                                "The user interrupted the previous turn on purpose.\n"
                                "</turn_aborted>\n"
                                "我什麼都沒做 自動發出的"
                            ),
                        }
                    ],
                },
            },
        ]
        path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")

        backend = CodexAppServerBackend("codex")
        backend._native_sessions_root = str(tmp_path / "sessions")
        history = backend._load_native_session_history(uid, limit=20, mode="snapshot")

        assert history["kind"] == "snapshot"
        assert len(history["messages"]) == 1
        assert "打包taildrop給我" in history["messages"][0]["content"]
        assert "我什麼都沒做" in history["messages"][0]["content"]
        assert "turn_aborted" not in history["messages"][0]["content"]

    def test_codex_gzip_rollout_registers_and_loads_history(self, tmp_path):
        """Codex 0.137 compressed rollout files should appear in native sessions/history."""
        from backends.codex_appserver import CodexAppServerBackend

        uid = _random_uuid()
        day = tmp_path / "sessions" / "2026" / "06" / "04"
        day.mkdir(parents=True)
        path = day / f"rollout-2026-06-04T01-27-37-{uid}.jsonl.gz"
        records = [
            {
                "timestamp": "2026-06-04T01:27:37.210Z",
                "type": "session_meta",
                "payload": {"id": uid, "cwd": "/tmp/codex137"},
            },
            {
                "timestamp": "2026-06-04T01:27:38.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "檢查壓縮 rollout"}],
                },
            },
            {
                "timestamp": "2026-06-04T01:27:39.000Z",
                "type": "response_item",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "可以讀取"}],
                },
            },
        ]
        with gzip.open(path, "wt", encoding="utf-8") as f:
            f.write("\n".join(json.dumps(r) for r in records) + "\n")

        backend = CodexAppServerBackend("codex")
        backend._native_sessions_root = str(tmp_path / "sessions")

        sessions = backend._load_native_codex_sessions(limit=10)
        assert sessions[0]["resume_id"] == uid
        assert sessions[0]["name"] == "檢查壓縮 rollout"
        history = backend._load_native_session_history(uid, limit=10)
        assert [m["content"] for m in history["messages"]] == ["檢查壓縮 rollout", "可以讀取"]


# ---------------------------------------------------------------------------
# 3. request_history handler must NOT spawn on load (static source check)
# ---------------------------------------------------------------------------

class TestRequestHistoryNoPrewarmSpawn:
    """After the fix, request_history must not call backend.spawn() as pre-warm."""

    def test_request_history_handler_does_not_spawn(self):
        """
        Verify that the routed request_history path does not pre-warm spawn.
        """
        router_src = Path(_BRIDGE_ROOT) / "message_router.py"
        source = router_src.read_text(encoding="utf-8")

        assert "Lazy spawn: do NOT pre-warm here" in source, (
            "Expected lazy-spawn comment not found — "
            "the pre-warm spawn guard was not preserved in request_history router"
        )

        import re
        block_match = re.search(
            r'mtype == "request_history"(.*?)(?=\n    if mtype ==)',
            source,
            re.DOTALL,
        )
        assert block_match, "Could not locate request_history router block"
        block = block_match.group(1)

        prewarm_pattern = re.compile(
            r'(?:backend\.spawn|_spawn_task)\(',
        )
        assert not prewarm_pattern.search(block), (
            "Pre-warm spawn still present in request_history router — "
            "the thundering-herd fix was not applied correctly"
        )


# ---------------------------------------------------------------------------
# 4. ClaudeCliBackend.send() triggers spawn when proc is None
# ---------------------------------------------------------------------------

class TestClaudeCliLazySpawnContract:
    """The backend only spawns when send() is called with proc=None."""

    def test_send_triggers_spawn_when_proc_is_none(self):
        """send() detects proc is None and initiates _spawn_proc."""
        from backends.claude_cli import ClaudeCliBackend, _ClaudeState

        backend = ClaudeCliBackend.__new__(ClaudeCliBackend)
        backend._states = {}
        backend._claude_bin = "claude"
        backend._persist_session_fn = None
        backend._notify_fcm_fn = None
        backend._broadcast_fn = None

        uid = _random_uuid()
        sid = f"send1_{uid[:8]}"
        s = _make_session(sid, resume_id=uid)

        state = _ClaudeState()
        state.proc = None
        state.spawning = False
        backend._states[sid] = state

        spawn_initiated = []

        async def run():
            async def fake_spawn(session):
                state.proc = MagicMock()
                state.proc.returncode = None
                state.proc.stdin = _fake_stdin()
                spawn_initiated.append(session.session_id)

            with patch.object(backend, "_spawn_proc", side_effect=fake_spawn):
                await backend.send(s, "hello world")

        asyncio.run(run())
        assert len(spawn_initiated) == 1, f"Expected 1 spawn, got {spawn_initiated}"

    def test_send_does_not_respawn_when_proc_already_running(self):
        """If proc is already running, send() skips _spawn_proc."""
        from backends.claude_cli import ClaudeCliBackend, _ClaudeState

        backend = ClaudeCliBackend.__new__(ClaudeCliBackend)
        backend._states = {}
        backend._claude_bin = "claude"
        backend._persist_session_fn = None
        backend._notify_fcm_fn = None
        backend._broadcast_fn = None

        uid = _random_uuid()
        sid = f"send2_{uid[:8]}"
        s = _make_session(sid, resume_id=uid)

        proc_mock = MagicMock()
        proc_mock.returncode = None
        proc_mock.stdin = _fake_stdin()

        state = _ClaudeState()
        state.proc = proc_mock
        state.spawning = False
        backend._states[sid] = state

        spawn_calls = []

        async def run():
            async def fake_spawn(session):
                spawn_calls.append(session.session_id)

            with patch.object(backend, "_spawn_proc", side_effect=fake_spawn):
                await backend.send(s, "second message")

        asyncio.run(run())
        assert spawn_calls == [], f"Unexpected respawn: {spawn_calls}"


# ---------------------------------------------------------------------------
# 5. idle_watchdog only touches sessions that have an active proc
# ---------------------------------------------------------------------------

class TestIdleWatchdogOnlySpawnedSessions:
    """Timeout tasks are only created inside send(), not at startup."""

    def test_idle_watchdog_not_created_for_unspawned_sessions(self):
        """
        After restore_sessions_from_disk, unspawned sessions have no
        timeout_task — so the watchdog never fires for them.
        """
        from backends.claude_cli import ClaudeCliBackend, _ClaudeState

        backend = ClaudeCliBackend.__new__(ClaudeCliBackend)
        backend._states = {}

        # 47 sessions never sent a message (no proc, no watchdog).
        for i in range(47):
            sid = f"unspawned_{i:04d}"
            state = _ClaudeState()  # proc=None, timeout_task=None
            backend._states[sid] = state

        # 3 sessions as-if they were spawned (proc set).
        spawned_sids = []
        for i in range(3):
            sid = f"spawned_{i:04d}"
            state = _ClaudeState()
            state.proc = MagicMock()
            state.proc.returncode = None
            backend._states[sid] = state
            spawned_sids.append(sid)

        # Unspawned sessions must have no timeout_task.
        for i in range(47):
            sid = f"unspawned_{i:04d}"
            state = backend._states[sid]
            assert state.timeout_task is None, (
                f"Session {sid} has a timeout_task before any send() call"
            )

        assert len(backend._states) == 50

        spawned_with_proc = [
            sid for sid, st in backend._states.items()
            if st.proc is not None
        ]
        assert set(spawned_with_proc) == set(spawned_sids)


def test_codex_invalidate_live_threads_clears_all_session_routes():
    """A singleton app-server exit invalidates every live thread mapping."""
    from backends.codex_appserver import CodexAppServerBackend

    backend = CodexAppServerBackend("codex")
    s1 = _make_session("s_codex_1", backend_name="codex")
    s2 = _make_session("s_codex_2", backend_name="codex")
    state1 = backend._get_state(s1)
    state2 = backend._get_state(s2)
    state1.thread_id = "thread-1"
    state1.current_turn_id = "turn-1"
    state2.thread_id = "thread-2"
    state2.current_turn_id = "turn-2"
    backend._thread_to_session = {"thread-1": s1, "thread-2": s2}

    backend._invalidate_live_threads()

    assert backend._thread_to_session == {}
    assert state1.thread_id is None
    assert state1.current_turn_id is None
    assert state2.thread_id is None
    assert state2.current_turn_id is None


@pytest.mark.asyncio
async def test_codex_turn_start_retries_thread_not_found(monkeypatch):
    """Codex retries stale live threads reported as `thread not found`."""
    from backends import codex_appserver
    from backends.codex_appserver import CodexAppServerBackend

    backend = CodexAppServerBackend("codex")
    session = _make_session("s_codex_retry", backend_name="codex")
    session.accumulated_text = ""
    state = backend._get_state(session)
    state.thread_id = "old-thread"
    backend._thread_to_session["old-thread"] = session
    calls: list[tuple[str, dict | None]] = []

    async def fake_spawn(spawn_session):
        assert spawn_session is session
        state.thread_id = "new-thread"
        backend._thread_to_session["new-thread"] = session

    async def fake_rpc(method, params, timeout=30.0):
        calls.append((method, params))
        if method == "turn/start" and params and params.get("threadId") == "old-thread":
            raise RuntimeError("{'message': 'thread not found: old-thread'}")
        if method == "turn/start" and params and params.get("threadId") == "new-thread":
            state.turn_error = None
            state.turn_done_event.set()
            return {}
        raise AssertionError(f"unexpected rpc call: {method} {params}")

    monkeypatch.setattr(backend, "spawn", fake_spawn)
    monkeypatch.setattr(backend, "_rpc", fake_rpc)
    monkeypatch.setattr(codex_appserver, "emit_turn_done", AsyncMock())

    await backend._run_turn(session, state, [{"type": "text", "text": "hello", "text_elements": []}])

    turn_threads = [
        params["threadId"]
        for method, params in calls
        if method == "turn/start" and params is not None
    ]
    assert turn_threads == ["old-thread", "new-thread"]
    assert "old-thread" not in backend._thread_to_session
    assert backend._thread_to_session["new-thread"] is session
