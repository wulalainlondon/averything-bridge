from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from backends import claude_cli
from backends.claude_cli import ClaudeCliBackend, _get_context_limit
from backends.history import _JSONL_HISTORY_CACHE


def make_session(session_id: str = "s1"):
    return SimpleNamespace(
        session_id=session_id,
        is_streaming=False,
        last_activity=0,
        accumulated_text="",
        is_stopping=False,
        resume_id=None,
        model="claude-sonnet-4",
    )


def decode_payload(call) -> dict:
    return call.args[1]


def test_context_limit_recognizes_claude_windows():
    assert _get_context_limit("claude-sonnet-4") == 200_000
    assert _get_context_limit("claude-sonnet-4[1m]") == 1_000_000
    assert _get_context_limit("claude-opus-4-1m") == 1_000_000
    assert _get_context_limit("gpt-5") == 0


def test_build_tool_result_content_covers_text_images_text_files_and_pdfs():
    content = ClaudeCliBackend._build_tool_result_content(
        "answer",
        images=[{"media_type": "image/png", "data": "abc"}],
        files=[
            {"name": "notes.md", "content": "# Hi", "media_type": "text/markdown"},
            {"name": "paper.pdf", "content": "ignored", "media_type": "application/pdf"},
        ],
    )

    assert content[0] == {"type": "text", "text": "answer"}
    assert content[1] == {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "abc"}}
    assert content[2] == {"type": "text", "text": "[File: notes.md]\n```md\n# Hi\n```"}
    assert content[3] == {"type": "text", "text": "[File attached (pdf, omitted from answer): paper.pdf]"}
    assert ClaudeCliBackend._build_tool_result_content("", [], []) == ""


@pytest.mark.asyncio
async def test_handle_message_during_user_input_writes_tool_results_and_resolves(monkeypatch):
    backend = ClaudeCliBackend()
    session = make_session()
    state = backend._get_state(session)
    state.tool_waiting_events["ask1"] = asyncio.Event()
    state.tool_waiting_events["ask2"] = asyncio.Event()
    state.tool_waiting_interactions["ask1"] = "req1"
    state.tool_waiting_interactions["ask2"] = "req2"
    backend._write_stream_json = AsyncMock()  # type: ignore[method-assign]
    resolved: list[str] = []
    monkeypatch.setattr(claude_cli.INTERACTIONS, "resolve", AsyncMock(side_effect=lambda rid: resolved.append(rid)))

    handled = await backend.handle_message_during_user_input(
        session,
        "free-text answer",
        files=[{"name": "a.txt", "content": "body", "media_type": "text/plain"}],
    )

    assert handled is True
    payload = decode_payload(backend._write_stream_json.call_args)
    blocks = payload["message"]["content"]
    assert blocks[0]["tool_use_id"] == "ask1"
    assert blocks[0]["content"][0] == {"type": "text", "text": "free-text answer"}
    assert blocks[1]["tool_use_id"] == "ask2"
    assert json.loads(blocks[1]["content"]) == {"cancelled": True}
    assert resolved == ["req1", "req2"]
    assert state.tool_waiting_events == {}
    assert state.tool_waiting_interactions == {}
    assert session.is_streaming is True


@pytest.mark.asyncio
async def test_handle_message_during_user_input_returns_false_when_no_pending_input():
    backend = ClaudeCliBackend()
    backend._write_stream_json = AsyncMock()  # type: ignore[method-assign]

    assert await backend.handle_message_during_user_input(make_session(), "unused") is False
    backend._write_stream_json.assert_not_called()


@pytest.mark.asyncio
async def test_handle_user_input_response_accepts_answers_and_unblocks_waiter():
    backend = ClaudeCliBackend()
    session = make_session()
    state = backend._get_state(session)
    state.tool_waiting_events["ask1"] = asyncio.Event()
    state.tool_waiting_interactions["ask1"] = "req1"
    backend._write_stream_json = AsyncMock()  # type: ignore[method-assign]
    interaction = SimpleNamespace(request_id="req1", tool_use_id="ask1")

    await backend.handle_user_input_response(session, interaction, {"answers": {"choice": "yes"}})

    payload = decode_payload(backend._write_stream_json.call_args)
    block = payload["message"]["content"][0]
    assert block["type"] == "tool_result"
    assert block["tool_use_id"] == "ask1"
    assert json.loads(block["content"]) == {"request_id": "req1", "answers": {"choice": "yes"}, "cancelled": False}
    assert state.tool_waiting_events == {}
    assert state.tool_waiting_interactions == {}
    assert session.is_streaming is True


class FakeStdin:
    def __init__(self):
        self.writes: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        return None


@pytest.mark.asyncio
async def test_cancel_pending_user_input_writes_cancelled_results_and_unblocks(monkeypatch):
    backend = ClaudeCliBackend()
    session = make_session()
    state = backend._get_state(session)
    stdin = FakeStdin()
    state.proc = SimpleNamespace(returncode=None, stdin=stdin)
    state.tool_waiting_events["ask1"] = asyncio.Event()
    state.tool_waiting_interactions["ask1"] = "req1"
    monkeypatch.setattr(claude_cli.INTERACTIONS, "resolve", AsyncMock())
    monkeypatch.setattr(claude_cli.asyncio, "sleep", AsyncMock())

    await backend._cancel_pending_user_input(session)

    payload = json.loads(stdin.writes[0].decode("utf-8"))
    assert payload["message"]["content"] == [{
        "type": "tool_result",
        "tool_use_id": "ask1",
        "content": '{"cancelled": true}',
    }]
    assert state.tool_waiting_events == {}
    assert state.tool_waiting_interactions == {}
    claude_cli.INTERACTIONS.resolve.assert_awaited_once_with("req1")


def test_detect_turn_end_only_accepts_main_assistant_terminal_reasons():
    backend = ClaudeCliBackend()
    assert backend.detect_turn_end([
        {"type": "assistant", "isSidechain": True, "message": {"stop_reason": "end_turn"}},
        {"type": "assistant", "message": {"stop_reason": "tool_use"}},
    ]) is False
    assert backend.detect_turn_end([
        {"type": "assistant", "message": {"stop_reason": "max_tokens"}},
    ]) is True


def write_jsonl(path: Path, records: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")


def test_load_session_history_sync_builds_text_and_tool_blocks(monkeypatch, tmp_path):
    _JSONL_HISTORY_CACHE.clear()
    monkeypatch.setattr(claude_cli, "sqlite_load", lambda *args, **kwargs: None)
    monkeypatch.setattr(claude_cli, "sqlite_save_background", lambda *args, **kwargs: None)
    project = tmp_path / "project"
    project.mkdir()
    resume_id = "resume-1"
    write_jsonl(project / f"{resume_id}.jsonl", [
        {
            "type": "user",
            "timestamp": "2026-01-01T00:00:00Z",
            "message": {"content": [{"type": "text", "text": "hello"}]},
        },
        {
            "type": "assistant",
            "timestamp": "2026-01-01T00:00:01Z",
            "message": {"content": [
                {"type": "text", "text": "running"},
                {"type": "tool_use", "id": "tool1", "name": "Bash", "input": {"command": "pwd"}},
            ], "stop_reason": "end_turn"},
        },
        {
            "type": "user",
            "timestamp": "2026-01-01T00:00:02Z",
            "message": {"content": [{"type": "tool_result", "tool_use_id": "tool1", "content": "ok"}]},
        },
        {
            "type": "assistant",
            "isSidechain": True,
            "message": {"content": [{"type": "text", "text": "hidden"}]},
        },
    ])
    backend = ClaudeCliBackend(claude_projects_dir=str(tmp_path))

    result = backend._load_session_history_sync(resume_id, limit=10)
    messages = result["messages"]

    assert [m["role"] for m in messages] == ["user", "assistant"]
    assert messages[0]["content"] == "hello"
    assert messages[1]["blocks"] == [
        {"type": "text", "text": "running"},
        {"type": "tool_call", "tool_use_id": "tool1", "name": "Bash", "command": "pwd", "output": "ok"},
    ]


def test_load_session_history_sync_supports_delta_and_before_cursor(monkeypatch, tmp_path):
    _JSONL_HISTORY_CACHE.clear()
    monkeypatch.setattr(claude_cli, "sqlite_load", lambda *args, **kwargs: None)
    monkeypatch.setattr(claude_cli, "sqlite_save_background", lambda *args, **kwargs: None)
    project = tmp_path / "project"
    project.mkdir()
    resume_id = "resume-2"
    write_jsonl(project / f"{resume_id}.jsonl", [
        {"type": "user", "message": {"content": "one"}},
        {"type": "assistant", "message": {"content": "two"}},
        {"type": "user", "message": {"content": "three"}},
    ])
    backend = ClaudeCliBackend(claude_projects_dir=str(tmp_path))

    all_messages = backend._load_session_history_sync(resume_id, limit=10)["messages"]
    delta = backend._load_session_history_sync(
        resume_id,
        limit=10,
        known_last_source_message_id=all_messages[0]["source_message_id"],
        mode="delta",
    )
    before = backend._load_session_history_sync(
        resume_id,
        limit=10,
        before_source_message_id=all_messages[2]["source_message_id"],
    )

    assert [m["content"] for m in delta["messages"]] == ["two", "three"]
    assert [m["content"] for m in before["messages"]] == ["one", "two"]
