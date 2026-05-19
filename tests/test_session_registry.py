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


def _recent(session, n: int) -> list:
    return [{"role": "user", "content": f"{session.session_id}:{n}"}]


def _session(session_id: str, *, created_at: float, last_activity: float = 0, resume_id: str | None = None):
    from session_registry import Session

    return Session(
        session_id=session_id,
        name=f"Session {session_id}",
        created_at=created_at,
        last_activity=last_activity,
        resume_id=resume_id,
        cwd=f"/tmp/{session_id}",
    )


def test_build_sessions_list_sorts_by_activity_and_limits_to_50():
    import session_registry

    sessions = {
        f"s{i}": _session(f"s{i}", created_at=float(i), last_activity=float(i))
        for i in range(60)
    }

    result = session_registry.build_sessions_list(sessions, recent_messages=_recent)

    assert result["type"] == "sessions_list"
    assert len(result["sessions"]) == 50
    assert result["sessions"][0]["id"] == "s59"
    assert result["sessions"][-1]["id"] == "s10"


def test_build_sessions_list_filters_invalid_claude_or_codex_resume_ids():
    import session_registry

    good_uuid = "12345678-1234-4234-9234-123456789abc"
    valid = _session("valid", created_at=3, resume_id=good_uuid)
    invalid_claude = _session("invalid_claude", created_at=2, resume_id="not-a-uuid")
    invalid_codex = _session("invalid_codex", created_at=1, resume_id="also-bad")
    ollama = _session("ollama", created_at=0, resume_id="not-a-uuid")
    invalid_claude.backend_name = "claude"
    invalid_codex.backend_name = "codex"
    ollama.backend_name = "ollama"

    result = session_registry.build_sessions_list(
        {
            "valid": valid,
            "invalid_claude": invalid_claude,
            "invalid_codex": invalid_codex,
            "ollama": ollama,
        },
        recent_messages=_recent,
    )

    assert [s["id"] for s in result["sessions"]] == ["valid", "ollama"]


def test_session_to_summary_includes_queue_length_and_recent_messages():
    import session_registry

    session = _session("s1", created_at=1)
    session.queue.append(session_registry.QueuedCommand(
        request_id="r1",
        device_id="d1",
        client_id="c1",
        content="hello",
        images=None,
        files=None,
        enqueued_at=1,
    ))

    summary = session_registry.session_to_summary(session, recent_messages=_recent)

    assert summary["id"] == "s1"
    assert summary["cwd"] == "/tmp/s1"
    assert summary["queue_length"] == 1
    assert summary["recent_messages"] == [{"role": "user", "content": "s1:2"}]


def test_send_all_sessions_batches_in_order():
    import session_registry

    async def run():
        sessions = {
            f"s{i}": _session(f"s{i}", created_at=float(i), last_activity=float(i))
            for i in range(5)
        }
        ws = _Ws()
        await session_registry.send_all_sessions(ws, sessions, recent_messages=_recent, batch_size=2)
        return ws.sent

    sent = asyncio.run(run())

    assert [payload["offset"] for payload in sent] == [0, 2, 4]
    assert [payload["done"] for payload in sent] == [False, False, True]
    assert [session["id"] for session in sent[0]["sessions"]] == ["s4", "s3"]
    assert [session["id"] for session in sent[-1]["sessions"]] == ["s0"]


def test_persist_and_remove_saved_session(tmp_path):
    import session_registry

    saved = tmp_path / "saved_sessions.json"
    session = _session("s1", created_at=1, resume_id="12345678-1234-4234-9234-123456789abc")
    session.backend_name = "codex"
    session.model = "gpt-test"

    session_registry.persist_session(session, saved_sessions_file=str(saved))
    data = json.loads(saved.read_text(encoding="utf-8"))

    assert data["s1"]["resume_id"] == session.resume_id
    assert data["s1"]["backend"] == "codex"
    assert data["s1"]["model"] == "gpt-test"

    session_registry.remove_saved_session("s1", saved_sessions_file=str(saved))
    assert json.loads(saved.read_text(encoding="utf-8")) == {}


def test_restore_sessions_drops_invalid_uuid_and_restores_valid(tmp_path):
    import session_registry

    saved = tmp_path / "saved_sessions.json"
    valid_uuid = "12345678-1234-4234-9234-123456789abc"
    saved.write_text(json.dumps({
        "valid": {
            "name": "Valid",
            "resume_id": valid_uuid,
            "last_used": time.time(),
            "cwd": "/tmp/project",
            "backend": "claude",
        },
        "bad": {
            "name": "Bad",
            "resume_id": "not-a-uuid",
            "last_used": time.time(),
            "backend": "claude",
        },
    }), encoding="utf-8")
    sessions: dict[str, session_registry.Session] = {}

    restored = session_registry.restore_sessions_from_disk(
        sessions,
        saved_sessions_file=str(saved),
        normalize_backend=lambda raw: raw or "claude",
    )

    assert restored == 1
    assert set(sessions) == {"valid"}
    assert sessions["valid"].resume_id == valid_uuid
    assert set(json.loads(saved.read_text(encoding="utf-8"))) == {"valid"}


def test_session_meta_and_read_cursor_persistence(tmp_path):
    import session_registry

    meta_file = tmp_path / "session_meta.json"
    cursor_file = tmp_path / "read_cursors.json"
    session = _session("s1", created_at=1)
    session.pinned = True
    session.hidden = False
    session.message_seq = 7

    session_registry.persist_session_meta({"s1": session}, session_meta_file=str(meta_file))
    loaded_session = _session("s1", created_at=1)
    applied = session_registry.apply_session_meta({"s1": loaded_session}, session_meta_file=str(meta_file))

    cursors: dict[str, dict[str, int]] = {}
    session_registry.mark_read(cursors, "s1", "device", 3)
    session_registry.mark_read(cursors, "s1", "device", 2)
    session_registry.persist_read_cursors(cursors, read_cursor_file=str(cursor_file))
    loaded_cursors = session_registry.load_read_cursors(str(cursor_file))

    assert applied == 1
    assert loaded_session.pinned is True
    assert loaded_session.hidden is False
    assert loaded_cursors == {"s1": {"device": 3}}
    assert session_registry.unread_for(loaded_cursors, session, "device") == 4
