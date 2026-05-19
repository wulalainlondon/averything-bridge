"""
Self-test: verify that _spawn_proc does NOT set session.resume_id
when allow_resume_fallback is False (the default), even when
resume_id is None and the cwd contains .jsonl files.

Scenario: user creates a new session in cwd=claude-bridge.
Expected: _spawn_proc must not pick up a stale jsonl from that directory.
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

_BRIDGE_ROOT = Path(__file__).parent.parent
_REPO_ROOT = _BRIDGE_ROOT.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_BRIDGE_ROOT))


def _random_uuid() -> str:
    return str(uuid.uuid4())


def _make_session(sid: str, cwd: str = ""):
    from bridge_v2 import Session
    s = Session(
        session_id=sid,
        name=f"Test {sid}",
        created_at=time.time(),
        backend_name="claude",
    )
    s.resume_id = None
    s.cwd = cwd
    return s


class TestSpawnNoFallbackDefault:
    """_spawn_proc(session) — default allow_resume_fallback=False must not set resume_id."""

    def test_new_session_does_not_auto_resume(self, tmp_path):
        """
        Given: session.resume_id=None, cwd has a valid .jsonl file.
        When:  _spawn_proc is called WITHOUT allow_resume_fallback=True.
        Then:  session.resume_id remains None after the call.
        """
        from backends.claude_cli import ClaudeCliBackend, _ClaudeState

        # Create a fake claude projects directory with a .jsonl for our cwd.
        cwd = str(tmp_path / "myproject")
        os.makedirs(cwd, exist_ok=True)
        mangled = "-" + cwd.lstrip("/").replace("/", "-")
        proj_dir = tmp_path / "claude_projects" / mangled
        proj_dir.mkdir(parents=True)

        stale_uuid = _random_uuid()
        jsonl_path = proj_dir / f"{stale_uuid}.jsonl"
        jsonl_path.write_text('{"type": "user"}\n', encoding="utf-8")

        # Build a backend that points at our fake projects dir.
        backend = ClaudeCliBackend.__new__(ClaudeCliBackend)
        backend._states = {}
        backend._claude_bin = "claude"
        backend._persist_session_fn = None
        backend._notify_fcm_fn = None
        backend._broadcast_fn = None
        backend._claude_projects_dir = str(tmp_path / "claude_projects")

        sid = f"new_session_{_random_uuid()[:8]}"
        session = _make_session(sid, cwd=cwd)

        state = _ClaudeState()
        state.proc = None
        state.spawning = False
        backend._states[sid] = state

        async def run():
            # Patch create_subprocess_exec to avoid actually launching claude.
            fake_proc = MagicMock()
            fake_proc.pid = 12345
            fake_proc.returncode = None
            fake_proc.stdin = AsyncMock()
            fake_proc.stdout = MagicMock()
            fake_proc.stderr = MagicMock()

            async def fake_create_subprocess(*args, **kwargs):
                return fake_proc

            with patch("asyncio.create_subprocess_exec", side_effect=fake_create_subprocess), \
                 patch.object(backend, "_stdout_reader", new=AsyncMock()), \
                 patch.object(backend, "_stderr_reader", new=AsyncMock()), \
                 patch.object(backend, "_watch_proc", new=AsyncMock()):
                # Default call — no allow_resume_fallback.
                await backend._spawn_proc(session)

        asyncio.run(run())

        assert session.resume_id is None, (
            f"resume_id was set to {session.resume_id!r} — "
            "fallback fired on a new session (over-trigger bug still present)"
        )

    def test_idle_timeout_respawn_does_auto_resume(self, tmp_path):
        """
        Given: session.resume_id=None, cwd has a valid .jsonl file.
        When:  _spawn_proc is called WITH allow_resume_fallback=True.
        Then:  session.resume_id is set to the stale jsonl stem.
        """
        from backends.claude_cli import ClaudeCliBackend, _ClaudeState

        cwd = str(tmp_path / "myproject2")
        os.makedirs(cwd, exist_ok=True)
        mangled = "-" + cwd.lstrip("/").replace("/", "-")
        proj_dir = tmp_path / "claude_projects2" / mangled
        proj_dir.mkdir(parents=True)

        stale_uuid = _random_uuid()
        jsonl_path = proj_dir / f"{stale_uuid}.jsonl"
        jsonl_path.write_text('{"type": "user"}\n', encoding="utf-8")

        backend = ClaudeCliBackend.__new__(ClaudeCliBackend)
        backend._states = {}
        backend._claude_bin = "claude"
        backend._persist_session_fn = None
        backend._notify_fcm_fn = None
        backend._broadcast_fn = None
        backend._claude_projects_dir = str(tmp_path / "claude_projects2")

        sid = f"respawn_{_random_uuid()[:8]}"
        session = _make_session(sid, cwd=cwd)

        state = _ClaudeState()
        state.proc = None
        state.spawning = False
        backend._states[sid] = state

        async def run():
            fake_proc = MagicMock()
            fake_proc.pid = 99999
            fake_proc.returncode = None
            fake_proc.stdin = AsyncMock()
            fake_proc.stdout = MagicMock()
            fake_proc.stderr = MagicMock()

            async def fake_create_subprocess(*args, **kwargs):
                return fake_proc

            with patch("asyncio.create_subprocess_exec", side_effect=fake_create_subprocess), \
                 patch.object(backend, "_stdout_reader", new=AsyncMock()), \
                 patch.object(backend, "_stderr_reader", new=AsyncMock()), \
                 patch.object(backend, "_watch_proc", new=AsyncMock()):
                # Idle-timeout path — allow_resume_fallback=True.
                await backend._spawn_proc(session, allow_resume_fallback=True)

        asyncio.run(run())

        assert session.resume_id == stale_uuid, (
            f"Expected resume_id={stale_uuid!r}, got {session.resume_id!r} — "
            "fallback did NOT fire on idle-timeout respawn"
        )
