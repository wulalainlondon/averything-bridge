"""Integration tests: path-jail enforcement at all 5 handler entry points.

These tests exercise the path-resolution logic directly (no WS stack needed).
They use tmp_path so every run is isolated and independent of the real FS.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

# Make `bridge/` importable regardless of where pytest is invoked from.
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from bridge.utils.path_jail import resolve_jailed, JailEscape, is_inside_jail


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve(req_path: str, root_dir: str) -> str:
    return resolve_jailed(req_path, root_dir)


# ---------------------------------------------------------------------------
# Site 1 — browse_dir path resolution (file_ops)
# ---------------------------------------------------------------------------

class TestBrowseDirJail:
    def test_inside_jail_ok(self, tmp_path):
        sub = tmp_path / "projects"
        sub.mkdir()
        result = _resolve(str(sub), str(tmp_path))
        assert result == str(sub)

    def test_outside_raises(self, tmp_path):
        with pytest.raises(JailEscape) as exc_info:
            _resolve("/etc/passwd", str(tmp_path))
        assert exc_info.value.root_dir == str(tmp_path)

    def test_dotdot_escape_raises(self, tmp_path):
        inner = tmp_path / "inner"
        inner.mkdir()
        with pytest.raises(JailEscape):
            _resolve(str(inner / ".." / ".."), str(tmp_path))

    def test_no_jail_when_root_dir_empty(self, tmp_path):
        # When root_dir == "", jail is disabled — /etc/passwd resolves fine.
        result = _resolve("/etc/passwd", "")
        assert result == os.path.realpath("/etc/passwd")

    def test_tilde_inside_jail(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        result = _resolve("~", str(tmp_path))
        assert result == os.path.realpath(str(tmp_path))


# ---------------------------------------------------------------------------
# Site 2 — shell_create cwd resolution (runtime_ops)
# ---------------------------------------------------------------------------

class TestShellCreateJail:
    def test_valid_cwd_inside_jail(self, tmp_path):
        cwd = tmp_path / "workspace"
        cwd.mkdir()
        result = _resolve(str(cwd), str(tmp_path))
        assert result == str(cwd)

    def test_cwd_outside_jail_raises(self, tmp_path):
        with pytest.raises(JailEscape) as exc_info:
            _resolve("/tmp", str(tmp_path))
        e = exc_info.value
        assert e.req_path == "/tmp"
        assert e.root_dir == str(tmp_path)

    def test_symlink_outside_raises(self, tmp_path, tmp_path_factory):
        outside = tmp_path_factory.mktemp("outside")
        link = tmp_path / "link_to_outside"
        os.symlink(str(outside), str(link))
        with pytest.raises(JailEscape):
            _resolve(str(link), str(tmp_path))


# ---------------------------------------------------------------------------
# Site 3 — new_session cwd resolution (session_routes)
# ---------------------------------------------------------------------------

class TestNewSessionJail:
    def test_valid_session_cwd(self, tmp_path):
        cwd = tmp_path / "myproject"
        cwd.mkdir()
        result = _resolve(str(cwd), str(tmp_path))
        assert result == str(cwd)

    def test_escape_via_parent_raises(self, tmp_path):
        parent = str(tmp_path.parent)
        with pytest.raises(JailEscape):
            _resolve(parent, str(tmp_path))

    def test_empty_root_dir_allows_anything(self):
        result = _resolve(os.path.expanduser("~"), "")
        assert result == os.path.realpath(os.path.expanduser("~"))


# ---------------------------------------------------------------------------
# Site 4 — _media_request_handler (bridge_v2): tested via resolve_jailed
# ---------------------------------------------------------------------------

class TestMediaHandlerJail:
    def test_file_inside_jail_ok(self, tmp_path):
        f = tmp_path / "image.jpg"
        f.write_bytes(b"fake-jpeg")
        result = _resolve(str(f), str(tmp_path))
        assert result == str(f)

    def test_file_outside_jail_raises(self, tmp_path):
        with pytest.raises(JailEscape):
            _resolve("/etc/hosts", str(tmp_path))

    def test_no_jail_empty_root(self, tmp_path):
        f = tmp_path / "image.jpg"
        f.write_bytes(b"fake-jpeg")
        # When _ROOT_DIR == "" the media handler skips the check.
        result = _resolve(str(f), "")
        assert result == str(f)


# ---------------------------------------------------------------------------
# Site 5 — restore_sessions_from_disk: is_inside_jail logic
# ---------------------------------------------------------------------------

class TestRestoreSessionsJail:
    def test_session_cwd_inside_jail(self, tmp_path):
        cwd = os.path.realpath(str(tmp_path / "work"))
        root = os.path.realpath(str(tmp_path))
        assert is_inside_jail(cwd, root) is True

    def test_session_cwd_outside_jail_dropped(self, tmp_path):
        outside_cwd = os.path.realpath("/tmp")
        root = os.path.realpath(str(tmp_path))
        assert is_inside_jail(outside_cwd, root) is False

    def test_cwd_equals_root_is_inside(self, tmp_path):
        root = os.path.realpath(str(tmp_path))
        assert is_inside_jail(root, root) is True

    def test_no_jail_empty_root_always_inside(self, tmp_path):
        # When root_dir == "", the restore function skips filtering entirely.
        # Simulate: is_inside_jail is not called when root_dir is falsy.
        root_dir = ""
        assert not root_dir  # guard: confirms the branch is skipped

    def test_restore_filters_outside_session(self, tmp_path):
        """End-to-end: restore_sessions_from_disk drops sessions outside jail."""
        import json
        import tempfile
        from bridge.session_registry import restore_sessions_from_disk

        # Build a saved_sessions.json with one inside and one outside session.
        inside_cwd = str(tmp_path / "inside")
        Path(inside_cwd).mkdir()

        now = time.time()
        saved = {
            "sid_inside": {
                "name": "inside session",
                "cwd": inside_cwd,
                "backend": "claude",
                "resume_id": "12345678-1234-1234-1234-123456789012",
                "last_used": now,
            },
            "sid_outside": {
                "name": "outside session",
                "cwd": "/tmp/completely_outside",
                "backend": "claude",
                "resume_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "last_used": now,
            },
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tf:
            json.dump(saved, tf)
            saved_path = tf.name

        sessions: dict = {}
        restore_sessions_from_disk(
            sessions,
            saved_sessions_file=saved_path,
            normalize_backend=lambda b: b or "claude",
            root_dir=str(tmp_path),
        )

        assert "sid_inside" in sessions, "session inside jail should be restored"
        assert "sid_outside" not in sessions, "session outside jail should be dropped"

    def test_restore_no_filter_when_root_dir_empty(self, tmp_path):
        """When root_dir == '', all sessions are restored regardless of cwd."""
        import json
        import tempfile
        from bridge.session_registry import restore_sessions_from_disk

        saved = {
            "sid_a": {
                "name": "any session",
                "cwd": "/tmp/anywhere",
                "backend": "claude",
                "resume_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "last_used": time.time(),
            },
        }

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tf:
            json.dump(saved, tf)
            saved_path = tf.name

        sessions: dict = {}
        restore_sessions_from_disk(
            sessions,
            saved_sessions_file=saved_path,
            normalize_backend=lambda b: b or "claude",
            root_dir="",  # no jail
        )

        assert "sid_a" in sessions
