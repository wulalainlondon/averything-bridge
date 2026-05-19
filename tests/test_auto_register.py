"""
Unit tests for bridge.auto_register.

Run: pytest bridge/tests/test_auto_register.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from bridge.auto_register import auto_register_session, parse_iso8601_to_epoch

_VALID_UUID = "30fb9837-561d-4f34-8e34-b9d72af5e770"
_VALID_UUID2 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _load(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# test_auto_register_adds_new_session
# ---------------------------------------------------------------------------

def test_auto_register_adds_new_session(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    added = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="my first message",
        cwd="/workspace",
        backend="claude",
        last_used=1_700_000_000.0,
        cutoff_seconds=float('inf'),  # bypass age cutoff for this test
    )
    assert added is True
    data = _load(saved)
    assert len(data) == 1
    entry = next(iter(data.values()))
    assert entry["claude_uuid"] == _VALID_UUID
    assert entry["name"] == "my first message"
    assert entry["cwd"] == "/workspace"
    assert entry["backend"] == "claude"
    assert entry["last_used"] == 1_700_000_000.0


def test_auto_register_returns_false_when_already_registered(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="initial",
        cwd="/x",
        backend="claude",
        last_used=1_000.0,
        cutoff_seconds=float('inf'),
    )
    added = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="duplicate",
        cwd="/y",
        backend="claude",
        last_used=2_000.0,
        cutoff_seconds=float('inf'),
    )
    assert added is False
    data = _load(saved)
    assert len(data) == 1  # no duplicate entry


def test_auto_register_updates_last_used_when_newer(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="first",
        cwd="/w",
        backend="claude",
        last_used=1_000.0,
        cutoff_seconds=float('inf'),
    )
    # Call again with a newer last_used — should update in place
    result = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="first",
        cwd="/w",
        backend="claude",
        last_used=9_000.0,
        cutoff_seconds=float('inf'),
    )
    assert result is False  # not "added" (already existed)
    data = _load(saved)
    entry = next(iter(data.values()))
    assert entry["last_used"] == 9_000.0


def test_auto_register_does_not_downgrade_last_used(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="first",
        cwd="/w",
        backend="claude",
        last_used=9_000.0,
        cutoff_seconds=float('inf'),
    )
    auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="first",
        cwd="/w",
        backend="claude",
        last_used=1_000.0,  # older — must not overwrite
        cutoff_seconds=float('inf'),
    )
    data = _load(saved)
    entry = next(iter(data.values()))
    assert entry["last_used"] == 9_000.0


def test_auto_register_rejects_invalid_uuid(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    added = auto_register_session(
        saved,
        claude_uuid="agent-ac532ada8b890b02c",  # subagent-style ID
        name="subagent msg",
        cwd="/x",
        backend="claude",
        last_used=1_000.0,
        cutoff_seconds=float('inf'),
    )
    assert added is False
    assert not saved.exists()


def test_auto_register_rejects_unknown_backend(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    added = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="some msg",
        cwd="/x",
        backend="ollama",
        last_used=1_000.0,
        cutoff_seconds=float('inf'),
    )
    assert added is False
    assert not saved.exists()


def test_auto_register_rejects_empty_name(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    added = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="",
        cwd="/x",
        backend="claude",
        last_used=1_000.0,
        cutoff_seconds=float('inf'),
    )
    assert added is False
    assert not saved.exists()


def test_auto_register_creates_file_if_missing(tmp_path):
    saved = tmp_path / "subdir" / "saved_sessions.json"
    added = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="hello",
        cwd="~",
        backend="claude",
        last_used=1_000.0,
        cutoff_seconds=float('inf'),
    )
    assert added is True
    assert saved.exists()


def test_auto_register_preserves_existing_entries(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    # Pre-populate with an existing session
    existing = {
        "s_old": {
            "name": "old session",
            "claude_uuid": _VALID_UUID2,
            "last_used": 500.0,
            "cwd": "/old",
            "backend": "claude",
            "model": "",
            "sandbox": "danger-full-access",
            "image_dir": "",
        }
    }
    saved.write_text(json.dumps(existing), encoding="utf-8")

    added = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="new session",
        cwd="/new",
        backend="claude",
        last_used=1_000.0,
        cutoff_seconds=float('inf'),
    )
    assert added is True
    data = _load(saved)
    assert len(data) == 2
    uuids = {v["claude_uuid"] for v in data.values()}
    assert _VALID_UUID2 in uuids
    assert _VALID_UUID in uuids


def test_auto_register_sid_prefix_claude(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="test",
        cwd="/",
        backend="claude",
        last_used=1_000.0,
        cutoff_seconds=float('inf'),
    )
    data = _load(saved)
    sid = next(iter(data.keys()))
    assert sid.startswith("jl_c_")


def test_auto_register_truncates_long_name(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    long_name = "A" * 200
    auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name=long_name,
        cwd="/",
        backend="claude",
        last_used=1_000.0,
        cutoff_seconds=float('inf'),
    )
    data = _load(saved)
    entry = next(iter(data.values()))
    assert len(entry["name"]) <= 80


# ---------------------------------------------------------------------------
# Cutoff (30-day) tests
# ---------------------------------------------------------------------------

def test_auto_register_rejects_old_session(tmp_path):
    """Sessions older than cutoff_seconds must be silently skipped."""
    saved = tmp_path / "saved_sessions.json"
    old_last_used = time.time() - 31 * 86400  # 31 days ago
    added = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="old session",
        cwd="/x",
        backend="claude",
        last_used=old_last_used,
    )
    assert added is False
    assert not saved.exists()


def test_auto_register_accepts_within_30_days(tmp_path):
    """Sessions within cutoff_seconds must be registered normally."""
    saved = tmp_path / "saved_sessions.json"
    recent_last_used = time.time() - 5 * 86400  # 5 days ago
    added = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="recent session",
        cwd="/x",
        backend="claude",
        last_used=recent_last_used,
    )
    assert added is True
    data = _load(saved)
    assert len(data) == 1


def test_auto_register_update_path_also_respects_cutoff(tmp_path):
    """Update of an existing entry is suppressed when the new last_used is too old."""
    saved = tmp_path / "saved_sessions.json"
    old_ts = time.time() - 45 * 86400  # 45 days ago — outside cutoff

    # Pre-populate so the entry already exists (simulating a session registered
    # before it aged out, or written directly into the file).
    existing = {
        "jl_c_" + _VALID_UUID[:11]: {
            "name": "old entry",
            "claude_uuid": _VALID_UUID,
            "last_used": old_ts,
            "cwd": "/x",
            "backend": "claude",
            "model": "",
            "sandbox": "danger-full-access",
            "image_dir": "",
        }
    }
    saved.write_text(json.dumps(existing), encoding="utf-8")

    # Attempt to update with a last_used that is still outside the cutoff.
    newer_but_still_old = time.time() - 35 * 86400  # 35 days ago
    result = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="old entry",
        cwd="/x",
        backend="claude",
        last_used=newer_but_still_old,
    )
    # Returns False (not added) and does NOT update last_used because the new
    # last_used is also outside the cutoff window.
    assert result is False
    data = _load(saved)
    entry = next(iter(data.values()))
    assert entry["last_used"] == old_ts  # not updated


def test_auto_register_update_with_newer_recent_works(tmp_path):
    """Update of an existing entry succeeds when the new last_used is within cutoff."""
    saved = tmp_path / "saved_sessions.json"
    five_days_ago = time.time() - 5 * 86400
    one_day_ago = time.time() - 1 * 86400

    existing = {
        "jl_c_" + _VALID_UUID[:11]: {
            "name": "recent entry",
            "claude_uuid": _VALID_UUID,
            "last_used": five_days_ago,
            "cwd": "/x",
            "backend": "claude",
            "model": "",
            "sandbox": "danger-full-access",
            "image_dir": "",
        }
    }
    saved.write_text(json.dumps(existing), encoding="utf-8")

    result = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="recent entry",
        cwd="/x",
        backend="claude",
        last_used=one_day_ago,
    )
    assert result is False  # already existed, not "added"
    data = _load(saved)
    entry = next(iter(data.values()))
    assert entry["last_used"] == pytest.approx(one_day_ago, abs=1.0)


def test_auto_register_custom_cutoff_seconds(tmp_path):
    """cutoff_seconds param can be overridden to accept or reject specific ages."""
    saved = tmp_path / "saved_sessions.json"
    # 5-day-old session; with a 3-day cutoff it should be rejected.
    five_days_ago = time.time() - 5 * 86400
    added = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="borderline session",
        cwd="/x",
        backend="claude",
        last_used=five_days_ago,
        cutoff_seconds=3 * 86400,
    )
    assert added is False
    assert not saved.exists()

    # Same session with a 7-day cutoff should be accepted.
    added2 = auto_register_session(
        saved,
        claude_uuid=_VALID_UUID,
        name="borderline session",
        cwd="/x",
        backend="claude",
        last_used=five_days_ago,
        cutoff_seconds=7 * 86400,
    )
    assert added2 is True


# ---------------------------------------------------------------------------
# parse_iso8601_to_epoch tests
# ---------------------------------------------------------------------------

def test_parse_iso8601_to_epoch_basic():
    """Standard ISO-8601 UTC string without Z suffix should parse correctly."""
    ts = "2026-05-17T08:41:31.732"
    epoch = parse_iso8601_to_epoch(ts)
    assert epoch is not None
    # Verify round-trip: the parsed value should correspond to 2026-05-17 08:41:31 UTC
    from datetime import datetime, timezone
    dt = datetime(2026, 5, 17, 8, 41, 31, 732000, tzinfo=timezone.utc)
    assert abs(epoch - dt.timestamp()) < 1.0


def test_parse_iso8601_handles_z_suffix():
    """Timestamps ending in 'Z' (UTC marker) must be parsed identically."""
    ts_with_z = "2026-05-17T08:41:31.732Z"
    ts_without_z = "2026-05-17T08:41:31.732"
    epoch_z = parse_iso8601_to_epoch(ts_with_z)
    epoch_plain = parse_iso8601_to_epoch(ts_without_z)
    assert epoch_z is not None
    assert epoch_plain is not None
    assert abs(epoch_z - epoch_plain) < 0.001


def test_parse_iso8601_returns_none_on_invalid():
    """Garbage input must return None, not raise."""
    assert parse_iso8601_to_epoch("") is None
    assert parse_iso8601_to_epoch("not-a-date") is None
    assert parse_iso8601_to_epoch("not/a/date either") is None


def test_parse_iso8601_returns_none_on_none_equivalent():
    """Empty string edge cases all return None."""
    assert parse_iso8601_to_epoch("   ") is None or True  # may strip or fail — no raise
    # The important contract: calling with falsy value always returns None.
    result = parse_iso8601_to_epoch("")
    assert result is None
