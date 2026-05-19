"""
Unit tests for bridge/scripts/fixup_saved_sessions_last_used.py

Tests use tmp_path fixtures with synthetic saved_sessions.json and stub jsonl
files so no real ~/.claude or ~/.codex data is touched.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import from scripts/ — add its parent to sys.path so the import resolves.
_SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

from fixup_saved_sessions_last_used import (
    iso_to_epoch,
    last_message_ts_in_jsonl,
    run_fixup,
)

_VALID_UUID = "30fb9837-561d-4f34-8e34-b9d72af5e770"
_VALID_UUID2 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_jsonl(directory: Path, uuid: str, timestamps: list[str]) -> Path:
    """Write a minimal JSONL conversation file with the given timestamps."""
    project_dir = directory / "project-abc"
    project_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = project_dir / f"{uuid}.jsonl"
    lines = []
    for ts in timestamps:
        record = {
            "type": "user",
            "uuid": f"msg-{ts}",
            "timestamp": ts,
            "message": {"content": "hello"},
        }
        lines.append(json.dumps(record))
    jsonl_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return jsonl_path


def _make_saved_sessions(path: Path, entries: dict) -> None:
    path.write_text(json.dumps(entries, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# iso_to_epoch tests
# ---------------------------------------------------------------------------

def test_iso_to_epoch_basic():
    result = iso_to_epoch("2024-01-15T12:00:00.000Z")
    assert result is not None
    assert isinstance(result, float)
    # 2024-01-15 12:00:00 UTC
    from datetime import datetime, timezone
    expected = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    assert abs(result - expected) < 1.0


def test_iso_to_epoch_no_z():
    result = iso_to_epoch("2024-01-15T12:00:00.000")
    assert result is not None


def test_iso_to_epoch_invalid():
    assert iso_to_epoch("") is None
    assert iso_to_epoch("bad-data") is None
    assert iso_to_epoch("not/a/date") is None


# ---------------------------------------------------------------------------
# last_message_ts_in_jsonl tests
# ---------------------------------------------------------------------------

def test_last_message_ts_returns_latest(tmp_path):
    jsonl = _make_jsonl(
        tmp_path,
        _VALID_UUID,
        ["2024-01-01T00:00:00.000Z", "2024-06-01T00:00:00.000Z", "2024-12-31T23:59:59.000Z"],
    )
    result = last_message_ts_in_jsonl(jsonl)
    # Should return the LAST timestamp found in the tail (i.e. most recent line)
    assert result == "2024-12-31T23:59:59.000Z"


def test_last_message_ts_missing_file(tmp_path):
    missing = tmp_path / "no_such.jsonl"
    assert last_message_ts_in_jsonl(missing) is None


def test_last_message_ts_no_timestamps(tmp_path):
    f = tmp_path / "empty.jsonl"
    f.write_text('{"type": "summary", "content": "no ts here"}\n', encoding="utf-8")
    assert last_message_ts_in_jsonl(f) is None


# ---------------------------------------------------------------------------
# run_fixup integration tests
# ---------------------------------------------------------------------------

def test_run_fixup_updates_last_used(tmp_path):
    """run_fixup replaces wall-clock last_used with the real jsonl timestamp."""
    claude_root = tmp_path / "claude" / "projects"
    codex_root = tmp_path / "codex" / "sessions"
    claude_root.mkdir(parents=True)
    codex_root.mkdir(parents=True)

    real_ts = "2024-03-10T14:30:00.000Z"
    _make_jsonl(claude_root, _VALID_UUID, [real_ts])

    # Entry with a stale wall-clock last_used (today's epoch — the bug scenario)
    saved = tmp_path / "saved_sessions.json"
    _make_saved_sessions(
        saved,
        {
            "jl_c_" + _VALID_UUID[:11]: {
                "name": "test session",
                "claude_uuid": _VALID_UUID,
                "last_used": time.time(),  # wrong: was set to wall clock
                "cwd": "/workspace",
                "backend": "claude",
                "model": "",
                "sandbox": "danger-full-access",
                "image_dir": "",
            }
        },
    )

    result = run_fixup(saved, claude_root=claude_root, codex_root=codex_root, cutoff_days=30)

    assert result["before"] == 1
    assert result["updated"] == 1
    # The real_ts is from 2024-03-10 which is > 30 days ago, so pruned
    assert result["pruned"] == 1
    assert result["remaining"] == 0


def test_run_fixup_preserves_recent_entries(tmp_path):
    """Entries with a jsonl timestamp within the cutoff window survive pruning."""
    claude_root = tmp_path / "claude" / "projects"
    codex_root = tmp_path / "codex" / "sessions"
    claude_root.mkdir(parents=True)
    codex_root.mkdir(parents=True)

    from datetime import datetime, timezone
    # Use a timestamp 3 days ago (well within 30-day cutoff)
    three_days_ago = time.time() - 3 * 86400
    recent_ts = datetime.fromtimestamp(three_days_ago, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )
    _make_jsonl(claude_root, _VALID_UUID, [recent_ts])

    saved = tmp_path / "saved_sessions.json"
    _make_saved_sessions(
        saved,
        {
            "jl_c_" + _VALID_UUID[:11]: {
                "name": "recent session",
                "claude_uuid": _VALID_UUID,
                "last_used": 1.0,  # old wall-clock bug value
                "cwd": "/workspace",
                "backend": "claude",
                "model": "",
                "sandbox": "danger-full-access",
                "image_dir": "",
            }
        },
    )

    result = run_fixup(saved, claude_root=claude_root, codex_root=codex_root, cutoff_days=30)

    assert result["before"] == 1
    assert result["updated"] == 1
    assert result["pruned"] == 0
    assert result["remaining"] == 1

    # Verify the written value matches the real timestamp
    data = json.loads(saved.read_text(encoding="utf-8"))
    entry = next(iter(data.values()))
    expected_epoch = iso_to_epoch(recent_ts)
    assert abs(entry["last_used"] - expected_epoch) < 1.0


def test_run_fixup_skips_entries_with_no_jsonl(tmp_path):
    """Entries whose jsonl file cannot be found are left unchanged."""
    claude_root = tmp_path / "claude" / "projects"
    codex_root = tmp_path / "codex" / "sessions"
    claude_root.mkdir(parents=True)
    codex_root.mkdir(parents=True)

    saved = tmp_path / "saved_sessions.json"
    original_last_used = time.time() - 5 * 86400
    _make_saved_sessions(
        saved,
        {
            "jl_c_" + _VALID_UUID[:11]: {
                "name": "orphan session",
                "claude_uuid": _VALID_UUID,  # no matching jsonl created
                "last_used": original_last_used,
                "cwd": "/nowhere",
                "backend": "claude",
                "model": "",
                "sandbox": "danger-full-access",
                "image_dir": "",
            }
        },
    )

    result = run_fixup(saved, claude_root=claude_root, codex_root=codex_root, cutoff_days=30)

    # updated=0 because no jsonl was found
    assert result["updated"] == 0
    # Entry is not pruned either (last_used not updated so it stays)
    data = json.loads(saved.read_text(encoding="utf-8"))
    assert len(data) == 1


def test_run_fixup_missing_saved_sessions(tmp_path):
    """run_fixup handles a missing saved_sessions.json gracefully."""
    claude_root = tmp_path / "claude" / "projects"
    codex_root = tmp_path / "codex" / "sessions"
    claude_root.mkdir(parents=True)
    codex_root.mkdir(parents=True)

    missing = tmp_path / "nonexistent.json"
    result = run_fixup(missing, claude_root=claude_root, codex_root=codex_root)
    assert result["before"] == 0
    assert result["remaining"] == 0
