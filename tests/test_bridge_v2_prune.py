"""
Unit tests for _prune_old_saved_sessions in bridge_v2.

Run: pytest bridge/tests/test_bridge_v2_prune.py
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import pytest

# Ensure the bridge package root is on the path so bridge_v2 imports resolve.
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from bridge.auto_register import prune_old_saved_sessions as _prune_old_saved_sessions


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_sessions(path: Path, entries: dict) -> None:
    path.write_text(json.dumps(entries, indent=2), encoding="utf-8")


def _read_sessions(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _ts(days_ago: float) -> float:
    return time.time() - days_ago * 86400


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_prune_removes_nothing_when_all_recent(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    entries = {
        "s1": {"last_used": _ts(1), "name": "a"},
        "s2": {"last_used": _ts(7), "name": "b"},
        "s3": {"last_used": _ts(29), "name": "c"},
    }
    _write_sessions(saved, entries)
    removed = _prune_old_saved_sessions(saved, days=30)
    assert removed == 0
    data = _read_sessions(saved)
    assert len(data) == 3


def test_prune_removes_stale_keeps_recent(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    entries = {
        "fresh": {"last_used": _ts(5), "name": "fresh"},
        "stale1": {"last_used": _ts(31), "name": "old1"},
        "stale2": {"last_used": _ts(60), "name": "old2"},
        "also_fresh": {"last_used": _ts(14), "name": "also fresh"},
    }
    _write_sessions(saved, entries)
    removed = _prune_old_saved_sessions(saved, days=30)
    assert removed == 2
    data = _read_sessions(saved)
    assert set(data.keys()) == {"fresh", "also_fresh"}


def test_prune_removes_all_when_all_stale(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    entries = {
        "old1": {"last_used": _ts(35), "name": "a"},
        "old2": {"last_used": _ts(90), "name": "b"},
    }
    _write_sessions(saved, entries)
    removed = _prune_old_saved_sessions(saved, days=30)
    assert removed == 2
    data = _read_sessions(saved)
    assert data == {}


def test_prune_returns_zero_when_file_missing(tmp_path):
    saved = tmp_path / "nonexistent.json"
    removed = _prune_old_saved_sessions(saved, days=30)
    assert removed == 0
    assert not saved.exists()


def test_prune_handles_missing_last_used(tmp_path):
    """Entries with no last_used field are treated as timestamp=0 (stale)."""
    saved = tmp_path / "saved_sessions.json"
    entries = {
        "no_ts": {"name": "no timestamp"},
        "zero_ts": {"last_used": 0, "name": "zero timestamp"},
        "recent": {"last_used": _ts(1), "name": "recent"},
    }
    _write_sessions(saved, entries)
    removed = _prune_old_saved_sessions(saved, days=30)
    assert removed == 2
    data = _read_sessions(saved)
    assert list(data.keys()) == ["recent"]


def test_prune_custom_days_param(tmp_path):
    saved = tmp_path / "saved_sessions.json"
    entries = {
        "five_days": {"last_used": _ts(5), "name": "a"},
        "ten_days": {"last_used": _ts(10), "name": "b"},
        "twenty_days": {"last_used": _ts(20), "name": "c"},
    }
    _write_sessions(saved, entries)
    # Prune with 7-day window: only five_days survives.
    removed = _prune_old_saved_sessions(saved, days=7)
    assert removed == 2
    data = _read_sessions(saved)
    assert list(data.keys()) == ["five_days"]


def test_prune_writes_atomically(tmp_path):
    """No .json.tmp file left behind after a successful prune."""
    saved = tmp_path / "saved_sessions.json"
    entries = {
        "stale": {"last_used": _ts(60), "name": "old"},
        "fresh": {"last_used": _ts(1), "name": "new"},
    }
    _write_sessions(saved, entries)
    _prune_old_saved_sessions(saved, days=30)
    tmp = saved.with_suffix('.json.tmp')
    assert not tmp.exists()
    assert saved.exists()
