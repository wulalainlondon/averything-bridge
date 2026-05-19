"""
Unit tests for the external-session done-event detection logic added to bridge_v2.

Because bridge_v2.py imports websockets (not available in test environments),
we copy the two pure helper functions under test here verbatim.  If those
functions change in bridge_v2.py, this file must be updated to match.

Tests cover:
- _read_new_jsonl_lines: reads only bytes past the known offset
- _jsonl_lines_contain_turn_end: detects assistant stop_reason (claude format)
- _jsonl_lines_contain_turn_end: detects assistant stop_reason (codex format)
- Integration scenario: baseline → append user line (no done) → append end_turn → done

Run: pytest bridge/tests/test_jsonl_done_detection.py -v
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Functions under test (copied from bridge_v2.py to avoid websockets import)
# ---------------------------------------------------------------------------

_JSONL_TURN_END_STOP_REASONS: frozenset[str] = frozenset(
    {"end_turn", "max_tokens", "stop_sequence"}
)


def _read_new_jsonl_lines(path: str, from_offset: int) -> tuple[list[dict], int]:
    """Read lines appended to a JSONL file since `from_offset` bytes.

    Returns (parsed_lines, new_offset).  Skips malformed JSON silently.
    """
    try:
        size = os.path.getsize(path)
        if size <= from_offset:
            return [], from_offset
        with open(path, "rb") as fh:
            fh.seek(from_offset)
            raw = fh.read(size - from_offset)
        lines = raw.decode("utf-8", errors="ignore").splitlines()
        parsed: list[dict] = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                parsed.append(json.loads(line))
            except Exception:
                pass
        return parsed, size
    except OSError:
        return [], from_offset


def _jsonl_lines_contain_turn_end(lines: list[dict], fmt: str) -> bool:
    """Return True if any line signals that an assistant turn has completed."""
    if fmt == "claude":
        for d in lines:
            if (
                d.get("type") == "assistant"
                and not d.get("isSidechain")
                and d.get("message", {}).get("stop_reason") in _JSONL_TURN_END_STOP_REASONS
            ):
                return True
    elif fmt == "codex":
        for d in lines:
            if d.get("type") != "response_item":
                continue
            payload = d.get("payload", {})
            if not isinstance(payload, dict):
                continue
            if payload.get("role") == "assistant" and payload.get("stop_reason") in _JSONL_TURN_END_STOP_REASONS:
                return True
    return False


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _append_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _assistant_line(stop_reason: str = "end_turn") -> dict:
    return {
        "type": "assistant",
        "isSidechain": False,
        "message": {"role": "assistant", "stop_reason": stop_reason},
    }


def _user_line() -> dict:
    return {
        "type": "user",
        "message": {"role": "user", "content": "hello"},
    }


def _codex_assistant_line(stop_reason: str = "end_turn") -> dict:
    return {
        "type": "response_item",
        "payload": {
            "type": "message",
            "role": "assistant",
            "stop_reason": stop_reason,
        },
    }


# ---------------------------------------------------------------------------
# _read_new_jsonl_lines
# ---------------------------------------------------------------------------

def test_read_new_jsonl_lines_reads_only_new_bytes(tmp_path: Path) -> None:
    """Bytes before from_offset must be ignored."""
    p = tmp_path / "session.jsonl"
    _write_jsonl(p, [_user_line()])
    baseline_size = p.stat().st_size

    _append_jsonl(p, [_assistant_line()])

    lines, new_offset = _read_new_jsonl_lines(str(p), baseline_size)
    assert len(lines) == 1
    assert lines[0]["type"] == "assistant"
    assert new_offset == p.stat().st_size


def test_read_new_jsonl_lines_empty_when_no_new_bytes(tmp_path: Path) -> None:
    p = tmp_path / "session.jsonl"
    _write_jsonl(p, [_user_line()])
    size = p.stat().st_size

    lines, new_offset = _read_new_jsonl_lines(str(p), size)
    assert lines == []
    assert new_offset == size


def test_read_new_jsonl_lines_skips_malformed_json(tmp_path: Path) -> None:
    p = tmp_path / "session.jsonl"
    with p.open("w", encoding="utf-8") as f:
        f.write("not json\n")
        f.write(json.dumps(_assistant_line()) + "\n")

    lines, _ = _read_new_jsonl_lines(str(p), 0)
    assert len(lines) == 1
    assert lines[0]["type"] == "assistant"


def test_read_new_jsonl_lines_missing_file(tmp_path: Path) -> None:
    p = tmp_path / "nonexistent.jsonl"
    lines, offset = _read_new_jsonl_lines(str(p), 0)
    assert lines == []
    assert offset == 0


# ---------------------------------------------------------------------------
# _jsonl_lines_contain_turn_end — claude format
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stop_reason", ["end_turn", "max_tokens", "stop_sequence"])
def test_claude_fmt_detects_turn_end(stop_reason: str) -> None:
    assert _jsonl_lines_contain_turn_end([_assistant_line(stop_reason)], "claude") is True


def test_claude_fmt_ignores_sidechain_assistant() -> None:
    lines = [{
        "type": "assistant",
        "isSidechain": True,
        "message": {"role": "assistant", "stop_reason": "end_turn"},
    }]
    assert _jsonl_lines_contain_turn_end(lines, "claude") is False


def test_claude_fmt_ignores_tool_use_stop_reason() -> None:
    assert _jsonl_lines_contain_turn_end([_assistant_line("tool_use")], "claude") is False


def test_claude_fmt_ignores_user_lines() -> None:
    assert _jsonl_lines_contain_turn_end([_user_line()], "claude") is False


def test_claude_fmt_empty_list() -> None:
    assert _jsonl_lines_contain_turn_end([], "claude") is False


# ---------------------------------------------------------------------------
# _jsonl_lines_contain_turn_end — codex format
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stop_reason", ["end_turn", "max_tokens", "stop_sequence"])
def test_codex_fmt_detects_turn_end(stop_reason: str) -> None:
    assert _jsonl_lines_contain_turn_end([_codex_assistant_line(stop_reason)], "codex") is True


def test_codex_fmt_ignores_tool_use_stop_reason() -> None:
    assert _jsonl_lines_contain_turn_end([_codex_assistant_line("tool_use")], "codex") is False


def test_codex_fmt_ignores_non_response_item() -> None:
    lines = [{"type": "session_meta", "payload": {"role": "assistant", "stop_reason": "end_turn"}}]
    assert _jsonl_lines_contain_turn_end(lines, "codex") is False


# ---------------------------------------------------------------------------
# Integration: simulate the full watcher loop sequence
# ---------------------------------------------------------------------------

def test_integration_only_new_lines_trigger_detection(tmp_path: Path) -> None:
    """
    Baseline captured after initial historical content.
    Appending a user-line alone must not trigger done.
    Appending an assistant end_turn line must trigger done.
    After consuming those bytes, subsequent calls return no new events.
    """
    p = tmp_path / "session.jsonl"

    # Historical content present when bridge first sees the file.
    _write_jsonl(p, [_user_line(), _assistant_line("tool_use")])
    baseline = p.stat().st_size  # seed — all bytes before this are skipped

    # User sends a follow-up (assistant turn not yet complete).
    _append_jsonl(p, [_user_line()])
    lines, after_user = _read_new_jsonl_lines(str(p), baseline)
    assert _jsonl_lines_contain_turn_end(lines, "claude") is False

    # Assistant finishes its turn.
    _append_jsonl(p, [_assistant_line("end_turn")])
    lines2, after_end = _read_new_jsonl_lines(str(p), after_user)
    assert _jsonl_lines_contain_turn_end(lines2, "claude") is True

    # No more new content — done must not fire again.
    lines3, _ = _read_new_jsonl_lines(str(p), after_end)
    assert _jsonl_lines_contain_turn_end(lines3, "claude") is False
