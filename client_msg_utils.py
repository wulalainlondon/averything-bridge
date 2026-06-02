"""
Pure client-message helpers.

Id shortening, inbound-message summary for debug logs, and the session-handoff
prompt builder. No module state — safe to import anywhere.
"""

from typing import Any


def _short_id(value: Any) -> str:
    text = str(value) if value is not None else ""
    if len(text) <= 18:
        return text
    return f"{text[:12]}...{text[-4:]}"


def _summarize_client_msg(msg: dict, raw_len: int) -> str:
    parts = [f"type={msg.get('type', '<missing>')}", f"bytes={raw_len}"]
    for key in ("session_id", "request_id", "client_id", "device_id"):
        if msg.get(key):
            parts.append(f"{key}={_short_id(msg.get(key))}")
    for key in ("content", "query", "message"):
        val = msg.get(key)
        if isinstance(val, str):
            parts.append(f"{key}_len={len(val)}")
    for key in ("files", "images", "items"):
        val = msg.get(key)
        if isinstance(val, list):
            parts.append(f"{key}_count={len(val)}")
    return " ".join(parts)


def _build_handoff_prompt(history: list[dict], user_request: str = "") -> str:
    lines: list[str] = [
        "Context handoff from previous session. Continue seamlessly.",
        "Use the transcript below as prior context.",
        "",
    ]
    for item in history[-80:]:
        role = str(item.get("role", "user")).upper()
        content = str(item.get("content", ""))
        lines.append(f"{role}:")
        lines.append(content)
        lines.append("")
    if user_request.strip():
        lines.append("LATEST USER REQUEST:")
        lines.append(user_request.strip())
    else:
        lines.append("LATEST USER REQUEST:")
        lines.append("Please continue from the latest point with the same task.")
    return "\n".join(lines)
