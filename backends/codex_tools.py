"""Codex tool-call normalization shared by live app-server and native history."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CodexToolCall:
    tool_use_id: str
    name: str
    command: str
    output: str = ""

    def history_block(self) -> dict:
        return {
            "type": "tool_call",
            "tool_use_id": self.tool_use_id,
            "name": self.name,
            "command": self.command,
            "output": self.output,
        }


_NAME_MAP = {
    "exec": "Bash",
    "shell": "Bash",
    "bash": "Bash",
    "exec_command": "Bash",
    "commandexecution": "Bash",
    "command_execution": "Bash",
    "apply_patch": "ApplyPatch",
    "filechange": "ApplyPatch",
    "file_change": "ApplyPatch",
    "write_stdin": "Stdin",
    "view_image": "ViewImage",
}


def _compact_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return str(value)


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return _compact_json(value)
    return str(value)


def _json_object(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    raw = value.strip()
    for _ in range(2):
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return parsed
        if isinstance(parsed, str):
            raw = parsed.strip()
            continue
        return {}
    return {}


def _first_text(*values: Any) -> str:
    for value in values:
        text = _as_text(value)
        if text:
            return text
    return ""


def _mapped_name(raw_name: str) -> str:
    key = raw_name.replace("-", "_").lower()
    return _NAME_MAP.get(key, raw_name or "codex")


def _command_for(name: str, data: dict, fallback: Any = "") -> str:
    mapped = _mapped_name(name)
    if mapped == "Bash":
        return _first_text(data.get("cmd"), data.get("command"), data.get("script"), fallback)
    if mapped == "ApplyPatch":
        return _first_text(data.get("patch"), data.get("cmd"), data.get("command"), fallback)
    if mapped == "Stdin":
        return _first_text(data.get("chars"), data.get("input"), data.get("text"), fallback)
    if mapped == "ViewImage":
        return _first_text(data.get("path"), data.get("image_path"), data.get("file"), fallback)
    return _first_text(data.get("command"), data.get("input"), fallback, data)


def normalize_codex_response_tool(payload: dict, output: str = "") -> CodexToolCall | None:
    """Return a shared tool_call block for Codex rollout response items."""
    if not isinstance(payload, dict):
        return None
    kind = str(payload.get("type") or "")
    if kind not in {"function_call", "custom_tool_call"}:
        return None

    raw_name = str(payload.get("name") or payload.get("tool_name") or kind)
    if raw_name == "update_plan":
        return None
    args = _json_object(payload.get("arguments") or payload.get("input") or payload.get("raw_input"))
    call_id = _first_text(
        payload.get("call_id"),
        payload.get("callId"),
        payload.get("id"),
        payload.get("item_id"),
    ) or "codex_tool"
    command = _command_for(raw_name, args, payload.get("arguments") or payload.get("input"))
    return CodexToolCall(call_id, _mapped_name(raw_name), command, _as_text(output))


def codex_response_tool_output(payload: dict) -> tuple[str, str] | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("type") not in {"function_call_output", "custom_tool_call_output"}:
        return None
    call_id = _first_text(
        payload.get("call_id"),
        payload.get("callId"),
        payload.get("id"),
        payload.get("item_id"),
    )
    output = _first_text(payload.get("output"), payload.get("result"), payload.get("content"))
    if not call_id:
        return None
    return call_id, output


def codex_payload_call_id(payload: dict) -> str:
    if not isinstance(payload, dict):
        return ""
    return _first_text(
        payload.get("call_id"),
        payload.get("callId"),
        payload.get("id"),
        payload.get("item_id"),
    )


def normalize_codex_live_tool(params: dict) -> CodexToolCall | None:
    """Normalize an app-server item/started notification into a shared tool call."""
    if not isinstance(params, dict):
        return None
    item = params.get("item") if isinstance(params.get("item"), dict) else {}
    raw_name = str(
        params.get("name")
        or params.get("toolName")
        or item.get("name")
        or item.get("type")
        or ""
    )
    if raw_name == "update_plan":
        return None
    args = _json_object(
        params.get("arguments")
        or params.get("input")
        or item.get("arguments")
        or item.get("input")
    )
    fallback = _first_text(
        params.get("command"),
        item.get("command"),
        params.get("input"),
        item.get("input"),
    )
    call_id = _first_text(
        params.get("itemId"),
        params.get("callId"),
        params.get("toolCallId"),
        params.get("toolUseId"),
        item.get("id"),
    ) or "codex_item"
    command = _command_for(raw_name, args, fallback)
    return CodexToolCall(call_id, _mapped_name(raw_name), command)
