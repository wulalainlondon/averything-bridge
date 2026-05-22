from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable


# App-side mirror: SQLITE_HYDRATE_LIMIT in app/src/config/limits.ts (must stay ≤ DEFAULT_HISTORY_LIMIT)
DEFAULT_HISTORY_LIMIT = int(os.environ.get("BRIDGE_HISTORY_LIMIT", "100"))
MAX_HISTORY_LIMIT = int(os.environ.get("BRIDGE_MAX_HISTORY_LIMIT", "10000"))
HISTORY_INDEX_TTL_SECONDS = float(os.environ.get("BRIDGE_HISTORY_INDEX_TTL_SECONDS", "300"))


@dataclass
class HistoryIndex:
    key: tuple[str, int, int]
    built_at: float
    messages: list[dict] = field(default_factory=list)
    by_source_id: dict[str, int] = field(default_factory=dict)


_JSONL_HISTORY_CACHE: dict[str, HistoryIndex] = {}


def clamp_history_limit(value: object, default: int = DEFAULT_HISTORY_LIMIT) -> int:
    try:
        n = int(value)
    except Exception:
        n = default
    return max(1, min(n, MAX_HISTORY_LIMIT))


def normalize_text(value: str) -> str:
    return value.rstrip()


def canonical_content_hash(role: str, content: str, blocks: list[dict] | None = None) -> str:
    normalized_blocks: list[dict] = []
    for block in blocks or []:
        if block.get("type") == "text":
            normalized_blocks.append({"type": "text", "text": normalize_text(str(block.get("text", "")))})
        elif block.get("type") == "tool_call":
            normalized_blocks.append({
                "type": "tool_call",
                "tool_use_id": str(block.get("tool_use_id", "")),
                "name": str(block.get("name", "")),
                "command": str(block.get("command", "")),
                "output": normalize_text(str(block.get("output", ""))),
            })
    payload = {
        "role": role,
        "normalizedText": normalize_text(content),
        "normalizedBlocks": normalized_blocks,
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def complete_history_message(
    *,
    source: str,
    source_session_id: str,
    source_message_id: str,
    role: str,
    content: str,
    timestamp: int | None = None,
    blocks: list[dict] | None = None,
) -> dict:
    msg: dict = {
        "role": role,
        "content": content,
        "source": source,
        "source_session_id": source_session_id,
        "source_message_id": source_message_id,
        "content_hash": canonical_content_hash(role, content, blocks),
    }
    if timestamp is not None:
        msg["timestamp"] = timestamp
    if blocks:
        msg["blocks"] = blocks
    return msg


def _file_cache_key(path: str) -> tuple[str, int, int]:
    st = os.stat(path)
    return (path, int(st.st_mtime_ns), int(st.st_size))


def load_indexed_jsonl_messages(
    *,
    cache_name: str,
    path: str,
    parse_line: Callable[[dict, int, int], dict | None],
) -> HistoryIndex:
    key = _file_cache_key(path)
    cached = _JSONL_HISTORY_CACHE.get(cache_name)
    now = time.time()
    if cached and cached.key == key and now - cached.built_at < HISTORY_INDEX_TTL_SECONDS:
        return cached

    messages: list[dict] = []
    offset = 0
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line_no, raw in enumerate(f, start=1):
            start_offset = offset
            offset += len(raw.encode("utf-8", errors="ignore"))
            line = raw.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            msg = parse_line(row, line_no, start_offset)
            if msg:
                messages.append(msg)

    index = HistoryIndex(
        key=key,
        built_at=now,
        messages=messages,
        by_source_id={str(m.get("source_message_id")): i for i, m in enumerate(messages) if m.get("source_message_id")},
    )
    _JSONL_HISTORY_CACHE[cache_name] = index
    return index


def slice_history(
    messages: list[dict],
    *,
    limit: int,
    known_last_source_message_id: str = "",
    mode: str = "auto",
    before_source_message_id: str = "",
) -> dict:
    limit = clamp_history_limit(limit)
    source_count = len(messages)

    if before_source_message_id:
        idx = next(
            (i for i, m in enumerate(messages) if m.get("source_message_id") == before_source_message_id),
            None,
        )
        if idx is not None:
            start = max(0, idx - limit)
            page = messages[start:idx]
            return {
                "kind": "snapshot",
                "messages": page,
                "source_count": source_count,
                "known_id_found": True,
                "snapshot_reason": "before_page",
                "has_more_before": start > 0,
            }
        # fallback: id not found, return last limit
        return {
            "kind": "snapshot",
            "messages": messages[-limit:],
            "source_count": source_count,
            "known_id_found": False,
            "snapshot_reason": "before_page",
            "has_more_before": source_count > limit,
        }

    if mode in {"auto", "delta"} and known_last_source_message_id:
        positions = {str(m.get("source_message_id")): i for i, m in enumerate(messages) if m.get("source_message_id")}
        idx = positions.get(known_last_source_message_id)
        if idx is not None:
            delta = messages[idx + 1:]
            return {
                "kind": "delta",
                "messages": delta[-limit:],
                "source_count": source_count,
                "known_id_found": True,
                "has_more_before": idx + 1 > 0,
            }
        if mode == "delta":
            return {
                "kind": "snapshot",
                "messages": messages[-limit:],
                "source_count": source_count,
                "known_id_found": False,
                "snapshot_reason": "known_id_not_found",
                "has_more_before": source_count > limit,
            }

    return {
        "kind": "snapshot",
        "messages": messages[-limit:],
        "source_count": source_count,
        "known_id_found": bool(not known_last_source_message_id),
        "snapshot_reason": "requested_snapshot" if mode == "snapshot" else "known_id_not_found" if known_last_source_message_id else "initial",
        "has_more_before": source_count > limit,
    }
