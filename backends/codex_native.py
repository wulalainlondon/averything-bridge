"""
Codex backend — native session-file reading mixin.

Reads Codex's own ~/.codex/sessions rollout files and session_index.jsonl to
list resumable sessions and load their history independently of the bridge's
live state: path-index building, ISO-timestamp parsing, transcript extraction,
and bootstrap-noise filtering.
"""

import datetime
import gzip
import json
import logging
import os
import re
import time
from typing import TYPE_CHECKING

from .history import (
    complete_history_message, clamp_history_limit, slice_history,
    load_indexed_jsonl_messages,
)
from .codex_common import _sanitize_session_name

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")

# Strips Codex's framework abort wrapper (<turn_aborted>…</turn_aborted>) out of
# transcript text so it is not rendered as user-authored content.
_CODEX_WRAPPER_CLOSED_RE = re.compile(
    r"<(turn_aborted)>.*?</\1>",
    re.IGNORECASE | re.DOTALL,
)


def _is_codex_rollout_file(filename: str) -> bool:
    return filename.startswith("rollout-") and (
        filename.endswith(".jsonl") or filename.endswith(".jsonl.gz")
    )


def _codex_rollout_uid(filename: str) -> str:
    stem = filename
    if stem.endswith(".gz"):
        stem = stem[:-3]
    if stem.endswith(".jsonl"):
        stem = stem[:-6]
    return stem[-36:]


def _open_codex_rollout_text(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")


class _CodexNativeSessionMixin:
    def _get_session_path_index(self) -> dict[str, str]:
        now = time.time()
        if self._session_path_index is not None and now - self._session_path_index_time < 300.0:
            return self._session_path_index
        index: dict[str, str] = {}
        if os.path.isdir(self._native_sessions_root):
            for root, _dirs, files in os.walk(self._native_sessions_root):
                for fn in files:
                    if fn.endswith(".jsonl") or fn.endswith(".jsonl.gz"):
                        uid = _codex_rollout_uid(fn)
                        index[uid] = os.path.join(root, fn)
        self._session_path_index = index
        self._session_path_index_time = now
        return index

    def _find_native_session_file(self, session_id: str) -> str:
        if not session_id:
            return ""
        return self._get_session_path_index().get(session_id, "")

    def _read_native_session_cwd(self, session_id: str) -> str:
        path = self._find_native_session_file(session_id)
        if not path:
            return os.path.expanduser("~")
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue
                    data = json.loads(line)
                    if data.get("type") != "session_meta":
                        continue
                    payload = data.get("payload", {})
                    if isinstance(payload, dict):
                        cwd = payload.get("cwd")
                        if isinstance(cwd, str) and cwd.strip():
                            return cwd
                    break
        except Exception:
            pass
        return os.path.expanduser("~")

    @staticmethod
    def _parse_iso_to_epoch(value: str | None) -> int:
        from datetime import datetime, timezone
        if not value:
            return 0
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return 0

    def _load_native_codex_sessions(self, limit: int = 200) -> list[dict]:
        out: list[dict] = []
        seen_ids: set[str] = set()

        # Legacy path: session_index.jsonl written by Codex versions before ~0.128.
        if os.path.isfile(self._native_session_index_path):
            try:
                with open(self._native_session_index_path, "r", encoding="utf-8", errors="ignore") as f:
                    for raw in f:
                        line = raw.strip()
                        if not line:
                            continue
                        try:
                            row = json.loads(line)
                        except Exception:
                            continue
                        sid = str(row.get("id") or "").strip()
                        if not sid or sid in seen_ids:
                            continue
                        seen_ids.add(sid)
                        raw_name = str(row.get("thread_name") or sid[:8])
                        out.append({
                            "id": f"native_{sid[:12]}",
                            "name": _sanitize_session_name(raw_name, sid[:8]),
                            "claude_uuid": sid,
                            "resume_id": sid,
                            "last_used": self._parse_iso_to_epoch(str(row.get("updated_at") or "")),
                            "cwd": self._read_native_session_cwd(sid),
                        })
            except Exception:
                pass

        # Newer path: Codex >=0.128 writes rollout-{DATETIME}-{UUID}.jsonl files
        # directly under ~/.codex/sessions/YYYY/MM/DD/ and no longer updates
        # session_index.jsonl.  Scan the directory tree for these files.
        if os.path.isdir(self._native_sessions_root):
            rollout_candidates: list[tuple[str, str, str]] = []
            for root, _dirs, files in os.walk(self._native_sessions_root):
                for fn in files:
                    if not _is_codex_rollout_file(fn):
                        continue
                    uid = _codex_rollout_uid(fn)
                    if uid in seen_ids:
                        continue
                    rollout_candidates.append((uid, os.path.join(root, fn), fn))

            # Sort by filename descending (filename encodes the creation timestamp),
            # so the newest sessions come first and we can stop after `limit` of them.
            rollout_candidates.sort(key=lambda x: x[2], reverse=True)

            # Rebuild a fresh per-file cache each scan, reusing entries for unchanged
            # files (thread-safe: read old via .get(), never mutate it in place).
            old_cache = self._codex_scan_cache
            new_cache: dict[str, tuple] = {}
            collected = 0
            for uid, filepath, fn in rollout_candidates:
                if uid in seen_ids:
                    continue
                # Newest-first: once we have `limit` rollout sessions, the rest are
                # strictly older and cannot enter the top-`limit` result. Stop early
                # so a poll opens ~limit files instead of every rollout file on disk.
                if collected >= limit:
                    break
                # Parse timestamp from filename:
                # rollout-2026-05-03T22-59-01-<UUID>.jsonl
                last_used = 0
                try:
                    date_part = fn[8:18]           # "2026-05-03"
                    time_part = fn[19:27].replace("-", ":")  # "22:59:01"
                    last_used = self._parse_iso_to_epoch(f"{date_part}T{time_part}Z")
                except Exception:
                    pass

                # Fast path: file unchanged since last scan — reuse parsed cwd/name
                # without opening it.
                try:
                    st = os.stat(filepath)
                    file_key: tuple | None = (st.st_mtime_ns, st.st_size)
                except OSError:
                    file_key = None
                cached = old_cache.get(filepath) if file_key is not None else None
                if cached is not None and cached[0] == file_key:
                    cwd, name = cached[1], cached[2]
                else:
                    cwd = os.path.expanduser("~")
                    name = uid[:8]
                    try:
                        with _open_codex_rollout_text(filepath) as f:
                            for raw in f:
                                raw = raw.strip()
                                if not raw:
                                    continue
                                try:
                                    d = json.loads(raw)
                                except Exception:
                                    continue
                                t = d.get("type", "")
                                p = d.get("payload", {})
                                if t == "session_meta" and isinstance(p, dict):
                                    cwd = str(p.get("cwd") or cwd)
                                elif t == "event_msg" and isinstance(p, dict) and p.get("type") == "user_message":
                                    msg = p.get("message", "")
                                    if isinstance(msg, str) and msg.strip():
                                        name = msg.strip()
                                    break
                                elif t == "response_item" and isinstance(p, dict) and p.get("role") == "user":
                                    msg = self._extract_text_from_content(p.get("content"))
                                    if msg and not self._is_codex_bootstrap_text(msg):
                                        name = msg
                                        break
                    except Exception:
                        pass
                if file_key is not None:
                    new_cache[filepath] = (file_key, cwd, name)

                seen_ids.add(uid)
                collected += 1
                out.append({
                    "id": f"native_{uid[:12]}",
                    "name": _sanitize_session_name(name, uid[:8]),
                    "claude_uuid": uid,
                    "resume_id": uid,
                    "last_used": last_used,
                    "cwd": cwd,
                })

            if new_cache:
                self._codex_scan_cache = new_cache

        out.sort(key=lambda x: x.get("last_used", 0), reverse=True)
        return out[:limit]

    def _extract_text_from_content(self, content: object) -> str:
        if isinstance(content, str):
            text = content
        elif isinstance(content, list):
            chunks: list[str] = []
            for item in content:
                if not isinstance(item, dict):
                    continue
                txt = item.get("text")
                if isinstance(txt, str) and txt.strip():
                    chunks.append(txt)
            text = "\n".join(chunks).strip()
        else:
            return ""
        text = _CODEX_WRAPPER_CLOSED_RE.sub("", text)
        return text.strip()

    @staticmethod
    def _is_codex_bootstrap_text(text: str) -> bool:
        stripped = text.lstrip()
        if (
            stripped.startswith("<environment_context>")
            and "</environment_context>" in stripped
            and "<cwd>" in stripped
        ):
            return True
        return (
            stripped.startswith("# AGENTS.md instructions")
            and (
                "<environment_context>" in stripped
                or "<INSTRUCTIONS>" in stripped
            )
        )

    def _load_native_session_history(
        self,
        resume_id: str,
        limit: int = 120,
        known_last_source_message_id: str = "",
        mode: str = "snapshot",
        before_source_message_id: str = "",
    ) -> list[dict] | dict:
        path = self._find_native_session_file(resume_id)
        if not path or not os.path.isfile(path):
            return []
        def parse(row: dict, line_no: int, offset: int) -> dict | None:
            if row.get("type") != "response_item":
                return None
            payload = row.get("payload", {})
            if not isinstance(payload, dict) or payload.get("type") != "message":
                return None
            role = payload.get("role")
            if role not in {"user", "assistant"}:
                return None
            if role == "assistant" and payload.get("phase") == "commentary":
                return None
            text = self._extract_text_from_content(payload.get("content"))
            if not text:
                return None
            if role == "user" and self._is_codex_bootstrap_text(text):
                return None
            ts = self._parse_iso_to_epoch(str(row.get("timestamp") or "")) * 1000
            return complete_history_message(
                source="codex",
                source_session_id=resume_id,
                source_message_id=f"codex:{resume_id}:line:{line_no}",
                role=role,
                content=text,
                timestamp=ts or None,
                blocks=[{"type": "text", "text": text}],
            )
        try:
            if path.endswith(".gz"):
                messages: list[dict] = []
                offset = 0
                with _open_codex_rollout_text(path) as f:
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
                        msg = parse(row, line_no, start_offset)
                        if msg:
                            messages.append(msg)
            else:
                index = load_indexed_jsonl_messages(cache_name=f"codex:{resume_id}", path=path, parse_line=parse)
                messages = index.messages
        except Exception:
            return []
        return slice_history(
            messages,
            limit=clamp_history_limit(limit),
            known_last_source_message_id=known_last_source_message_id,
            mode=mode,
            before_source_message_id=before_source_message_id,
        )


# ------------------------------------------------------------------ module helpers
