"""
Claude backend — JSONL history / agent-tree parsing mixin.

Reads ~/.claude/projects/<cwd>/*.jsonl session transcripts: resume-id lookup,
history slicing, the sub-agent (sidechain) tree builder, resumable-session
scanning, and the project-path decoder. All filesystem-heavy work runs in the
executor; results are memoised via the agent-tree mtime cache below and the
shared _JSONL_HISTORY_CACHE.
"""

import asyncio
import logging
import datetime
import json
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Optional, TYPE_CHECKING

from utils.uuid_helper import is_valid_uuid
from .history import (
    complete_history_message, clamp_history_limit, slice_history,
    _JSONL_HISTORY_CACHE, _file_cache_key, HistoryIndex,
    HISTORY_INDEX_TTL_SECONDS, DEFAULT_HISTORY_LIMIT,
)
from .history_sqlite import sqlite_load, sqlite_save_background

if TYPE_CHECKING:
    from bridge_v2 import Session

log = logging.getLogger("bridge_v2")


# ---------------------------------------------------------------------------
# Agent-tree mtime-based result cache
# ---------------------------------------------------------------------------
@dataclass
class _AgentTreeCacheEntry:
    main_mtime_ns: int
    agent_mtimes: dict  # agent_id → mtime_ns
    result: dict        # the full return value of _build_agent_tree_sync

_AGENT_TREE_CACHE: OrderedDict = OrderedDict()   # key=main_jsonl_path → _AgentTreeCacheEntry
_AGENT_TREE_CACHE_LOCK: threading.Lock = threading.Lock()
_AGENT_TREE_CACHE_MAX = 200
# ---------------------------------------------------------------------------


class _ClaudeHistoryMixin:
    def find_session_file(self, resume_id: str) -> "str | None":
        return self._find_session_file_sync(resume_id)

    async def get_resumable_sessions(self, limit: int = 100) -> list[dict]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._scan_local_sessions_sync, limit)

    async def load_session_history(
        self,
        resume_id: str,
        limit: int = 120,
        known_last_source_message_id: str = "",
        mode: str = "snapshot",
        before_source_message_id: str = "",
    ) -> list[dict] | dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._load_session_history_sync,
            resume_id,
            limit,
            known_last_source_message_id,
            mode,
            before_source_message_id,
        )

    # ------------------------------------------------------------------
    # Private sync helpers (Claude session file scanning)
    # ------------------------------------------------------------------

    def _find_session_file_sync(self, uuid: str) -> Optional[str]:
        try:
            for proj in os.scandir(self._claude_projects_dir):
                if not proj.is_dir():
                    continue
                candidate = os.path.join(proj.path, uuid + ".jsonl")
                if os.path.isfile(candidate):
                    return candidate
        except Exception:
            pass
        return None

    def _load_session_history_sync(
        self,
        resume_id: str,
        limit: int = 120,
        known_last_source_message_id: str = "",
        mode: str = "snapshot",
        before_source_message_id: str = "",
    ) -> list | dict:
        path = self._find_session_file_sync(resume_id)
        if not path:
            return []

        import time
        cache_name = f"claude:{resume_id}"
        try:
            cache_key = _file_cache_key(path)
            cached = _JSONL_HISTORY_CACHE.get(cache_name)
            if cached and cached.key == cache_key and time.time() - cached.built_at < HISTORY_INDEX_TTL_SECONDS:
                return slice_history(
                    cached.messages,
                    limit=clamp_history_limit(limit),
                    known_last_source_message_id=known_last_source_message_id,
                    mode=mode,
                    before_source_message_id=before_source_message_id,
                )
        except Exception:
            cache_key = None

        # SQLite persistent cache — survives bridge restarts / Mac sleep-wake.
        # Only reached on memory miss; key (mtime_ns, size) guards staleness.
        if cache_key is not None:
            try:
                cached_messages = sqlite_load(cache_name, cache_key)
                if cached_messages is not None:
                    import time as _time
                    idx = HistoryIndex(
                        key=cache_key,
                        built_at=_time.time(),
                        messages=cached_messages,
                        by_source_id={
                            str(m.get("source_message_id")): i
                            for i, m in enumerate(cached_messages)
                            if m.get("source_message_id")
                        },
                    )
                    _JSONL_HISTORY_CACHE[cache_name] = idx
                    return slice_history(
                        cached_messages,
                        limit=clamp_history_limit(limit),
                        known_last_source_message_id=known_last_source_message_id,
                        mode=mode,
                        before_source_message_id=before_source_message_id,
                    )
            except Exception:
                pass

        _MAX_OUTPUT = 256 * 1024

        def _flatten_tool_result_content(c) -> str:
            if isinstance(c, str):
                return c
            if isinstance(c, list):
                parts = []
                for item in c:
                    if isinstance(item, dict):
                        parts.append(item.get("text", "") or item.get("content", ""))
                    elif isinstance(item, str):
                        parts.append(item)
                return "\n".join(p for p in parts if p)
            return ""

        # Single pass: collect all records, then derive tool_outputs and build messages.
        raw_records: list[tuple[int, dict]] = []  # (line_no, parsed_dict)
        try:
            with open(path, encoding="utf-8", errors="ignore") as f:
                for line_no, raw in enumerate(f, start=1):
                    try:
                        d = json.loads(raw)
                        raw_records.append((line_no, d))
                    except Exception:
                        pass
        except Exception as exc:
            log.warning("Failed to load session history: %s", exc)
            return []

        # Derive tool_use_id -> output mapping (equivalent to former Pass 1)
        tool_outputs: dict = {}
        for _ln, d in raw_records:
            content = d.get("message", {}).get("content", "")
            if not isinstance(content, list):
                continue
            for blk in content:
                if not isinstance(blk, dict):
                    continue
                if blk.get("type") == "tool_result":
                    tid = blk.get("tool_use_id", "")
                    if tid:
                        output = _flatten_tool_result_content(blk.get("content", ""))
                        if len(output) > _MAX_OUTPUT:
                            output = output[:_MAX_OUTPUT] + "\n…(truncated)"
                        tool_outputs[tid] = output

        # Build message list with blocks (equivalent to former Pass 2)
        messages = []
        try:
            file_mtime_ms = int(os.path.getmtime(path) * 1000)
        except Exception:
            file_mtime_ms = int(time.time() * 1000)
        for line_no, d in raw_records:
            try:
                if (
                    d.get("isSidechain")
                    or d.get("type") not in ("user", "assistant")
                    or d.get("isCompactSummary")
                    or d.get("isVisibleInTranscriptOnly")
                ):
                    continue
                role = d["type"]
                content = d.get("message", {}).get("content", "")
                text = ""
                blocks = []
                if isinstance(content, str):
                    text = content
                elif isinstance(content, list):
                    text_parts = []
                    for blk in content:
                        if not isinstance(blk, dict):
                            continue
                        btype = blk.get("type")
                        if btype == "text":
                            t = blk.get("text", "")
                            if t:
                                text_parts.append(t)
                                blocks.append({"type": "text", "text": t})
                        elif btype == "tool_use" and role == "assistant":
                            tid = blk.get("id", "")
                            name = blk.get("name", "")
                            inp = blk.get("input", {})
                            command = inp.get("command", json.dumps(inp)) if isinstance(inp, dict) else json.dumps(inp)
                            output = tool_outputs.get(tid, "")
                            blocks.append({
                                "type": "tool_call",
                                "tool_use_id": tid,
                                "name": name,
                                "command": command,
                                "output": output,
                            })
                    text = "\n".join(text_parts)
                if not text or text.startswith("<") or text.startswith("[Request interrupted"):
                    continue
                # Filter system-injected skill instructions (injected as user text by Claude Code harness).
                if text.startswith("Base directory for this skill:"):
                    continue
                # If no blocks built (e.g. plain-string content), synthesise a text block
                if not blocks:
                    blocks = [{"type": "text", "text": text}]
                ts_ms = 0
                ts_str = d.get("timestamp", "")
                if ts_str:
                    try:
                        ts_ms = int(datetime.datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp() * 1000)
                    except Exception:
                        pass
                if not ts_ms:
                    ts_ms = file_mtime_ms
                messages.append(complete_history_message(
                    source="claude",
                    source_session_id=resume_id,
                    source_message_id=f"claude:{resume_id}:line:{line_no}",
                    role=role,
                    content=text,
                    timestamp=ts_ms,
                    blocks=blocks,
                ))
            except Exception:
                pass

        if cache_key is not None:
            import time as _time
            _JSONL_HISTORY_CACHE[cache_name] = HistoryIndex(
                key=cache_key,
                built_at=_time.time(),
                messages=messages,
                by_source_id={
                    str(m.get("source_message_id")): i
                    for i, m in enumerate(messages)
                    if m.get("source_message_id")
                },
            )
            sqlite_save_background(cache_name, cache_key, messages)

        return slice_history(
            messages,
            limit=clamp_history_limit(limit),
            known_last_source_message_id=known_last_source_message_id,
            mode=mode,
            before_source_message_id=before_source_message_id,
        )

    @staticmethod
    def _scan_main_jsonl_once(main_path: str) -> dict:
        """Single-pass scan of a main session JSONL.

        Returns:
            main_prompt_ids: set of all promptIds from user records
            latest_prompt_id: promptId of the most-recent non-sidechain user record
        """
        main_prompt_ids: set = set()
        latest_prompt_id: "str | None" = None
        latest_ts = 0
        try:
            with open(main_path, encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        rec = json.loads(raw)
                    except Exception:
                        continue
                    if rec.get("type") == "user":
                        pid = rec.get("promptId")
                        if pid:
                            main_prompt_ids.add(pid)
                        if not rec.get("isSidechain") and pid:
                            ts_str = rec.get("timestamp", "")
                            ts_ms = 0
                            if ts_str:
                                try:
                                    ts_ms = int(datetime.datetime.fromisoformat(
                                        ts_str.replace("Z", "+00:00")
                                    ).timestamp() * 1000)
                                except Exception:
                                    pass
                            if ts_ms >= latest_ts:
                                latest_ts = ts_ms
                                latest_prompt_id = pid
        except Exception:
            pass
        return {"main_prompt_ids": main_prompt_ids, "latest_prompt_id": latest_prompt_id}

    @staticmethod
    def _scan_agent_jsonl_once(agent_path: str) -> dict:
        """Single-pass scan of a subagent JSONL.

        Returns:
            prompt_ids: set of all promptIds from user records (for parent-linking)
            first_prompt_id: promptId from the first user record
            start_ts, end_ts: epoch-ms timestamps
            description: first 150 chars of first user message text
            tool_calls: list of {name, ts} dicts (capped at 50)
            last_assistant_record: raw dict of the last assistant record
        """
        prompt_ids: set = set()
        first_prompt_id: "str | None" = None
        start_ts: "int | None" = None
        end_ts: "int | None" = None
        description = ""
        tool_calls: list = []
        last_assistant_record: "dict | None" = None
        first_user_found = False

        try:
            with open(agent_path, encoding="utf-8", errors="ignore") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        rec = json.loads(raw)
                    except Exception:
                        continue

                    if start_ts is None:
                        ts_str = rec.get("timestamp", "")
                        if ts_str:
                            try:
                                start_ts = int(datetime.datetime.fromisoformat(
                                    ts_str.replace("Z", "+00:00")
                                ).timestamp() * 1000)
                            except Exception:
                                pass

                    ts_str = rec.get("timestamp", "")
                    if ts_str:
                        try:
                            end_ts = int(datetime.datetime.fromisoformat(
                                ts_str.replace("Z", "+00:00")
                            ).timestamp() * 1000)
                        except Exception:
                            pass

                    rtype = rec.get("type", "")

                    if rtype == "user":
                        pid = rec.get("promptId")
                        if pid:
                            prompt_ids.add(pid)
                        if not first_user_found:
                            first_user_found = True
                            first_prompt_id = pid
                            content = rec.get("message", {}).get("content", "")
                            text = ""
                            if isinstance(content, str):
                                text = content
                            elif isinstance(content, list):
                                for blk in content:
                                    if isinstance(blk, dict) and blk.get("type") == "text":
                                        text = blk.get("text", "")
                                        break
                            description = text[:150]

                    if rtype == "assistant":
                        last_assistant_record = rec
                        if len(tool_calls) < 50:
                            rec_ts_str = rec.get("timestamp", "")
                            rec_ts: "int | None" = None
                            if rec_ts_str:
                                try:
                                    rec_ts = int(datetime.datetime.fromisoformat(
                                        rec_ts_str.replace("Z", "+00:00")
                                    ).timestamp() * 1000)
                                except Exception:
                                    pass
                            content = rec.get("message", {}).get("content", [])
                            if isinstance(content, list):
                                for blk in content:
                                    if isinstance(blk, dict) and blk.get("type") == "tool_use":
                                        if len(tool_calls) < 50:
                                            tool_calls.append({
                                                "name": blk.get("name", ""),
                                                "ts": rec_ts,
                                            })
        except Exception:
            pass

        return {
            "prompt_ids": prompt_ids,
            "first_prompt_id": first_prompt_id,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "description": description,
            "tool_calls": tool_calls,
            "last_assistant_record": last_assistant_record,
        }

    def _build_agent_tree_sync(self, resume_id: str) -> dict:
        _empty = {"resume_id": resume_id, "total_agents": 0, "tree": []}
        try:
            main_path = self._find_session_file_sync(resume_id)
            if not main_path:
                return _empty
            subagent_dir = os.path.join(os.path.dirname(main_path), resume_id, "subagents")
            if not os.path.isdir(subagent_dir):
                return _empty
        except Exception:
            return _empty

        # --- mtime-based cache check ------------------------------------------
        try:
            main_mtime_ns = os.stat(main_path).st_mtime_ns
        except OSError:
            main_mtime_ns = 0

        agent_mtimes: dict = {}
        try:
            for _e in os.scandir(subagent_dir):
                if _e.name.startswith("agent-") and _e.name.endswith(".jsonl") and _e.is_file():
                    _agent_id = _e.name[len("agent-"):-len(".jsonl")]
                    try:
                        agent_mtimes[_agent_id] = _e.stat().st_mtime_ns
                    except OSError:
                        agent_mtimes[_agent_id] = 0
        except OSError:
            pass

        with _AGENT_TREE_CACHE_LOCK:
            entry = _AGENT_TREE_CACHE.get(main_path)
            if (
                entry is not None
                and entry.main_mtime_ns == main_mtime_ns
                and entry.agent_mtimes == agent_mtimes
            ):
                # Move to end (LRU: most-recently-used stays at tail)
                _AGENT_TREE_CACHE.move_to_end(main_path)
                return entry.result
        # --- end cache check --------------------------------------------------

        # Single scan of main JSONL — covers Steps 2 and the latest-turn filter.
        main_scan = self._scan_main_jsonl_once(main_path)
        main_prompt_ids: set = main_scan["main_prompt_ids"]
        latest_prompt_id: "str | None" = main_scan["latest_prompt_id"]

        # Step 1: scan all agent-{id}.jsonl files in subagent_dir (one pass each).
        agents: dict[str, dict] = {}  # agent_id → node dict (without children yet)
        subagent_prompt_ids: dict[str, str] = {}  # promptId → agent_id (for parent-linking)

        for entry in os.scandir(subagent_dir):
            if not entry.name.endswith(".jsonl") or not entry.is_file():
                continue
            if not entry.name.startswith("agent-"):
                continue
            agent_id = entry.name[len("agent-"):-len(".jsonl")]
            try:
                # Read meta
                meta_path = os.path.join(subagent_dir, f"agent-{agent_id}.meta.json")
                agent_type = "unknown"
                try:
                    with open(meta_path, encoding="utf-8", errors="ignore") as mf:
                        meta = json.loads(mf.read())
                        agent_type = meta.get("agentType", "unknown") or "unknown"
                except Exception:
                    pass

                # Single-pass scan of this agent's JSONL.
                scan = self._scan_agent_jsonl_once(entry.path)

                # Register all this agent's promptIds for parent-linking.
                for pid in scan["prompt_ids"]:
                    subagent_prompt_ids[pid] = agent_id

                # output_preview: last text block of last assistant record
                output_preview = ""
                last_rec = scan["last_assistant_record"]
                if last_rec is not None:
                    content = last_rec.get("message", {}).get("content", [])
                    if isinstance(content, list):
                        last_text = ""
                        for blk in content:
                            if isinstance(blk, dict) and blk.get("type") == "text":
                                last_text = blk.get("text", "")
                        output_preview = last_text[:200]

                start_ts = scan["start_ts"]
                end_ts = scan["end_ts"]
                duration_ms: "int | None" = None
                if start_ts is not None and end_ts is not None:
                    duration_ms = end_ts - start_ts

                agents[agent_id] = {
                    "agent_id": agent_id,
                    "agent_type": agent_type,
                    "description": scan["description"],
                    "prompt_id": scan["first_prompt_id"],
                    "parent_agent_id": None,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "duration_ms": duration_ms,
                    "tool_calls": scan["tool_calls"],
                    "output_preview": output_preview,
                    "children": [],
                }
            except Exception:
                pass

        if not agents:
            return _empty

        # Step 4: determine parent_agent_id for each agent
        for agent_id, node in agents.items():
            fp = node["prompt_id"]
            if fp is None:
                node["parent_agent_id"] = None
            elif fp in main_prompt_ids:
                node["parent_agent_id"] = None
            else:
                parent = subagent_prompt_ids.get(fp)
                # Avoid self-reference
                node["parent_agent_id"] = parent if parent and parent != agent_id else None

        # Step 5: build tree recursively
        root_nodes = []
        children_map: dict[str, list] = {aid: [] for aid in agents}
        for agent_id, node in agents.items():
            p = node["parent_agent_id"]
            if p is not None and p in children_map:
                children_map[p].append(agent_id)
            else:
                root_nodes.append(agent_id)

        # Iterative BFS build to avoid recursion limit and cycle crashes.
        built: dict[str, dict] = {}
        queue = list(root_nodes)
        visited: set[str] = set()
        while queue:
            aid = queue.pop(0)
            if aid in visited:
                continue
            visited.add(aid)
            node = dict(agents[aid])
            node["children"] = []
            built[aid] = node
            queue.extend(c for c in children_map.get(aid, []) if c not in visited)

        # Attach children in reverse BFS order so parents are populated last.
        for aid in reversed(list(visited)):
            node = built[aid]
            node["children"] = [built[c] for c in children_map.get(aid, []) if c in built]

        tree = [built[aid] for aid in root_nodes if aid in built]

        # Filter to latest conversation turn only.
        if latest_prompt_id:
            filtered = [n for n in tree if n.get("prompt_id") == latest_prompt_id]
            if filtered:
                tree = filtered

        result = {
            "resume_id": resume_id,
            "total_agents": len(agents),
            "tree": tree,
        }

        # --- write to cache ---------------------------------------------------
        with _AGENT_TREE_CACHE_LOCK:
            _AGENT_TREE_CACHE[main_path] = _AgentTreeCacheEntry(
                main_mtime_ns=main_mtime_ns,
                agent_mtimes=agent_mtimes,
                result=result,
            )
            _AGENT_TREE_CACHE.move_to_end(main_path)
            # Evict oldest entries beyond the cap
            while len(_AGENT_TREE_CACHE) > _AGENT_TREE_CACHE_MAX:
                _AGENT_TREE_CACHE.popitem(last=False)
        # --- end cache write --------------------------------------------------

        return result

    async def build_agent_tree(self, resume_id: str) -> dict:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._build_agent_tree_sync, resume_id)

    async def warmup_history_cache(self, max_sessions: int = 30) -> None:
        """Bridge 啟動後在背景預建近期 session 的 history index。"""
        try:
            sessions = await self.get_resumable_sessions(limit=max_sessions)
        except Exception:
            return
        loop = asyncio.get_event_loop()
        warmed = 0
        for info in sessions:
            resume_id = info.get("resume_id") or ""
            if not resume_id:
                continue
            if f"claude:{resume_id}" in _JSONL_HISTORY_CACHE:
                continue
            try:
                await loop.run_in_executor(
                    None,
                    self._load_session_history_sync,
                    resume_id, DEFAULT_HISTORY_LIMIT, "", "snapshot",
                )
                warmed += 1
                await asyncio.sleep(0.01)
            except Exception:
                pass
        if warmed:
            log.info("warmup_history_cache: pre-built index for %d sessions", warmed)

    @staticmethod
    def _decode_project_path(proj_name: str) -> str:
        """Decode a Claude project directory name back to filesystem path.

        Claude encodes paths by replacing '/' with '-' and prepending '-'.
        Since '-' is ambiguous (separator vs literal hyphen in dir names),
        we use exhaustive search with OS stat checks to find the real path.
        """
        home = os.path.expanduser("~")
        if not proj_name.startswith("-"):
            return home
        atoms = proj_name[1:].split("-")

        def candidates(component: str) -> list[str]:
            variants = [component]
            underscore = component.replace("-", "_")
            if underscore != component:
                variants.append(underscore)
            return variants

        def search(cur: str, idx: int) -> str | None:
            if idx >= len(atoms):
                return cur if os.path.isdir(cur) else None
            component = ""
            for end in range(idx, len(atoms)):
                component = component + ("-" if component else "") + atoms[end]
                for variant in candidates(component):
                    candidate = os.path.join(cur, variant)
                    if os.path.isdir(candidate):
                        result = search(candidate, end + 1)
                        if result is not None:
                            return result
            return None

        return search("/", 0) or home

    _OVERRIDES_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "path_overrides.json")

    @staticmethod
    def _load_path_overrides() -> dict[str, str]:
        try:
            with open(_ClaudeHistoryMixin._OVERRIDES_FILE, encoding="utf-8") as f:
                data = json.load(f)
            return {str(k): str(v) for k, v in data.items() if k and v}
        except FileNotFoundError:
            return {}
        except Exception as exc:
            log.warning("path_overrides.json load failed: %s", exc)
            return {}

    def _scan_local_sessions_sync(self, limit: int = 100) -> list:
        sessions = []
        saved_names: dict = {}
        if self._load_saved_sessions_fn is not None:
            # Accept both "resume_id" (new canonical) and legacy "claude_uuid" key.
            saved_names = {
                (v.get("resume_id") or v.get("claude_uuid", "")): v["name"]
                for v in self._load_saved_sessions_fn().values()
                if v.get("resume_id") or v.get("claude_uuid")
            }
        path_overrides = self._load_path_overrides()
        # Build a fresh cache each scan, reusing entries for unchanged files. Reading
        # the old cache via .get() is thread-safe; we never mutate the shared dict in
        # place (multiple scans may run concurrently in the executor). Rebuilding also
        # naturally prunes entries for deleted session files.
        old_cache = self._scan_file_cache
        new_cache: dict[str, tuple] = {}
        try:
            for proj in os.scandir(self._claude_projects_dir):
                if not proj.is_dir():
                    continue

                # cwd is resolved per-project: read from JSONL (authoritative),
                # fall back to directory-name decoding for old/empty files.
                proj_cwd: str | None = None

                for entry in os.scandir(proj.path):
                    if not entry.name.endswith(".jsonl") or not entry.is_file():
                        continue
                    uuid = entry.name[:-6]
                    st = entry.stat()
                    mtime = int(st.st_mtime)
                    file_key = (st.st_mtime_ns, st.st_size)

                    # Fast path: file unchanged since last scan — reuse the parsed
                    # cwd/name without opening it. This is the whole point of the cache.
                    cached = old_cache.get(entry.path)
                    if cached is not None and cached[0] == file_key:
                        file_cwd, content_name = cached[1], cached[2]
                    else:
                        file_cwd = None
                        content_name = ""
                        try:
                            with open(entry.path, encoding="utf-8", errors="ignore") as f:
                                for raw in f:
                                    try:
                                        d = json.loads(raw)
                                        # Pick up cwd from any record that carries it.
                                        if file_cwd is None:
                                            raw_cwd = d.get("cwd")
                                            if isinstance(raw_cwd, str) and raw_cwd.strip():
                                                file_cwd = raw_cwd.strip()
                                        # Pick up name from first non-empty user text.
                                        if not content_name and d.get("type") == "user":
                                            content = d.get("message", {}).get("content", "")
                                            text = ""
                                            if isinstance(content, str):
                                                text = content
                                            elif isinstance(content, list):
                                                for blk in content:
                                                    if isinstance(blk, dict) and blk.get("type") == "text":
                                                        text = blk.get("text", "")
                                                        break
                                            if text and not text.startswith("<"):
                                                content_name = text[:50].strip()
                                        if file_cwd and content_name:
                                            break
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                    new_cache[entry.path] = (file_key, file_cwd, content_name)

                    # Saved (user-renamed) name takes precedence over content-derived name.
                    name = saved_names.get(uuid) or content_name
                    if not name:
                        name = proj.name.split("-")[-1] or uuid[:8]

                    # Cache the first cwd we found for this project directory.
                    if file_cwd and proj_cwd is None:
                        proj_cwd = file_cwd

                    cwd = file_cwd or proj_cwd or self._decode_project_path(proj.name)
                    cwd = path_overrides.get(cwd, cwd)
                    sessions.append({
                        "id": uuid,
                        "name": name,
                        "claude_uuid": uuid,
                        "resume_id": uuid,
                        "last_used": mtime,
                        "cwd": cwd,
                    })
        except FileNotFoundError:
            log.info("Local Claude sessions dir not found yet; skipping scan")
        except OSError as exc:
            # Windows first-run commonly raises WinError 3 (path not found).
            if getattr(exc, "winerror", None) == 3:
                log.info("Local Claude sessions path not found yet; skipping scan")
            else:
                log.warning("Failed to scan local sessions: %s", exc)
        except Exception as exc:
            log.warning("Failed to scan local sessions: %s", exc)
        # Commit the rebuilt cache (atomic reference swap; safe vs. concurrent scans).
        # On an early exception above, new_cache may be partial — only commit when we
        # actually walked the tree (i.e. it has entries) to avoid wiping a good cache.
        if new_cache:
            self._scan_file_cache = new_cache
        sessions.sort(key=lambda x: x["last_used"], reverse=True)
        return sessions[:limit]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _find_newest_jsonl_uuid(self, cwd: str, exclude: "str | None" = None) -> "str | None":
        """Scan ~/.claude/projects/<mangled-cwd>/ for the newest .jsonl and return
        its stem (UUID) if it differs from *exclude* and is a valid UUID.

        cwd mangling rule: replace '/' with '-' and prepend '-'.
        Example: /Users/alice/foo → -Users-alice-foo
        """
        if not cwd:
            return None
        try:
            mangled = "-" + cwd.lstrip("/").replace("/", "-")
            proj_dir = os.path.join(self._claude_projects_dir, mangled)
            if not os.path.isdir(proj_dir):
                return None
            best_mtime = -1.0
            best_uuid: "str | None" = None
            for entry in os.scandir(proj_dir):
                if not entry.name.endswith(".jsonl") or not entry.is_file():
                    continue
                stem = entry.name[:-6]
                if not is_valid_uuid(stem):
                    continue
                if stem == exclude:
                    continue
                mtime = entry.stat().st_mtime
                if mtime > best_mtime:
                    best_mtime = mtime
                    best_uuid = stem
            return best_uuid
        except Exception as exc:
            log.warning("_find_newest_jsonl_uuid failed for cwd=%s: %s", cwd, exc)
            return None
