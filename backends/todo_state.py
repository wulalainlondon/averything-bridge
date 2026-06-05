"""
Todo/plan normalization — leaf module, dependency-free.

Different backends expose the agent's task list under different tool schemas:

  Claude (legacy)   TodoWrite        {todos:[{content,status,activeForm}]}   full replace
  Claude (2.1.142+) TaskCreate       {subject,description?,activeForm?}       append; id in result
                    TaskUpdate       {taskId,status}                          mutate by id
                    TaskDelete       {taskId}                                 remove by id
  Codex             update_plan      {plan:[{step,status}]}                   full replace
  Gemini            write_todos      {todos:[{description,status}]}           full replace

`TodoStore` folds the stateful Claude Task* protocol into one ordered list so the
bridge can emit a single normalized `todo_update` event. Full-replace backends
(TodoWrite / update_plan / write_todos) don't need the store — callers can map
them directly with `normalize_full_list`.

Canonical item shape:
  {"id": str, "content": str, "status": "pending"|"in_progress"|"completed",
   "activeForm": str | None}
"""

import re

_VALID_STATUS = ("pending", "in_progress", "completed")
_TASK_ID_RE = re.compile(r"#(\d+)")


def _coerce_item(content, status, active_form, item_id="") -> dict | None:
    if not content or status not in _VALID_STATUS:
        return None
    return {
        "id": str(item_id or ""),
        "content": str(content),
        "status": status,
        "activeForm": active_form if isinstance(active_form, str) else None,
    }


def normalize_full_list(items, content_key="content") -> list:
    """Map a full-replace payload (TodoWrite/update_plan/write_todos) to canonical
    items. `content_key` is the per-item text field ('content' | 'step' | 'description')."""
    out = []
    if not isinstance(items, list):
        return out
    for raw in items:
        if not isinstance(raw, dict):
            continue
        it = _coerce_item(raw.get(content_key), raw.get("status"), raw.get("activeForm"))
        if it:
            out.append(it)
    return out


class TodoStore:
    """Per-session accumulator for Claude's TaskCreate/TaskUpdate/TaskDelete and the
    legacy TodoWrite tool. Not thread-safe; driven from a single stdout reader."""

    def __init__(self):
        self._items: list[dict] = []
        self._pending: dict[str, dict] = {}  # tool_use_id -> item awaiting server id

    def reset(self) -> None:
        self._items = []
        self._pending = {}

    def is_empty(self) -> bool:
        return not self._items

    def as_list(self) -> list:
        return [dict(it) for it in self._items]

    # --- TodoWrite: full replace ---
    def apply_todowrite(self, inp: dict) -> bool:
        if not isinstance(inp, dict):
            return False
        self._items = normalize_full_list(inp.get("todos"), content_key="content")
        self._pending = {}
        return True

    # --- TaskCreate: append now (status pending), resolve server id from result ---
    def note_create(self, tool_use_id: str, inp: dict) -> bool:
        if not isinstance(inp, dict):
            return False
        item = _coerce_item(
            inp.get("subject") or inp.get("content"),
            "pending",
            inp.get("activeForm"),
        )
        if item is None:
            return False
        self._items.append(item)
        if tool_use_id:
            self._pending[tool_use_id] = item
        return True

    def resolve_create(self, tool_use_id: str, result_text: str) -> bool:
        item = self._pending.pop(tool_use_id, None)
        if item is None:
            return False
        m = _TASK_ID_RE.search(result_text or "")
        if m:
            item["id"] = m.group(1)
        return True

    # --- TaskUpdate: mutate by id ---
    def apply_update(self, inp: dict) -> bool:
        if not isinstance(inp, dict):
            return False
        tid = str(inp.get("taskId") or inp.get("task_id") or "")
        if not tid:
            return False
        status = inp.get("status")
        for it in self._items:
            if it["id"] == tid:
                changed = False
                if status in _VALID_STATUS:
                    it["status"] = status
                    changed = True
                elif status == "deleted":
                    self._items.remove(it)
                    return True
                if isinstance(inp.get("subject"), str):
                    it["content"] = inp["subject"]
                    changed = True
                if isinstance(inp.get("activeForm"), str):
                    it["activeForm"] = inp["activeForm"]
                    changed = True
                return changed
        return False

    # --- TaskDelete: remove by id ---
    def apply_delete(self, inp: dict) -> bool:
        if not isinstance(inp, dict):
            return False
        tid = str(inp.get("taskId") or inp.get("task_id") or "")
        for it in list(self._items):
            if it["id"] == tid:
                self._items.remove(it)
                return True
        return False
