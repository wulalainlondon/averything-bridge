from __future__ import annotations

import fcntl
import json
import os
import re
import shutil
import tempfile

DEFAULT_INSTANCES_PATH = os.path.join(os.path.dirname(__file__), "instances.json")

_DEFAULT_PORT = 8766

# Only allow safe name characters to prevent ~/.bridge-instances/{name} path traversal.
_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

# Refuse root_dir values that are ancestors of sensitive directories.
_SENSITIVE_ROOTS = {
    "/",
    os.path.expanduser("~"),
}

_VALID_BACKENDS = {"", "claude", "codex", "ollama", "gemini"}


def _resolve_path(path: str | None) -> str:
    return path if path is not None else DEFAULT_INSTANCES_PATH


def _is_default(item: dict) -> bool:
    """Return True when item is (or represents) the default instance."""
    return item.get("name") == "default" or (
        item.get("port") == _DEFAULT_PORT and item.get("root_dir", "") == ""
    )


def load_instances(path: str | None = None) -> list[dict]:
    """Read instances.json; return empty list when file is not found."""
    resolved = _resolve_path(path)
    try:
        with open(resolved, encoding="utf-8") as fh:
            data = json.load(fh)
        return data.get("instances", [])
    except FileNotFoundError:
        return []


def save_instances(items: list[dict], path: str | None = None) -> None:
    """Atomic write via tmpfile + os.replace(). Caller must hold the file lock."""
    resolved = _resolve_path(path)
    dir_name = os.path.dirname(os.path.abspath(resolved))

    payload = json.dumps({"instances": items}, indent=2, ensure_ascii=False)

    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        # Back up only after a successful write so .bak is always valid JSON.
        if os.path.exists(resolved):
            shutil.copy2(resolved, resolved + ".bak")
        os.replace(tmp_path, resolved)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def validate_instance(
    item: dict,
    existing: list[dict],
    *,
    allow_same_name: bool = False,
) -> str | None:
    """
    Validate a candidate instance dict against the existing list.

    Returns an error code string on failure, or None when the item is valid.

    Error codes
    -----------
    name_empty        — name is missing or blank
    name_invalid      — name contains unsafe characters or is too long
    name_duplicate    — another instance already uses this name
    port_invalid      — port is not an integer in [1024, 65535]
    port_in_use       — another instance already uses this port
    root_dir_invalid  — root_dir is not an absolute path or contains ".."
    root_dir_sensitive — root_dir is a sensitive system directory
    root_dir_missing  — root_dir is non-empty but the path does not exist
    backend_invalid   — backend is not a recognized value
    default_immutable — attempting to change the default instance's port
    """
    name: str = item.get("name", "")
    if not name or not name.strip():
        return "name_empty"

    if not _NAME_RE.match(name):
        return "name_invalid"

    # Duplicate-name check.
    for existing_item in existing:
        if existing_item.get("name") == name and not allow_same_name:
            return "name_duplicate"

    port = item.get("port")
    try:
        port_int = int(port)
    except (TypeError, ValueError):
        return "port_invalid"

    if not (1024 <= port_int <= 65535):
        return "port_invalid"

    # Guard against changing the default instance's port.
    for existing_item in existing:
        if _is_default(existing_item) and existing_item.get("name") == name:
            if existing_item.get("port") != port_int:
                return "default_immutable"
            break

    # Guard against port collisions with other instances.
    for existing_item in existing:
        if (
            existing_item.get("port") == port_int
            and existing_item.get("name") != name
        ):
            return "port_in_use"

    # root_dir validation.
    root_dir: str = item.get("root_dir", "")
    if root_dir:
        if ".." in root_dir:
            return "root_dir_invalid"
        if not os.path.isabs(root_dir):
            return "root_dir_invalid"
        normalized = os.path.normpath(root_dir)
        if normalized in _SENSITIVE_ROOTS:
            return "root_dir_sensitive"
        # Also refuse paths that are parents of the home dir or root
        for sensitive in _SENSITIVE_ROOTS:
            if sensitive.startswith(normalized + os.sep) or normalized == sensitive:
                return "root_dir_sensitive"
        if not os.path.exists(normalized):
            return "root_dir_missing"

    # backend validation.
    backend: str = str(item.get("backend") or "")
    if backend not in _VALID_BACKENDS:
        return "backend_invalid"

    return None


def upsert_instance(
    item: dict,
    path: str | None = None,
) -> tuple[bool, str | None]:
    """
    Insert or update an instance keyed by name.

    Uses an exclusive file lock to prevent TOCTOU races.
    Returns (True, None) on success, or (False, error_code) on validation failure.
    """
    resolved = _resolve_path(path)
    lock_path = resolved + ".lock"

    with open(lock_path, "w") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        existing = load_instances(path)
        is_update = any(e.get("name") == item.get("name") for e in existing)
        error = validate_instance(item, existing, allow_same_name=is_update)
        if error is not None:
            return False, error
        if is_update:
            updated = [item if e.get("name") == item.get("name") else e for e in existing]
        else:
            updated = existing + [item]
        save_instances(updated, path)
    return True, None


def delete_instance(
    name: str,
    path: str | None = None,
) -> tuple[bool, str | None]:
    """
    Delete the instance with the given name.

    Uses an exclusive file lock to prevent TOCTOU races.
    Returns:
        (False, "default_immutable") if attempting to delete the default instance.
        (False, "not_found") if no instance with that name exists.
        (True, None) on success.
    """
    resolved = _resolve_path(path)
    lock_path = resolved + ".lock"

    with open(lock_path, "w") as lock_fh:
        fcntl.flock(lock_fh, fcntl.LOCK_EX)
        existing = load_instances(path)
        target = next((e for e in existing if e.get("name") == name), None)
        if target is None:
            return False, "not_found"
        if _is_default(target):
            return False, "default_immutable"
        updated = [e for e in existing if e.get("name") != name]
        save_instances(updated, path)
    return True, None
