"""
Config loader: YAML file + env var override merge logic.

Priority (lowest → highest):
  1. Built-in defaults (defaults.py)
  2. ~/.claude-bridge/config.yaml  (if exists)
  3. --config FILE path            (if provided)
  4. BRIDGE_<SECTION>__<KEY>=val  environment variables
"""
from __future__ import annotations

import copy
import logging
import os
from pathlib import Path
from typing import Any, Optional

from .defaults import DEFAULTS
from .schema import BridgeConfig, SearchConfig, SourcesConfig, ServerConfig

log = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path("~/.claude-bridge/config.yaml").expanduser()


def _load_yaml(path: Path) -> dict:
    """Load a YAML file; raises ValueError with clear message on parse error."""
    try:
        import yaml  # type: ignore
    except ImportError:
        raise ImportError(
            "PyYAML is required for YAML config files. "
            "Install it with: pip install pyyaml"
        )
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        return data or {}
    except Exception as exc:
        raise ValueError(
            f"Failed to parse config file {path}: {exc}"
        ) from exc


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base; override wins on scalar conflicts."""
    result = copy.deepcopy(base)
    for key, val in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def _apply_env_overrides(merged: dict) -> dict:
    """
    Scan environment for BRIDGE_<SECTION>__<KEY> and apply overrides.
    Double-underscore separates nesting levels.
    E.g. BRIDGE_SEARCH__TOKENIZER=trigram → merged['search']['tokenizer'] = 'trigram'
    """
    result = copy.deepcopy(merged)
    prefix = "BRIDGE_"
    for raw_key, raw_val in os.environ.items():
        if not raw_key.startswith(prefix):
            continue
        without_prefix = raw_key[len(prefix):]  # e.g. SEARCH__TOKENIZER
        parts = without_prefix.lower().split("__")
        if len(parts) < 2:
            # Single-segment key like BRIDGE_PORT — not nested, skip
            continue
        section = parts[0]   # e.g. "search"
        key = "__".join(parts[1:])  # e.g. "tokenizer"
        if section not in result:
            result[section] = {}
        result[section][key] = _coerce_env_value(raw_val)
    return result


def _coerce_env_value(val: str) -> Any:
    """Coerce string env var to appropriate Python type."""
    if val.lower() in ("true", "1", "yes"):
        return True
    if val.lower() in ("false", "0", "no"):
        return False
    try:
        return int(val)
    except ValueError:
        pass
    try:
        return float(val)
    except ValueError:
        pass
    return val


_HOME = Path.home()
_ALLOWED_INDEX_ROOTS = (_HOME, Path("/tmp").resolve())


def _validate_index_path(raw: str, default: str) -> str:
    """Validate index_path: must resolve under ~ or /tmp.

    Returns the validated path string, or the default with a warning log.
    """
    try:
        resolved = Path(raw).expanduser().resolve()
    except Exception:
        log.warning(
            "config: index_path %r could not be resolved — using default %r", raw, default
        )
        return default

    for root in _ALLOWED_INDEX_ROOTS:
        try:
            resolved.relative_to(root)
            return raw  # valid
        except ValueError:
            continue

    log.warning(
        "config: index_path %r is outside allowed roots (%s) — using default %r",
        raw,
        ", ".join(str(r) for r in _ALLOWED_INDEX_ROOTS),
        default,
    )
    return default


def _validate_source_dir(raw: str, default: str, field_name: str) -> str:
    """Validate a sources directory path: must be absolute, no '..' segments.

    Returns the validated path string, or the default with a warning log.
    """
    try:
        p = Path(raw).expanduser()
    except Exception:
        log.warning(
            "config: %s %r is not a valid path — using default %r", field_name, raw, default
        )
        return default

    if not p.is_absolute():
        log.warning(
            "config: %s %r is not an absolute path — using default %r", field_name, raw, default
        )
        return default

    # Reject paths with literal '..' parts (even after expanduser, before resolve)
    try:
        parts = Path(raw).expanduser().parts
    except Exception:
        parts = ()
    if ".." in parts:
        log.warning(
            "config: %s %r contains '..' segments — using default %r", field_name, raw, default
        )
        return default

    return raw


def _dict_to_config(data: dict, config_file_path: Optional[Path]) -> BridgeConfig:
    """Convert a merged flat dict to a BridgeConfig instance."""
    s = data.get("search", {})
    src = data.get("sources", {})
    srv = data.get("server", {})

    _default_index_path = "~/.claude-bridge-runtime/search.db"
    _raw_index_path = str(s.get("index_path", _default_index_path))
    _safe_index_path = _validate_index_path(_raw_index_path, _default_index_path)

    # BRIDGE_SEARCH_FULL_INDEX=0 → metadata-only index (messages body skipped)
    _raw_full_index = os.environ.get("BRIDGE_SEARCH_FULL_INDEX", "")
    if _raw_full_index:
        _full_index = _coerce_env_value(_raw_full_index)
        if isinstance(_full_index, bool):
            _full_index_bool = _full_index
        else:
            _full_index_bool = bool(_full_index)
    else:
        _full_index_bool = bool(s.get("full_index", True))

    search = SearchConfig(
        enabled=bool(s.get("enabled", True)),
        tokenizer=s.get("tokenizer", "auto"),
        index_path=Path(_safe_index_path).expanduser(),
        max_index_size_mb=int(s.get("max_index_size_mb", 1024)),
        ingest_on_startup=bool(s.get("ingest_on_startup", True)),
        ingest_startup_delay_sec=float(s.get("ingest_startup_delay_sec", 2.0)),
        ingest_bulk_pause_every_files=int(s.get("ingest_bulk_pause_every_files", 50)),
        ingest_bulk_pause_sec=float(s.get("ingest_bulk_pause_sec", 0.01)),
        ingest_idle_recent_window_sec=float(s.get("ingest_idle_recent_window_sec", 3.0)),
        ingest_idle_pause_sec=float(s.get("ingest_idle_pause_sec", 0.05)),
        watch_enabled=bool(s.get("watch_enabled", True)),
        watch_interval_sec=int(s.get("watch_interval_sec", 2)),
        full_index=_full_index_bool,
    )

    _default_claude_dir = "~/.claude/projects"
    _raw_claude_dir = str(src.get("claude_projects_dir", _default_claude_dir))
    _safe_claude_dir = _validate_source_dir(_raw_claude_dir, _default_claude_dir, "claude_projects_dir")

    _default_codex_dir = "~/.codex/sessions"
    _raw_codex_dir = str(src.get("codex_sessions_dir", _default_codex_dir))
    _safe_codex_dir = _validate_source_dir(_raw_codex_dir, _default_codex_dir, "codex_sessions_dir")

    sources = SourcesConfig(
        claude_enabled=src.get("claude_enabled", "auto"),
        claude_projects_dir=Path(_safe_claude_dir).expanduser(),
        codex_enabled=src.get("codex_enabled", "auto"),
        codex_sessions_dir=Path(_safe_codex_dir).expanduser(),
        ollama_enabled=src.get("ollama_enabled", "no"),
    )

    firebase_path = srv.get("push_firebase_credentials_path")
    server = ServerConfig(
        bind=str(srv.get("bind", "0.0.0.0")),
        port=int(srv.get("port", 8766)),
        push_firebase_credentials_path=Path(firebase_path).expanduser() if firebase_path else None,
    )

    return BridgeConfig(
        search=search,
        sources=sources,
        server=server,
        config_file_path=config_file_path,
    )


def load_config(
    extra_config_path: Optional[Path] = None,
) -> BridgeConfig:
    """
    Build and return a BridgeConfig by merging all config layers.

    Args:
        extra_config_path: Path from --config CLI flag (optional).
    """
    merged = copy.deepcopy(DEFAULTS)
    loaded_file: Optional[Path] = None

    # Layer 2: user default config file
    if _DEFAULT_CONFIG_PATH.exists():
        user_data = _load_yaml(_DEFAULT_CONFIG_PATH)
        merged = _deep_merge(merged, user_data)
        loaded_file = _DEFAULT_CONFIG_PATH

    # Layer 3: --config FILE override
    if extra_config_path is not None:
        extra_data = _load_yaml(extra_config_path)
        merged = _deep_merge(merged, extra_data)
        loaded_file = extra_config_path

    # Layer 4: env var overrides
    merged = _apply_env_overrides(merged)

    return _dict_to_config(merged, loaded_file)
